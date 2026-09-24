use std::{collections::HashSet, future::Future, panic::AssertUnwindSafe, time::Duration};

use async_nats::jetstream;
use futures_util::{FutureExt, TryStreamExt};
use swarmy_bus::{
    Bus, Config, Error, LiveFeed, SubjectToken, WorkMessage, WorkMessages, WorkQueue,
};
use swarmy_core::{EncodingError, NodeId, SessionId, WakeReply};
use tokio::time::{sleep, timeout};
use ulid::Ulid;

const ACK_WAIT: Duration = Duration::from_secs(1);
const WAIT: Duration = Duration::from_secs(15);

#[derive(Clone)]
struct Fixture {
    bus: Bus,
    admin: async_nats::Client,
    config: Config,
    prefix: String,
}

impl Fixture {
    fn names(&self) -> [String; 4] {
        ["INFER_REQ", "SCHED_RUNNABLE", "TOOL_REMOTE", "TOOL_NODE"]
            .map(|name| format!("{}_{name}", self.prefix))
    }
}

async fn run<F: Future<Output = ()>>(test: impl FnOnce(Fixture) -> F) {
    let url = match std::env::var("SWARMY_NATS_URL") {
        Ok(url) => url,
        Err(std::env::VarError::NotPresent) => {
            // Test skips must be visible even without a tracing subscriber.
            eprintln!("skipping NATS integration test: SWARMY_NATS_URL is unset");
            return;
        }
        Err(error) => panic!("invalid SWARMY_NATS_URL: {error}"),
    };
    let prefix = Ulid::generate().to_string();
    let config = Config {
        prefix: Some(SubjectToken::new(&prefix).unwrap()),
        ack_wait: ACK_WAIT,
        max_deliver: 3,
    };
    let fixture = Fixture {
        bus: Bus::connect(&url, config.clone()).await.unwrap(),
        admin: async_nats::connect(&url).await.unwrap(),
        config,
        prefix,
    };
    let result = AssertUnwindSafe(async {
        fixture.bus.setup(&[WorkQueue::RemoteTools]).await.unwrap();
        test(fixture.clone()).await;
    })
    .catch_unwind()
    .await;
    let context = jetstream::new(fixture.admin.clone());
    for name in fixture.names() {
        if let Err(error) = context.delete_stream(name).await {
            assert!(
                matches!(error.kind(), jetstream::context::DeleteStreamErrorKind::JetStream(ref error) if error.code() == 404),
                "cleanup failed: {error}"
            );
        }
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

async fn next(messages: &mut WorkMessages<u64>) -> WorkMessage<u64> {
    timeout(WAIT, messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn unacknowledged_work_is_redelivered() {
    run(|f| async move {
        let mut messages = f.bus.consume(&WorkQueue::RemoteTools).await.unwrap();
        f.bus
            .publish_work(&WorkQueue::RemoteTools, &42_u64)
            .await
            .unwrap();
        let first = next(&mut messages).await;
        assert_eq!(first.delivery_count().unwrap(), 1);
        assert_eq!(first.value, 42);
        let retried = next(&mut messages).await;
        assert_eq!(retried.delivery_count().unwrap(), 2);
        assert_eq!(retried.value, 42);
        retried.acknowledge().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn acknowledged_work_stays_acknowledged() {
    run(|f| async move {
        let mut messages = f.bus.consume(&WorkQueue::RemoteTools).await.unwrap();
        f.bus
            .publish_work(&WorkQueue::RemoteTools, &42_u64)
            .await
            .unwrap();
        next(&mut messages).await.acknowledge().await.unwrap();
        assert!(timeout(ACK_WAIT * 3, messages.next()).await.is_err());
    })
    .await;
}

#[tokio::test]
async fn oversized_work_reports_advertised_limit() {
    run(|f| async move {
        let limit = f.admin.max_payload();
        let encoded_overhead = swarmy_core::encode(&Vec::<u8>::new()).unwrap().len();
        let value = vec![0_u8; limit + 1 - encoded_overhead];
        let encoded_size = swarmy_core::encode(&value).unwrap().len();
        let error = timeout(
            Duration::from_secs(2),
            f.bus.publish_work(&WorkQueue::RemoteTools, &value),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(error.permanent_publish_failure());
        assert!(
            matches!(&error, Error::PayloadTooLarge { size, limit: advertised }
            if *size == encoded_size && *advertised == limit)
        );
        assert!(error.to_string().contains(&encoded_size.to_string()));
        assert!(error.to_string().contains(&limit.to_string()));
    })
    .await;
}

#[tokio::test]
async fn two_workers_share_one_durable_consumer() {
    run(|f| async move {
        let mut first = f.bus.consume::<u64>(&WorkQueue::RemoteTools).await.unwrap();
        let mut second = f.bus.consume::<u64>(&WorkQueue::RemoteTools).await.unwrap();
        for value in 0..50_u64 {
            f.bus
                .publish_work(&WorkQueue::RemoteTools, &value)
                .await
                .unwrap();
        }
        let mut received = HashSet::new();
        let mut counts = [0; 2];
        timeout(Duration::from_secs(15), async {
            while received.len() < 50 {
                let (worker, delivery) = tokio::select! {
                    item = first.next() => (0, item),
                    item = second.next() => (1, item),
                };
                let delivery = delivery.unwrap().unwrap();
                assert!(received.insert(delivery.value), "duplicate delivery");
                counts[worker] += 1;
                delivery.acknowledge().await.unwrap();
            }
        })
        .await
        .unwrap();
        assert!(counts.into_iter().all(|count| count > 0));
        assert!(
            timeout(ACK_WAIT * 3, async {
                tokio::select! { item = first.next() => item, item = second.next() => item }
            })
            .await
            .is_err()
        );
    })
    .await;
}

#[tokio::test]
async fn live_deltas_and_events_reach_observers_without_persistence() {
    run(|f| async move {
        let session = SessionId::from_ulid(Ulid::generate());
        let url = std::env::var("SWARMY_NATS_URL").unwrap();
        let publisher = Bus::connect(&url, f.config.clone()).await.unwrap();
        for feed in [
            LiveFeed::ModelDeltas(session),
            LiveFeed::SessionEvents(session),
        ] {
            let mut first = f.bus.subscribe_live::<String>(feed).await.unwrap();
            let mut second = f.bus.subscribe_live::<String>(feed).await.unwrap();
            publisher.publish_live(feed, "delta").await.unwrap();
            for observer in [&mut first, &mut second] {
                assert_eq!(
                    timeout(WAIT, observer.next())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap(),
                    "delta"
                );
            }
            let mut late = f.bus.subscribe_live::<String>(feed).await.unwrap();
            assert!(
                timeout(Duration::from_millis(150), late.next())
                    .await
                    .is_err()
            );
        }
        let context = jetstream::new(f.admin.clone());
        for name in f.names() {
            assert_eq!(
                context
                    .get_stream(name)
                    .await
                    .unwrap()
                    .cached_info()
                    .state
                    .messages,
                0
            );
        }
    })
    .await;
}

#[tokio::test]
async fn setup_is_idempotent_and_rejects_configuration_drift() {
    run(|f| async move {
        let queues = [
            WorkQueue::Inference(SubjectToken::new("mock").unwrap()),
            WorkQueue::Runnable(7),
            WorkQueue::RemoteTools,
            WorkQueue::NodeTools(NodeId::from_ulid(Ulid::generate())),
        ];
        f.bus.setup(&queues).await.unwrap();
        let context = jetstream::new(f.admin.clone());
        let mut before = Vec::new();
        for name in f.names() {
            let stream = context.get_stream(name).await.unwrap();
            let consumer = stream.consumers().try_next().await.unwrap().unwrap();
            before.push((stream.cached_info().clone(), consumer));
        }
        f.bus
            .publish_work(&WorkQueue::RemoteTools, &7_u64)
            .await
            .unwrap();
        f.bus.setup(&queues).await.unwrap();
        for (name, (previous, previous_consumer)) in f.names().into_iter().zip(before) {
            let stream = context.get_stream(name).await.unwrap();
            let current = stream.cached_info();
            assert_eq!(current.config, previous.config);
            assert_eq!(current.created, previous.created);
            assert_eq!(current.state.consumer_count, previous.state.consumer_count);
            let consumer = stream.consumers().try_next().await.unwrap().unwrap();
            assert_eq!(consumer.config, previous_consumer.config);
            assert_eq!(consumer.created, previous_consumer.created);
        }
        let mut work = f.bus.consume(&WorkQueue::RemoteTools).await.unwrap();
        assert_eq!(next(&mut work).await.value, 7);
        let mut changed = f.config.clone();
        changed.ack_wait *= 2;
        let url = std::env::var("SWARMY_NATS_URL").unwrap();
        let conflicting = Bus::connect(&url, changed).await.unwrap();
        assert!(matches!(
            conflicting.setup(&queues).await,
            Err(Error::ConfigMismatch(_))
        ));
        let mut stream = context
            .get_stream(&f.names()[0])
            .await
            .unwrap()
            .cached_info()
            .config
            .clone();
        stream.max_messages = 100;
        context.update_stream(stream).await.unwrap();
        assert!(matches!(
            f.bus.setup(&queues).await,
            Err(Error::ConfigMismatch(_))
        ));
    })
    .await;
}

#[tokio::test]
async fn negative_acknowledgements_support_immediate_and_delayed_retry() {
    run(|f| async move {
        let mut work = f.bus.consume(&WorkQueue::RemoteTools).await.unwrap();
        f.bus
            .publish_work(&WorkQueue::RemoteTools, &1_u64)
            .await
            .unwrap();
        let first = next(&mut work).await;
        first.negative_acknowledge(None).await.unwrap();
        let second = next(&mut work).await;
        assert_eq!(second.value, 1);
        second
            .negative_acknowledge(Some(ACK_WAIT * 2))
            .await
            .unwrap();
        assert!(timeout(ACK_WAIT, work.next()).await.is_err());
        let third = next(&mut work).await;
        assert_eq!(third.value, 1);
        third.acknowledge().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn progress_extends_the_deadline() {
    run(|f| async move {
        let mut work = f.bus.consume(&WorkQueue::RemoteTools).await.unwrap();
        f.bus
            .publish_work(&WorkQueue::RemoteTools, &1_u64)
            .await
            .unwrap();
        let first = next(&mut work).await;
        for _ in 0..4 {
            sleep(Duration::from_millis(400)).await;
            first.extend_deadline().await.unwrap();
        }
        assert!(
            timeout(Duration::from_millis(400), work.next())
                .await
                .is_err()
        );
        assert_eq!(next(&mut work).await.value, 1);
    })
    .await;
}

#[tokio::test]
async fn malformed_work_surfaces_errors_and_stops_at_delivery_limit() {
    run(|f| async move {
        let context = jetstream::new(f.admin.clone());
        let mut work = f.bus.consume::<u64>(&WorkQueue::RemoteTools).await.unwrap();
        context
            .publish(format!("{}.tool.remote", f.prefix), vec![255].into())
            .await
            .unwrap()
            .await
            .unwrap();
        for _ in 0..f.config.max_deliver {
            assert!(matches!(
                timeout(WAIT, work.next()).await.unwrap().unwrap(),
                Err(Error::Encoding(EncodingError::UnknownVersion(255)))
            ));
        }
        assert!(timeout(ACK_WAIT * 3, work.next()).await.is_err());
        f.bus
            .publish_work(&WorkQueue::RemoteTools, &9_u64)
            .await
            .unwrap();
        let valid = next(&mut work).await;
        assert_eq!(valid.value, 9);
        valid.acknowledge().await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn routes_and_prefixes_isolate_work() {
    run(|f| async move {
        let routes = [
            WorkQueue::Inference(SubjectToken::new("mock").unwrap()),
            WorkQueue::Inference(SubjectToken::new("other").unwrap()),
            WorkQueue::Runnable(0),
            WorkQueue::Runnable(1),
            WorkQueue::RemoteTools,
            WorkQueue::NodeTools(NodeId::from_ulid(Ulid::generate())),
            WorkQueue::NodeTools(NodeId::from_ulid(Ulid::generate())),
        ];
        f.bus.setup(&routes).await.unwrap();
        for (value, route) in (0_u64..).zip(&routes) {
            f.bus.publish_work(route, &value).await.unwrap();
        }
        for (value, route) in (0_u64..).zip(&routes) {
            let mut work = f.bus.consume(route).await.unwrap();
            let message = next(&mut work).await;
            assert_eq!(message.value, value);
            message.acknowledge().await.unwrap();
        }
        // A second deployment with the same routes must not see the first one's work.
        run(|other| async move {
            f.bus
                .publish_work(&WorkQueue::RemoteTools, &99_u64)
                .await
                .unwrap();
            let mut isolated = other
                .bus
                .consume::<u64>(&WorkQueue::RemoteTools)
                .await
                .unwrap();
            assert!(timeout(ACK_WAIT, isolated.next()).await.is_err());
        })
        .await;
    })
    .await;
}

#[tokio::test]
async fn live_decode_failures_do_not_end_the_subscription() {
    run(|f| async move {
        let session = SessionId::from_ulid(Ulid::generate());
        let feed = LiveFeed::ModelDeltas(session);
        let mut live = f.bus.subscribe_live::<u64>(feed).await.unwrap();
        f.admin
            .publish(
                format!("{}.infer.live.{session}", f.prefix),
                vec![255].into(),
            )
            .await
            .unwrap();
        f.admin.flush().await.unwrap();
        assert!(matches!(
            timeout(WAIT, live.next()).await.unwrap().unwrap(),
            Err(Error::Encoding(_))
        ));
        f.bus.publish_live(feed, &7_u64).await.unwrap();
        assert_eq!(
            timeout(WAIT, live.next()).await.unwrap().unwrap().unwrap(),
            7
        );
    })
    .await;
}

#[test]
fn routing_tokens_reject_subject_injection() {
    for invalid in ["", "*", ">", "foo.bar", "has space", "a\nb", "a/b", "é"] {
        assert!(SubjectToken::new(invalid).is_err(), "accepted {invalid:?}");
    }
    assert!(SubjectToken::new("provider_class-1").is_ok());
}

#[tokio::test]
async fn wake_requests_round_trip_without_persisting_and_bad_requests_are_skipped() {
    run(|f| async move {
        let session_id = SessionId::from_ulid(Ulid::generate());
        let server = f.bus.clone();
        let serving = tokio::spawn(async move {
            server
                .serve_wake_requests(|request| async move {
                    if request.session_id == session_id {
                        WakeReply::Runnable
                    } else {
                        WakeReply::Failed("test failure".into())
                    }
                })
                .await
        });
        timeout(WAIT, async {
            loop {
                if let Ok(reply) = f.bus.request_wake(session_id, ACK_WAIT).await {
                    assert_eq!(reply, WakeReply::Runnable);
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let malformed = f
            .admin
            .send_request(
                format!("{}.sched.wake", f.prefix),
                async_nats::Request::new()
                    .payload(vec![255].into())
                    .timeout(Some(Duration::from_millis(50))),
            )
            .await;
        assert!(malformed.is_err());
        assert_eq!(
            f.bus
                .request_wake(SessionId::from_ulid(Ulid::generate()), ACK_WAIT)
                .await
                .unwrap(),
            WakeReply::Failed("test failure".into())
        );
        assert_eq!(
            f.bus.request_wake(session_id, ACK_WAIT).await.unwrap(),
            WakeReply::Runnable
        );
        let context = jetstream::new(f.admin.clone());
        for name in f.names() {
            assert_eq!(
                context
                    .get_stream(name)
                    .await
                    .unwrap()
                    .cached_info()
                    .state
                    .messages,
                0
            );
        }
        serving.abort();
        assert!(serving.await.unwrap_err().is_cancelled());
    })
    .await;
}

#[tokio::test]
async fn absent_or_unresponsive_scheduler_is_named_in_request_errors() {
    run(|f| async move {
        let session_id = SessionId::from_ulid(Ulid::generate());
        let error = f.bus.request_wake(session_id, ACK_WAIT).await.unwrap_err();
        assert!(matches!(error, Error::Scheduler(_)));
        assert!(error.to_string().contains("scheduler"));
        // A listener that never replies exercises the caller's deadline.
        let _silent = f
            .admin
            .subscribe(format!("{}.sched.wake", f.prefix))
            .await
            .unwrap();
        let inbox = f.admin.new_inbox();
        f.admin
            .send_request(inbox.clone(), async_nats::Request::new().inbox(inbox))
            .await
            .unwrap();
        let deadline = Duration::from_millis(100);
        let started = tokio::time::Instant::now();
        let error = timeout(ACK_WAIT, f.bus.request_wake(session_id, deadline))
            .await
            .unwrap()
            .unwrap_err();
        assert!(started.elapsed() >= deadline);
        assert!(matches!(error, Error::Scheduler(_)));
        assert!(error.to_string().contains("scheduler"));
    })
    .await;
}

#[tokio::test]
async fn nudges_deduplicate_the_same_head_but_not_fresh_steps_or_reaped_leases() {
    use futures_util::StreamExt;
    use swarmy_core::{Nudge, decode, runnable_partition};
    run(|f| async move {
        let id = SessionId::from_ulid(Ulid::generate());
        let subject = format!("{}.sched.runnable.{}", f.prefix, runnable_partition(id));
        let mut events = f.admin.subscribe(subject).await.unwrap();
        f.admin.flush().await.unwrap();
        let resend = Duration::from_millis(300);
        f.bus.nudge(id, 1, None, resend, false).await.unwrap();
        let first = timeout(WAIT, events.next()).await.unwrap().unwrap();
        assert_eq!(decode::<Nudge>(&first.payload).unwrap().session_id, id);
        f.bus
            .clone()
            .nudge(id, 1, None, resend, false)
            .await
            .unwrap();
        assert!(timeout(resend / 3, events.next()).await.is_err());
        f.bus.nudge(id, 2, None, resend, false).await.unwrap();
        timeout(WAIT, events.next()).await.unwrap().unwrap();
        f.bus.nudge(id, 2, None, resend, true).await.unwrap();
        timeout(WAIT, events.next()).await.unwrap().unwrap();
        timeout(WAIT, async {
            loop {
                f.bus.nudge(id, 2, None, resend, false).await.unwrap();
                if let Ok(event) = timeout(Duration::from_millis(10), events.next()).await {
                    break event.unwrap();
                }
            }
        })
        .await
        .unwrap();
    })
    .await;
}
