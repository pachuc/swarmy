#![deny(clippy::disallowed_methods)]
//! Turn-metric stack tests: one turn's staged writes, tool batches, long
//! turns, and queued flushes against the dev stack.

use std::sync::Arc;

use swarmy_core::{MessageId, RequestId, SessionId, TurnEvent, TurnStage};
use swarmy_store::{
    InferenceMetric, MetricPatch, ToolMetric, blob::MemoryBlobStore, completion_patches,
    dispatch_patches,
};

#[tokio::test]
async fn incremental_records_merge_under_one_turn_key() {
    let Some(stack) = swarmy_testkit::Stack::load("turn_metrics") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    let session = SessionId::from_ulid(ulid::Ulid::generate());
    let turn = MessageId::from_ulid(ulid::Ulid::generate());
    let request = RequestId::for_step(session, 1);
    for (stage, ns) in [
        (TurnStage::Appended, 1_000_000),
        (TurnStage::InferenceStarted, 2_000_000),
        (TurnStage::FirstToken, 3_000_000),
        (TurnStage::InferenceFinished, 5_000_000),
        (TurnStage::Idle, 6_000_000),
    ] {
        store
            .record_turn_metrics(
                session,
                turn,
                vec![MetricPatch::Stage(TurnEvent {
                    session_id: session,
                    turn_id: turn,
                    stage,
                    request_id: Some(request),
                    clock_id: "boot".into(),
                    monotonic_ns: ns,
                    unix_ns: i128::from(ns),
                })],
            )
            .await
            .unwrap();
    }
    // The appended and idle anchors carry no request id in production;
    // record them that way so the wall-time derivation matches.
    store
        .record_turn_metrics(
            session,
            turn,
            vec![MetricPatch::Stage(TurnEvent {
                session_id: session,
                turn_id: turn,
                stage: TurnStage::Appended,
                request_id: None,
                clock_id: "boot".into(),
                monotonic_ns: 1_000_000,
                unix_ns: 1_000_000,
            })],
        )
        .await
        .unwrap();
    store
        .record_turn_metrics(
            session,
            turn,
            vec![MetricPatch::Stage(TurnEvent {
                session_id: session,
                turn_id: turn,
                stage: TurnStage::Idle,
                request_id: None,
                clock_id: "boot".into(),
                monotonic_ns: 6_000_000,
                unix_ns: 6_000_000,
            })],
        )
        .await
        .unwrap();
    store
        .record_turn_metrics(
            session,
            turn,
            vec![MetricPatch::Inference(InferenceMetric {
                request_id: request.to_string(),
                provider: "fake".into(),
                model: "scripted".into(),
                output_tokens: 10,
                ..InferenceMetric::default()
            })],
        )
        .await
        .unwrap();
    let records = store
        .list_turn_metrics_paged(session, None, 10, None, None)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].append_to_first_token_ms, Some(2.0));
    assert_eq!(records[0].inference_duration_ms, Some(3.0));
    assert_eq!(records[0].append_to_idle_ms, Some(5.0));
    assert_eq!(records[0].dropped_stages, 0);
    assert_eq!(records[0].dropped_inference, 0);
    assert_eq!(records[0].dropped_tools, 0);
    // Throughput now covers the whole request (2 ms to 5 ms), not the
    // streaming tail (3 ms to 5 ms).
    assert_eq!(records[0].inference[0].request_duration_ms, Some(3.0));
    assert_eq!(
        records[0].inference[0].output_tokens_per_second,
        Some(10_000.0 / 3.0)
    );
    assert!(
        store
            .list_turn_metrics_paged(session, Some(turn, None, None), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn batched_tool_patches_merge_in_one_transaction() {
    let Some(stack) = swarmy_testkit::Stack::load("turn_metrics") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    let session = SessionId::from_ulid(ulid::Ulid::generate());
    let turn = MessageId::from_ulid(ulid::Ulid::generate());
    let request = RequestId::for_step(session, 7);
    let dispatched = TurnEvent {
        session_id: session,
        turn_id: turn,
        stage: TurnStage::ToolDispatched,
        request_id: Some(request),
        clock_id: "boot".into(),
        monotonic_ns: 1,
        unix_ns: 1,
    };
    let completed = TurnEvent {
        session_id: session,
        turn_id: turn,
        stage: TurnStage::ToolCompleted,
        request_id: Some(request),
        clock_id: "boot".into(),
        monotonic_ns: 2,
        unix_ns: 2,
    };
    store
        .record_turn_metrics(session, turn, dispatch_patches(dispatched, "bash"))
        .await
        .unwrap();
    store
        .record_turn_metrics(
            session,
            turn,
            completion_patches(
                ToolMetric {
                    request_id: request.to_string(),
                    exit_status: Some(0),
                    output_bytes: Some(9),
                    process_wall_ms: Some(0.5),
                    ..ToolMetric::default()
                },
                None,
                completed,
            ),
        )
        .await
        .unwrap();
    let records = store
        .list_turn_metrics_paged(session, None, 10, None, None)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].tools.len(), 1);
    assert_eq!(records[0].tools[0].name, "bash");
    assert_eq!(records[0].tools[0].exit_status, Some(0));
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "the required 200-tool, 100-request turn builds its rows inline"
)]
async fn long_turn_with_two_hundred_tools_and_one_hundred_requests_reads_back_complete() {
    let Some(stack) = swarmy_testkit::Stack::load("turn_metrics") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    let session = SessionId::from_ulid(ulid::Ulid::generate());
    let turn = MessageId::from_ulid(ulid::Ulid::generate());
    let appended_ns: i128 = 1_000_000;
    let idle_ns: i128 = 2_000_000_000;
    store
        .record_turn_metrics(
            session,
            turn,
            vec![MetricPatch::Stage(TurnEvent {
                session_id: session,
                turn_id: turn,
                stage: TurnStage::Appended,
                request_id: None,
                clock_id: "boot".into(),
                monotonic_ns: 1_000_000,
                unix_ns: appended_ns,
            })],
        )
        .await
        .unwrap();
    for index in 0..200_u64 {
        let request = RequestId::for_step(session, index + 1);
        store
            .record_turn_metrics(
                session,
                turn,
                dispatch_patches(
                    TurnEvent {
                        session_id: session,
                        turn_id: turn,
                        stage: TurnStage::ToolDispatched,
                        request_id: Some(request),
                        clock_id: "boot".into(),
                        monotonic_ns: 1_000_001 + index,
                        unix_ns: appended_ns + i128::from(index + 1),
                    },
                    "bash",
                ),
            )
            .await
            .unwrap();
        store
            .record_turn_metrics(
                session,
                turn,
                completion_patches(
                    ToolMetric {
                        request_id: request.to_string(),
                        exit_status: Some(0),
                        output_bytes: Some(9),
                        ..ToolMetric::default()
                    },
                    None,
                    TurnEvent {
                        session_id: session,
                        turn_id: turn,
                        stage: TurnStage::ToolCompleted,
                        request_id: Some(request),
                        clock_id: "boot".into(),
                        monotonic_ns: 1_000_002 + index,
                        unix_ns: appended_ns + i128::from(index + 2),
                    },
                ),
            )
            .await
            .unwrap();
    }
    for index in 0..100_u64 {
        let request = RequestId::for_step(session, 10_000 + index);
        store
            .record_turn_metrics(
                session,
                turn,
                vec![MetricPatch::Stage(TurnEvent {
                    session_id: session,
                    turn_id: turn,
                    stage: TurnStage::InferenceStarted,
                    request_id: Some(request),
                    clock_id: "boot".into(),
                    monotonic_ns: 2_000_000 + index,
                    unix_ns: appended_ns + i128::from(500 + index),
                })],
            )
            .await
            .unwrap();
        store
            .record_turn_metrics(
                session,
                turn,
                vec![MetricPatch::Inference(InferenceMetric {
                    request_id: request.to_string(),
                    provider: "fake".into(),
                    model: "scripted".into(),
                    output_tokens: 4,
                    ..InferenceMetric::default()
                })],
            )
            .await
            .unwrap();
        store
            .record_turn_metrics(
                session,
                turn,
                vec![MetricPatch::Stage(TurnEvent {
                    session_id: session,
                    turn_id: turn,
                    stage: TurnStage::InferenceFinished,
                    request_id: Some(request),
                    clock_id: "boot".into(),
                    monotonic_ns: 3_000_000 + index,
                    unix_ns: appended_ns + i128::from(600 + index),
                })],
            )
            .await
            .unwrap();
    }
    let idle_event = TurnEvent {
        session_id: session,
        turn_id: turn,
        stage: TurnStage::Idle,
        request_id: None,
        clock_id: "boot".into(),
        monotonic_ns: 4_000_000,
        unix_ns: idle_ns,
    };
    store
        .record_turn_metrics(session, turn, vec![MetricPatch::Stage(idle_event)])
        .await
        .unwrap();
    let records = store
        .list_turn_metrics_paged(session, None, 10, None, None)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.tools.len(), 200);
    assert_eq!(record.inference.len(), 100);
    assert_eq!(record.dropped_stages, 0);
    assert_eq!(record.dropped_inference, 0);
    assert_eq!(record.dropped_tools, 0);
    #[expect(
        clippy::cast_precision_loss,
        reason = "nanosecond wall times exceed f64 integer precision; the millisecond check does not need it"
    )]
    let expected_idle_ms = (idle_ns - appended_ns) as f64 / 1_000_000.0;
    assert_eq!(record.append_to_idle_ms, Some(expected_idle_ms));
    // Paging on the arrays truncates deterministically and reports the
    // remainder in the dropped counters.
    let paged = store
        .list_turn_metrics_paged(session, None, 10, Some(10), Some(20))
        .await
        .unwrap();
    assert_eq!(paged[0].inference.len(), 10);
    assert_eq!(paged[0].tools.len(), 20);
    assert_eq!(paged[0].dropped_inference, 90);
    assert_eq!(paged[0].dropped_tools, 180);
}

#[tokio::test]
async fn queued_metrics_are_visible_after_flush() {
    let Some(stack) = swarmy_testkit::Stack::load("turn_metrics") else {
        return;
    };
    let (store, _guard) = stack.open_store(Arc::new(MemoryBlobStore::default())).await;
    let session = SessionId::from_ulid(ulid::Ulid::generate());
    let turn = MessageId::from_ulid(ulid::Ulid::generate());
    // Queue without awaiting the write, then flush: the drain must have
    // committed before the read below.
    store.observe_turn_metric(
        session,
        turn,
        MetricPatch::Stage(TurnEvent {
            session_id: session,
            turn_id: turn,
            stage: TurnStage::Appended,
            request_id: None,
            clock_id: "boot".into(),
            monotonic_ns: 1_000_000,
            unix_ns: 1_000_000,
        }),
    );
    store.observe_turn_metric(
        session,
        turn,
        MetricPatch::Stage(TurnEvent {
            session_id: session,
            turn_id: turn,
            stage: TurnStage::Idle,
            request_id: None,
            clock_id: "boot".into(),
            monotonic_ns: 2_000_000,
            unix_ns: 2_000_000,
        }),
    );
    store.flush_turn_metrics().await.unwrap();
    let records = store
        .list_turn_metrics_paged(session, None, 10, None, None)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].turn_id, turn.to_string());
}
