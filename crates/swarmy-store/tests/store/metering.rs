use super::*;

struct CompletionInput<'a> {
    provider: &'a str,
    model: &'a str,
    entry: &'a str,
    kind: &'a str,
    usage: swarmy_core::TokenUsage,
    cost: u64,
    now: Timestamp,
}

async fn complete_with(store: &Store, id: SessionId, input: &CompletionInput<'_>) -> RequestId {
    let lease = store
        .claim_lease(
            id,
            owner(),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let head = store.fetch_session(id).await.unwrap().unwrap().head_seq;
    let step = head + 1;
    let record = InflightRecord {
        session_id: id,
        seq: step,
        provider: input.provider.into(),
        key_id: String::new(),
    };
    store
        .submit_inference(head, &lease, &record, &"input")
        .await
        .unwrap();
    let request_id = RequestId::for_step(id, step);
    let claim = swarmy_store::InferenceClaim {
        session_id: id,
        request_id,
        owner: owner(),
        expires_at: Timestamp::now()
            .checked_add(std::time::Duration::from_secs(60))
            .unwrap(),
    };
    store
        .start_inference(&claim, Timestamp::now())
        .await
        .unwrap();
    store
        .set_inference_entry(request_id, Some(input.entry))
        .await
        .unwrap();
    store
        .set_inference_entry_kind(request_id, Some(input.kind))
        .await
        .unwrap();
    let message = swarmy_core::Message {
        id: swarmy_core::MessageId::from_ulid(ulid::Ulid::generate()),
        role: MessageRole::Assistant,
        parts: vec![Part::Text {
            text: "done".into(),
        }],
    };
    let completion = swarmy_store::InferenceCompletion {
        claim,
        expected_head: step,
        event: Event::InferenceCompleted {
            seq: 0,
            request_id,
            message,
            provider: input.provider.into(),
            model: input.model.into(),
            effort_used: None,
            usage: input.usage.clone(),
            cost_micros: input.cost,
            effort_requested: None,
            effort_clamped: false,
        },
        now: input.now,
    };
    assert!(
        store
            .complete_inference(&completion, &"answer")
            .await
            .unwrap()
    );
    request_id
}

fn usage(input: u64, output: u64, cost: u64) -> (swarmy_core::TokenUsage, u64) {
    (
        swarmy_core::TokenUsage {
            input_tokens: input,
            cached_input_tokens: 1,
            output_tokens: output,
            reasoning_output_tokens: 0,
            total_tokens: input + output + 1,
            cache_write_input_tokens: 0,
        },
        cost,
    )
}

#[tokio::test]
async fn completion_updates_every_dimension_bucket_for_its_hour() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let session = store.fetch_session(id).await.unwrap().unwrap();
    let now = Timestamp::from_second(1_700_000_000).unwrap();
    let (tokens, cost) = usage(10, 20, 500);
    let request = complete_with(
        store,
        id,
        &CompletionInput {
            provider: "openai",
            model: "gpt-5",
            entry: "primary",
            kind: "api-key",
            usage: tokens.clone(),
            cost,
            now,
        },
    )
    .await;
    let record = store
        .inference_usage_record(request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.provider, "openai");
    assert_eq!(record.entry.as_deref(), Some("primary"));
    assert_eq!(record.model, "gpt-5");
    assert_eq!(record.entry_kind.as_deref(), Some("api-key"));
    assert_eq!(record.session, Some(id));
    assert_eq!(record.agent, Some(session.agent_id));
    let hour = swarmy_store::metering::hour_floor(now.as_second());
    assert_eq!(hour, now.as_second() - now.as_second().rem_euclid(3_600));
    let from = Timestamp::from_second(hour).unwrap();
    let to = Timestamp::from_second(hour + 3_600).unwrap();
    let group_by = swarmy_store::UsageGroupBy::Day;
    let checks = [
        (swarmy_store::MeteringDimension::Session, id.to_string()),
        (
            swarmy_store::MeteringDimension::Agent,
            session.agent_id.to_string(),
        ),
        (swarmy_store::MeteringDimension::Provider, "openai".into()),
        (
            swarmy_store::MeteringDimension::Entry,
            swarmy_store::metering::entry_key("openai", "primary"),
        ),
        (swarmy_store::MeteringDimension::EntryKind, "api-key".into()),
        (swarmy_store::MeteringDimension::Model, "gpt-5".into()),
    ];
    for (dimension, key) in checks {
        let groups = store
            .usage(dimension, &key, from, to, group_by)
            .await
            .unwrap();
        assert_eq!(groups.len(), 1, "{dimension:?}");
        assert_eq!(groups[0].completions, 1);
        assert_eq!(groups[0].totals.usage.input_tokens, 10);
        assert_eq!(groups[0].totals.usage.output_tokens, 20);
        assert_eq!(groups[0].totals.cost_micros, 500);
    }
    test.cleanup().await;
}

#[tokio::test]
async fn bucket_sums_match_session_totals_and_failed_commits_leave_nothing() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let session = store.fetch_session(id).await.unwrap().unwrap();
    let base = Timestamp::from_second(1_700_000_000).unwrap();
    let (first, first_cost) = usage(5, 7, 100);
    let (second, second_cost) = usage(3, 9, 200);
    complete_with(
        store,
        id,
        &CompletionInput {
            provider: "openai",
            model: "gpt-5",
            entry: "primary",
            kind: "api-key",
            usage: first.clone(),
            cost: first_cost,
            now: base,
        },
    )
    .await;
    complete_with(
        store,
        id,
        &CompletionInput {
            provider: "openai",
            model: "gpt-5",
            entry: "primary",
            kind: "api-key",
            usage: second.clone(),
            cost: second_cost,
            now: base,
        },
    )
    .await;
    let totals = store.session_usage(id).await.unwrap();
    assert_eq!(totals.usage.input_tokens, 8);
    assert_eq!(totals.usage.output_tokens, 16);
    assert_eq!(totals.cost_micros, 300);
    let hour = swarmy_store::metering::hour_floor(base.as_second());
    let from = Timestamp::from_second(hour).unwrap();
    let to = Timestamp::from_second(hour + 3_600).unwrap();
    let groups = store
        .usage(
            swarmy_store::MeteringDimension::Session,
            &id.to_string(),
            from,
            to,
            swarmy_store::UsageGroupBy::Day,
        )
        .await
        .unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].completions, 2);
    assert_eq!(groups[0].totals, totals);
    assert_eq!(store.agent_usage(session.agent_id).await.unwrap(), totals);
    let head = store.fetch_session(id).await.unwrap().unwrap().head_seq;
    let stale = swarmy_store::InferenceCompletion {
        claim: swarmy_store::InferenceClaim {
            session_id: id,
            request_id: RequestId::for_step(id, head + 1),
            owner: owner(),
            expires_at: Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        },
        expected_head: head + 5,
        event: Event::InferenceFailed {
            seq: 0,
            request_id: RequestId::for_step(id, head + 1),
            error: "stale".into(),
            retryable: false,
            retry_at: None,
        },
        now: base,
    };
    assert!(store.complete_inference(&stale, &"nope").await.is_err());
    let groups = store
        .usage(
            swarmy_store::MeteringDimension::Session,
            &id.to_string(),
            from,
            to,
            swarmy_store::UsageGroupBy::Day,
        )
        .await
        .unwrap();
    assert_eq!(groups[0].completions, 2);
    test.cleanup().await;
}

#[tokio::test]
async fn observed_quota_reports_remaining_and_configured_uses_rollups() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let now = Timestamp::now();
    let (tokens, cost) = usage(2, 2, 10);
    complete_with(
        store,
        id,
        &CompletionInput {
            provider: "openai",
            model: "gpt-5",
            entry: "primary",
            kind: "api-key",
            usage: tokens.clone(),
            cost,
            now,
        },
    )
    .await;
    complete_with(
        store,
        id,
        &CompletionInput {
            provider: "openai",
            model: "gpt-5",
            entry: "primary",
            kind: "api-key",
            usage: tokens.clone(),
            cost,
            now,
        },
    )
    .await;
    let remaining = std::collections::BTreeMap::from([
        ("x-ratelimit-remaining-requests".into(), 97_u64),
        ("x-ratelimit-remaining-tokens".into(), 11_000_u64),
    ]);
    store
        .observe_entry_quota("openai", "primary", &remaining)
        .await
        .unwrap();
    let quota = store.entry_quota("openai", "primary").await.unwrap();
    assert!(matches!(quota.source, swarmy_store::QuotaSource::Observed));
    assert_eq!(
        quota.remaining.get("x-ratelimit-remaining-requests"),
        Some(&97)
    );
    assert_eq!(quota.free, Some(97));
    store
        .set_entry_quota_config("openai", "primary", 1_000, 18_000)
        .await
        .unwrap();
    let quota = store.entry_quota("openai", "primary").await.unwrap();
    assert!(matches!(
        quota.source,
        swarmy_store::QuotaSource::Configured
    ));
    assert_eq!(quota.used, 2);
    assert_eq!(quota.free, Some(998));
    assert_eq!(quota.limit, Some(1_000));
    assert_eq!(quota.window_seconds, Some(18_000));
    test.cleanup().await;
}

#[tokio::test]
async fn pruning_removes_raw_records_but_keeps_rollups() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let old = Timestamp::from_second(1_000_000).unwrap();
    let fresh = Timestamp::now();
    let (tokens, cost) = usage(4, 4, 40);
    let stale = complete_with(
        store,
        id,
        &CompletionInput {
            provider: "openai",
            model: "gpt-5",
            entry: "primary",
            kind: "api-key",
            usage: tokens.clone(),
            cost,
            now: old,
        },
    )
    .await;
    let live = complete_with(
        store,
        id,
        &CompletionInput {
            provider: "openai",
            model: "gpt-5",
            entry: "primary",
            kind: "api-key",
            usage: tokens.clone(),
            cost,
            now: fresh,
        },
    )
    .await;
    let cutoff = Timestamp::from_second(old.as_second() + 10).unwrap();
    let pruned = store.prune_metering_raw(cutoff, 64).await.unwrap();
    assert_eq!(pruned, 1);
    assert!(store.inference_usage_record(stale).await.unwrap().is_none());
    assert!(store.inference_usage_record(live).await.unwrap().is_some());
    let hour = swarmy_store::metering::hour_floor(old.as_second());
    let groups = store
        .usage(
            swarmy_store::MeteringDimension::Session,
            &id.to_string(),
            Timestamp::from_second(hour).unwrap(),
            Timestamp::from_second(hour + 3_600).unwrap(),
            swarmy_store::UsageGroupBy::Day,
        )
        .await
        .unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].completions, 1);
    test.cleanup().await;
}
