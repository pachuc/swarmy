use super::*;

struct CompletionInput<'a> {
    provider: &'a str,
    model: &'a str,
    entry: &'a str,
    kind: &'a str,
    usage: swarmy_core::TokenUsage,
    cost: u64,
    now: Timestamp,
    quota_remaining: std::collections::BTreeMap<String, u64>,
    quota_resets: std::collections::BTreeMap<String, u64>,
}

fn input<'a>(
    provider: &'a str,
    model: &'a str,
    entry: &'a str,
    kind: &'a str,
    usage: swarmy_core::TokenUsage,
    cost: u64,
    now: Timestamp,
) -> CompletionInput<'a> {
    CompletionInput {
        provider,
        model,
        entry,
        kind,
        usage,
        cost,
        now,
        quota_remaining: std::collections::BTreeMap::new(),
        quota_resets: std::collections::BTreeMap::new(),
    }
}

async fn start_claim(store: &Store, id: SessionId) -> (u64, swarmy_store::InferenceClaim) {
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
        provider: "openai".into(),
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
    (step, claim)
}

fn completion_event(
    request: RequestId,
    provider: &str,
    model: &str,
    usage: &swarmy_core::TokenUsage,
    cost: u64,
) -> Event {
    let message = swarmy_core::Message {
        id: swarmy_core::MessageId::from_ulid(ulid::Ulid::generate()),
        role: MessageRole::Assistant,
        parts: vec![Part::Text {
            text: "done".into(),
        }],
    };
    Event::InferenceCompleted {
        seq: 0,
        request_id: request,
        message,
        provider: provider.into(),
        model: model.into(),
        effort_used: None,
        usage: usage.clone(),
        cost_micros: cost,
        effort_requested: None,
        effort_clamped: false,
        entry: None,
        route: None,
        route_step: None,
    }
}

async fn complete_with(store: &Store, id: SessionId, input: &CompletionInput<'_>) -> RequestId {
    let (step, claim) = start_claim(store, id).await;
    let request_id = claim.request_id;
    let completion = swarmy_store::InferenceCompletion {
        claim,
        expected_head: step,
        event: completion_event(
            request_id,
            input.provider,
            input.model,
            &input.usage,
            input.cost,
        ),
        now: input.now,
        entry: Some(input.entry.into()),
        entry_kind: Some(input.kind.into()),
        quota_remaining: input.quota_remaining.clone(),
        quota_resets: input.quota_resets.clone(),
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

async fn session_groups(store: &Store, id: SessionId, hour: i64) -> Vec<swarmy_store::UsageGroup> {
    let from = Timestamp::from_second(hour).unwrap();
    let to = Timestamp::from_second(hour + 3_600).unwrap();
    store
        .usage(
            swarmy_store::MeteringDimension::Session,
            &id.to_string(),
            from,
            to,
            swarmy_store::UsageGroupBy::Day,
        )
        .await
        .unwrap()
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
        &input(
            "openai",
            "gpt-5",
            "primary",
            "api-key",
            tokens.clone(),
            cost,
            now,
        ),
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
    let entry = swarmy_store::metering::entry_key("openai", "primary");
    let checks = [
        (swarmy_store::MeteringDimension::Session, id.to_string()),
        (
            swarmy_store::MeteringDimension::Agent,
            session.agent_id.to_string(),
        ),
        (swarmy_store::MeteringDimension::Provider, "openai".into()),
        (swarmy_store::MeteringDimension::Entry, entry.clone()),
        (swarmy_store::MeteringDimension::EntryKind, "api-key".into()),
        (swarmy_store::MeteringDimension::Model, "gpt-5".into()),
        (
            swarmy_store::MeteringDimension::AgentEntry,
            format!("{}/{entry}", session.agent_id),
        ),
        (
            swarmy_store::MeteringDimension::SessionEntry,
            format!("{id}/{entry}"),
        ),
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
        &input(
            "openai",
            "gpt-5",
            "primary",
            "api-key",
            first.clone(),
            first_cost,
            base,
        ),
    )
    .await;
    complete_with(
        store,
        id,
        &input(
            "openai",
            "gpt-5",
            "primary",
            "api-key",
            second.clone(),
            second_cost,
            base,
        ),
    )
    .await;
    let totals = store.session_usage(id).await.unwrap();
    assert_eq!(totals.usage.input_tokens, 8);
    assert_eq!(totals.usage.output_tokens, 16);
    assert_eq!(totals.cost_micros, 300);
    let hour = swarmy_store::metering::hour_floor(base.as_second());
    let groups = session_groups(store, id, hour).await;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].completions, 2);
    assert_eq!(groups[0].totals, totals);
    assert_eq!(store.agent_usage(session.agent_id).await.unwrap(), totals);
    // Two concurrent commits for the same step race inside one transaction
    // each: the metering write happens after the head check, so the loser
    // either conflicts and retries onto the idempotency record or sees it
    // immediately. Exactly one commit wins and the buckets hold one
    // completion, proving a failed attempt leaves no partial bucket adds.
    let (step, claim) = start_claim(store, id).await;
    let request = claim.request_id;
    let racer = |usage: swarmy_core::TokenUsage, cost: u64| swarmy_store::InferenceCompletion {
        claim: claim.clone(),
        expected_head: step,
        event: completion_event(request, "openai", "gpt-5", &usage, cost),
        now: base,
        entry: Some("primary".into()),
        entry_kind: Some("api-key".into()),
        quota_remaining: std::collections::BTreeMap::new(),
        quota_resets: std::collections::BTreeMap::new(),
    };
    let first_attempt = racer(first.clone(), first_cost);
    let second_attempt = racer(second.clone(), second_cost);
    let (winner, loser) = tokio::join!(
        store.complete_inference(&first_attempt, &"answer"),
        store.complete_inference(&second_attempt, &"answer"),
    );
    let outcomes = [winner.unwrap(), loser.unwrap()];
    assert_eq!(outcomes.iter().filter(|done| **done).count(), 1);
    let groups = session_groups(store, id, hour).await;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].completions, 3);
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
    let mut first = input(
        "openai",
        "gpt-5",
        "primary",
        "api-key",
        tokens.clone(),
        cost,
        now,
    );
    first.quota_remaining = std::collections::BTreeMap::from([
        ("x-ratelimit-remaining-requests".into(), 97_u64),
        ("x-ratelimit-remaining-tokens".into(), 11_000_u64),
    ]);
    first.quota_resets = std::collections::BTreeMap::from([
        ("x-ratelimit-reset-requests".into(), 60_u64),
        ("x-ratelimit-reset-tokens".into(), 300_u64),
    ]);
    complete_with(store, id, &first).await;
    complete_with(
        store,
        id,
        &input(
            "openai",
            "gpt-5",
            "primary",
            "api-key",
            tokens.clone(),
            cost,
            now,
        ),
    )
    .await;
    let quota = store.entry_quota("openai", "primary").await.unwrap();
    assert!(matches!(quota.source, swarmy_store::QuotaSource::Observed));
    assert_eq!(
        quota.remaining.get("x-ratelimit-remaining-requests"),
        Some(&97)
    );
    assert_eq!(quota.requests_remaining, Some(97));
    assert_eq!(quota.tokens_remaining, Some(11_000));
    assert_eq!(quota.free, Some(97));
    assert_eq!(quota.window_seconds, Some(60));
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

struct BreakdownSeed {
    second: SessionId,
    agents: [swarmy_core::AgentId; 2],
    from: Timestamp,
    to: Timestamp,
    cost: u64,
}

/// Two sessions sharing one entry, with the first billing a second entry.
async fn seed_two_entries(test: &TestStore) -> BreakdownSeed {
    let store = &test.store;
    let first = test.create().await;
    let second = test.create().await;
    let agents = [
        store.fetch_session(first).await.unwrap().unwrap().agent_id,
        store.fetch_session(second).await.unwrap().unwrap().agent_id,
    ];
    let base = Timestamp::from_second(1_700_000_000).unwrap();
    let (tokens, cost) = usage(10, 20, 500);
    for (id, provider, entry) in [
        (first, "openai", "primary"),
        (first, "xai", "secondary"),
        (second, "openai", "primary"),
    ] {
        complete_with(
            store,
            id,
            &input(
                provider,
                "gpt-5",
                entry,
                "api-key",
                tokens.clone(),
                cost,
                base,
            ),
        )
        .await;
    }
    let from =
        Timestamp::from_second(swarmy_store::metering::hour_floor(base.as_second())).unwrap();
    let to = Timestamp::from_second(from.as_second() + 3_600).unwrap();
    BreakdownSeed {
        second,
        agents,
        from,
        to,
        cost,
    }
}

#[tokio::test]
async fn entry_breakdown_splits_combined_keys_with_costs() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let seed = seed_two_entries(&test).await;
    let store = &test.store;
    // One owner's entries come back with their costs, without the owner.
    let breakdown = store
        .dimension_totals(
            swarmy_store::MeteringDimension::AgentEntry,
            &seed.agents[0].to_string(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(breakdown.len(), 2);
    for total in &breakdown {
        assert!(["openai/primary", "xai/secondary"].contains(&total.key.as_str()));
        assert_eq!(total.totals.cost_micros, seed.cost);
        assert_eq!(total.completions, 1);
    }
    // The other owner sees only its own entry.
    let breakdown = store
        .dimension_totals(
            swarmy_store::MeteringDimension::SessionEntry,
            &seed.second.to_string(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(breakdown.len(), 1);
    assert_eq!(breakdown[0].totals.cost_micros, seed.cost);
    test.cleanup().await;
}

/// An owner's day slice holds exactly that day: rows for the same owner on
/// other days and for other owners never reach the totals, however many the
/// fleet has written, because the read is one bounded range.
#[tokio::test]
async fn owner_day_totals_ignore_rows_outside_their_range() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let session = store.fetch_session(id).await.unwrap().unwrap();
    let owner = session.agent_id.to_string();
    let day = Timestamp::from_second(1_700_000_000).unwrap();
    let next = Timestamp::from_second(1_700_000_000 + 86_400).unwrap();
    let far = Timestamp::from_second(1_700_000_000 + 30 * 86_400).unwrap();
    let (tokens, cost) = usage(10, 20, 500);
    // Same owner, queried day, two entries.
    for entry in ["primary", "secondary"] {
        complete_with(
            store,
            id,
            &input("openai", "gpt-5", entry, "api-key", tokens.clone(), cost, day),
        )
        .await;
    }
    // Same owner, other days: must not leak into the day slice.
    for at in [next, far] {
        complete_with(
            store,
            id,
            &input("openai", "gpt-5", "primary", "api-key", tokens.clone(), cost, at),
        )
        .await;
    }
    // Another owner on the queried day: hidden by the owner range.
    let other = test.create().await;
    complete_with(
        store,
        other,
        &input("xai", "grok", "aux", "api-key", tokens.clone(), 9_000, day),
    )
    .await;
    let end = Timestamp::from_second(day.as_second() + 86_400).unwrap();
    let totals = store
        .dimension_totals(
            swarmy_store::MeteringDimension::AgentEntry,
            &owner,
            Some((day, end)),
        )
        .await
        .unwrap();
    assert_eq!(totals.len(), 2);
    for total in &totals {
        assert_eq!(total.completions, 1);
        assert_eq!(total.totals.cost_micros, cost);
    }
    // The unwindowed read still sees the owner's whole history.
    let all = store
        .dimension_totals(swarmy_store::MeteringDimension::AgentEntry, &owner, None)
        .await
        .unwrap();
    assert_eq!(
        all.iter().map(|total| total.completions).sum::<u64>(),
        4
    );
    test.cleanup().await;
}

/// Completions without an entry keep their provider in the rollups, so the
/// breakdown names the provider with a `-` entry instead of `unknown`.
#[tokio::test]
async fn unattributed_completions_keep_their_provider() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let session = store.fetch_session(id).await.unwrap().unwrap();
    let (step, claim) = start_claim(store, id).await;
    let request = claim.request_id;
    let (tokens, cost) = usage(4, 4, 40);
    let completion = swarmy_store::InferenceCompletion {
        claim,
        expected_head: step,
        event: completion_event(request, "openai", "gpt-5", &tokens, cost),
        now: Timestamp::from_second(1_700_000_000).unwrap(),
        entry: None,
        entry_kind: None,
        quota_remaining: std::collections::BTreeMap::new(),
        quota_resets: std::collections::BTreeMap::new(),
    };
    assert!(
        store
            .complete_inference(&completion, &"answer")
            .await
            .unwrap()
    );
    let totals = store
        .dimension_totals(
            swarmy_store::MeteringDimension::AgentEntry,
            &session.agent_id.to_string(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(totals.len(), 1);
    assert_eq!(totals[0].key, "openai/-");
    assert_eq!(totals[0].totals.cost_micros, cost);
    test.cleanup().await;
}

#[tokio::test]
async fn aggregate_series_sums_single_key_views() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let seed = seed_two_entries(&test).await;
    let store = &test.store;
    let aggregate = store
        .usage_aggregate(
            swarmy_store::MeteringDimension::Agent,
            seed.from,
            seed.to,
            swarmy_store::UsageGroupBy::Day,
        )
        .await
        .unwrap();
    assert_eq!(aggregate.len(), 1);
    assert_eq!(aggregate[0].completions, 3);
    assert_eq!(aggregate[0].totals.cost_micros, seed.cost * 3);
    let mut summed = swarmy_core::UsageTotals::default();
    for agent in &seed.agents {
        let groups = store
            .usage(
                swarmy_store::MeteringDimension::Agent,
                &agent.to_string(),
                seed.from,
                seed.to,
                swarmy_store::UsageGroupBy::Day,
            )
            .await
            .unwrap();
        for group in &groups {
            summed.add(&group.totals.usage, group.totals.cost_micros);
        }
    }
    assert_eq!(aggregate[0].totals, summed);
    test.cleanup().await;
}

#[test]
fn legacy_four_field_usage_records_decode() {
    let bytes = swarmy_core::encode(&LegacyUsage {
        provider: "openai".into(),
        entry: Some("primary".into()),
        usage: swarmy_core::TokenUsage::default(),
        cost_micros: 7,
    })
    .unwrap();
    let record: swarmy_store::UsageRecord = swarmy_core::decode(&bytes).unwrap();
    assert_eq!(record.provider, "openai");
    assert_eq!(record.entry.as_deref(), Some("primary"));
    assert_eq!(record.cost_micros, 7);
    assert_eq!(record.session, None);
    assert_eq!(record.recorded_at, None);
}

#[derive(serde::Serialize)]
struct LegacyUsage {
    provider: String,
    entry: Option<String>,
    usage: swarmy_core::TokenUsage,
    cost_micros: u64,
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
        &input(
            "openai",
            "gpt-5",
            "primary",
            "api-key",
            tokens.clone(),
            cost,
            old,
        ),
    )
    .await;
    let live = complete_with(
        store,
        id,
        &input(
            "openai",
            "gpt-5",
            "primary",
            "api-key",
            tokens.clone(),
            cost,
            fresh,
        ),
    )
    .await;
    let cutoff = Timestamp::from_second(old.as_second() + 10).unwrap();
    let pruned = store.prune_metering_raw(cutoff, 64).await.unwrap();
    assert_eq!(pruned, 1);
    assert!(store.inference_usage_record(stale).await.unwrap().is_none());
    assert!(store.inference_usage_record(live).await.unwrap().is_some());
    let hour = swarmy_store::metering::hour_floor(old.as_second());
    let groups = session_groups(store, id, hour).await;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].completions, 1);
    test.cleanup().await;
}

async fn complete_many(store: &Store, id: SessionId, now: Timestamp, count: usize) {
    let (tokens, cost) = usage(1, 1, 1);
    for _ in 0..count {
        complete_with(
            store,
            id,
            &input(
                "openai",
                "gpt-5",
                "primary",
                "api-key",
                tokens.clone(),
                cost,
                now,
            ),
        )
        .await;
    }
}

async fn drain_prune(store: &Store, before: Timestamp, batch: usize) -> usize {
    let mut total = 0;
    for _ in 0..32 {
        let pruned = store.prune_metering_raw(before, batch).await.unwrap();
        total += pruned;
        if pruned < batch {
            break;
        }
    }
    total
}

#[tokio::test]
async fn pruning_drains_more_than_one_batch_across_ticks() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let old = Timestamp::from_second(1_000_000).unwrap();
    complete_many(store, id, old, 70).await;
    let cutoff = Timestamp::from_second(old.as_second() + 10).unwrap();
    let total = drain_prune(store, cutoff, 16).await;
    assert_eq!(total, 70);
    let hour = swarmy_store::metering::hour_floor(old.as_second());
    let groups = session_groups(store, id, hour).await;
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].completions, 70);
    test.cleanup().await;
}

#[tokio::test]
async fn pruning_before_a_cutoff_hour_removes_whole_earlier_hours() {
    let Some(test) = TestStore::memory() else {
        return;
    };
    let store = &test.store;
    let id = test.create().await;
    let base = Timestamp::from_second(1_000_000).unwrap();
    let second = Timestamp::from_second(base.as_second() + 3_600).unwrap();
    let third = Timestamp::from_second(base.as_second() + 7_200).unwrap();
    let (tokens, cost) = usage(1, 1, 1);
    let first = complete_with(
        store,
        id,
        &input(
            "openai",
            "gpt-5",
            "primary",
            "api-key",
            tokens.clone(),
            cost,
            base,
        ),
    )
    .await;
    let middle = complete_with(
        store,
        id,
        &input(
            "openai",
            "gpt-5",
            "primary",
            "api-key",
            tokens.clone(),
            cost,
            second,
        ),
    )
    .await;
    // The third record lands late in its hour so the cutoff-hour scan keeps it.
    let late = Timestamp::from_second(third.as_second() + 500).unwrap();
    let last = complete_with(
        store,
        id,
        &input(
            "openai",
            "gpt-5",
            "primary",
            "api-key",
            tokens.clone(),
            cost,
            late,
        ),
    )
    .await;
    let cutoff = Timestamp::from_second(third.as_second() + 10).unwrap();
    let pruned = store.prune_metering_raw(cutoff, 64).await.unwrap();
    assert_eq!(pruned, 2);
    assert!(store.inference_usage_record(first).await.unwrap().is_none());
    assert!(
        store
            .inference_usage_record(middle)
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.inference_usage_record(last).await.unwrap().is_some());
    for (moment, want) in [(base, 1), (second, 1), (late, 1)] {
        let hour = swarmy_store::metering::hour_floor(moment.as_second());
        let groups = session_groups(store, id, hour).await;
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].completions, want);
    }
    test.cleanup().await;
}
