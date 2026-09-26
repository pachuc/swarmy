//! Cost series, quota listings, and per-entry show breakdowns through the CLI.
use super::*;
use swarmy_core::{CredentialScope, LeaseOwnerId};

fn tokens(input: u64, output: u64) -> swarmy_core::TokenUsage {
    swarmy_core::TokenUsage {
        input_tokens: input,
        cached_input_tokens: 1,
        output_tokens: output,
        reasoning_output_tokens: 0,
        total_tokens: input + output + 1,
        cache_write_input_tokens: 0,
    }
}

async fn complete(
    store: &Store,
    id: SessionId,
    provider: &str,
    model: &str,
    entry: &str,
    at: Timestamp,
    cost: u64,
) {
    let lease = store
        .claim_lease(
            id,
            LeaseOwnerId::from_ulid(Ulid::generate()),
            Timestamp::now()
                .checked_add(std::time::Duration::from_secs(60))
                .unwrap(),
        )
        .await
        .unwrap();
    let head = store.fetch_session(id).await.unwrap().unwrap().head_seq;
    let step = head + 1;
    store
        .submit_inference(
            head,
            &lease,
            &swarmy_core::InflightRecord {
                session_id: id,
                seq: step,
                provider: provider.into(),
                key_id: String::new(),
            },
            &"input",
        )
        .await
        .unwrap();
    let request = RequestId::for_step(id, step);
    let claim = swarmy_store::InferenceClaim {
        session_id: id,
        request_id: request,
        owner: LeaseOwnerId::from_ulid(Ulid::generate()),
        expires_at: Timestamp::now()
            .checked_add(std::time::Duration::from_secs(60))
            .unwrap(),
    };
    store
        .start_inference(&claim, Timestamp::now())
        .await
        .unwrap();
    let event = Event::InferenceCompleted {
        seq: 0,
        request_id: request,
        message: Message {
            id: MessageId::from_ulid(Ulid::generate()),
            role: MessageRole::Assistant,
            parts: vec![Part::Text {
                text: "done".into(),
            }],
        },
        provider: provider.into(),
        model: model.into(),
        effort_used: None,
        usage: tokens(10, 20),
        cost_micros: cost,
        effort_requested: None,
        effort_clamped: false,
        entry: None,
        route: None,
        route_step: None,
    };
    assert!(
        store
            .complete_inference(
                &swarmy_store::InferenceCompletion {
                    claim,
                    expected_head: step,
                    event,
                    now: at,
                    entry: Some(entry.into()),
                    entry_kind: Some("api-key".into()),
                    quota_remaining: std::collections::BTreeMap::new(),
                    quota_resets: std::collections::BTreeMap::new(),
                },
                &"answer",
            )
            .await
            .unwrap()
    );
}

async fn session_for(fixture: &Fixture, agent: Option<swarmy_core::AgentId>) -> SessionId {
    let id = SessionId::from_ulid(Ulid::generate());
    // Named agents pin their own image; only ephemeral sessions take one.
    let image = if agent.is_none() {
        Some("fixture:test")
    } else {
        None
    };
    fixture
        .store
        .create_session_for_agent(id, agent, image, Timestamp::now())
        .await
        .unwrap();
    fixture
        .store
        .wake_session(id, Timestamp::now())
        .await
        .unwrap();
    id
}

/// One completion in each of the twelve calendar months ending with the
/// current one, all strictly before `now`. Stepping calendar months back
/// from an hour ago keeps every seed inside (`parse_bound("1y", now)`,
/// `now`] no matter the day or the hour: seeding forward from the window
/// start at month-start plus one hour lands in the future during the first
/// hour of any month, and the usage query excludes that future hour, so
/// the series prints eleven rows instead of twelve.
fn seed_months(now: Timestamp) -> Vec<Timestamp> {
    use jiff::ToSpan as _;
    let base = now
        .checked_sub(1.hours())
        .expect("test window in range")
        .to_zoned(jiff::tz::TimeZone::UTC);
    (0_i64..12)
        .map(|back| {
            base.checked_sub(back.months())
                .expect("test month in range")
                .timestamp()
        })
        .collect()
}

fn cost_of(line: &str) -> f64 {
    line.split_whitespace()
        .find_map(|field| field.strip_prefix("cost=$"))
        .expect("cost field")
        .parse()
        .unwrap()
}

fn completions_of(line: &str) -> u64 {
    line.split_whitespace()
        .find_map(|field| field.strip_prefix("completions="))
        .expect("completions field")
        .parse()
        .unwrap()
}

#[tokio::test]
async fn cost_by_agent_month_prints_twelve_rows_and_matching_total() {
    run(|fixture| async move {
        let id = session_for(&fixture, None).await;
        // One completion per calendar month for the trailing year.
        for (index, at) in seed_months(Timestamp::now()).iter().enumerate() {
            complete(
                &fixture.store,
                id,
                "openai",
                "gpt-5",
                "primary",
                *at,
                100 * u64::try_from(index + 1).unwrap(),
            )
            .await;
        }
        let output = fixture
            .output(&["cost", "--by", "agent", "--group", "month", "--since", "1y"])
            .await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        let mut lines = text.lines();
        let mut rows: Vec<&str> = Vec::new();
        for line in &mut lines {
            if line.starts_with("total ") {
                let total = line;
                let rows_cost: f64 = rows.iter().map(|row| cost_of(row)).sum();
                assert_eq!(rows.len(), 12, "{text}");
                assert!((cost_of(total) - rows_cost).abs() < 0.000_05, "{text}");
                assert_eq!(
                    rows.iter().map(|row| completions_of(row)).sum::<u64>(),
                    completions_of(total)
                );
                assert_eq!(completions_of(total), 12);
            } else {
                rows.push(line);
            }
        }
        // The JSON series round-trips through the API types and matches the
        // store aggregate the route reads.
        let output = fixture
            .output(&[
                "cost", "--by", "agent", "--group", "month", "--since", "1y", "--json",
            ])
            .await;
        assert!(output.status.success());
        let response: swarmy_api_types::UsageResponse =
            serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(response.groups.len(), 12);
        let expected = fixture
            .store
            .usage_aggregate(
                swarmy_store::MeteringDimension::Agent,
                swarmy_core::time::parse_bound("1y", Timestamp::now()).unwrap(),
                Timestamp::now(),
                swarmy_store::UsageGroupBy::Month,
            )
            .await
            .unwrap();
        assert_eq!(response.groups.len(), expected.len());
        assert_eq!(
            response.total.completions,
            expected.iter().map(|group| group.completions).sum::<u64>()
        );
    })
    .await;
}

#[tokio::test]
async fn auth_quota_lists_observed_and_configured_entries() {
    run(|fixture| async move {
        let keyring = swarmy_config::Keyring::from_bytes([7; 32]);
        let credentials = fixture.store.credentials(keyring);
        credentials
            .put_entry(
                CredentialScope::Cluster,
                "openai",
                "main",
                &swarmy_core::CredentialRecord {
                    kind: swarmy_core::CredentialKind::ApiKey {
                        key: "test".into(),
                        extra: std::collections::BTreeMap::new(),
                    },
                    updated_at: Timestamp::now(),
                },
            )
            .await
            .unwrap();
        fixture
            .store
            .observe_entry_quota(
                "openai",
                "main",
                &std::collections::BTreeMap::from([
                    ("x-ratelimit-remaining-requests".into(), 97_u64),
                    ("x-ratelimit-remaining-tokens".into(), 11_000_u64),
                ]),
                &std::collections::BTreeMap::from([("x-ratelimit-reset-requests".into(), 60_u64)]),
            )
            .await
            .unwrap();
        credentials
            .put_entry(
                CredentialScope::Cluster,
                "chatgpt",
                "default",
                &swarmy_core::CredentialRecord {
                    kind: swarmy_core::CredentialKind::OAuth {
                        access: "test".into(),
                        refresh: "test".into(),
                        expires_at: Timestamp::now()
                            .checked_add(std::time::Duration::from_secs(3_600))
                            .unwrap(),
                        extra: std::collections::BTreeMap::new(),
                    },
                    updated_at: Timestamp::now(),
                },
            )
            .await
            .unwrap();
        fixture
            .store
            .set_entry_quota_config("chatgpt", "default", 200, 3_600)
            .await
            .unwrap();
        let output = fixture.output(&["auth", "quota"]).await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.contains("openai/main"), "{text}");
        assert!(text.contains("observed"), "{text}");
        assert!(text.contains("requests=97"), "{text}");
        assert!(text.contains("tokens=11000"), "{text}");
        assert!(text.contains("chatgpt/default"), "{text}");
        assert!(text.contains("subscription"), "{text}");
        assert!(text.contains("configured"), "{text}");
        assert!(text.contains("limit=200"), "{text}");
        // The JSON listing round-trips through the API types.
        let output = fixture.output(&["auth", "quota", "--json"]).await;
        assert!(output.status.success());
        let entries: Vec<swarmy_api_types::QuotaEntry> =
            serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(entries.len(), 2);
        // One entry's quota with its monthly usage for the year.
        let output = fixture
            .output(&[
                "auth",
                "quota",
                "--entry",
                "openai/main",
                "--group",
                "month",
                "--since",
                "1y",
                "--json",
            ])
            .await;
        assert!(output.status.success());
        let detail: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        let quota: swarmy_api_types::QuotaEntry =
            serde_json::from_value(detail["quota"].clone()).unwrap();
        assert_eq!(quota.provider, "openai");
        assert_eq!(quota.quota.requests_remaining, Some(97));
        let _: swarmy_api_types::UsageResponse =
            serde_json::from_value(detail["usage"].clone()).unwrap();
    })
    .await;
}

#[tokio::test]
async fn agent_and_session_show_name_entries_and_cost_per_entry() {
    run(|fixture| async move {
        let created = fixture.output(&["agent", "create", "coster"]).await;
        assert!(
            created.status.success(),
            "{}",
            String::from_utf8_lossy(&created.stderr)
        );
        let shown = fixture.output(&["agent", "show", "coster", "--json"]).await;
        assert!(shown.status.success());
        let agent: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
        let agent_id: swarmy_core::AgentId =
            serde_json::from_value(agent["agent_id"].clone()).unwrap();
        let first = session_for(&fixture, Some(agent_id)).await;
        let second = session_for(&fixture, Some(agent_id)).await;
        complete(
            &fixture.store,
            first,
            "openai",
            "gpt-5",
            "main",
            Timestamp::now(),
            500,
        )
        .await;
        complete(
            &fixture.store,
            second,
            "xai",
            "grok",
            "aux",
            Timestamp::now(),
            1_500,
        )
        .await;
        let shown = fixture.output(&["agent", "show", "coster"]).await;
        assert!(shown.status.success());
        let text = String::from_utf8(shown.stdout).unwrap();
        assert!(text.contains("entry openai/main cost=$0.0005"), "{text}");
        assert!(text.contains("entry xai/aux cost=$0.0015"), "{text}");
        assert!(text.contains("providers=openai,xai"), "{text}");
        // The JSON detail carries the same entries through the API types.
        let shown = fixture.output(&["agent", "show", "coster", "--json"]).await;
        let agent: serde_json::Value = serde_json::from_slice(&shown.stdout).unwrap();
        let entries: Vec<swarmy_api_types::EntryUsageView> =
            serde_json::from_value(agent["entries"].clone()).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entry, "xai/aux");
        assert_eq!(entries[0].totals.cost_micros, 1_500);
        assert_eq!(entries[1].entry, "openai/main");
        assert_eq!(entries[1].totals.cost_micros, 500);
        let shown = fixture
            .output(&["session", "show", &first.to_string()])
            .await;
        assert!(shown.status.success());
        let text = String::from_utf8(shown.stdout).unwrap();
        assert!(text.contains("entry openai/main cost=$0.0005"), "{text}");
        assert!(text.contains("providers=openai"), "{text}");
        let shown = fixture
            .output(&["session", "show", &first.to_string(), "--json"])
            .await;
        let mut lines = String::from_utf8(shown.stdout).unwrap();
        lines = lines.lines().nth(1).unwrap().into();
        let usage: serde_json::Value = serde_json::from_str(&lines).unwrap();
        let entries: Vec<swarmy_api_types::EntryUsageView> =
            serde_json::from_value(usage["entries"].clone()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].provider, "openai");
        // The agent filter answers the weekly cost question for one agent.
        let output = fixture
            .output(&[
                "cost", "--agent", "coster", "--group", "week", "--since", "3mo",
            ])
            .await;
        assert!(output.status.success());
        let text = String::from_utf8(output.stdout).unwrap();
        let total = text.lines().last().unwrap();
        assert!(total.contains("cost=$0.0020"), "{text}");
        assert_eq!(completions_of(total), 2);
    })
    .await;
}
