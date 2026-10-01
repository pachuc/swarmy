//! Turn-metric integration and pure-logic tests.
#![deny(clippy::disallowed_methods)]
use std::collections::BTreeMap;

use swarmy_core::{MessageId, SessionId, TurnEvent, TurnStage};

use crate::{
    metrics_codec::{
        StoredInferenceMetricCurrent, StoredToolMetricCurrent, StoredTurnInferenceCurrent,
        StoredTurnSummaryCurrent,
    },
    metrics_model::{
        InferenceMetric, MetricPatch, StageTiming, ToolMetric, TurnMetrics, TurnWrite, apply_one,
        completion_patches, dispatch_patches, sort_inference_rows, sort_tool_rows,
    },
};

#[test]
fn detail_rows_sort_chronologically_not_by_hash() {
    let row = |id: &str, started: Option<i64>| StoredTurnInferenceCurrent {
        metric: StoredInferenceMetricCurrent {
            request_id: id.into(),
            ..StoredInferenceMetricCurrent::default()
        },
        started_ns: started,
        ..StoredTurnInferenceCurrent::default()
    };
    // Ids chosen so hash order disagrees with time order; the later
    // request must still sort first when its start is earlier.
    let mut rows = vec![row("zzz", Some(3_000_000)), row("aaa", Some(2_000_000))];
    sort_inference_rows(&mut rows);
    assert_eq!(rows[0].metric.request_id, "aaa");
    assert_eq!(rows[1].metric.request_id, "zzz");
    // Rows without timestamps sort last with ties broken by id.
    let mut rows = vec![row("b", None), row("a", None)];
    sort_inference_rows(&mut rows);
    assert_eq!(rows[0].metric.request_id, "a");
    let tool = |id: &str, dispatched: Option<i64>| StoredToolMetricCurrent {
        request_id: id.into(),
        dispatched_ns: dispatched,
        ..StoredToolMetricCurrent::default()
    };
    let mut tools = vec![tool("zzz", Some(5)), tool("aaa", Some(1))];
    sort_tool_rows(&mut tools);
    assert_eq!(tools[0].request_id, "aaa");
}

#[test]
fn unbounded_turn_keeps_every_row_and_derives_idle() {
    let session = SessionId::from_ulid(ulid::Ulid::nil());
    let turn = MessageId::from_ulid(ulid::Ulid::nil());
    let mut state = TurnWrite {
        summary: StoredTurnSummaryCurrent {
            session_id: session.to_string(),
            turn_id: turn.to_string(),
            ..StoredTurnSummaryCurrent::default()
        },
        inference: BTreeMap::new(),
        tools: BTreeMap::new(),
    };
    let event = |stage: TurnStage, request: Option<swarmy_core::RequestId>, ns: i128| TurnEvent {
        session_id: session,
        turn_id: turn,
        stage,
        request_id: request,
        clock_id: "boot".into(),
        monotonic_ns: u64::try_from(ns).unwrap_or(0),
        unix_ns: ns,
    };
    apply_one(
        &mut state,
        &MetricPatch::Stage(event(TurnStage::Appended, None, 1_000_000)),
    );
    for index in 0..200_u64 {
        let request = swarmy_core::RequestId::for_step(session, index + 1);
        apply_one(
            &mut state,
            &MetricPatch::Stage(event(
                TurnStage::ToolDispatched,
                Some(request),
                i128::from(2_000_000 + index),
            )),
        );
        apply_one(
            &mut state,
            &MetricPatch::Tool(ToolMetric {
                request_id: request.to_string(),
                name: "bash".into(),
                ..ToolMetric::default()
            }),
        );
    }
    for index in 0..100_u64 {
        let request = swarmy_core::RequestId::for_step(session, 1000 + index);
        apply_one(
            &mut state,
            &MetricPatch::Stage(event(
                TurnStage::InferenceStarted,
                Some(request),
                i128::from(3_000_000 + index),
            )),
        );
        apply_one(
            &mut state,
            &MetricPatch::Inference(InferenceMetric {
                request_id: request.to_string(),
                provider: "fake".into(),
                model: "scripted".into(),
                output_tokens: 4,
                ..InferenceMetric::default()
            }),
        );
        apply_one(
            &mut state,
            &MetricPatch::Stage(event(
                TurnStage::InferenceFinished,
                Some(request),
                i128::from(4_000_000 + index),
            )),
        );
    }
    apply_one(
        &mut state,
        &MetricPatch::Stage(event(TurnStage::Idle, None, 6_000_000)),
    );
    assert_eq!(state.tools.len(), 200);
    assert_eq!(state.inference.len(), 100);
    assert_eq!(state.summary.append_to_idle_ms, Some(5.0));
}

#[test]
fn throughput_covers_the_whole_request() {
    let mut turn = TurnMetrics::default();
    let row = |stage: &str, ns, request: Option<&str>| StageTiming {
        stage: stage.into(),
        request_id: request.map(str::to_owned),
        clock_id: "boot".into(),
        monotonic_ns: ns,
        unix_ns: i64::try_from(ns).unwrap(),
    };
    turn.stages.push(row("appended", 1_000_000_000, None));
    turn.stages
        .push(row("inference_started", 2_000_000_000, Some("r")));
    turn.stages
        .push(row("first_token", 3_000_000_000, Some("r")));
    // A single-chunk provider finishes a millisecond after the first
    // token; the old streaming-only throughput would divide by that
    // millisecond and report an absurd rate.
    turn.stages
        .push(row("inference_finished", 3_001_000_000, Some("r")));
    turn.stages.push(row("idle", 6_000_000_000, None));
    turn.inference.push(InferenceMetric {
        request_id: "r".into(),
        output_tokens: 363,
        ..InferenceMetric::default()
    });
    turn.derive();
    let request = &turn.inference[0];
    assert_eq!(request.request_duration_ms, Some(1001.0));
    // 363 tokens over the whole 1001 ms request, not over the 1 ms
    // streaming tail.
    let expected = 363.0 * 1000.0 / 1001.0;
    assert!((request.output_tokens_per_second.unwrap() - expected).abs() < 1.0);
    assert_eq!(turn.append_to_idle_ms, Some(5000.0));
    assert_eq!(InferenceMetric::tokens_per_second(0, 1.0), None);
    assert_eq!(InferenceMetric::tokens_per_second(1, 0.0), None);
}

#[test]
fn batched_patches_equal_sequential_writes() {
    // Batched tool patches must fold to the same rows as sequential writes:
    // one dispatch folds name plus stage, one completion folds tool plus
    // stage, so batching changes transaction shape, never row content.
    let session = SessionId::from_ulid(ulid::Ulid::nil());
    let turn = MessageId::from_ulid(ulid::Ulid::nil());
    let dispatched = |request: swarmy_core::RequestId| TurnEvent {
        session_id: session,
        turn_id: turn,
        stage: TurnStage::ToolDispatched,
        request_id: Some(request),
        clock_id: "boot".into(),
        monotonic_ns: 1,
        unix_ns: 1,
    };
    let completed = |request: swarmy_core::RequestId| TurnEvent {
        session_id: session,
        turn_id: turn,
        stage: TurnStage::ToolCompleted,
        request_id: Some(request),
        clock_id: "boot".into(),
        monotonic_ns: 2,
        unix_ns: 2,
    };
    let apply_batch = |state: &mut TurnWrite, patches: Vec<MetricPatch>| {
        for patch in &patches {
            apply_one(state, patch);
        }
    };
    let mut batched = TurnWrite {
        summary: StoredTurnSummaryCurrent::default(),
        inference: BTreeMap::new(),
        tools: BTreeMap::new(),
    };
    let mut sequential = TurnWrite {
        summary: StoredTurnSummaryCurrent::default(),
        inference: BTreeMap::new(),
        tools: BTreeMap::new(),
    };
    for (index, name) in ["a", "b", "c"].iter().enumerate() {
        let request = swarmy_core::RequestId::for_step(session, u64::try_from(index).unwrap() + 1);
        let dispatch = dispatch_patches(dispatched(request), name);
        let completion = completion_patches(
            ToolMetric {
                request_id: request.to_string(),
                exit_status: Some(0),
                output_bytes: Some(1),
                ..ToolMetric::default()
            },
            None,
            completed(request),
        );
        apply_batch(&mut batched, dispatch.clone());
        apply_batch(&mut batched, completion.clone());
        for patch in dispatch.into_iter().chain(completion) {
            apply_one(&mut sequential, &patch);
        }
    }
    assert_eq!(batched.tools, sequential.tools);
    assert_eq!(batched.tools.len(), 3);
}
