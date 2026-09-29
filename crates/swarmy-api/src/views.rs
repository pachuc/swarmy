//! Convert persistence-layer metric shapes into API response shapes.
//!
//! The store owns its metric structs and returns them; this module maps them
//! field for field onto the versioned JSON contract. The shapes stay
//! identical so stored rows keep decoding, and the conversion stays here so
//! the store never compiles the `OpenAPI` tooling.

use swarmy_api_types as api;
use swarmy_store as store;

#[must_use]
pub(crate) fn into_api_stage(value: store::StageTiming) -> api::StageTiming {
    api::StageTiming {
        stage: value.stage,
        request_id: value.request_id,
        clock_id: value.clock_id,
        monotonic_ns: value.monotonic_ns,
        unix_ns: value.unix_ns,
    }
}

#[must_use]
pub(crate) fn into_api_inference(value: store::InferenceMetric) -> api::InferenceMetric {
    api::InferenceMetric {
        request_id: value.request_id,
        provider: value.provider,
        model: value.model,
        input_tokens: value.input_tokens,
        cached_input_tokens: value.cached_input_tokens,
        output_tokens: value.output_tokens,
        reasoning_tokens: value.reasoning_tokens,
        cost_micros: value.cost_micros,
        time_to_first_token_ms: value.time_to_first_token_ms,
        streaming_duration_ms: value.streaming_duration_ms,
        request_duration_ms: value.request_duration_ms,
        streamed: value.streamed,
        output_tokens_per_second: value.output_tokens_per_second,
        retries: value.retries,
        rate_limit_waits: value.rate_limit_waits,
        gateway_waits: value.gateway_waits,
        provider_failures: value.provider_failures,
        error: value.error,
    }
}

#[must_use]
pub(crate) fn into_api_tool(value: store::ToolMetric) -> api::ToolMetric {
    api::ToolMetric {
        request_id: value.request_id,
        name: value.name,
        dispatched_ns: value.dispatched_ns,
        started_ns: value.started_ns,
        completed_ns: value.completed_ns,
        exit_status: value.exit_status,
        output_bytes: value.output_bytes,
        queue_ms: value.queue_ms,
        process_wall_ms: value.process_wall_ms,
    }
}

#[must_use]
pub(crate) fn into_api_computer(value: &store::ComputerMetric) -> api::ComputerMetric {
    api::ComputerMetric {
        placement_ms: value.placement_ms,
        cold: value.cold,
        chunks_fetched: value.chunks_fetched,
        bytes_fetched: value.bytes_fetched,
        fetch_p50_ms: value.fetch_p50_ms,
        fetch_p95_ms: value.fetch_p95_ms,
        first_tool_chunks_fetched: value.first_tool_chunks_fetched,
        first_tool_bytes_fetched: value.first_tool_bytes_fetched,
        first_tool_fetch_p50_ms: value.first_tool_fetch_p50_ms,
        first_tool_fetch_p95_ms: value.first_tool_fetch_p95_ms,
    }
}

#[must_use]
pub(crate) fn into_api_turn(value: store::TurnMetrics) -> api::TurnMetrics {
    api::TurnMetrics {
        session_id: value.session_id,
        turn_id: value.turn_id,
        stages: value.stages.into_iter().map(into_api_stage).collect(),
        inference: value
            .inference
            .into_iter()
            .map(into_api_inference)
            .collect(),
        tools: value.tools.into_iter().map(into_api_tool).collect(),
        computer: value.computer.as_ref().map(into_api_computer),
        append_to_first_token_ms: value.append_to_first_token_ms,
        inference_duration_ms: value.inference_duration_ms,
        append_to_idle_ms: value.append_to_idle_ms,
        error: value.error,
        dropped_stages: value.dropped_stages,
        dropped_inference: value.dropped_inference,
        dropped_tools: value.dropped_tools,
    }
}

#[must_use]
pub(crate) fn into_api_latency(value: &store::LatencyPercentiles) -> api::LatencyPercentiles {
    api::LatencyPercentiles {
        p50_ms: value.p50_ms,
        p95_ms: value.p95_ms,
    }
}

#[must_use]
pub(crate) fn into_api_agent(value: store::AgentMetrics) -> api::AgentMetrics {
    api::AgentMetrics {
        agent_id: value.agent_id,
        main_session_id: value.main_session_id,
        turns: value.turns,
        latencies: value
            .latencies
            .into_iter()
            .map(|(name, sample)| (name, into_api_latency(&sample)))
            .collect(),
        input_tokens: value.input_tokens,
        cached_input_tokens: value.cached_input_tokens,
        output_tokens: value.output_tokens,
        reasoning_tokens: value.reasoning_tokens,
        mean_output_tokens_per_second: value.mean_output_tokens_per_second,
        retries: value.retries,
        errors: value.errors,
        cost_micros: value.cost_micros,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Pin the full API JSON for one fully populated turn: every converted
    /// field appears, so a dropped or renamed field fails here instead of
    /// silently vanishing from the contract.
    // The pinned literal must stay adjacent to the fixture it pins;
    // splitting them into helpers would let the two drift apart unseen.
    #[expect(
        clippy::too_many_lines,
        reason = "the exact-JSON pin needs its full fixture and literal inline"
    )]
    #[test]
    fn metric_views_round_trip() {
        let stage = store::StageTiming {
            stage: "appended".into(),
            request_id: None,
            clock_id: "boot".into(),
            monotonic_ns: 1_000_000,
            unix_ns: 1_000_000,
        };
        assert_eq!(
            into_api_stage(stage.clone()),
            api::StageTiming {
                stage: stage.stage.clone(),
                request_id: stage.request_id.clone(),
                clock_id: stage.clock_id.clone(),
                monotonic_ns: stage.monotonic_ns,
                unix_ns: stage.unix_ns,
            }
        );
        let inference = store::InferenceMetric {
            request_id: "r".into(),
            provider: "fake".into(),
            model: "scripted".into(),
            output_tokens: 4,
            streamed: Some(false),
            ..store::InferenceMetric::default()
        };
        let converted = into_api_inference(inference.clone());
        assert_eq!(converted.provider, inference.provider);
        assert_eq!(converted.model, inference.model);
        assert_eq!(converted.output_tokens, inference.output_tokens);
        assert_eq!(converted.streamed, inference.streamed);
        assert_eq!(converted.request_id, inference.request_id);
        let tool = store::ToolMetric {
            request_id: "c".into(),
            name: "bash".into(),
            ..store::ToolMetric::default()
        };
        let converted_tool = into_api_tool(tool.clone());
        assert_eq!(converted_tool.name, tool.name);
        assert_eq!(converted_tool.request_id, tool.request_id);
        let computer = store::ComputerMetric {
            chunks_fetched: 7,
            ..store::ComputerMetric::default()
        };
        let converted_computer = into_api_computer(&computer);
        assert_eq!(converted_computer.chunks_fetched, computer.chunks_fetched);
        assert_eq!(converted_computer.bytes_fetched, computer.bytes_fetched);
        let turn = store::TurnMetrics {
            session_id: "s".into(),
            turn_id: "t".into(),
            stages: vec![store::StageTiming {
                stage: "appended".into(),
                request_id: None,
                clock_id: "boot".into(),
                monotonic_ns: 1_000_000,
                unix_ns: 1_000_000,
            }],
            inference: vec![store::InferenceMetric {
                request_id: "r".into(),
                provider: "fake".into(),
                model: "scripted".into(),
                input_tokens: 10,
                cached_input_tokens: 2,
                output_tokens: 4,
                reasoning_tokens: 1,
                cost_micros: 8,
                time_to_first_token_ms: Some(1.5),
                streaming_duration_ms: Some(2.5),
                request_duration_ms: Some(3.5),
                streamed: Some(true),
                output_tokens_per_second: Some(4.5),
                retries: 1,
                rate_limit_waits: 2,
                gateway_waits: 3,
                provider_failures: 4,
                error: Some("boom".into()),
            }],
            tools: vec![store::ToolMetric {
                request_id: "c".into(),
                name: "bash".into(),
                dispatched_ns: Some(1),
                started_ns: Some(2),
                completed_ns: Some(3),
                exit_status: Some(0),
                output_bytes: Some(9),
                queue_ms: Some(0.5),
                process_wall_ms: Some(1.5),
            }],
            computer: Some(store::ComputerMetric {
                placement_ms: Some(2.5),
                cold: Some(true),
                chunks_fetched: 7,
                bytes_fetched: 8,
                fetch_p50_ms: Some(3.5),
                fetch_p95_ms: Some(4.5),
                first_tool_chunks_fetched: Some(5),
                first_tool_bytes_fetched: Some(6),
                first_tool_fetch_p50_ms: Some(7.5),
                first_tool_fetch_p95_ms: Some(8.5),
            }),
            append_to_first_token_ms: Some(9.5),
            inference_duration_ms: Some(10.5),
            append_to_idle_ms: Some(11.5),
            error: Some("turn failed".into()),
            dropped_stages: 1,
            dropped_inference: 2,
            dropped_tools: 3,
        };
        let converted_turn = into_api_turn(turn.clone());
        assert_eq!(converted_turn.turn_id, turn.turn_id);
        assert_eq!(converted_turn.session_id, turn.session_id);
        assert_eq!(converted_turn.stages.len(), 1);
        assert_eq!(converted_turn.inference.len(), 1);
        assert_eq!(converted_turn.tools.len(), 1);
        assert!(converted_turn.computer.is_some());
        assert_eq!(converted_turn.dropped_stages, turn.dropped_stages);
        assert_eq!(converted_turn.dropped_inference, turn.dropped_inference);
        assert_eq!(converted_turn.dropped_tools, turn.dropped_tools);
        let latency = store::LatencyPercentiles {
            p50_ms: 1.0,
            p95_ms: 2.0,
        };
        assert!((into_api_latency(&latency).p50_ms - latency.p50_ms).abs() < f64::EPSILON);
        let agent = store::AgentMetrics {
            agent_id: "a".into(),
            main_session_id: Some("s".into()),
            turns: 7,
            latencies: std::collections::BTreeMap::from([(
                "append_to_idle".into(),
                store::LatencyPercentiles {
                    p50_ms: 1.0,
                    p95_ms: 2.0,
                },
            )]),
            input_tokens: 10,
            cached_input_tokens: 2,
            output_tokens: 4,
            reasoning_tokens: 1,
            mean_output_tokens_per_second: Some(3.5),
            retries: 5,
            errors: 6,
            cost_micros: 8,
        };
        let converted_agent = into_api_agent(agent.clone());
        assert_eq!(converted_agent.agent_id, agent.agent_id);
        assert_eq!(converted_agent.latencies.len(), 1);
        assert_eq!(converted_agent.turns, agent.turns);
    }
}
