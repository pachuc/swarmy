//! Convert persistence-layer metric shapes into API response shapes.
//!
//! The store owns its metric structs and returns them; this module maps them
//! field for field onto the versioned JSON contract. The shapes stay
//! identical so stored rows keep decoding, and the conversion stays here so
//! the store never compiles the OpenAPI tooling.

use swarmy_api_types as api;
use swarmy_store as store;

#[must_use]
pub fn into_api_stage(value: store::StageTiming) -> api::StageTiming {
    api::StageTiming {
        stage: value.stage,
        request_id: value.request_id,
        clock_id: value.clock_id,
        monotonic_ns: value.monotonic_ns,
        unix_ns: value.unix_ns,
    }
}

#[must_use]
pub fn from_api_stage(value: api::StageTiming) -> store::StageTiming {
    store::StageTiming {
        stage: value.stage,
        request_id: value.request_id,
        clock_id: value.clock_id,
        monotonic_ns: value.monotonic_ns,
        unix_ns: value.unix_ns,
    }
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn into_api_inference(value: store::InferenceMetric) -> api::InferenceMetric {
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
#[allow(clippy::too_many_lines)]
pub fn from_api_inference(value: api::InferenceMetric) -> store::InferenceMetric {
    store::InferenceMetric {
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
pub fn into_api_tool(value: store::ToolMetric) -> api::ToolMetric {
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
pub fn from_api_tool(value: api::ToolMetric) -> store::ToolMetric {
    store::ToolMetric {
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
pub fn into_api_computer(value: store::ComputerMetric) -> api::ComputerMetric {
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
pub fn from_api_computer(value: api::ComputerMetric) -> store::ComputerMetric {
    store::ComputerMetric {
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
pub fn into_api_turn(value: store::TurnMetrics) -> api::TurnMetrics {
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
        computer: value.computer.map(into_api_computer),
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
pub fn from_api_turn(value: api::TurnMetrics) -> store::TurnMetrics {
    store::TurnMetrics {
        session_id: value.session_id,
        turn_id: value.turn_id,
        stages: value.stages.into_iter().map(from_api_stage).collect(),
        inference: value
            .inference
            .into_iter()
            .map(from_api_inference)
            .collect(),
        tools: value.tools.into_iter().map(from_api_tool).collect(),
        computer: value.computer.map(from_api_computer),
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
pub fn into_api_latency(value: store::LatencyPercentiles) -> api::LatencyPercentiles {
    api::LatencyPercentiles {
        p50_ms: value.p50_ms,
        p95_ms: value.p95_ms,
    }
}

#[must_use]
pub fn from_api_latency(value: api::LatencyPercentiles) -> store::LatencyPercentiles {
    store::LatencyPercentiles {
        p50_ms: value.p50_ms,
        p95_ms: value.p95_ms,
    }
}

#[must_use]
pub fn into_api_agent(value: store::AgentMetrics) -> api::AgentMetrics {
    api::AgentMetrics {
        agent_id: value.agent_id,
        main_session_id: value.main_session_id,
        turns: value.turns,
        latencies: value
            .latencies
            .into_iter()
            .map(|(name, sample)| (name, into_api_latency(sample)))
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

#[must_use]
pub fn from_api_agent(value: api::AgentMetrics) -> store::AgentMetrics {
    store::AgentMetrics {
        agent_id: value.agent_id,
        main_session_id: value.main_session_id,
        turns: value.turns,
        latencies: value
            .latencies
            .into_iter()
            .map(|(name, sample)| (name, from_api_latency(sample)))
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
