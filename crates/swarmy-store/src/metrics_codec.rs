//! Stored turn-metric rows: the `FoundationDB` envelope and its fixed bytes.
use serde::{Deserialize, Serialize};

use crate::metrics_model::ComputerMetric;
use crate::{Result, StoreError, decode};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredTurnSummaryCurrent {
    pub(crate) session_id: String,
    pub(crate) turn_id: String,
    pub(crate) submitted_ns: Option<i64>,
    pub(crate) appended_ns: Option<i64>,
    pub(crate) first_token_ns: Option<i64>,
    pub(crate) first_tool_ns: Option<i64>,
    pub(crate) idle_ns: Option<i64>,
    pub(crate) inference_started_ns: Option<i64>,
    pub(crate) inference_finished_ns: Option<i64>,
    pub(crate) computer: Option<ComputerMetric>,
    pub(crate) append_to_first_token_ms: Option<f64>,
    pub(crate) inference_duration_ms: Option<f64>,
    pub(crate) append_to_idle_ms: Option<f64>,
    pub(crate) error: Option<String>,
    pub(crate) input_tokens: u64,
    pub(crate) cached_input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) reasoning_tokens: u64,
    pub(crate) cost_micros: u64,
    pub(crate) retries: u64,
    pub(crate) rate_limit_waits: u64,
    pub(crate) gateway_waits: u64,
    pub(crate) provider_failures: u64,
    pub(crate) inference_errors: u64,
    pub(crate) throughput_sum: f64,
    pub(crate) throughput_count: u64,
    /// Paging counters. New turns store zero until a response is paginated.
    pub(crate) dropped_stages: u64,
    pub(crate) dropped_inference: u64,
    pub(crate) dropped_tools: u64,
}

/// Frozen inference fields for one current request row. This mirrors
/// [`InferenceMetric`] at the current layout revision: postcard is positional, so
/// the API type cannot be embedded directly (the next field added to the API
/// type would make every stored row undecodable). Convert to and from the API
/// type with the helpers below. Changing this positional layout requires a
/// new fixed-byte fixture and a one-way break.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredInferenceMetricCurrent {
    pub(crate) request_id: String,
    pub(crate) provider: String,
    pub(crate) model: String,
    pub(crate) input_tokens: u64,
    pub(crate) cached_input_tokens: u64,
    pub(crate) output_tokens: u64,
    pub(crate) reasoning_tokens: u64,
    pub(crate) cost_micros: u64,
    pub(crate) time_to_first_token_ms: Option<f64>,
    pub(crate) streaming_duration_ms: Option<f64>,
    pub(crate) request_duration_ms: Option<f64>,
    pub(crate) streamed: Option<bool>,
    pub(crate) output_tokens_per_second: Option<f64>,
    pub(crate) retries: u32,
    pub(crate) rate_limit_waits: u32,
    pub(crate) gateway_waits: u32,
    pub(crate) provider_failures: u32,
    pub(crate) error: Option<String>,
}

/// Frozen tool fields for one current tool row. Mirrors [`ToolMetric`] at the
/// current layout revision for the same positional-encoding reason as
/// [`StoredInferenceMetricCurrent`]; future stored fields go last through
/// a new fixed-byte fixture and a one-way break.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredToolMetricCurrent {
    pub(crate) request_id: String,
    pub(crate) name: String,
    pub(crate) dispatched_ns: Option<i64>,
    pub(crate) started_ns: Option<i64>,
    pub(crate) completed_ns: Option<i64>,
    pub(crate) exit_status: Option<i32>,
    pub(crate) output_bytes: Option<u64>,
    pub(crate) queue_ms: Option<f64>,
    pub(crate) process_wall_ms: Option<f64>,
}

/// One inference request row. Token counts and the streamed flag arrive with
/// the terminal patch; the stage path fills the wall-clock anchors that
/// throughput derives from.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredTurnInferenceCurrent {
    pub(crate) metric: StoredInferenceMetricCurrent,
    pub(crate) started_ns: Option<i64>,
    pub(crate) first_token_ns: Option<i64>,
    pub(crate) finished_ns: Option<i64>,
}

/// Stored summary and detail rows. Each keyspace accepts only its own variant.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum StoredTurnMetrics {
    Summary(Box<StoredTurnSummaryCurrent>),
    Inference(StoredTurnInferenceCurrent),
    Tool(StoredToolMetricCurrent),
}

pub(crate) fn decode_summary(bytes: &[u8]) -> Result<StoredTurnSummaryCurrent> {
    match decode::<StoredTurnMetrics>(bytes)? {
        StoredTurnMetrics::Summary(summary) => Ok(*summary),
        StoredTurnMetrics::Inference(_) | StoredTurnMetrics::Tool(_) => {
            Err(StoreError::Storage(crate::StorageError::Corrupt))
        }
    }
}

pub(crate) fn decode_inference(bytes: &[u8]) -> Result<StoredTurnInferenceCurrent> {
    match decode::<StoredTurnMetrics>(bytes)? {
        StoredTurnMetrics::Inference(row) => Ok(row),
        StoredTurnMetrics::Summary(_) | StoredTurnMetrics::Tool(_) => {
            Err(StoreError::Storage(crate::StorageError::Corrupt))
        }
    }
}

pub(crate) fn decode_tool(bytes: &[u8]) -> Result<StoredToolMetricCurrent> {
    match decode::<StoredTurnMetrics>(bytes)? {
        StoredTurnMetrics::Tool(row) => Ok(row),
        StoredTurnMetrics::Summary(_) | StoredTurnMetrics::Inference(_) => {
            Err(StoreError::Storage(crate::StorageError::Corrupt))
        }
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    use swarmy_core::decode;
    fn hex_to_bytes(hex: &str) -> Vec<u8> {
        let hex: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    /// Checked-in current summary bytes. Generated with
    /// `swarmy_core::encode(&StoredTurnMetrics::Summary(fixture_summary()))`;
    /// decoding them pins the unbounded layout.
    const CURRENT_SUMMARY_HEX: &str = "010001730174000180897a00000180b6dc050000000000000000000000000000000000000000000000000000000000";
    /// Checked-in current inference-row bytes for a single-chunk request.
    const CURRENT_INFERENCE_HEX: &str = "010101720466616b6508736372697074656400000400000000000100000000000000018092f401000180ade204";
    /// Checked-in current tool-row bytes.
    const CURRENT_TOOL_HEX: &str = "01020163046261736800000000000000";

    fn fixture_summary() -> StoredTurnSummaryCurrent {
        StoredTurnSummaryCurrent {
            session_id: "s".into(),
            turn_id: "t".into(),
            appended_ns: Some(1_000_000),
            idle_ns: Some(6_000_000),
            ..StoredTurnSummaryCurrent::default()
        }
    }

    fn fixture_inference() -> StoredTurnInferenceCurrent {
        StoredTurnInferenceCurrent {
            metric: StoredInferenceMetricCurrent {
                request_id: "r".into(),
                provider: "fake".into(),
                model: "scripted".into(),
                output_tokens: 4,
                streamed: Some(false),
                ..StoredInferenceMetricCurrent::default()
            },
            started_ns: Some(2_000_000),
            finished_ns: Some(5_000_000),
            ..StoredTurnInferenceCurrent::default()
        }
    }

    fn fixture_tool() -> StoredToolMetricCurrent {
        StoredToolMetricCurrent {
            request_id: "c".into(),
            name: "bash".into(),
            ..StoredToolMetricCurrent::default()
        }
    }

    #[test]
    fn detail_row_at_summary_key_is_corrupt() {
        let bytes = swarmy_core::encode(&StoredTurnMetrics::Tool(fixture_tool())).unwrap();
        assert!(matches!(
            decode_summary(&bytes),
            Err(StoreError::Storage(crate::StorageError::Corrupt))
        ));
    }
    #[test]
    fn versioned_envelope_decodes_checked_in_v2_bytes() {
        let summary = decode::<StoredTurnMetrics>(&hex_to_bytes(CURRENT_SUMMARY_HEX)).unwrap();
        let StoredTurnMetrics::Summary(decoded) = summary else {
            panic!("expected currentSummary");
        };
        assert_eq!(*decoded, fixture_summary());
        let inference = decode::<StoredTurnMetrics>(&hex_to_bytes(CURRENT_INFERENCE_HEX)).unwrap();
        let StoredTurnMetrics::Inference(row) = inference else {
            panic!("expected currentInference");
        };
        assert_eq!(row, fixture_inference());
        assert_eq!(row.metric.streamed, Some(false));
        let tool = decode::<StoredTurnMetrics>(&hex_to_bytes(CURRENT_TOOL_HEX)).unwrap();
        let StoredTurnMetrics::Tool(row) = tool else {
            panic!("expected currentTool");
        };
        assert_eq!(row, fixture_tool());
    }
    #[test]
    fn v2_summary_and_rows_round_trip() {
        let summary = StoredTurnSummaryCurrent {
            session_id: "s".into(),
            turn_id: "t".into(),
            appended_ns: Some(1_000_000),
            idle_ns: Some(6_000_000),
            ..StoredTurnSummaryCurrent::default()
        };
        for value in [
            StoredTurnMetrics::Summary(Box::new(summary)),
            StoredTurnMetrics::Inference(StoredTurnInferenceCurrent {
                metric: StoredInferenceMetricCurrent {
                    request_id: "r".into(),
                    ..StoredInferenceMetricCurrent::default()
                },
                started_ns: Some(2_000_000),
                finished_ns: Some(5_000_000),
                ..StoredTurnInferenceCurrent::default()
            }),
            StoredTurnMetrics::Tool(StoredToolMetricCurrent {
                request_id: "c".into(),
                name: "bash".into(),
                ..StoredToolMetricCurrent::default()
            }),
        ] {
            let bytes = swarmy_core::encode(&value).unwrap();
            let decoded: StoredTurnMetrics = swarmy_core::decode(&bytes).unwrap();
            assert_eq!(decoded, value);
        }
    }
}
