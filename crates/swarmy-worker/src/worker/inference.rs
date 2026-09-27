use super::*;

pub(super) enum StepFailure<'a> {
    Publication(&'a swarmy_bus::Error),
    Unserved { provider: &'a str, retryable: bool },
}

impl Worker {
    pub(super) async fn prepare_request(
        &self,
        session: &SessionRecord,
        request: &mut swarmy_llm::Request,
        preceding: &mut Vec<Event>,
    ) -> Result<ResolvedAttempt> {
        // Resolve on every inference so existing sessions see later agent updates.
        // A summary request keeps its own prompt; every other request gets the agent's
        // prompt override first and then the memory directory and contents appended.
        // The agent and its route resolve in one transaction so an inference
        // costs no more store transactions than before routes.
        let summarizing = request.system_prompt == swarmy_harness::SUMMARY_PROMPT;
        let mut defaults = swarmy_core::ResolvedSelection {
            provider: self.config.provider.clone(),
            model: request.settings.model.clone(),
            effort: request
                .settings
                .reasoning_effort
                .unwrap_or(swarmy_core::ReasoningEffort::None),
        };
        if let swarmy_core::SessionKind::Named { agent_id } = session.kind {
            let (record, snapshot) = self
                .store
                .agent_and_route_snapshot(
                    agent_id,
                    session.route.as_deref(),
                    session.inference.provider.as_deref(),
                    self.config.default_route.as_deref(),
                    &self.config.provider,
                    Timestamp::now(),
                )
                .await?;
            if let Some(record) = record.as_ref() {
                defaults = record.inference().resolve(&defaults);
                if let Some(prompt) = record.system_prompt.clone()
                    && !summarizing
                {
                    request.system_prompt = prompt;
                }
            }
            warn_on_route_fallback(session, snapshot.name.as_deref(), &snapshot.skipped);
            let selection = session.inference.resolve(&defaults);
            return self
                .finish_prepare(
                    session,
                    request,
                    preceding,
                    selection,
                    snapshot,
                    summarizing,
                )
                .await;
        }
        let selection = session.inference.resolve(&defaults);
        let snapshot = self.route_snapshot(session).await?;
        warn_on_route_fallback(session, snapshot.name.as_deref(), &snapshot.skipped);
        self.finish_prepare(
            session,
            request,
            preceding,
            selection,
            snapshot,
            summarizing,
        )
        .await
    }

    /// Build the request against the resolved route step: model override,
    /// reasoning downgrade, and modality handling. Shared by named sessions,
    /// which resolve the agent and route together, and ephemeral sessions,
    /// which resolve the implicit chain.
    pub(super) async fn finish_prepare(
        &self,
        session: &SessionRecord,
        request: &mut swarmy_llm::Request,
        preceding: &mut Vec<Event>,
        selection: swarmy_core::ResolvedSelection,
        snapshot: swarmy_store::RouteSnapshot,
        summarizing: bool,
    ) -> Result<ResolvedAttempt> {
        if !summarizing {
            apply_display_tools(request, self.session_display(session).await?);
        }
        let index = snapshot.pick_or_earliest(session.route_step);
        let step = snapshot.steps.get(index).context("empty route snapshot")?;
        let model_id = step
            .model
            .clone()
            .unwrap_or_else(|| selection.model.clone());
        request.settings.model.clone_from(&model_id);
        request.settings.reasoning_effort = Some(selection.effort);
        // Reasoning blocks replay only for the provider and model that
        // produced them; a failover step must not inherit another step's
        // signatures, so downgrade them here where the request is built.
        swarmy_llm::reasoning::downgrade_mismatched_reasoning(
            &mut request.messages,
            &step.provider,
            &model_id,
        );

        if let Some(model) = self.config.catalog.model(&step.provider, &model_id) {
            if !model
                .input_modalities
                .iter()
                .any(|modality| modality == "image")
            {
                omit_unsupported_images(request);
            }
            if model
                .input_modalities
                .iter()
                .any(|modality| modality == "image")
            {
                self.hydrate_images(request).await?;
            }
            let (effort, changed) = model.clamp_effort(selection.effort);
            request.settings.reasoning_effort = Some(effort);
            if changed && !self.has_effort_notice(session.session_id).await? {
                preceding.push(Event::MessageAppended {
                    seq: 0,
                    message: swarmy_core::Message {
                        id: MessageId::from_ulid(Ulid::generate()),
                        role: swarmy_core::MessageRole::System,
                        parts: vec![swarmy_core::Part::Text {
                            text: format!(
                                "Reasoning effort clamped from {} to {effort} for {}/{}",
                                selection.effort, step.provider, model_id
                            ),
                        }],
                    },
                });
            }
        }
        if !summarizing {
            request.system_prompt = request
                .system_prompt
                .replace("{memory_dir}", &self.config.memory_dir);
            if matches!(session.kind, swarmy_core::SessionKind::Named { .. }) {
                let memory = self.prompt_context(session.agent_id, false).await?;
                write!(
                    request.system_prompt,
                    "\n\nAgent memory ({}):\n{memory}",
                    self.config.memory_dir
                )?;
            }
            let instructions = self.prompt_context(session.agent_id, true).await?;
            if !instructions.is_empty() {
                write!(
                    request.system_prompt,
                    "\n\nRepository instructions (apply within each listed repository):\n{instructions}"
                )?;
            }
        }
        Ok(ResolvedAttempt {
            provider: step.provider.clone(),
            entry: step.label.clone(),
            route: snapshot.name.clone(),
            route_step: u32::try_from(index).unwrap_or(u32::MAX),
            snapshot,
        })
    }

    pub(super) async fn hydrate_images(&self, request: &mut swarmy_llm::Request) -> Result<()> {
        for message in &mut request.messages {
            for part in &mut message.parts {
                if let swarmy_core::Part::Image {
                    bytes,
                    object_key: Some(key),
                    ..
                } = part
                    && bytes.is_empty()
                {
                    *bytes = self.blobs.get(key).await?.to_vec();
                }
            }
        }
        let mut expanded = Vec::with_capacity(request.messages.len());
        for message in request.messages.drain(..) {
            let mut images = Vec::new();
            for part in &message.parts {
                if let swarmy_core::Part::ToolResult {
                    result: swarmy_core::ToolResult::Completed { metadata, .. },
                    ..
                } = part
                    && let (Some(key), Some(media_type)) = (
                        metadata
                            .get("image_object_key")
                            .and_then(serde_json::Value::as_str),
                        metadata
                            .get("image_media_type")
                            .and_then(serde_json::Value::as_str),
                    )
                {
                    images.push(swarmy_core::Part::Image {
                        media_type: media_type.to_owned(),
                        bytes: self.blobs.get(key).await?.to_vec(),
                        object_key: Some(key.to_owned()),
                        detail: None,
                    });
                }
            }
            expanded.push(message);
            if !images.is_empty() {
                expanded.push(swarmy_core::Message {
                    id: MessageId::from_ulid(Ulid::generate()),
                    role: swarmy_core::MessageRole::User,
                    parts: images,
                });
            }
        }
        request.messages = expanded;
        Ok(())
    }

    pub(super) async fn build_inference(
        &self,
        session: &mut SessionRecord,
        lease: &HeldLease,
        preceding: &[Event],
        mut request: swarmy_llm::Request,
    ) -> Result<()> {
        let mut preceding = preceding.to_vec();
        let attempt = self
            .prepare_request(session, &mut request, &mut preceding)
            .await?;
        // Persist the picked step with the request so a retryable failure
        // advances from the attempt that actually ran, not from a stale
        // position. A wrap to a recovered earlier step carries no range.
        let route = (attempt.route_step != session.route_step).then(|| {
            let mut reasons = attempt.snapshot.skipped.clone();
            if attempt.route_step > session.route_step {
                for skipped in session.route_step..attempt.route_step {
                    if let Some(step) = attempt
                        .snapshot
                        .steps
                        .get(usize::try_from(skipped).unwrap_or(usize::MAX))
                        && let Some(reason) = &step.reason
                    {
                        reasons.push(reason.clone());
                    }
                }
            }
            swarmy_store::SubmitRouteStep {
                step: attempt.route_step,
                reasons,
            }
        });
        for (event, seq) in preceding.iter_mut().zip(session.head_seq + 1..) {
            event.set_seq(seq);
        }
        let id = session.session_id;
        let step = session
            .head_seq
            .checked_add(u64::try_from(preceding.len())?)
            .and_then(|head| head.checked_add(1))
            .context("sequence overflow")?;
        let job = InferenceJob {
            provider: attempt.provider,
            entry: attempt.entry,
            route: attempt.route,
            route_step: attempt.route_step,
            session_id: id,
            step,
            request_id: RequestId::for_step(id, step),
            request,
        };
        self.kill("before_release");
        let event = {
            let mut token = lease.lock().await;
            let event = self
                .store
                .submit_inference(
                    session.head_seq,
                    token.as_ref().context("lease released")?,
                    &InflightRecord {
                        session_id: id,
                        seq: step,
                        provider: job.provider.clone(),
                        key_id: job.entry.clone().unwrap_or_default(),
                    },
                    &job,
                    Some(SubmitInferenceOptions {
                        request: Some(&job.request),
                        before: &preceding,
                        route,
                    }),
                )
                .await?;
            token.release();
            event
        };
        session.route_step = attempt.route_step;
        session.head_seq = event.seq();
        self.publish_events(id, &preceding).await?;
        self.publish_events(id, std::slice::from_ref(&event))
            .await?;
        self.kill("after_request_event");
        self.kill("after_release");
        if self.fail_unserved(&job).await? {
            return Ok(());
        }
        self.publish_inference(&job).await
    }

    pub(super) async fn submit(&self, job: &InferenceJob, lease: &HeldLease) -> Result<()> {
        {
            let token = lease.lock().await;
            self.store
                .put_inflight_leased(
                    job.request_id,
                    &InflightRecord {
                        session_id: job.session_id,
                        seq: job.step,
                        provider: self.job_provider(job).to_owned(),
                        key_id: job.entry.clone().unwrap_or_default(),
                    },
                    token.as_ref().context("lease released")?,
                    Timestamp::now(),
                )
                .await?;
        }
        self.kill("before_release");
        self.transition(job.session_id, lease, SessionState::WaitingInference)
            .await?;
        self.kill("after_release");
        if self.fail_unserved(job).await? {
            return Ok(());
        }
        self.publish_inference(job).await
    }

    pub(super) async fn publish_inference(&self, job: &InferenceJob) -> Result<()> {
        let published = self
            .bus
            .publish_work(
                &WorkQueue::Inference(SubjectToken::new(self.job_provider(job))?),
                &InferenceJobRef::from(job),
            )
            .await;
        if let Err(error) = published {
            if error.permanent_publish_failure() {
                self.fail_step(job, StepFailure::Publication(&error))
                    .await?;
            } else {
                return Err(error.into());
            }
        }
        Ok(())
    }

    pub(super) async fn fail_step(
        &self,
        job: &InferenceJob,
        kind: StepFailure<'_>,
    ) -> Result<Option<Event>> {
        let now = Timestamp::now();
        let claim = swarmy_store::InferenceClaim {
            session_id: job.session_id,
            request_id: job.request_id,
            owner: LeaseOwnerId::from_ulid(Ulid::generate()),
            expires_at: now.checked_add(std::time::Duration::from_secs(30))?,
        };
        if !self.store.start_inference(&claim, now).await? {
            return Ok(None);
        }
        let session = self
            .store
            .fetch_session(job.session_id)
            .await?
            .context("session missing")?;
        let (error, retryable, retry_at) = match kind {
            StepFailure::Publication(error) => (error.to_string(), false, None),
            StepFailure::Unserved {
                provider,
                retryable,
            } => (
                format!(
                    "no gateway serves provider {provider}; run swarmy auth set {provider} or start a gateway with it"
                ),
                retryable,
                retryable
                    .then(|| now.checked_add(self.config.gateway_wait))
                    .transpose()?,
            ),
        };
        let event = Event::InferenceFailed {
            seq: session
                .head_seq
                .checked_add(1)
                .context("sequence overflow")?,
            request_id: job.request_id,
            error,
            retryable,
            retry_at,
        };
        let committed = self
            .store
            .complete_inference(
                &swarmy_store::InferenceCompletion {
                    claim,
                    expected_head: session.head_seq,
                    event: event.clone(),
                    now,
                    entry: None,
                    entry_kind: None,
                    quota_remaining: std::collections::BTreeMap::new(),
                    quota_resets: std::collections::BTreeMap::new(),
                },
                &(),
            )
            .await?;
        if committed {
            self.publish_events(job.session_id, std::slice::from_ref(&event))
                .await?;
            Ok(Some(event))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn prompt_context(
        &self,
        agent: swarmy_core::AgentId,
        instructions: bool,
    ) -> Result<String> {
        let Some(placement) = self
            .store
            .get_by_agent(agent)
            .await?
            .filter(|placement| placement.expires_at > Timestamp::now())
        else {
            return Ok(String::new());
        };
        let request = swarmy_core::MemoryRequest {
            agent_id: agent,
            epoch: placement.epoch,
            directory: if instructions {
                "/home/agent/work".into()
            } else {
                self.config.memory_dir.clone()
            },
            max_bytes: if instructions {
                32_768
            } else {
                self.config.memory_max_bytes
            },
        };
        let reply = if instructions {
            self.bus
                .request_instructions(placement.node_id, &request)
                .await?
        } else {
            self.bus.request_memory(placement.node_id, &request).await?
        };
        reply.map_err(anyhow::Error::msg)
    }

    pub(super) fn job_provider<'a>(&'a self, job: &'a InferenceJob) -> &'a str {
        if job.provider.is_empty() {
            &self.config.provider
        } else {
            &job.provider
        }
    }

    pub(super) async fn has_effort_notice(&self, id: SessionId) -> Result<bool> {
        let mut after = 0;
        loop {
            let events = self.store.read_events(id, after, MAX_SCAN_LIMIT).await?;
            if events.is_empty() {
                return Ok(false);
            }
            for event in events {
                after = event.seq();
                if let Event::MessageAppended { message, .. } = event
                    && message.role == swarmy_core::MessageRole::System
                    && message.parts.iter().any(|part| matches!(part, swarmy_core::Part::Text { text } if text.starts_with("Reasoning effort clamped from "))) {
                    return Ok(true);
                }
            }
        }
    }
}
pub(super) fn apply_display_tools(request: &mut swarmy_llm::Request, display: bool) {
    if display {
        request.system_prompt.push_str("\n\n");
        request.system_prompt.push_str(swarmy_tools::DISPLAY_PROMPT);
    } else {
        request
            .tools
            .retain(|tool| !swarmy_tools::is_display_name(&tool.name));
    }
}

pub(super) fn omit_unsupported_images(request: &mut swarmy_llm::Request) {
    for message in &mut request.messages {
        for part in &mut message.parts {
            if matches!(part, swarmy_core::Part::Image { .. }) {
                *part = swarmy_core::Part::Text {
                    text: "[An image was omitted because this model does not accept images.]"
                        .into(),
                };
            } else if let swarmy_core::Part::ToolResult {
                result:
                    swarmy_core::ToolResult::Completed {
                        output, metadata, ..
                    },
                ..
            } = part
                && metadata.contains_key("image_object_key")
            {
                output
                    .push_str(" [An image was omitted because this model does not accept images.]");
                metadata.remove("image_object_key");
                metadata.remove("image_media_type");
            }
        }
    }
}
