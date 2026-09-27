use super::inference::StepFailure;
use super::*;

impl Worker {
    pub(super) async fn fail_unserved(&self, job: &InferenceJob) -> Result<bool> {
        let provider = self.job_provider(job);
        let retryable = self.config.catalog.provider(provider).is_some()
            && self
                .config
                .allowed_providers
                .as_ref()
                .is_none_or(|allowed| allowed.iter().any(|id| id == provider));
        // The scripted provider needs no credentials; the gateway task advertises real providers.
        if retryable && (provider == "fake" || self.store.gateway_serves(provider).await?) {
            return Ok(false);
        }
        if let Some(event) = self
            .fail_step(
                job,
                StepFailure::Unserved {
                    provider,
                    retryable,
                },
            )
            .await?
        {
            if let Some(turn) = job
                .request
                .messages
                .iter()
                .rev()
                .find(|message| message.role == swarmy_core::MessageRole::User)
                .map(|message| message.id)
            {
                self.store.observe_turn_metric(
                    job.session_id,
                    turn,
                    swarmy_store::MetricPatch::Wait {
                        request_id: job.request_id.to_string(),
                        kind: swarmy_store::WaitKind::MissingGateway,
                    },
                );
                if !retryable && let Event::InferenceFailed { error, .. } = event {
                    self.store.observe_turn_metric(
                        job.session_id,
                        turn,
                        swarmy_store::MetricPatch::Error(error),
                    );
                }
            }
        }
        Ok(true)
    }

    pub(super) async fn load_job(&self, id: RequestId) -> Result<InferenceJob> {
        let job: InferenceJob = self
            .store
            .get_inference_input(id)
            .await?
            .context("inference input missing")?;
        ensure!(
            job.request_id == id && RequestId::for_step(job.session_id, job.step) == id,
            "invalid stored inference job"
        );
        Ok(job)
    }

    pub async fn recovery_loop(&self) {
        let mut ticks = interval(self.config.recovery_interval);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            if let Err(error) = self.recover_tools().await {
                tracing::warn!(%error, "tool recovery scan failed");
            }
            if let Err(error) = self.recover().await {
                tracing::warn!(%error, "inference recovery scan failed");
            }
        }
    }

    pub(super) async fn recover(&self) -> Result<()> {
        let mut after = None;
        loop {
            let page = self.store.scan_inflight(after, MAX_SCAN_LIMIT).await?;
            if page.is_empty() {
                return Ok(());
            }
            for record in page {
                let request_id = RequestId::for_step(record.session_id, record.seq);
                after = Some(request_id);
                if self
                    .config
                    .partitions
                    .contains(&runnable_partition(record.session_id))
                    && let Err(error) = self.republish(&record, request_id).await
                {
                    tracing::warn!(%request_id, %error, "request recovery failed");
                }
            }
        }
    }

    pub(super) async fn republish(
        &self,
        record: &InflightRecord,
        request_id: RequestId,
    ) -> Result<()> {
        let session = self
            .store
            .fetch_session(record.session_id)
            .await?
            .context("session missing")?;
        if session.state == SessionState::WaitingInference {
            let job = self.load_job(request_id).await?;
            if self.fail_unserved(&job).await? {
                return Ok(());
            }
            self.store_missing_request(&job).await?;
            let published = self
                .bus
                .publish_work(
                    &WorkQueue::Inference(SubjectToken::new(&record.provider)?),
                    &InferenceJobRef::from(&job),
                )
                .await;
            if let Err(error) = published {
                if error.permanent_publish_failure() {
                    self.fail_step(&job, StepFailure::Publication(&error))
                        .await?;
                } else {
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }
}
impl Worker {
    /// Jobs published inline before requests were stored by reference have an
    /// input but no request row. Store it so the gateway can resolve the
    /// reference, unless the request already completed and was cleared.
    pub(super) async fn store_missing_request(&self, job: &InferenceJob) -> Result<()> {
        if self
            .store
            .get_inference_request::<swarmy_llm::Request>(job.request_id)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let completed = self
            .store
            .get_idempotency(job.request_id)
            .await?
            .is_some_and(|record| record.state == swarmy_core::IdempotencyState::Completed);
        if completed {
            return Ok(());
        }
        tracing::info!(request_id = %job.request_id, "storing request for inline job");
        self.store
            .put_inference_request(job.request_id, &job.request)
            .await?;
        Ok(())
    }
}
