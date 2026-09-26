pub async fn run(dry_run: bool, grace_seconds: Option<u64>, json: bool) -> anyhow::Result<()> {
    let (client, endpoint) = crate::api_client::connect()?;
    if let Some(grace) = grace_seconds {
        anyhow::ensure!(grace > 0, "grace window must be positive");
    }
    let started = crate::api_client::call(
        &endpoint,
        client.start_gc_run(&swarmy_api_types::StartGcRun {
            idempotency_key: ulid::Ulid::generate().to_string(),
            dry_run,
            grace_seconds,
        }),
    )
    .await
    .map_err(|error| {
        if error.to_string().contains("gc_busy") {
            anyhow::anyhow!("another collection run is already in progress")
        } else {
            error
        }
    })?;
    // The sweep continues on the control plane; follow its durable record.
    // Collection scans the whole object namespace, so allow it time. A run
    // that loses its lease records the failure, which stops this loop early.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3600);
    let run = loop {
        let run = crate::api_client::call(&endpoint, client.gc_run(&started.run_id)).await?;
        if run.finished {
            break run;
        }
        if std::time::Instant::now() > deadline {
            anyhow::bail!("collection run {} is still in progress", run.run_id);
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    };
    if let Some(error) = run.error.as_deref() {
        anyhow::bail!("collection run {} failed: {error}", run.run_id);
    }
    if json {
        println!("{}", serde_json::to_string(&run)?);
    } else {
        println!(
            "Run {}: {} manifests, {} chunks scanned; {} candidates ({} bytes); {} deleted ({} bytes freed); {} ms{}",
            run.run_id,
            run.manifests,
            run.scanned,
            run.candidates,
            run.candidate_bytes,
            run.deleted,
            run.bytes_freed,
            run.duration_ms,
            if dry_run { " (dry run)" } else { "" }
        );
    }
    Ok(())
}
