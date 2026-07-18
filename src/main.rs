use clap::Parser;
use flywheel::{
    Flywheel,
    cli::{Cli, Command},
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Logs must stay off stdout: the cacheprog subcommand speaks the GOCACHEPROG
    // protocol there, and a stray log line would corrupt it.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Command::Serve(arguments) => serve(*arguments).await,
        Command::Cacheprog(arguments) => flywheel::cacheprog::run(arguments).await,
        Command::Agent(arguments) => flywheel::agent::run(arguments).await,
    }
}

async fn serve(arguments: flywheel::cli::ServeArgs) -> anyhow::Result<()> {
    let listen = arguments.listen;
    let flywheel = Arc::new(Flywheel::open(arguments.config()).await?);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let cancellation = CancellationToken::new();

    let maintenance = tokio::spawn(maintenance_worker(
        Arc::clone(&flywheel),
        cancellation.clone(),
    ));
    tracing::info!(%listen, "Flywheel is ready");
    axum::serve(listener, flywheel.router())
        .with_graceful_shutdown({
            let cancellation = cancellation.clone();
            async move {
                if let Err(error) = tokio::signal::ctrl_c().await {
                    tracing::error!(%error, "shutdown signal failed");
                }
                cancellation.cancel();
            }
        })
        .await?;
    cancellation.cancel();
    let _ = maintenance.await;
    let _ = flywheel.run_maintenance_once().await;
    Ok(())
}

async fn maintenance_worker(flywheel: Arc<Flywheel>, cancellation: CancellationToken) {
    loop {
        tokio::select! {
            () = cancellation.cancelled() => break,
            () = tokio::time::sleep(Duration::from_secs(30)) => {
                run_maintenance_burst(&flywheel, &cancellation).await;
            }
        }
    }
}

/// Runs one maintenance pass, then — while the controller stays in Reclaiming — keeps
/// running bounded passes back-to-back with a cooperative yield between them until the
/// high watermark is restored, so a backlog is not left in bypass for a full interval.
/// A pass that reclaims nothing backs off briefly to avoid a busy spin when nothing is
/// currently evictable.
async fn run_maintenance_burst(flywheel: &Arc<Flywheel>, cancellation: &CancellationToken) {
    loop {
        let reclaimed = match flywheel.run_maintenance_once().await {
            Ok(reclaimed) => reclaimed,
            Err(error) => {
                tracing::warn!(%error, "maintenance pass failed");
                return;
            }
        };
        if !flywheel.is_reclaiming() || cancellation.is_cancelled() {
            return;
        }
        if reclaimed == 0 {
            // Still under pressure but nothing was evictable this pass; wait a short
            // interval rather than spinning against an unchanging catalog.
            tokio::select! {
                () = cancellation.cancelled() => return,
                () = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        } else {
            tokio::task::yield_now().await;
        }
    }
}
