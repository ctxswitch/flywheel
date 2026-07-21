use clap::Parser;
use flywheel::{
    Flywheel,
    cli::{Cli, Command},
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::MakeWriter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        // Cacheprog speaks the GOCACHEPROG protocol on stdout. Its diagnostics must
        // stay on stderr so a log record cannot corrupt the protocol stream.
        Command::Cacheprog(_) => init_logging(std::io::stderr, cli.debug),
        Command::Serve(_) | Command::Agent(_) => init_logging(std::io::stdout, cli.debug),
    }
    match cli.command {
        Command::Serve(arguments) => serve(*arguments).await,
        Command::Cacheprog(arguments) => flywheel::cacheprog::run(arguments).await,
        Command::Agent(arguments) => flywheel::agent::run(arguments).await,
    }
}

fn init_logging<W>(writer: W, debug: bool)
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    let mut filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing::Level::INFO.into())
        .from_env_lossy();
    if debug {
        filter = filter
            .add_directive("flywheel=debug".parse().expect("valid debug directive"))
            .add_directive(
                "tower_http=debug"
                    .parse()
                    .expect("valid HTTP debug directive"),
            );
    }
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .flatten_event(true)
        .with_current_span(true)
        .with_span_list(false)
        .with_ansi(false)
        .with_writer(writer)
        .init();
}

async fn serve(arguments: flywheel::cli::ServeArgs) -> anyhow::Result<()> {
    let startup = Instant::now();
    let listen = arguments.listen;
    let data_dir = arguments.data_dir.clone();
    let foreground_concurrency = arguments.foreground_concurrency;
    let flywheel = Arc::new(Flywheel::open(arguments.config()).await?);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let cancellation = CancellationToken::new();

    let maintenance = tokio::spawn(maintenance_worker(
        Arc::clone(&flywheel),
        cancellation.clone(),
    ));
    tracing::info!(
        component = "server",
        version = env!("CARGO_PKG_VERSION"),
        %listen,
        data_dir = %data_dir.display(),
        foreground_concurrency,
        startup_ms = startup.elapsed().as_millis() as u64,
        "Flywheel is ready"
    );
    axum::serve(listener, flywheel.router())
        .with_graceful_shutdown({
            let cancellation = cancellation.clone();
            async move {
                if let Err(error) = tokio::signal::ctrl_c().await {
                    tracing::error!(%error, "shutdown signal failed");
                }
                tracing::info!(component = "server", "shutdown requested");
                cancellation.cancel();
            }
        })
        .await?;
    cancellation.cancel();
    let _ = maintenance.await;
    let _ = flywheel.run_maintenance_once().await;
    tracing::info!(component = "server", "shutdown complete");
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
        if reclaimed > 0 {
            tracing::info!(reclaimed, "maintenance pass complete");
        }
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
