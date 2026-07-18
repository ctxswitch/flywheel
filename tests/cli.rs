use clap::Parser;
use flywheel::cli::{Cli, Command};

#[test]
fn exposes_agent_command() {
    let agent = Cli::try_parse_from([
        "flywheel",
        "agent",
        "--srv",
        "_flywheel._tcp.flywheel-shards.cache.svc.cluster.local",
    ])
    .expect("agent command parses");
    let Command::Agent(arguments) = agent.command else {
        panic!("expected the agent command");
    };
    assert_eq!(arguments.listen.to_string(), "127.0.0.1:9080");
    assert_eq!(arguments.refresh_max, 30);
    assert_eq!(arguments.failure_limit, 1);
    assert_eq!(arguments.retry_timeout, 30);
    assert_eq!(arguments.connect_timeout, 5);
    assert_eq!(arguments.deadline, 60);

    assert!(
        Cli::try_parse_from(["flywheel", "agent"]).is_err(),
        "the SRV name is required"
    );
}

#[test]
fn exposes_serve_and_cacheprog_commands() {
    let serve = Cli::try_parse_from(["flywheel", "serve", "--data-dir", "/tmp/flywheel-test"])
        .expect("serve command parses");
    assert!(matches!(serve.command, Command::Serve(_)));

    let cacheprog = Cli::try_parse_from([
        "flywheel",
        "cacheprog",
        "--url",
        "http://127.0.0.1:9999/build-cache/http/",
    ])
    .expect("cacheprog command parses");
    assert!(matches!(cacheprog.command, Command::Cacheprog(_)));
}
