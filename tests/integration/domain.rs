use flywheel::{
    artifact::{ArtifactId, Digest},
    cacheprog::session::{UsedEntry, compose_label, merge_manifest, parse_module_path},
    channel::{ChannelId, ChannelToken},
    manifest::{
        MANIFEST_MAX_AGE_SECONDS, MANIFEST_MAX_ENTRIES, MANIFEST_VERSION, Manifest, ManifestEntry,
    },
    reference::Reference,
};
use std::collections::HashMap;

#[test]
fn artifact_identity_accepts_only_canonical_sha256() {
    let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let id = ArtifactId::parse("sha256", hex).expect("valid identity");

    assert_eq!(id.algorithm(), "sha256");
    assert_eq!(id.digest().to_string(), hex);
    assert!(ArtifactId::parse("sha512", hex).is_err());
    assert!(ArtifactId::parse("sha256", &hex.to_uppercase()).is_err());
    assert!(Digest::parse("abcd").is_err());
}

#[test]
fn channel_ids_round_trip_as_canonical_ulids() {
    assert_eq!(ChannelId::DEFAULT.to_string(), "00000000000000000000000000");
    assert_eq!(ChannelId::default(), ChannelId::DEFAULT);
    let channel = ChannelId::new();
    let encoded = channel.to_string();

    assert_eq!(encoded.len(), 26);
    assert_eq!(encoded.parse::<ChannelId>().unwrap(), channel);
    assert!(encoded.to_lowercase().parse::<ChannelId>().is_err());
    for _ in 0..1024 {
        assert_ne!(ChannelId::new(), ChannelId::DEFAULT);
    }
}

#[test]
fn channel_tokens_verify_without_storing_the_secret() {
    let issued = ChannelToken::generate();
    let digest = issued.digest();

    assert!(digest.verify(issued.expose()));
    assert!(!digest.verify("flywheel_not-the-token"));
    assert!(!format!("{digest:?}").contains(issued.expose()));
}

#[test]
fn public_references_are_url_safe_and_bounded() {
    assert_eq!(
        Reference::parse("some-key_1.2~x").unwrap().as_str(),
        "some-key_1.2~x"
    );
    assert!(Reference::parse("").is_err());
    assert!(Reference::parse("has/a/slash").is_err());
    assert!(Reference::parse("x".repeat(513)).is_err());
}

fn manifest_entry(output: &str, last_seen: u64) -> ManifestEntry {
    ManifestEntry {
        output: output.to_owned(),
        size: 1,
        last_seen,
    }
}

#[test]
fn manifest_merge_unions_usage_and_ages_out_stale_entries() {
    let now = MANIFEST_MAX_AGE_SECONDS * 3;
    let stored = Manifest {
        version: MANIFEST_VERSION,
        entries: HashMap::from([
            ("kept".to_owned(), manifest_entry("aa", now - 60)),
            ("updated".to_owned(), manifest_entry("bb", now - 60)),
            (
                "stale".to_owned(),
                manifest_entry("cc", now - MANIFEST_MAX_AGE_SECONDS - 1),
            ),
        ]),
    };
    let used = HashMap::from([
        (
            "updated".to_owned(),
            UsedEntry {
                output: "dd".to_owned(),
                size: 7,
            },
        ),
        (
            "new".to_owned(),
            UsedEntry {
                output: "ee".to_owned(),
                size: 9,
            },
        ),
    ]);

    let merged = merge_manifest(Some(stored), &used, now);

    assert_eq!(merged.version, MANIFEST_VERSION);
    assert_eq!(merged.entries.len(), 3);
    assert_eq!(merged.entries["kept"], manifest_entry("aa", now - 60));
    assert_eq!(merged.entries["updated"].output, "dd");
    assert_eq!(merged.entries["updated"].last_seen, now);
    assert_eq!(merged.entries["new"].size, 9);
    assert!(!merged.entries.contains_key("stale"));
}

#[test]
fn manifest_merge_caps_size_by_evicting_the_oldest() {
    let now = MANIFEST_MAX_AGE_SECONDS;
    let stored = Manifest {
        version: MANIFEST_VERSION,
        entries: (0..MANIFEST_MAX_ENTRIES + 5)
            .map(|index| {
                (
                    format!("action-{index}"),
                    manifest_entry("aa", index as u64),
                )
            })
            .collect(),
    };

    let merged = merge_manifest(Some(stored), &HashMap::new(), now);

    assert_eq!(merged.entries.len(), MANIFEST_MAX_ENTRIES);
    for index in 0..5 {
        assert!(!merged.entries.contains_key(&format!("action-{index}")));
    }
}

#[test]
fn session_labels_prefer_the_module_path_and_platform() {
    assert_eq!(
        parse_module_path("// a comment\nmodule example.com/widget\n\ngo 1.24\n"),
        Some("example.com/widget".to_owned())
    );
    assert_eq!(
        parse_module_path("module \"quoted/path\" // trailing\n"),
        Some("quoted/path".to_owned())
    );
    assert_eq!(parse_module_path("go 1.24\n"), None);

    let cwd = std::path::Path::new("/work/repo");
    assert_eq!(
        compose_label(Some("example.com/widget"), "linux", "arm64", cwd),
        "example.com/widget linux/arm64"
    );
    assert_eq!(compose_label(None, "linux", "arm64", cwd), "/work/repo");
}
