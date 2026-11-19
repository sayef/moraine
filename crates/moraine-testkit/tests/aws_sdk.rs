//! moraine's `aws-sdk` feature against the S3-compatible server (D29): a database opened with
//! only a bucket, an endpoint and a profile name takes its keys from the profile through the AWS
//! SDK's credential chain, and a profile that does not exist fails cleanly.
//!
//! Runs with `--features aws-sdk` when `source <(spike/s3-server.sh env)` has set the server's
//! variables; otherwise it returns at once. It changes the process environment, so it is the
//! only test in its binary.
#![cfg(feature = "aws-sdk")]

use std::collections::HashMap;

use moraine::{Database, TableOptions};
use moraine_testkit::{SimRng, datagen};

#[tokio::test]
async fn credentials_from_a_profile() {
    let Ok(endpoint) = std::env::var("MORAINE_SPIKE_S3_ENDPOINT") else {
        return;
    };
    let bucket =
        std::env::var("MORAINE_SPIKE_S3_BUCKET").unwrap_or_else(|_| "moraine-spike".into());
    let key = std::env::var("AWS_ACCESS_KEY_ID").expect("server key");
    let secret = std::env::var("AWS_SECRET_ACCESS_KEY").expect("server secret");
    let dir = tempfile::tempdir().unwrap();
    let credentials = dir.path().join("credentials");
    let config = dir.path().join("config");
    std::fs::write(
        &credentials,
        format!("[moraine-test]\naws_access_key_id = {key}\naws_secret_access_key = {secret}\n"),
    )
    .unwrap();
    std::fs::write(&config, "[profile moraine-test]\nregion = us-east-1\n").unwrap();
    // SAFETY: the only test in this binary, before any thread reads the environment.
    unsafe {
        for k in [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AWS_PROFILE",
        ] {
            std::env::remove_var(k);
        }
        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &credentials);
        std::env::set_var("AWS_CONFIG_FILE", &config);
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
    }
    let prefix = format!("aws-sdk-{}", std::process::id());
    let options = |profile: &str| {
        HashMap::from([
            ("aws_endpoint".to_string(), endpoint.clone()),
            ("aws_allow_http".to_string(), "true".to_string()),
            ("aws_profile".to_string(), profile.to_string()),
        ])
    };
    let location = format!("s3://{bucket}/{prefix}");
    let db = Database::builder(&location)
        .storage_options(options("moraine-test"))
        .open()
        .await
        .unwrap();
    let t = db
        .create_table("events", datagen::row_schema(), TableOptions::default())
        .await
        .unwrap();
    t.append(vec![datagen::rows(&mut SimRng::new(1), "a", 5, None, 2)])
        .await
        .unwrap();
    assert_eq!(t.scan().count().await.unwrap(), 5);

    // A profile that does not exist: opening works, the first request fails without panicking.
    let db = Database::builder(&location)
        .storage_options(options("no-such-profile"))
        .open()
        .await
        .unwrap();
    assert!(db.open_table("events").await.is_err());
}
