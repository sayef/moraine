//! The AWS SDK's credential chain and bucket region resolution for `s3://` databases (cargo
//! feature `aws-sdk`, D29).
//!
//! object_store reads static keys, web identity and instance credentials from the environment,
//! but not profiles, SSO sessions or `credential_process`, and it assumes `us-east-1` when no
//! region is set. With this feature, a database opened with only a bucket (and optionally a
//! profile) gets:
//!
//! - **credentials** from the SDK's default chain (environment, shared config and credentials
//!   files with the chosen profile, SSO, `credential_process`, web identity, container and
//!   instance metadata), cached until five minutes before they expire, unless static keys or
//!   unsigned requests were configured;
//! - **a region**: the bucket's own, from an unauthenticated `HeadBucket` against AWS, falling
//!   back to the region the SDK resolves (environment or profile), unless a region or a custom
//!   endpoint was configured. A custom endpoint (an S3-compatible server) keeps whatever region
//!   was given.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential};
use object_store::{ClientOptions, CredentialProvider};

/// Credentials are renewed this long before they expire.
const RENEW_BEFORE: Duration = Duration::from_secs(300);

/// What the SDK has to supply for a builder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Needs {
    pub credentials: bool,
    pub region: bool,
    /// The bucket is on AWS proper, so its region can be asked for.
    pub ask_bucket_region: bool,
}

/// What is missing from `b` after the environment and the caller's options were applied.
pub(crate) fn needs(b: &AmazonS3Builder) -> Needs {
    let set = |k: AmazonS3ConfigKey| b.get_config_value(&k).is_some_and(|v| !v.is_empty());
    let unsigned = b
        .get_config_value(&AmazonS3ConfigKey::SkipSignature)
        .is_some_and(|v| v == "true");
    let endpoint = set(AmazonS3ConfigKey::Endpoint);
    let region = set(AmazonS3ConfigKey::Region);
    Needs {
        credentials: !set(AmazonS3ConfigKey::AccessKeyId) && !unsigned,
        region: !region && !endpoint,
        ask_bucket_region: !region && !endpoint,
    }
}

/// Fills in credentials and region from the SDK where `b` has none. `profile` selects a named
/// profile of the shared config files (otherwise `AWS_PROFILE` or `default`).
pub(crate) async fn complete(
    mut b: AmazonS3Builder,
    bucket: &str,
    profile: Option<&str>,
) -> Result<AmazonS3Builder, String> {
    let need = needs(&b);
    if !need.credentials && !need.region {
        return Ok(b);
    }
    let mut loader = aws_config::defaults(BehaviorVersion::latest());
    if let Some(p) = profile {
        loader = loader.profile_name(p);
    }
    let sdk = loader.load().await;
    if need.credentials {
        let provider = sdk.credentials_provider().ok_or_else(|| {
            "the AWS SDK found no credential provider; set keys, a profile or a role".to_string()
        })?;
        b = b.with_credentials(Arc::new(SdkCredentials::new(provider)));
    }
    if need.region {
        let bucket_region = if need.ask_bucket_region {
            object_store::aws::resolve_bucket_region(bucket, &ClientOptions::default())
                .await
                .ok()
        } else {
            None
        };
        if let Some(r) = bucket_region.or_else(|| sdk.region().map(|r| r.to_string())) {
            b = b.with_region(r);
        }
    }
    Ok(b)
}

/// The SDK's credential chain as an object_store credential provider, with a cache: the chain
/// itself does not cache (SDK clients do that in their identity cache).
#[derive(Debug)]
pub(crate) struct SdkCredentials {
    provider: SharedCredentialsProvider,
    cached: tokio::sync::Mutex<Option<(Arc<AwsCredential>, Option<SystemTime>)>>,
}

impl SdkCredentials {
    pub(crate) fn new(provider: SharedCredentialsProvider) -> Self {
        SdkCredentials {
            provider,
            cached: tokio::sync::Mutex::new(None),
        }
    }
}

#[async_trait]
impl CredentialProvider for SdkCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let mut g = self.cached.lock().await;
        if let Some((c, expiry)) = &*g
            && expiry.is_none_or(|e| e > SystemTime::now() + RENEW_BEFORE)
        {
            return Ok(c.clone());
        }
        let creds = self.provider.provide_credentials().await.map_err(|e| {
            object_store::Error::Generic {
                store: "S3",
                source: Box::new(e),
            }
        })?;
        let c = Arc::new(AwsCredential {
            key_id: creds.access_key_id().to_string(),
            secret_key: creds.secret_access_key().to_string(),
            token: creds.session_token().map(str::to_string),
        });
        *g = Some((c.clone(), creds.expiry()));
        Ok(c)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};

    use aws_credential_types::Credentials;
    use aws_credential_types::provider::future::ProvideCredentials as Fut;

    use super::*;

    /// Hands out numbered credentials that expire `ttl` from now.
    #[derive(Debug)]
    struct Counting {
        calls: Arc<AtomicU32>,
        ttl: Option<Duration>,
    }

    impl ProvideCredentials for Counting {
        fn provide_credentials<'a>(&'a self) -> Fut<'a>
        where
            Self: 'a,
        {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let expiry = self.ttl.map(|t| SystemTime::now() + t);
            Fut::ready(Ok(Credentials::new(
                format!("key-{n}"),
                "secret",
                Some("token".into()),
                expiry,
                "test",
            )))
        }
    }

    fn provider(ttl: Option<Duration>) -> (SdkCredentials, Arc<AtomicU32>) {
        let calls = Arc::new(AtomicU32::new(0));
        let p = SharedCredentialsProvider::new(Counting {
            calls: calls.clone(),
            ttl,
        });
        (SdkCredentials::new(p), calls)
    }

    #[tokio::test]
    async fn credentials_are_cached_until_near_expiry() {
        let (p, calls) = provider(Some(Duration::from_secs(3600)));
        let a = p.get_credential().await.unwrap();
        let b = p.get_credential().await.unwrap();
        assert_eq!((a.key_id.as_str(), b.key_id.as_str()), ("key-0", "key-0"));
        assert_eq!(a.token.as_deref(), Some("token"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Expiring within the renewal window: fetched again every time.
        let (p, calls) = provider(Some(Duration::from_secs(60)));
        p.get_credential().await.unwrap();
        let c = p.get_credential().await.unwrap();
        assert_eq!(c.key_id, "key-1");
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // No expiry: kept.
        let (p, calls) = provider(None);
        p.get_credential().await.unwrap();
        p.get_credential().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn needs_follow_what_is_configured() {
        let b = || AmazonS3Builder::new().with_bucket_name("b");
        let all = Needs {
            credentials: true,
            region: true,
            ask_bucket_region: true,
        };
        assert_eq!(needs(&b()), all);
        assert_eq!(
            needs(&b().with_access_key_id("k").with_secret_access_key("s")),
            Needs {
                credentials: false,
                ..all
            }
        );
        assert_eq!(
            needs(&b().with_region("eu-central-1")),
            Needs {
                region: false,
                ask_bucket_region: false,
                ..all
            }
        );
        // An S3-compatible server: its region is whatever was configured.
        assert_eq!(
            needs(&b().with_endpoint("http://127.0.0.1:9000")),
            Needs {
                region: false,
                ask_bucket_region: false,
                ..all
            }
        );
        assert!(!needs(&b().with_skip_signature(true)).credentials);
    }

    /// Asks AWS for a public bucket's region. Needs the internet, so it runs only when
    /// `MORAINE_TEST_AWS_BUCKET=<bucket>:<region>` names a bucket and its expected region.
    #[tokio::test]
    async fn bucket_region_from_aws() {
        let Ok(spec) = std::env::var("MORAINE_TEST_AWS_BUCKET") else {
            return;
        };
        let (bucket, region) = spec.split_once(':').expect("<bucket>:<region>");
        let b = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_skip_signature(true);
        let b = complete(b, bucket, None).await.unwrap();
        assert_eq!(
            b.get_config_value(&AmazonS3ConfigKey::Region).as_deref(),
            Some(region)
        );
    }
}
