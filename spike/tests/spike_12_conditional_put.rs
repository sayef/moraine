//! Spike item 12: do the in-memory, local and S3 (RustFS) backends honour `PutMode::Create` and
//! `PutMode::Update`? Also races 32 concurrent creates of one key: exactly one may win.

use std::sync::Arc;

use moraine_spike::{Backend, run_prefix};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion};

fn describe(r: &object_store::Result<object_store::PutResult>) -> String {
    match r {
        Ok(_) => "ok".into(),
        Err(object_store::Error::AlreadyExists { .. }) => "AlreadyExists".into(),
        Err(object_store::Error::Precondition { .. }) => "Precondition".into(),
        Err(object_store::Error::NotImplemented { .. }) => "NotImplemented".into(),
        Err(object_store::Error::NotFound { .. }) => "NotFound".into(),
        Err(e) => format!("other: {e}"),
    }
}

async fn put(store: &dyn ObjectStore, p: &Path, body: &str, mode: PutMode) -> String {
    let opts = PutOptions {
        mode,
        ..Default::default()
    };
    describe(&store.put_opts(p, body.to_string().into(), opts).await)
}

async fn check(name: &str, backend: Backend) {
    let (store, root) = backend.store().unwrap();
    let p = Path::from(moraine_spike::join(&root, "probe/obj"));

    let first = put(store.as_ref(), &p, "one", PutMode::Create).await;
    let second = put(store.as_ref(), &p, "two", PutMode::Create).await;
    let head = store.head(&p).await.unwrap();
    let current = UpdateVersion {
        e_tag: head.e_tag.clone(),
        version: head.version.clone(),
    };
    let update_ok = put(
        store.as_ref(),
        &p,
        "three",
        PutMode::Update(current.clone()),
    )
    .await;
    let update_stale = put(store.as_ref(), &p, "four", PutMode::Update(current)).await;
    let missing = Path::from(moraine_spike::join(&root, "probe/missing"));
    let update_missing = put(
        store.as_ref(),
        &missing,
        "x",
        PutMode::Update(UpdateVersion {
            e_tag: Some("\"0\"".into()),
            version: None,
        }),
    )
    .await;
    let body = store.get(&p).await.unwrap().bytes().await.unwrap();

    // Race: 32 tasks create the same new key at once.
    let race = Path::from(moraine_spike::join(&root, "probe/race"));
    let mut tasks = vec![];
    for i in 0..32 {
        let store = Arc::clone(&store);
        let race = race.clone();
        tasks.push(tokio::spawn(async move {
            let opts = PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            };
            store
                .put_opts(&race, format!("writer {i}").into(), opts)
                .await
                .is_ok()
        }));
    }
    let mut winners = 0;
    for t in tasks {
        winners += t.await.unwrap() as u32;
    }

    println!(
        "RESULT item12 backend={name} create={first} create_again={second} update_current={update_ok} \
         update_stale={update_stale} update_missing={update_missing} final_body={:?} race_winners={winners}/32",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(first, "ok");
    assert_eq!(second, "AlreadyExists");
    assert_eq!(winners, 1, "exactly one concurrent create may succeed");
    if update_ok == "ok" {
        assert_eq!(update_stale, "Precondition");
    }
    let _ = backend.clear().await;
}

#[tokio::test]
async fn item12_memory() {
    check("memory", Backend::memory()).await;
}

#[tokio::test]
async fn item12_local() {
    let dir = tempfile::tempdir().unwrap();
    check("local", Backend::Local(dir.path().into())).await;
}

#[tokio::test]
async fn item12_s3_compatible() {
    let Some(b) = Backend::s3_compatible(&run_prefix("item12")) else {
        println!("RESULT item12 backend=s3 not run (MORAINE_SPIKE_S3_ENDPOINT not set)");
        return;
    };
    check("s3-rustfs", b).await;
}

#[tokio::test]
#[ignore = "needs a real bucket: set MORAINE_SPIKE_REAL_S3_BUCKET and AWS credentials"]
async fn item12_real_s3() {
    let b = Backend::real_s3(&run_prefix("item12")).expect("MORAINE_SPIKE_REAL_S3_BUCKET");
    check("s3-aws", b).await;
}
