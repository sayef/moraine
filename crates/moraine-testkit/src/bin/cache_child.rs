//! One process of the two-process cache test (T10, case K1): fetches a sidecar from a local
//! store into a shared disk cache and prints what happened.
//!
//!     cache_child <cache dir> <store dir> <object key>

use std::sync::Arc;

use moraine::testing::Listed;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use {moraine::index::CacheMode, moraine::testing::SidecarCache};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let [_, cache, store, key] = args.as_slice() else {
        eprintln!("usage: cache_child <cache dir> <store dir> <object key>");
        std::process::exit(2);
    };
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(store).unwrap());
    let location = Path::from(key.as_str());
    let meta = store.head(&location).await.unwrap();
    let listed = Listed {
        location,
        spec_hash: "0123456789abcdef".into(),
        size: meta.size,
        etag: meta.e_tag,
    };
    let c = SidecarCache::open(CacheMode::Disk {
        dir: cache.into(),
        max_bytes: 1 << 30,
    })
    .unwrap();
    let got = c
        .get(&store, "file:///store", &listed, &|_| Ok(()))
        .await
        .unwrap();
    let v = got.expect("disk cache returns the sidecar");
    println!(
        "ok sections={} lost_races={}",
        v.view.sections().len(),
        c.stats().lost_races
    );
}
