//! Sidecar discovery: which bundles exist under a table's `idx/` prefix.
//!
//! Existence is not in the Delta log. A table lists `idx/` once per snapshot change, and at
//! most every poll interval otherwise, into a map from (data file, index name) to the listed
//! bundles. A listing that fails or is cut short keeps the previous map (scenario 18): it is
//! never read as "no sidecars". A sidecar that fails to download or validate is marked bad for
//! a while and its file takes the exact route.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::path::{Path, PathPart};

/// One listed sidecar.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Listed {
    /// The sidecar's object path.
    pub location: Path,
    /// The spec hash in its name.
    pub spec_hash: String,
    /// Its size in bytes, from the listing.
    pub size: u64,
    /// Its ETag, from the listing, when the store gives one.
    pub etag: Option<String>,
}

/// (data file path, index name) to the bundles listed for it, in listing order.
pub type SidecarMap = HashMap<(String, String), Vec<Listed>>;

/// The path segment under `idx/` for a data file. Building and discovery both use this one
/// function, so the key is derived from the `add` path exactly one way.
pub fn data_segment(data_path: &str) -> String {
    PathPart::from(data_path).as_ref().to_string()
}

/// Where the sidecar of `index` with `hash` for `data_path` lives.
pub fn sidecar_location(idx: &Path, data_path: &str, index: &str, hash: &str) -> Path {
    idx.clone()
        .join(data_path)
        .join(format!("{index}.{hash}.osix").as_str())
}

/// Parses a listed location relative to `idx`: (data file segment, index, hash).
pub(crate) fn parse(idx: &Path, location: &Path) -> Option<(String, String, String)> {
    let parts: Vec<PathPart<'_>> = location.prefix_match(idx)?.collect();
    let [data, file] = parts.as_slice() else {
        return None;
    };
    let name = file.as_ref().strip_suffix(".osix")?;
    let (index, hash) = name.rsplit_once('.')?;
    if hash.len() != 16 || !hash.bytes().all(|c| c.is_ascii_hexdigit()) || index.is_empty() {
        return None;
    }
    Some((
        data.as_ref().to_string(),
        index.to_string(),
        hash.to_string(),
    ))
}

#[derive(Default)]
struct State {
    map: Option<Arc<SidecarMap>>,
    /// Snapshot version and client time of the last successful listing.
    version: u64,
    at: Duration,
    /// Sidecars this handle wrote itself since that listing (read-your-writes for builds).
    own: Vec<(String, String, Listed)>,
}

/// The discovery state of one table handle.
#[derive(Default)]
pub struct Discovery {
    state: Mutex<State>,
    bad: Mutex<HashMap<String, Duration>>,
}

impl std::fmt::Debug for Discovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Discovery").finish_non_exhaustive()
    }
}

impl Discovery {
    /// The sidecar map, listing `idx` again when the snapshot moved or the map is older than
    /// `poll`. On a failed listing, the previous map (or the error, when there is none).
    pub async fn map(
        &self,
        store: &Arc<dyn ObjectStore>,
        idx: &Path,
        version: u64,
        now: Duration,
        poll: Duration,
    ) -> object_store::Result<Arc<SidecarMap>> {
        {
            let g = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(m) = &g.map
                && g.version == version
                && now.saturating_sub(g.at) < poll
            {
                return Ok(m.clone());
            }
        }
        match list(store, idx).await {
            Ok(mut fresh) => {
                let mut g = self.state.lock().unwrap_or_else(|p| p.into_inner());
                for (data, index, l) in g.own.drain(..) {
                    let v = fresh.entry((data, index)).or_default();
                    if !v.iter().any(|x| x.location == l.location) {
                        v.push(l);
                    }
                }
                let m = Arc::new(fresh);
                g.map = Some(m.clone());
                g.version = version;
                g.at = now;
                Ok(m)
            }
            Err(e) => {
                let g = self.state.lock().unwrap_or_else(|p| p.into_inner());
                match &g.map {
                    Some(m) => Ok(m.clone()),
                    None => Err(e),
                }
            }
        }
    }

    /// Lists again only when the last listing is older than `poll`, whatever the snapshot
    /// version (key lookups: at most one listing per poll interval). Failures keep what is
    /// known.
    pub async fn refresh_if_stale(
        &self,
        store: &Arc<dyn ObjectStore>,
        idx: &Path,
        now: Duration,
        poll: Duration,
    ) {
        let version = {
            let g = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if g.map.is_some() && now.saturating_sub(g.at) < poll {
                return;
            }
            g.version
        };
        let _ = self.map(store, idx, version, now, Duration::ZERO).await;
    }

    /// Records a sidecar this handle uploaded, so its next query sees it without a listing.
    pub fn add_own(&self, data_path: &str, index: &str, l: Listed) {
        let mut g = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let key = (data_segment(data_path), index.to_string());
        if let Some(m) = &g.map {
            let mut m = m.as_ref().clone();
            let v = m.entry(key.clone()).or_default();
            if !v.iter().any(|x| x.location == l.location) {
                v.push(l.clone());
            }
            g.map = Some(Arc::new(m));
        }
        g.own.push((key.0, key.1, l));
    }

    /// What is already known about `(data file, index)`, without any request: the last
    /// listing plus this handle's own uploads. Key lookups use this, so they never list.
    pub fn known(&self, data_path: &str, index: &str) -> Vec<Listed> {
        let g = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let seg = data_segment(data_path);
        let mut out: Vec<Listed> = g
            .map
            .as_ref()
            .and_then(|m| m.get(&(seg.clone(), index.to_string())).cloned())
            .unwrap_or_default();
        for (d, i, l) in &g.own {
            if *d == seg && i == index && !out.iter().any(|x| x.location == l.location) {
                out.push(l.clone());
            }
        }
        out
    }

    /// Forgets the map, so the next query lists again.
    pub fn invalidate(&self) {
        let mut g = self.state.lock().unwrap_or_else(|p| p.into_inner());
        g.map = None;
    }

    /// Marks a sidecar bad until `until` (client time).
    pub fn mark_bad(&self, location: &Path, until: Duration) {
        if let Ok(mut g) = self.bad.lock() {
            g.insert(location.to_string(), until);
        }
    }

    /// Whether the sidecar at `location` is marked bad at client time `now`.
    pub fn is_bad(&self, location: &Path, now: Duration) -> bool {
        let Ok(mut g) = self.bad.lock() else {
            return false;
        };
        match g.get(location.as_ref()) {
            Some(until) if *until > now => true,
            Some(_) => {
                g.remove(location.as_ref());
                false
            }
            None => false,
        }
    }
}

/// Lists `idx` completely; any error (including one cutting the listing short) fails it.
async fn list(store: &Arc<dyn ObjectStore>, idx: &Path) -> object_store::Result<SidecarMap> {
    let mut out = SidecarMap::new();
    let mut s = store.list(Some(idx));
    while let Some(m) = s.try_next().await? {
        if let Some((data, index, hash)) = parse(idx, &m.location) {
            out.entry((data, index)).or_default().push(Listed {
                location: m.location,
                spec_hash: hash,
                size: m.size,
                etag: m.e_tag,
            });
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn locations_round_trip() {
        let idx = Path::from("db/t/idx");
        for data in ["part-1.parquet", "x=a b/part-2.parquet", "%41.parquet"] {
            let loc = sidecar_location(&idx, data, "emb", "0123456789abcdef");
            let (d, i, h) = parse(&idx, &loc).unwrap();
            assert_eq!(d, data_segment(data));
            assert_eq!(i, "emb");
            assert_eq!(h, "0123456789abcdef");
        }
        assert!(parse(&idx, &Path::from("db/t/idx/p/emb.osix")).is_none());
        assert!(parse(&idx, &Path::from("db/t/idx/p/emb.zz.osix")).is_none());
        assert!(parse(&idx, &Path::from("db/t/idx/a/b/c.0123456789abcdef.osix")).is_none());
        // An index name with dots parses from the right.
        let loc = sidecar_location(&idx, "p", "a.b", "0123456789abcdef");
        assert_eq!(parse(&idx, &loc).unwrap().1, "a.b");
    }

    #[test]
    fn bad_marks_expire() {
        let d = Discovery::default();
        let p = Path::from("x");
        d.mark_bad(&p, Duration::from_secs(60));
        assert!(d.is_bad(&p, Duration::from_secs(59)));
        assert!(!d.is_bad(&p, Duration::from_secs(60)));
        assert!(
            !d.is_bad(&p, Duration::from_secs(1)),
            "an expired mark is gone"
        );
    }
}
