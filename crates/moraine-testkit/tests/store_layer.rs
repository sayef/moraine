//! The store layer against injected faults: what is retried, what is not, and the capability
//! probe (cases S1 to S4, S12, S13).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use moraine::Error;
use moraine::clock::Clock;
use moraine::store::{Op, Path, RetryPolicy};
use moraine::testing::{
    Class, Store, Support, classify, probe_conditional_writes, require_conditional_writes,
    server_now,
};
use moraine_testkit::chaos::FaultCounts;
use moraine_testkit::{ChaosStore, ClientClock, Conditional, FaultRules, SimClock, SimStore};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions};

fn stack(rules: FaultRules) -> (Store, SimStore, Arc<FaultCounts>, SimClock) {
    let clock = SimClock::new();
    let sim = SimStore::new(clock.clone());
    let chaos = ChaosStore::new(Arc::new(sim.clone()), rules, 42).with_clock(clock.clone());
    let faults = chaos.faults().clone();
    let client: Arc<dyn Clock> = Arc::new(ClientClock::new(clock.clone(), None, 0, 1));
    (
        Store::new(Arc::new(chaos), client, RetryPolicy::REQUEST),
        sim,
        faults,
        clock,
    )
}

fn create() -> PutOptions {
    PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    }
}

#[tokio::test]
async fn s3_throttled_requests_are_retried_with_backoff() {
    let (s, sim, faults, clock) = stack(FaultRules {
        throttle: 0.5,
        ..Default::default()
    });
    for i in 0..50 {
        let p = Path::from(format!("t/{i}"));
        s.object_store().put(&p, "x".into()).await.unwrap();
        s.object_store()
            .put_opts(&Path::from(format!("c/{i}")), "x".into(), create())
            .await
            .unwrap();
    }
    assert_eq!(sim.snapshot().len(), 100);
    let throttled = faults.throttle.load(std::sync::atomic::Ordering::Relaxed);
    assert!(throttled > 20, "{throttled}");
    // Every throttled request was sent again, and backoff slept in simulated time.
    assert_eq!(s.metrics().snapshot().requests(), 100 + throttled);
    assert!(clock.now() > Duration::ZERO);
}

#[tokio::test]
async fn s1_ambiguous_conditional_writes_are_not_repeated() {
    let (s, sim, _, _) = stack(FaultRules {
        fail_after: 1.0,
        ..Default::default()
    });
    let e = s
        .object_store()
        .put_opts(&Path::from("log/1.json"), "a".into(), create())
        .await
        .unwrap_err();
    // Applied, reported as ambiguous, sent once: a retry would have said "already exists" for
    // the client's own write.
    assert_eq!(classify(&e), Class::Ambiguous);
    assert!(sim.peek("log/1.json").is_some());
    assert_eq!(s.metrics().snapshot().count(Op::PutCreate), 1);
}

#[tokio::test]
async fn s1_ambiguous_overwrites_are_repeated() {
    let (s, sim, faults, _) = stack(FaultRules {
        fail_after: 0.5,
        ..Default::default()
    });
    for i in 0..20 {
        s.object_store()
            .put(&Path::from(format!("d/{i}")), "x".into())
            .await
            .unwrap();
    }
    assert_eq!(sim.snapshot().len(), 20);
    assert!(faults.ambiguous() > 0);
}

#[tokio::test]
async fn s13_truncated_bodies_are_read_again() {
    let (s, sim, faults, _) = stack(FaultRules {
        truncate: 0.5,
        ..Default::default()
    });
    let body = vec![7u8; 4096];
    sim.put(&Path::from("f"), body.clone().into())
        .await
        .unwrap();
    for _ in 0..20 {
        let got = s
            .object_store()
            .get(&Path::from("f"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(&got[..], &body[..]);
    }
    assert!(faults.truncate.load(std::sync::atomic::Ordering::Relaxed) > 0);
}

#[tokio::test]
async fn s4_partial_listings_are_retried_never_shortened() {
    let (s, sim, faults, _) = stack(FaultRules {
        cut_listing: 0.6,
        ..Default::default()
    });
    for i in 0..30 {
        sim.put(&Path::from(format!("idx/{i:02}")), "x".into())
            .await
            .unwrap();
    }
    for _ in 0..10 {
        let items: Vec<_> = s
            .object_store()
            .list(Some(&Path::from("idx")))
            .collect()
            .await;
        assert_eq!(items.len(), 30);
        assert!(items.iter().all(|r| r.is_ok()));
    }
    assert!(
        faults
            .cut_listing
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
    );

    // A listing that always fails is an error, not a shorter answer.
    let (s, sim, _, _) = stack(FaultRules {
        cut_listing: 1.0,
        ..Default::default()
    });
    sim.put(&Path::from("idx/a"), "x".into()).await.unwrap();
    let items: Vec<_> = s
        .object_store()
        .list(Some(&Path::from("idx")))
        .collect()
        .await;
    assert_eq!(items.len(), 1);
    assert!(items[0].is_err());
}

#[tokio::test]
async fn s12_stalled_requests_count_against_the_deadline() {
    let (s, _, _, clock) = stack(FaultRules {
        stall: 1.0,
        stall_for: Duration::from_secs(30),
        throttle: 1.0,
        ..Default::default()
    });
    let e = s
        .object_store()
        .put(&Path::from("x"), "x".into())
        .await
        .unwrap_err();
    assert_eq!(classify(&e), Class::Throttled);
    // Two stalled attempts pass the 60 s deadline; no third attempt starts.
    assert!(clock.now() >= Duration::from_secs(60));
    assert!(s.metrics().snapshot().requests() <= 3);
}

#[tokio::test]
async fn s2_store_without_conditional_put_is_refused_for_writing() {
    for (mode, create, update) in [
        (Conditional::Honour, Support::Honoured, Support::Honoured),
        (Conditional::Ignore, Support::Ignored, Support::Ignored),
        (Conditional::Reject, Support::Rejected, Support::Rejected),
    ] {
        let (s, sim, _, _) = stack(FaultRules {
            conditional: mode,
            ..Default::default()
        });
        let clock = s.clock().clone();
        let caps = probe_conditional_writes(
            s.object_store().as_ref(),
            &Path::from("meta"),
            clock.as_ref(),
        )
        .await
        .unwrap();
        assert_eq!((caps.create, caps.update), (create, update), "{mode:?}");
        assert!(sim.snapshot().is_empty(), "probe objects left behind");
        let r = require_conditional_writes(
            s.object_store(),
            &Path::from(format!("meta-{mode:?}")),
            clock.as_ref(),
        )
        .await;
        match mode {
            Conditional::Honour => assert!(r.is_ok()),
            _ => assert!(matches!(r, Err(Error::ConditionalWritesUnsupported(_)))),
        }
    }
}

#[tokio::test]
async fn g1_server_time_ignores_client_clock_skew() {
    let (s, _, _, clock) = stack(FaultRules::default());
    clock.advance(Duration::from_secs(7200));
    let skewed = ClientClock::new(clock.clone(), None, 0, 1).with_skew(-3_600_000);
    let t = server_now(s.object_store().as_ref(), &Path::from("meta"), &skewed)
        .await
        .unwrap();
    assert_eq!(t.server, clock.server_time());
    assert!(skewed.wall_now() < t.server);
    clock.advance(Duration::from_secs(10));
    assert_eq!(t.now(&skewed), clock.server_time());
}
