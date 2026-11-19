//! The group-commit queue (D18) and validation of every write.
//!
//! Every write goes through the handle's queue. A caller that finds no commit in flight starts
//! the handle's commit task (D64), which takes the queue's compatible prefix, runs it as one
//! commit ([`super::mutate`]), answers every member, and goes on with the next group until the
//! queue is empty. Callers only wait for their answer, so a caller that stops waiting (a
//! timeout, an aborted task) never fails another caller's write: a write cancelled before the
//! task took it is never committed, and one already taken commits or fails with its group.
//! Each caller gets its own result, and a caller whose batch is invalid fails alone (case C17)
//! because validation happens before queueing.
//!
//! Compatible means the same kind: appends merge with appends up to the target file size;
//! upserts merge with upserts whose keys they do not share (so every caller's rows are committed
//! whole, and the order of callers inside a commit never matters); deletes by key merge with
//! deletes by key, deletes by filter with deletes by filter. An update, an added column and a
//! file replace are commits of their own. A mixed queue becomes consecutive commits in arrival
//! order, so per-caller order is kept (C16).
//!
//! Writes that carry a caller's transaction id (D32) group like any other, and one commit holds
//! one `txn` action per caller id; a second write with an application id already in the group
//! starts the next group, so an application's writes commit in the order they were queued. A
//! member whose id is already applied is answered [`WriteOutcome::AlreadyApplied`] before the
//! group runs, or, when the commit finds it applied by another writer, after which the rest of
//! the group runs again without it.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::RecordBatch;
use arrow::datatypes::Field;
use datafusion::logical_expr::Expr;

use super::TableInner;
use super::mutate::{Keys, Matcher, ReplaceWork, Rewrite, Work};
use crate::error::{Error, Result};
use crate::log::TxnId;
use crate::options::{Committed, WriteOutcome};

/// A validated write waiting in the queue.
#[derive(Clone)]
pub(crate) enum Pending {
    Append(Vec<RecordBatch>),
    Upsert(Vec<RecordBatch>, Keys),
    DeleteKeys(Keys),
    Delete(Expr),
    Update(Expr, Vec<(String, Expr)>),
    AddColumn(Field),
    Replace(Box<ReplaceWork>),
}

impl Pending {
    fn kind(&self) -> u8 {
        match self {
            Pending::Append(_) => 0,
            Pending::Upsert(..) => 1,
            Pending::DeleteKeys(_) => 2,
            Pending::Delete(_) => 3,
            Pending::Update(..) => 4,
            Pending::AddColumn(_) => 5,
            Pending::Replace(_) => 6,
        }
    }
}

/// What a caller is told.
enum Answer {
    Done(Result<WriteOutcome>),
    /// The group commit panicked: the caller panics with this message, as it would have had it
    /// committed the group itself.
    Panicked(String),
}

/// A caller's mailbox: its answer and a wake-up.
#[derive(Default)]
struct Slot {
    msg: Mutex<Option<Answer>>,
    notify: tokio::sync::Notify,
}

impl Slot {
    fn send(&self, m: Result<WriteOutcome>) {
        self.answer(Answer::Done(m));
    }

    fn answer(&self, a: Answer) {
        if let Ok(mut g) = self.msg.lock() {
            *g = Some(a);
        }
        self.notify.notify_one();
    }

    fn take(&self) -> Option<Answer> {
        self.msg.lock().ok().and_then(|mut g| g.take())
    }
}

struct Waiter {
    work: Pending,
    /// The caller's transaction id, if any.
    txn: Option<TxnId>,
    bytes: u64,
    slot: Arc<Slot>,
}

#[derive(Default)]
pub(crate) struct Queue {
    busy: bool,
    waiting: VecDeque<Waiter>,
}

impl std::fmt::Debug for Queue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Queue")
            .field("busy", &self.busy)
            .field("waiting", &self.waiting.len())
            .finish()
    }
}

impl TableInner {
    /// Queues a validated write and waits for its result.
    pub(crate) async fn enqueue(
        self: &Arc<Self>,
        work: Pending,
        txn: Option<TxnId>,
    ) -> Result<WriteOutcome> {
        self.db.require_writable().await?;
        let bytes = match &work {
            Pending::Append(b) | Pending::Upsert(b, _) => {
                b.iter().map(|b| b.get_array_memory_size() as u64).sum()
            }
            Pending::Replace(r) => r
                .batches
                .iter()
                .map(|b| b.get_array_memory_size() as u64)
                .sum(),
            _ => 0,
        };
        self.submit(Waiter {
            work,
            txn,
            bytes,
            slot: Arc::new(Slot::default()),
        })
        .await
    }

    async fn submit(self: &Arc<Self>, w: Waiter) -> Result<WriteOutcome> {
        let slot = w.slot.clone();
        let start = {
            let mut q = self.queue.lock().unwrap_or_else(|p| p.into_inner());
            q.waiting.push_back(w);
            !std::mem::replace(&mut q.busy, true)
        };
        // From here the caller may be cancelled at any await (D56): the guard takes its write
        // out of the queue if the commit task has not taken it yet.
        let _queued = QueuedGuard {
            table: self.clone(),
            slot: slot.clone(),
        };
        let clock = self.db.clock().clone();
        if start {
            let me = self.clone();
            clock.spawn(Box::pin(async move { me.commit_task().await }));
        }
        loop {
            match slot.take() {
                Some(Answer::Done(r)) => return r,
                Some(Answer::Panicked(m)) => std::panic::resume_unwind(Box::new(m)),
                None => {
                    let s = slot.clone();
                    clock
                        .wait_for(Box::pin(async move { s.notify.notified().await }))
                        .await;
                }
            }
        }
    }

    /// Takes the queue's compatible prefix: the first waiter and those after it of the same
    /// kind that may share its commit. With nothing queued, marks the queue idle (under the
    /// same lock, so a write queued meanwhile starts a new task) and returns `None`.
    fn take_group(&self) -> Option<Vec<Waiter>> {
        let target = self.current().def.target_file_bytes;
        let mut q = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        if q.waiting.is_empty() {
            q.busy = false;
            return None;
        }
        let mut g: Vec<Waiter> = Vec::new();
        let mut bytes = 0;
        let mut keys: Option<Keys> = None;
        let mut apps: std::collections::HashSet<String> = std::collections::HashSet::new();
        while let Some(w) = q.waiting.front() {
            if let Some(first) = g.first() {
                let fits = w.work.kind() == first.work.kind()
                    && bytes + w.bytes <= target
                    && w.txn.as_ref().is_none_or(|t| !apps.contains(&t.app_id))
                    && match &w.work {
                        Pending::Append(_) | Pending::DeleteKeys(_) | Pending::Delete(_) => true,
                        Pending::Upsert(_, k) => keys.as_ref().is_some_and(|g| !g.overlaps(k)),
                        Pending::Update(..) | Pending::AddColumn(_) | Pending::Replace(_) => false,
                    };
                if !fits {
                    break;
                }
            }
            bytes += w.bytes;
            let Some(w) = q.waiting.pop_front() else {
                break;
            };
            if let Some(t) = &w.txn {
                apps.insert(t.app_id.clone());
            }
            if let Pending::Upsert(_, k) = &w.work {
                let copy = Keys {
                    cols: k.cols.clone(),
                    set: k.set.clone(),
                    prune: None,
                };
                match &mut keys {
                    Some(all) => all.extend(copy),
                    None => keys = Some(copy),
                }
            }
            g.push(w);
        }
        Some(g)
    }

    /// The handle's commit task: commits groups from the front of the queue, answering every
    /// member, until the queue is empty (D64).
    async fn commit_task(self: Arc<Self>) {
        let mut guard = TaskGuard {
            table: self.clone(),
            slots: Vec::new(),
            put_started: Arc::new(AtomicBool::new(false)),
            done: false,
        };
        loop {
            let linger = self.db.opts.linger;
            if !linger.is_zero() {
                self.db.clock().sleep(linger).await;
            }
            let Some(group) = self.take_group() else {
                break;
            };
            guard.slots = group.iter().map(|w| w.slot.clone()).collect();
            guard.put_started = Arc::new(AtomicBool::new(false));
            let run = std::panic::AssertUnwindSafe(self.commit_group(group, &mut guard));
            if let Err(p) = futures::FutureExt::catch_unwind(run).await {
                let msg = p
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                    .unwrap_or_else(|| "the group commit panicked".into());
                for s in &guard.slots {
                    s.answer(Answer::Panicked(msg.clone()));
                }
            }
            guard.slots.clear();
        }
        guard.done = true;
    }

    /// Commits one group and sends every member its result.
    async fn commit_group(self: &Arc<Self>, mut group: Vec<Waiter>, guard: &mut TaskGuard) {
        let r = loop {
            // Members whose transaction id is already applied are answered without a commit.
            let cur = self.current();
            group.retain(|w| match &w.txn {
                Some(t) => match cur.txns.applied(std::slice::from_ref(t)) {
                    Some((_, txn_version)) => {
                        guard.slots.retain(|s| !Arc::ptr_eq(s, &w.slot));
                        w.slot.send(Ok(WriteOutcome::AlreadyApplied {
                            version: cur.version,
                            txn_version,
                        }));
                        false
                    }
                    None => true,
                },
                None => true,
            });
            if group.is_empty() {
                break None;
            }
            let txns: Vec<TxnId> = group.iter().filter_map(|w| w.txn.clone()).collect();
            // The members are kept only when another writer can make one of them applied
            // while the group commits; then the rest commit again without it.
            let works: Vec<Pending> = if txns.is_empty() {
                std::mem::take(&mut group)
                    .into_iter()
                    .map(|w| w.work)
                    .collect()
            } else {
                group.iter().map(|w| w.work.clone()).collect()
            };
            let superseded = Arc::new(AtomicBool::new(false));
            let r = match merge(works) {
                Ok(work) => {
                    self.run_job(work, txns, guard.put_started.clone(), superseded.clone())
                        .await
                }
                Err(e) => Err(e),
            };
            if r.is_err() && superseded.load(Ordering::SeqCst) {
                continue;
            }
            break Some(r);
        };
        if let Some(r) = r {
            for s in &guard.slots {
                s.send(match &r {
                    Ok(v) => Ok(WriteOutcome::Committed(Committed::new(*v))),
                    Err(e) => Err(e.clone()),
                });
            }
        }
    }
}

/// One commit's work from a group of the same kind.
fn merge(group: Vec<Pending>) -> Result<Work> {
    let mut it = group.into_iter();
    let Some(first) = it.next() else {
        return Err(Error::Internal("empty group".into()));
    };
    Ok(match first {
        Pending::Append(mut b) => {
            for p in it {
                if let Pending::Append(more) = p {
                    b.extend(more);
                }
            }
            Work::Append(b)
        }
        Pending::Upsert(mut b, mut keys) => {
            for p in it {
                if let Pending::Upsert(more, k) = p {
                    b.extend(more);
                    keys.extend(k);
                }
            }
            Work::Rewrite(Rewrite {
                operation: "MERGE",
                matcher: Matcher::Keys(keys),
                assignments: None,
                insert: b,
            })
        }
        Pending::DeleteKeys(mut keys) => {
            for p in it {
                if let Pending::DeleteKeys(k) = p {
                    keys.extend(k);
                }
            }
            Work::Rewrite(Rewrite {
                operation: "DELETE",
                matcher: Matcher::Keys(keys),
                assignments: None,
                insert: Vec::new(),
            })
        }
        Pending::Delete(mut e) => {
            for p in it {
                if let Pending::Delete(more) = p {
                    e = e.or(more);
                }
            }
            Work::Rewrite(Rewrite {
                operation: "DELETE",
                matcher: Matcher::Predicate(e),
                assignments: None,
                insert: Vec::new(),
            })
        }
        Pending::Update(e, a) => Work::Rewrite(Rewrite {
            operation: "UPDATE",
            matcher: Matcher::Predicate(e),
            assignments: Some(a),
            insert: Vec::new(),
        }),
        Pending::AddColumn(f) => Work::AddColumn(f),
        Pending::Replace(r) => Work::Replace(*r),
    })
}

/// Held by a caller from the moment its write is queued until it has its answer. When the
/// caller is cancelled first (D56), it removes the write if the commit task has not taken it
/// yet, under the queue lock; a write already taken commits or fails with its group, and its
/// answer goes unread.
struct QueuedGuard {
    table: Arc<TableInner>,
    slot: Arc<Slot>,
}

impl Drop for QueuedGuard {
    fn drop(&mut self) {
        let mut q = self.table.queue.lock().unwrap_or_else(|p| p.into_inner());
        q.waiting.retain(|w| !Arc::ptr_eq(&w.slot, &self.slot));
    }
}

/// Answers every caller if the commit task stops before the queue is empty, which only a
/// panic or a runtime shutting down does: the group in flight gets an unknown outcome once a
/// log put may have been sent and a plain failure otherwise, every queued caller a failure,
/// and the queue is marked idle so the next write starts a new task. Nothing ever waits for
/// an answer that cannot come.
struct TaskGuard {
    table: Arc<TableInner>,
    slots: Vec<Arc<Slot>>,
    put_started: Arc<AtomicBool>,
    done: bool,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let sent = self.put_started.load(Ordering::SeqCst);
        for s in &self.slots {
            s.send(Err(if sent {
                Error::OutcomeUnknown {
                    txn: "a group commit whose task stopped".into(),
                }
            } else {
                Error::Cancelled
            }));
        }
        let mut q = self.table.queue.lock().unwrap_or_else(|p| p.into_inner());
        for w in q.waiting.drain(..) {
            w.slot.send(Err(Error::Cancelled));
        }
        q.busy = false;
    }
}
