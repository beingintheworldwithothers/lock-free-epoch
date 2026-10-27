//! Michael-Scott lock-free queue.
//! Usable with any number of producers and consumers.

use core::mem::{self, MaybeUninit};
use core::sync::atomic::Ordering::*;

use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use crossbeam_utils::CachePadded;

// the `tail` pointer may lag behind the actual tail.
#[derive(Debug)]
pub struct Queue<T> {
    head: CachePadded<Atomic<Node<T>>>,
    tail: CachePadded<Atomic<Node<T>>>,
}

#[derive(Debug)]
struct Node<T> {
    data: MaybeUninit<T>,

    next: Atomic<Node<T>>,
}

unsafe impl<T: Send> Sync for Queue<T> {}
unsafe impl<T: Send> Send for Queue<T> {}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Queue<T> {
    pub fn new() -> Self {
        let sentinel = Box::into_raw(Box::new(Node {
            data: MaybeUninit::uninit(),
            next: Atomic::null(),
        }))
        .cast_const();

        Self {
            head: CachePadded::new(sentinel.into()),
            tail: CachePadded::new(sentinel.into()),
        }
    }

    pub fn push(&self, t: T, guard: &mut Guard) {
        let mut new = Owned::new(Node {
            data: MaybeUninit::new(t),
            next: Atomic::null(),
        });

        loop {
            let tail = self.tail.load(Acquire, guard);

            let tail_ref = unsafe { tail.deref() };
            let next = tail_ref.next.load(Acquire, guard);

            if !next.is_null() {
                let _ = self
                    .tail
                    .compare_exchange(tail, next, Release, Relaxed, guard);
                continue;
            }

            match tail_ref
                .next
                .compare_exchange(Shared::null(), new, Release, Relaxed, guard)
            {
                Ok(new) => {
                    // try to move the tail pointer forward.
                    let _ = self
                        .tail
                        .compare_exchange(tail, new, Release, Relaxed, guard);
                    break;
                }
                Err(e) => new = e.new,
            }
            guard.repin();
        }
    }

    pub fn try_pop(&self, guard: &mut Guard) -> Option<T> {
        loop {
            let head = self.head.load(Acquire, guard);
            let next = unsafe { head.deref() }.next.load(Acquire, guard);

            let next_ref = unsafe { next.as_ref() }?;

            // Moves `tail` if it's stale.
            let tail = self.tail.load(Relaxed, guard);
            if tail == head {
                let _ = self
                    .tail
                    .compare_exchange(tail, next, Release, Relaxed, guard);
            }

            if self
                .head
                .compare_exchange(head, next, Release, Relaxed, guard)
                .is_ok()
            {
                // SAFETY: `head` is detached from `self` so is unreachable from other threads.
                // unreachability is guaranteed by fences which are executed internally!
                let result = unsafe { next_ref.data.assume_init_read() };
                
                // SAFETY: `head` is unreachable, and we no longer access `head`.
                unsafe { guard.defer_destroy(head) };

                return Some(result);
            }
            guard.repin();
        }
    }
}

impl<T> Drop for Queue<T> {
    fn drop(&mut self) {
        // Destroy the sentinel node.
        let sentinel = mem::take(&mut *self.head);

        let mut o_curr = unsafe { sentinel.into_owned() }.into_box().next;

        // SAFETY: we have unique ownership via `&mut self`.
        while let Some(curr) = unsafe { o_curr.try_into_owned() }.map(Owned::into_box) {
            // SAFETY: Not sentinel node, so `data` is valid.
            drop(unsafe { curr.data.assume_init() });
            o_curr = curr.next;
        }
    }
}