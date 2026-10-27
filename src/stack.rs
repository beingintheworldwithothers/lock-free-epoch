//! Treiber's lock-free stack.
//! Usable with any number of producers and consumers.

use core::mem::{self, MaybeUninit};
use core::ptr;
use core::sync::atomic::Ordering::*;

use crossbeam_epoch::{Atomic, Owned, Shared};

#[derive(Debug)]
pub struct Stack<T> {
    head: Atomic<Node<T>>,
}

#[derive(Debug)]
struct Node<T> {
    // MaybeUninit as the data may be taken out of the node.
    data: MaybeUninit<T>,
    next: *const Node<T>,
}

unsafe impl<T: Send> Send for Stack<T> {}
unsafe impl<T: Send> Sync for Stack<T> {}

impl<T> Default for Stack<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Stack<T> {
    pub fn new() -> Stack<T> {
        Self {
            head: Atomic::null(),
        }
    }

    pub fn push(&self, t: T) {
        let mut node = Owned::new(Node {
            data: MaybeUninit::new(t),
            next: ptr::null(),
        });

        // SAFETY: We don't dereference any pointers obtained from this guard.
        let guard = unsafe { crossbeam_epoch::unprotected() };

        let mut head = self.head.load(Relaxed, guard);
        loop {
            node.next = head.as_raw();

            match self
                .head
                .compare_exchange(head, node, Release, Relaxed, guard)
            {
                Ok(_) => break,
                Err(e) => {
                    head = e.current;
                    node = e.new;
                }
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        let mut guard = crossbeam_epoch::pin();

        loop {
            let head = self.head.load(Acquire, &guard);
            let h = unsafe { head.as_ref() }?;
            let next = Shared::from(h.next);

            if self
                .head
                .compare_exchange(head, next, Relaxed, Relaxed, &guard)
                .is_ok()
            {
                // SAFETY: `head` is detached from `self` so is unreachable from other threads.
                // unreachability is guaranteed by fences which are executed internally!
                let result = unsafe { h.data.assume_init_read() };

                // SAFETY: `head` is unreachable, and we no longer access `head`.
                unsafe { guard.defer_destroy(head) };

                return Some(result);
            }

            guard.repin();
        }
    }

    pub fn is_empty(&self) -> bool {
        let guard = crossbeam_epoch::pin();
        self.head.load(Acquire, &guard).is_null()
    }
}

impl<T> Drop for Stack<T> {
    fn drop(&mut self) {
        let mut o_curr = mem::take(&mut self.head);

        // SAFETY: we have unique ownership via `&mut self`.
        while let Some(curr) = unsafe { o_curr.try_into_owned() }.map(Owned::into_box) {
            drop(unsafe { curr.data.assume_init() });
            o_curr = curr.next.into();
        }
    }
}

#[cfg(test)]
mod test {
    use std::thread::scope;

    use super::*;

    #[test]
    fn push() {
        let stack = Stack::new();

        scope(|scope| {
            for _ in 0..10 {
                scope.spawn(|| {
                    for i in 0..10_000 {
                        stack.push(i);
                        assert!(stack.pop().is_some());
                    }
                });
            }
        });

        assert!(stack.is_empty());
    }
}
