//! Intrusive linked list-based implementation of `event-listener`.
//!
//! This implementation crates an intrusive linked list of listeners. This list
//! is secured using either a libstd mutex or a critical section.

use crate::notify::{GenericNotify, Internal, Notification};
use crate::sync::atomic::Ordering;
use crate::sync::cell::{Cell, UnsafeCell};
use crate::{RegisterResult, State, Task, TaskRef};

use alloc::vec::Vec;
use small_wake_buffer::SmallWakeBuffer;

#[cfg(feature = "critical-section")]
use core::cell::RefCell;
#[cfg(all(feature = "std", not(feature = "critical-section")))]
use core::ops::{Deref, DerefMut};

use core::marker::PhantomPinned;
use core::mem;
use core::pin::Pin;
use core::ptr::NonNull;

pub(super) struct List<T>(
    /// libstd-based implementation uses a normal Mutex to secure the data.
    #[cfg(all(feature = "std", not(feature = "critical-section")))]
    crate::sync::Mutex<Inner<T>>,
    /// Critical-section-based implementation uses a CS cell that wraps a RefCell.
    #[cfg(feature = "critical-section")]
    critical_section::Mutex<RefCell<Inner<T>>>,
    /// Spinlock implementation uses a spinlock.
    #[cfg(not(any(feature = "std", feature = "critical-section")))]
    spinlock::Spinlock<Inner<T>>,
);

struct Inner<T> {
    /// The head of the linked list.
    head: Option<NonNull<Link<T>>>,

    /// The tail of the linked list.
    tail: Option<NonNull<Link<T>>>,

    /// The first unnotified listener.
    next: Option<NonNull<Link<T>>>,

    /// Total number of listeners.
    len: usize,

    /// The number of notified listeners.
    notified: usize,

    /// Tasks whose listeners were notified while the lock was held.
    ///
    /// Wakers are collected here instead of being invoked immediately so that every wake is
    /// emitted *after* the lock has been released. Otherwise a woken task would immediately
    /// block trying to acquire the same lock again, turning the wake into wasted work.
    pending_wakes: SmallWakeBuffer<Task>,
}

impl<T> List<T> {
    /// Create a new, empty event listener list.
    pub(super) fn new() -> Self {
        let inner = Inner {
            head: None,
            tail: None,
            next: None,
            len: 0,
            notified: 0,
            pending_wakes: SmallWakeBuffer::new(),
        };

        #[cfg(feature = "critical-section")]
        {
            Self(critical_section::Mutex::new(RefCell::new(inner)))
        }

        #[cfg(all(not(feature = "critical-section"), feature = "std"))]
        {
            Self(crate::sync::Mutex::new(inner))
        }

        #[cfg(not(any(feature = "std", feature = "critical-section")))]
        {
            Self(spinlock::Spinlock::new(inner))
        }
    }

    /// Get the total number of listeners without blocking.
    #[cfg(all(feature = "std", not(feature = "critical-section")))]
    pub(crate) fn try_total_listeners(&self) -> Option<usize> {
        self.0.try_lock().ok().map(|list| list.len)
    }

    /// Get the total number of listeners without blocking.
    #[cfg(feature = "critical-section")]
    pub(crate) fn try_total_listeners(&self) -> Option<usize> {
        Some(self.total_listeners())
    }

    /// Get the total number of listeners without blocking.
    #[cfg(not(any(feature = "std", feature = "critical-section")))]
    pub(crate) fn try_total_listeners(&self) -> Option<usize> {
        self.0.try_with(|list| list.len)
    }

    /// Count listeners whose state is `Task`, i.e. that are parked waiting for a wake.
    #[cfg(all(
        test,
        feature = "std",
        not(feature = "critical-section"),
        not(target_family = "wasm"),
        not(loom)
    ))]
    pub(crate) fn task_count(&self) -> usize {
        let inner = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let mut count = 0;
        let mut cursor = inner.head;
        while let Some(link) = cursor {
            // SAFETY: the list lock is held, so every link is stable and readable.
            let link_ref = unsafe { link.as_ref() };
            // SAFETY: the list lock is held, so the cell's contents are not mutated concurrently.
            let state = unsafe { &*link_ref.state.as_ptr() };
            if matches!(state, State::Task(_)) {
                count += 1;
            }
            cursor = link_ref.next.get();
        }
        count
    }

    /// Get the total number of listeners with blocking.
    #[cfg(all(feature = "std", not(feature = "critical-section")))]
    pub(crate) fn total_listeners(&self) -> usize {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).len
    }

    /// Get the total number of listeners with blocking.
    #[cfg(feature = "critical-section")]
    #[allow(unused)]
    pub(crate) fn total_listeners(&self) -> usize {
        critical_section::with(|cs| self.0.borrow(cs).borrow().len)
    }
}

impl<T> crate::Inner<T> {
    #[cfg(all(feature = "std", not(feature = "critical-section")))]
    fn with_inner<R>(&self, f: impl FnOnce(&mut Inner<T>) -> R) -> R {
        struct ListLock<'a, 'b, T> {
            lock: crate::sync::MutexGuard<'a, Inner<T>>,
            inner: &'b crate::Inner<T>,
            #[cfg(all(test, not(feature = "critical-section"), not(target_family = "wasm")))]
            _held: LockHeldGuard,
        }

        impl<T> Deref for ListLock<'_, '_, T> {
            type Target = Inner<T>;

            fn deref(&self) -> &Self::Target {
                &self.lock
            }
        }

        impl<T> DerefMut for ListLock<'_, '_, T> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.lock
            }
        }

        impl<T> Drop for ListLock<'_, '_, T> {
            fn drop(&mut self) {
                update_notified(&self.inner.notified, &self.lock);
            }
        }

        let mut list = ListLock {
            inner: self,
            lock: self.list.0.lock().unwrap_or_else(|e| e.into_inner()),
            #[cfg(all(test, not(feature = "critical-section"), not(target_family = "wasm")))]
            _held: LockHeldGuard::new(),
        };
        let result = f(&mut list);

        // Take the wakers out while still holding the lock, then drop the guard so that the
        // wakes happen with no lock held. This stops woken tasks from immediately blocking on
        // the very lock they were notified under.
        if !list.pending_wakes.is_empty() {
            let pending_wakes = list.pending_wakes.drain().collect::<Vec<_>>();
            drop(list);
            for task in pending_wakes {
                task.wake();
            }
        }

        result
    }

    #[cfg(any(not(feature = "std"), feature = "critical-section"))]
    fn with_inner<R>(&self, f: impl FnOnce(&mut Inner<T>) -> R) -> R {
        struct ListWrapper<'a, T> {
            inner: &'a crate::Inner<T>,
            list: &'a mut Inner<T>,
            #[cfg(all(test, not(feature = "critical-section"), not(target_family = "wasm")))]
            _held: LockHeldGuard,
        }

        impl<T> Drop for ListWrapper<'_, T> {
            fn drop(&mut self) {
                update_notified(&self.inner.notified, self.list);
            }
        }

        #[cfg(feature = "critical-section")]
        return critical_section::with(move |cs| {
            let mut list = self.list.0.borrow_ref_mut(cs);
            let wrapper = ListWrapper {
                inner: self,
                list: &mut *list,
                #[cfg(all(test, not(feature = "critical-section"), not(target_family = "wasm")))]
                _held: LockHeldGuard::new(),
            };

            let result = f(wrapper.list);
            if !wrapper.list.pending_wakes.is_empty() {
                let pending_wakes = wrapper.list.pending_wakes.drain().collect::<Vec<_>>();
                drop(wrapper);
                drop(list);
                for task in pending_wakes {
                    task.wake();
                }
            }
            result
        });

        #[cfg(not(feature = "critical-section"))]
        self.list.0.with(move |list| {
            let wrapper = ListWrapper {
                inner: self,
                list: &mut *list,
                #[cfg(all(test, not(feature = "critical-section"), not(target_family = "wasm")))]
                _held: LockHeldGuard::new(),
            };

            let result = f(wrapper.list);
            if !wrapper.list.pending_wakes.is_empty() {
                let pending_wakes = wrapper.list.pending_wakes.drain().collect::<Vec<_>>();
                drop(wrapper);
                for task in pending_wakes {
                    task.wake();
                }
            }
            result
        })
    }

    /// Add a new listener to the list.
    pub(crate) fn insert(&self, mut listener: Pin<&mut Option<Listener<T>>>) {
        self.with_inner(|inner| {
            listener.as_mut().set(Some(Listener {
                link: UnsafeCell::new(Link {
                    state: Cell::new(State::Created),
                    prev: Cell::new(inner.tail),
                    next: Cell::new(None),
                }),
                _pin: PhantomPinned,
            }));
            let listener = listener.as_pin_mut().unwrap();

            {
                let entry_guard = listener.link.get();
                // SAFETY: We are locked, so we can access the inner `link`.
                let entry = unsafe { entry_guard.deref() };

                // Replace the tail with the new entry.
                match inner.tail.replace(entry.into()) {
                    None => inner.head = Some(entry.into()),
                    Some(t) => unsafe { t.as_ref().next.set(Some(entry.into())) },
                };
            }

            // If there are no unnotified entries, this is the first one.
            if inner.next.is_none() {
                inner.next = inner.tail;
            }

            // Bump the entry count.
            inner.len += 1;
        });
    }

    /// Remove a listener from the list.
    pub(crate) fn remove(
        &self,
        listener: Pin<&mut Option<Listener<T>>>,
        propagate: bool,
    ) -> Option<State<T>> {
        self.with_inner(|inner| inner.remove(listener, propagate))
    }

    /// Notifies a number of entries.
    #[cold]
    pub(crate) fn notify(&self, notify: impl Notification<Tag = T>) -> usize {
        self.with_inner(|inner| inner.notify(notify))
    }

    /// Register a task to be notified when the event is triggered.
    ///
    /// Returns `true` if the listener was already notified, and `false` otherwise. If the listener
    /// isn't inserted, returns `None`.
    pub(crate) fn register(
        &self,
        mut listener: Pin<&mut Option<Listener<T>>>,
        task: TaskRef<'_>,
    ) -> RegisterResult<T> {
        self.with_inner(|inner| {
            let entry_guard = match listener.as_mut().as_pin_mut() {
                Some(listener) => listener.link.get(),
                None => return RegisterResult::NeverInserted,
            };
            // SAFETY: We are locked, so we can access the inner `link`.
            let entry = unsafe { entry_guard.deref() };

            // Take out the state and check it.
            match entry.state.replace(State::NotifiedTaken) {
                State::Notified { tag, .. } => {
                    // We have been notified, remove the listener.
                    inner.remove(listener, false);
                    RegisterResult::Notified(tag)
                }

                State::Task(other_task) => {
                    // Only replace the task if it's different.
                    entry.state.set(State::Task({
                        if !task.will_wake(other_task.as_task_ref()) {
                            task.into_task()
                        } else {
                            other_task
                        }
                    }));

                    RegisterResult::Registered
                }

                _ => {
                    // We have not been notified, register the task.
                    entry.state.set(State::Task(task.into_task()));
                    RegisterResult::Registered
                }
            }
        })
    }
}

impl<T> Inner<T> {
    fn remove(
        &mut self,
        mut listener: Pin<&mut Option<Listener<T>>>,
        propagate: bool,
    ) -> Option<State<T>> {
        let entry_guard = listener.as_mut().as_pin_mut()?.link.get();
        let entry = unsafe { entry_guard.deref() };

        let prev = entry.prev.get();
        let next = entry.next.get();

        // Unlink from the previous entry.
        match prev {
            None => self.head = next,
            Some(p) => unsafe {
                p.as_ref().next.set(next);
            },
        }

        // Unlink from the next entry.
        match next {
            None => self.tail = prev,
            Some(n) => unsafe {
                n.as_ref().prev.set(prev);
            },
        }

        // If this was the first unnotified entry, update the next pointer.
        if self.next == Some(entry.into()) {
            self.next = next;
        }

        // The entry is now fully unlinked, so we can now take it out safely.
        let entry = unsafe {
            listener
                .get_unchecked_mut()
                .take()
                .unwrap()
                .link
                .into_inner()
        };

        // This State::Created is immediately dropped and exists as a workaround for the absence of
        // loom::cell::Cell::into_inner. The intent is `let mut state = entry.state.into_inner();`
        //
        // refs: https://github.com/tokio-rs/loom/pull/341
        let mut state = entry.state.replace(State::Created);

        // Update the notified count.
        if state.is_notified() {
            self.notified -= 1;

            if propagate {
                let state = mem::replace(&mut state, State::NotifiedTaken);
                if let State::Notified { additional, tag } = state {
                    let tags = {
                        let mut tag = Some(tag);
                        move || tag.take().expect("tag already taken")
                    };
                    self.notify(GenericNotify::new(1, additional, tags));
                }
            }
        }
        self.len -= 1;

        Some(state)
    }

    #[cold]
    fn notify(&mut self, mut notify: impl Notification<Tag = T>) -> usize {
        let mut n = notify.count(Internal::new());
        let is_additional = notify.is_additional(Internal::new());

        if !is_additional {
            if n < self.notified {
                return 0;
            }
            n -= self.notified;
        }

        let original_count = n;
        while n > 0 {
            n -= 1;

            // Notify the next entry.
            match self.next {
                None => return original_count - n - 1,

                Some(e) => {
                    // Get the entry and move the pointer forwards.
                    let entry = unsafe { e.as_ref() };
                    self.next = entry.next.get();

                    // Set the state to `Notified` and notify.
                    let tag = notify.next_tag(Internal::new());
                    if let State::Task(task) = entry.state.replace(State::Notified {
                        additional: is_additional,
                        tag,
                    }) {
                        // Defer the wake until the lock is released (see `with_inner`).
                        self.pending_wakes.push(task);
                    }

                    // Bump the notified count.
                    self.notified += 1;
                }
            }
        }

        original_count - n
    }
}

fn update_notified<T>(slot: &crate::sync::atomic::AtomicUsize, list: &Inner<T>) {
    // Update the notified count.
    let notified = if list.notified < list.len {
        list.notified
    } else {
        usize::MAX
    };

    slot.store(notified, Ordering::Release);
}

/// A buffer of tasks to wake that avoids heap allocation for small batches.
///
/// Notifications of a handful of listeners (the common `notify(1)` case) are stored inline.
/// Only batches larger than [`SmallWakeBuffer::INLINE_CAPACITY`] spill over to a heap-allocated
/// [`Vec`], keeping the hot path allocation-free.
mod small_wake_buffer {
    use alloc::vec::Vec;
    use core::mem::MaybeUninit;

    /// Number of tasks that can be buffered without a heap allocation.
    const INLINE_CAPACITY: usize = 32;

    pub(super) struct SmallWakeBuffer<T> {
        /// Inline storage used until `len` reaches `INLINE_CAPACITY`.
        inline: [MaybeUninit<T>; INLINE_CAPACITY],

        /// Heap storage used once the inline capacity is exhausted.
        spilled: Vec<T>,

        /// Number of tasks currently held in the inline storage.
        len: usize,
    }

    impl<T> SmallWakeBuffer<T> {
        /// Create an empty buffer.
        pub(super) const fn new() -> Self {
            Self {
                inline: unsafe { MaybeUninit::uninit().assume_init() },
                spilled: Vec::new(),
                len: 0,
            }
        }

        /// Whether the buffer currently holds no tasks.
        pub(super) fn is_empty(&self) -> bool {
            self.len == 0 && self.spilled.is_empty()
        }

        /// Append a task to the buffer.
        pub(super) fn push(&mut self, value: T) {
            if self.len < INLINE_CAPACITY {
                self.inline[self.len].write(value);
                self.len += 1;
            } else {
                self.spilled.push(value);
            }
        }

        /// Drain all tasks, leaving the buffer empty.
        pub(super) fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
            let len = self.len;
            self.len = 0;
            let inline = &mut self.inline[..len];

            // SAFETY: the first `len` slots were initialized by `push` and are read once here.
            let inline_iter = unsafe { inline.iter_mut().map(|slot| slot.assume_init_read()) };

            inline_iter.chain(self.spilled.drain(..))
        }
    }

    impl<T> Drop for SmallWakeBuffer<T> {
        fn drop(&mut self) {
            for slot in self.inline[..self.len].iter_mut() {
                // SAFETY: the first `len` slots were initialized by `push`.
                unsafe { slot.assume_init_drop() };
            }
        }
    }
}

/// Test-only guard marking that the current thread holds the internal listener-list lock.
#[cfg(all(test, not(feature = "critical-section"), not(target_family = "wasm")))]
struct LockHeldGuard {
    _private: (),
}

#[cfg(all(test, not(feature = "critical-section"), not(target_family = "wasm")))]
impl LockHeldGuard {
    fn new() -> Self {
        crate::wake_stats::set_lock_held(true);
        LockHeldGuard { _private: () }
    }
}

#[cfg(all(test, not(feature = "critical-section"), not(target_family = "wasm")))]
impl Drop for LockHeldGuard {
    fn drop(&mut self) {
        crate::wake_stats::set_lock_held(false);
    }
}

pub(crate) struct Listener<T> {
    /// The inner link in the linked list.
    ///
    /// # Safety
    ///
    /// This can only be accessed while the central mutex is locked.
    link: UnsafeCell<Link<T>>,

    /// This listener cannot be moved after being pinned.
    _pin: PhantomPinned,
}

struct Link<T> {
    /// The current state of the listener.
    state: Cell<State<T>>,

    /// The previous link in the linked list.
    prev: Cell<Option<NonNull<Link<T>>>>,

    /// The next link in the linked list.
    next: Cell<Option<NonNull<Link<T>>>>,
}

/// Dangerous spinlock for when neither std nor critical-section are enabled.
#[cfg(not(any(feature = "std", feature = "critical-section")))]
mod spinlock {
    use crate::sync::atomic::{AtomicBool, Ordering};
    use crate::sync::cell::UnsafeCell;

    /// A spinlock.
    pub(super) struct Spinlock<T> {
        /// Is this spinlock locked?
        locked: AtomicBool,

        /// The data contained within the lock.
        data: UnsafeCell<T>,
    }

    impl<T> Spinlock<T> {
        /// Create a new [`Spinlock`].
        #[inline]
        pub(super) fn new(data: T) -> Self {
            Self {
                locked: AtomicBool::new(false),
                data: UnsafeCell::new(data),
            }
        }

        /// Try to acquire a spinlock, run a closure, then release.
        #[inline]
        pub(super) fn try_with<R>(&self, f: impl FnOnce(&mut T) -> R) -> Option<R> {
            if self
                .locked
                .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                let _guard = CallOnDrop(|| self.locked.store(false, Ordering::Release));
                // SAFETY: We have the lock held.
                Some(f(unsafe { self.data.get().deref_mut() }))
            } else {
                None
            }
        }

        /// Run an operation with the spinlock held.
        #[inline]
        pub(super) fn with<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
            loop {
                if self
                    .locked
                    .compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
                    .is_ok()
                {
                    let _guard = CallOnDrop(|| self.locked.store(false, Ordering::Release));
                    // SAFETY: We have the lock held.
                    return f(unsafe { self.data.get().deref_mut() });
                }

                while self.locked.load(Ordering::Relaxed) {
                    core::hint::spin_loop();
                }
            }
        }
    }

    struct CallOnDrop<F: FnMut()>(F);
    impl<F: FnMut()> Drop for CallOnDrop<F> {
        #[inline]
        fn drop(&mut self) {
            (self.0)();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::pin;

    #[cfg(target_family = "wasm")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    macro_rules! make_listeners {
        ($($id:ident),*) => {
            $(
                let $id = Option::<Listener<()>>::None;
                pin!($id);
            )*
        };
    }

    #[test]
    fn insert() {
        let inner = crate::Inner::new();
        make_listeners!(listen1, listen2, listen3);

        // Register the listeners.
        inner.insert(listen1.as_mut());
        inner.insert(listen2.as_mut());
        inner.insert(listen3.as_mut());

        assert_eq!(inner.list.try_total_listeners(), Some(3));

        // Remove one.
        assert_eq!(inner.remove(listen2, false), Some(State::Created));
        assert_eq!(inner.list.try_total_listeners(), Some(2));

        // Remove another.
        assert_eq!(inner.remove(listen1, false), Some(State::Created));
        assert_eq!(inner.list.try_total_listeners(), Some(1));
    }

    #[test]
    fn drop_non_notified() {
        let inner = crate::Inner::new();
        make_listeners!(listen1, listen2, listen3);

        // Register the listeners.
        inner.insert(listen1.as_mut());
        inner.insert(listen2.as_mut());
        inner.insert(listen3.as_mut());

        // Notify one.
        inner.notify(GenericNotify::new(1, false, || ()));

        // Remove one.
        inner.remove(listen3, true);

        // Remove the rest.
        inner.remove(listen1, true);
        inner.remove(listen2, true);
    }

    /// With hundreds of blocked waiters, assert that no wake is delivered while the internal
    /// lock is held and report the wasted-wake ratio.
    #[cfg(all(
        feature = "std",
        not(feature = "critical-section"),
        not(target_family = "wasm"),
        not(loom)
    ))]
    #[test]
    fn no_wakes_while_locked() {
        use crate::wake_stats;
        use crate::Listener;
        use std::sync::Barrier;
        use std::thread;

        const WAITERS: usize = 200;

        wake_stats::reset();

        let event = std::sync::Arc::new(crate::Event::new());
        let ready = std::sync::Arc::new(Barrier::new(WAITERS + 1));

        let mut handles = Vec::with_capacity(WAITERS);
        for _ in 0..WAITERS {
            let event = event.clone();
            let ready = ready.clone();
            handles.push(thread::spawn(move || {
                let listener = event.listen();
                ready.wait();
                listener.wait();
            }));
        }

        // Wait until every listener is registered and actually parked.
        ready.wait();
        while event.task_count() != WAITERS {
            thread::yield_now();
        }

        assert_eq!(event.notify(WAITERS), WAITERS);

        for handle in handles {
            handle.join().unwrap();
        }

        let (wakes_while_locked, wakes_while_unlocked) = wake_stats::counts();
        let total = wakes_while_locked + wakes_while_unlocked;

        assert_eq!(total, WAITERS, "each parked task is woken exactly once");
        assert_eq!(
            wakes_while_locked, 0,
            "no wake may be delivered while the internal lock is held"
        );
        assert_eq!(wakes_while_unlocked, WAITERS);

        let wasted_ratio = wakes_while_locked as f64 / total as f64;
        eprintln!(
            "waiters={WAITERS} total_wakes={total} wakes_while_locked={wakes_while_locked} \
             wakes_while_unlocked={wakes_while_unlocked} wasted_wake_ratio={wasted_ratio:.4}"
        );
    }
}
