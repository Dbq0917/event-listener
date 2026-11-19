//! Stress test aimed at detecting lost notifications.
//!
//! Requires the `std` feature (blocking `wait_deadline` and threads).
#![cfg(all(feature = "std", not(target_family = "wasm")))]
//!
//! A single consumer repeatedly:
//! 1. checks the shared queue,
//! 2. registers a listener (two-step check/register semantics),
//! 3. re-checks the queue,
//! 4. blocks on the listener.
//!
//! Producer threads push items and notify. If a notification is ever lost
//! between the check and the registration, the consumer parks forever and the
//! test fails via the timeout.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use event_listener::{Event, Listener};

#[test]
fn no_lost_wakeup_stress() {
    const PRODUCERS: usize = 8;
    const ITEMS_PER_PRODUCER: usize = 5_000;
    const TOTAL: usize = PRODUCERS * ITEMS_PER_PRODUCER;

    struct Channel {
        queue: Mutex<VecDeque<usize>>,
        event: Event,
    }

    let channel = Arc::new(Channel {
        queue: Mutex::new(VecDeque::new()),
        event: Event::new(),
    });

    let mut producers = Vec::new();
    for p in 0..PRODUCERS {
        let channel = channel.clone();
        producers.push(thread::spawn(move || {
            for i in 0..ITEMS_PER_PRODUCER {
                {
                    let mut queue = channel.queue.lock().unwrap();
                    queue.push_back(p * ITEMS_PER_PRODUCER + i);
                }
                channel.event.notify(1);
            }
        }));
    }

    let consumer = thread::spawn({
        let channel = channel.clone();
        move || {
            let mut received = 0usize;
            let deadline = Instant::now() + Duration::from_secs(60);

            while received < TOTAL {
                // First check before registering.
                if let Some(_item) = channel.queue.lock().unwrap().pop_front() {
                    received += 1;
                    continue;
                }

                // Register, then check again, then sleep.
                let listener = channel.event.listen();
                if let Some(_item) = channel.queue.lock().unwrap().pop_front() {
                    received += 1;
                    drop(listener);
                    continue;
                }

                assert!(
                    listener.wait_deadline(deadline).is_some(),
                    "lost wakeup: consumer timed out after {received}/{TOTAL} items"
                );

                // Drain everything available.
                while let Some(_item) = channel.queue.lock().unwrap().pop_front() {
                    received += 1;
                }
            }

            received
        }
    });

    for handle in producers {
        handle.join().unwrap();
    }
    let got = consumer.join().unwrap();
    assert_eq!(got, TOTAL);
}
