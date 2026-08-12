//! The Command Pattern (the `@colyseus/command` counterpart).
//!
//! Commands decouple *what triggers* an operation (a message handler, a
//! timer, a lifecycle hook) from *how it is executed*. Each command is a
//! small, independently unit-testable unit of game logic:
//!
//! ```ignore
//! struct DealCards { count: usize }
//!
//! impl Command<PokerRoom> for DealCards {
//!     fn execute<'a>(
//!         self: Box<Self>,
//!         room: &'a mut PokerRoom,
//!         ctx: &'a mut RoomContext,
//!         dispatcher: &'a mut Dispatcher<PokerRoom>,
//!     ) -> BoxFuture<'a, ()> {
//!         Box::pin(async move {
//!             // ... mutate room / ctx.state ...
//!             dispatcher.enqueue(CheckWinner); // chain follow-up commands
//!         })
//!     }
//! }
//! ```
//!
//! Rooms opt in by holding a [`Dispatcher`] and implementing [`Dispatchable`]:
//!
//! ```ignore
//! struct PokerRoom { dispatcher: Dispatcher<PokerRoom>, /* ... */ }
//!
//! impl Dispatchable for PokerRoom {
//!     fn dispatcher_mut(&mut self) -> &mut Dispatcher<Self> { &mut self.dispatcher }
//! }
//!
//! // then, in any handler:
//! self.dispatch(ctx, DealCards { count: 2 }).await;
//! ```
//!
//! Rules of the road:
//! - Inside `execute`, enqueue follow-ups via the passed `dispatcher` —
//!   never via `room.dispatch(...)` (the dispatcher is temporarily detached
//!   from the room while the queue drains).
//! - Commands run sequentially on the room actor, like everything else.

use std::collections::VecDeque;

use async_trait::async_trait;

use crate::room::{BoxFuture, Room, RoomContext};

/// A unit of game logic. Implementors are plain structs carrying their
/// payload as fields.
pub trait Command<R: Room>: Send + 'static {
    fn execute<'a>(
        self: Box<Self>,
        room: &'a mut R,
        ctx: &'a mut RoomContext,
        dispatcher: &'a mut Dispatcher<R>,
    ) -> BoxFuture<'a, ()>;
}

/// A FIFO command queue bound to a room type.
pub struct Dispatcher<R: Room> {
    queue: VecDeque<Box<dyn Command<R>>>,
    stopped: bool,
}

impl<R: Room> Default for Dispatcher<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: Room> Dispatcher<R> {
    pub fn new() -> Self {
        Dispatcher {
            queue: VecDeque::new(),
            stopped: false,
        }
    }

    /// Queue a command. No-op after [`stop`](Self::stop).
    pub fn enqueue<C: Command<R>>(&mut self, command: C) {
        if !self.stopped {
            self.queue.push_back(Box::new(command));
        }
    }

    /// Execute queued commands until the queue is empty or stopped.
    /// Commands may enqueue more commands while running.
    pub async fn drain(&mut self, room: &mut R, ctx: &mut RoomContext) {
        while !self.stopped {
            let Some(command) = self.queue.pop_front() else {
                break;
            };
            command.execute(room, ctx, self).await;
        }
    }

    /// Discard queued commands and ignore future ones (call in `on_dispose`).
    pub fn stop(&mut self) {
        self.stopped = true;
        self.queue.clear();
    }

    pub fn is_running(&self) -> bool {
        !self.stopped
    }

    pub fn pending(&self) -> usize {
        self.queue.len()
    }
}

/// Rooms that hold a dispatcher. Provides the ergonomic `dispatch` entrypoint.
#[async_trait]
pub trait Dispatchable: Room + Sized {
    fn dispatcher_mut(&mut self) -> &mut Dispatcher<Self>;

    /// Enqueue a command and run the queue to completion.
    async fn dispatch<C: Command<Self>>(&mut self, ctx: &mut RoomContext, command: C) {
        let mut dispatcher = std::mem::take(self.dispatcher_mut());
        dispatcher.enqueue(command);
        dispatcher.drain(self, ctx).await;
        *self.dispatcher_mut() = dispatcher;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;
    use serde_json::Value;

    struct CounterRoom {
        dispatcher: Dispatcher<CounterRoom>,
        value: i64,
        stopped_seen: bool,
    }

    impl Dispatchable for CounterRoom {
        fn dispatcher_mut(&mut self) -> &mut Dispatcher<Self> {
            &mut self.dispatcher
        }
    }

    #[async_trait]
    impl Room for CounterRoom {
        async fn on_create(&mut self, _ctx: &mut RoomContext, _options: Value) -> Result<()> {
            Ok(())
        }
    }

    struct Add(i64);
    impl Command<CounterRoom> for Add {
        fn execute<'a>(
            self: Box<Self>,
            room: &'a mut CounterRoom,
            _ctx: &'a mut RoomContext,
            dispatcher: &'a mut Dispatcher<CounterRoom>,
        ) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                room.value += self.0;
                if self.0 > 0 {
                    dispatcher.enqueue(Add(self.0 - 1)); // chaining
                }
            })
        }
    }

    struct Stop;
    impl Command<CounterRoom> for Stop {
        fn execute<'a>(
            self: Box<Self>,
            room: &'a mut CounterRoom,
            _ctx: &'a mut RoomContext,
            dispatcher: &'a mut Dispatcher<CounterRoom>,
        ) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                room.stopped_seen = true;
                dispatcher.enqueue(Add(100)); // never runs: stopped below
                dispatcher.stop();
            })
        }
    }

    #[tokio::test]
    async fn commands_chain_via_queue() {
        let mut room = CounterRoom {
            dispatcher: Dispatcher::new(),
            value: 0,
            stopped_seen: false,
        };
        let mut ctx = RoomContext::default();
        room.dispatch(&mut ctx, Add(3)).await; // 3 + 2 + 1
        assert_eq!(room.value, 6);
        assert_eq!(room.dispatcher.pending(), 0);
    }

    #[tokio::test]
    async fn stop_discards_pending() {
        let mut room = CounterRoom {
            dispatcher: Dispatcher::new(),
            value: 0,
            stopped_seen: false,
        };
        let mut ctx = RoomContext::default();
        room.dispatch(&mut ctx, Stop).await;
        assert!(room.stopped_seen);
        assert_eq!(room.value, 0); // enqueued Add(100) was discarded
    }
}
