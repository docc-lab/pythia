use std::collections::{HashSet, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use abomonation::Abomonation;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Unary;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

use super::SessionizableMessage;

pub mod stats;

/// Hash a value with the default `Hasher` (internal algorithm unspecified).
///
/// The specified value will be hashed with this hasher and then the resulting
/// hash will be returned.
fn hash_code<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[derive(Debug, Clone)]
pub struct MessagesForSession<Message: SessionizableMessage> {
    pub session: String,
    pub messages: Vec<Message>,
}

impl<Message: SessionizableMessage> Abomonation for MessagesForSession<Message> {
    unsafe fn entomb<W: ::std::io::Write>(&self, writer: &mut W) -> ::std::io::Result<()> {
        self.session.entomb(writer)?;
        self.messages.entomb(writer)?;
        Ok(())
    }

    unsafe fn exhume<'a, 'b>(&'a mut self, mut bytes: &'b mut [u8]) -> Option<&'b mut [u8]> {
        let temp = bytes;
        bytes = if let Some(bytes) = self.session.exhume(temp) {
            bytes
        } else {
            return None;
        };
        let temp = bytes;
        bytes = if let Some(bytes) = self.messages.exhume(temp) {
            bytes
        } else {
            return None;
        };
        Some(bytes)
    }
}

pub trait Sessionize<S: Scope, Message: SessionizableMessage> {
    // epoch_time is the time of an epoch
    // session_time is the time out to wait after which a session is declared as closed
    fn sessionize(
        &self,
        epoch_time: u64,
        session_time: u64,
    ) -> Stream<S, MessagesForSession<Message>>;
}

struct SessionState<Message: SessionizableMessage> {
    messages: Vec<Message>,
    epoch_to_flush_at: u64,
}

impl<Message: SessionizableMessage> SessionState<Message> {
    fn new() -> SessionState<Message> {
        SessionState {
            messages: Vec::new(),
            epoch_to_flush_at: 0,
        }
    }

    fn in_same_session(&self, session_time: u64, msg_time: u64) -> bool {
        let last_msg = self.messages.last();
        if last_msg.is_some() {
            let last_time = last_msg.unwrap().time();
            assert!(last_time <= msg_time);
            msg_time - last_time <= session_time
        } else {
            false
        }
    }
}

impl<
    S: Scope<Timestamp = Product<RootTimestamp, u64>>,
    Message: SessionizableMessage + ::std::fmt::Debug,
> Sessionize<S, Message> for Stream<S, Message> {
    fn sessionize(
        &self,
        epoch_time: u64,
        session_time: u64,
    ) -> Stream<S, MessagesForSession<Message>> {
        let num_epochs_for_session = (session_time + epoch_time - 1) / epoch_time;
        assert!(num_epochs_for_session > 0);
        let exchange = Exchange::new(|msg: &Message| hash_code(&msg.session()));

        // A buffer holding messages that is looked at in time order
        // epoch -> [vector of messages]
        let mut messages_for_time: HashMap<u64, Vec<Message>> = HashMap::new();

        // In flight sessions: session -> SessionState
        let mut active_sessions: HashMap<String, SessionState<Message>> = HashMap::new();

        // Sessions to check -> These sessions might have expired by this time
        let mut sessions_to_check: HashMap<u64, HashSet<String>> = HashMap::new();

        // Used to ensure monotonicity
        let mut last_time_seen: u64 = 0;

        let mut last_notification_epoch: u64 = 0;

        self.unary_notify(exchange, "Sessionize", vec![], move |input,
              output,
              notificator| {
            input.for_each(|epoch, data| for msg in data.drain(..) {
                assert!(
                    msg.time() / epoch_time == epoch.inner,
                    "Unexpected message and epoch time mismatch"
                );
                let message_buffer = messages_for_time.entry(epoch.inner).or_insert_with(
                    Vec::new,
                );
                message_buffer.push(msg.clone());
                notificator.notify_at(epoch.clone());
            });

            notificator.for_each(|ts, _count, notify| {
                assert!(ts.inner >= last_notification_epoch);
                last_notification_epoch = ts.inner;
                let mut out = output.session(&ts);
                let messages_for_time =
                    &mut messages_for_time.remove(&ts.inner).unwrap_or_else(Vec::new);
                messages_for_time.sort_by(|m1, m2| m1.time().cmp(&m2.time()));
                for msg in messages_for_time.drain(..) {
                    let msg_time = msg.time();
                    assert!(last_time_seen <= msg_time);
                    last_time_seen = msg_time;
                    let session_state = active_sessions
                        .entry(msg.session().to_owned())
                        .or_insert_with(SessionState::new);
                    if !session_state.in_same_session(session_time, msg_time) &&
                        !session_state.messages.is_empty()
                    {
                        out.give(MessagesForSession::<Message> {
                            session: msg.session().to_owned(),
                            messages: session_state.messages.clone(),
                        });
                        session_state.messages.clear();
                    }
                    session_state.messages.push(msg.clone());
                    session_state.epoch_to_flush_at = ts.inner + num_epochs_for_session;
                    sessions_to_check
                        .entry(session_state.epoch_to_flush_at)
                        .or_insert_with(|| {
                            notify.notify_at(ts.delayed(
                                &RootTimestamp::new(session_state.epoch_to_flush_at),
                            ));
                            HashSet::new()
                        })
                        .insert(msg.session().to_owned());
                }

                let mut sessions_to_remove: Vec<String> = Vec::new();

                for session in sessions_to_check.remove(&ts.inner).unwrap_or_default() {
                    if let Some(session_state) = active_sessions.get(&session) {
                        if session_state.epoch_to_flush_at <= ts.inner {
                            let session_copy = session.clone();
                            sessions_to_remove.push(session);
                            assert!(
                                !session_state.messages.is_empty(),
                                "Messages for session cannot be empty here"
                            );
                            out.give(MessagesForSession::<Message> {
                                session: session_copy,
                                messages: session_state.messages.clone(),
                            });
                        }
                    }
                }

                for session in sessions_to_remove {
                    active_sessions.remove(&session);
                }
            });
        })

    }
}
