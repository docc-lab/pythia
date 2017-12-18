extern crate abomonation;
extern crate timely;

pub mod operators;

use abomonation::Abomonation;
use timely::ExchangeData;

pub type Timestamp = u64;
pub type TraceId = u32;
pub type Degree = u32;

/// A sessionizable message.
///
/// Sessionizion requires two properties for each recorded message:
///
///    - a session identifier
///    - the log record timestamp
pub trait SessionizableMessage: ExchangeData {
//    type Timestamp: ExchangeData;

    fn time(&self) -> Timestamp;
    fn session(&self) -> &str;
}

pub trait TracedMessage {
    fn call_trace(&self) -> &Vec<TraceId>;
}

#[derive(Debug, Clone)]
pub struct MessagesForSession<M: SessionizableMessage> {
    pub session: String,
    pub messages: Vec<M>,
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

// Method to convert a Vec<Vec<u32>> indicating paths through a tree to a canonical
// representation of the tree
//
// the result is a sequence of degrees of a BFS traversal of the graph.
pub fn canonical_shape<M: TracedMessage>(messages: &Vec<M>) -> Vec<Degree> {
    let paths: Vec<&Vec<TraceId>> = messages.into_iter().map(|m| m.call_trace()).collect();
    let mut position = vec![0; paths.len()];
    let mut degrees = vec![0];
    let mut offsets = vec![1]; // where do children start?

    if let Some(max_depth) = paths.iter().map(|p| p.len()).max() {
        for depth in 0 .. max_depth {
            // advance each position based on its offset
            // ensure that the max degree of the associated node is at least as high as it should be.
            for index in 0..paths.len() {
                if paths[index].len() > depth {
                    if depth > 0 {
                        position[index] = (offsets[position[index]] + paths[index][depth-1]) as usize;
                    }

                    degrees[position[index]] = ::std::cmp::max(degrees[position[index]], paths[index][depth] + 1);
                }
            }

            // add zeros and transform degrees to offsets.
            let mut last = 0;
            for &x in &degrees { last += x as usize; }

            while degrees.len() <= last {
                degrees.push(0);
                offsets.push(0);
            }

            for i in 1..degrees.len() {
                offsets[i] = offsets[i-1] + degrees[i-1];
            }

        }
    }

    return degrees;
}

#[cfg(test)]
mod tests {
    use super::{canonical_shape, TraceId, TracedMessage};

    #[derive(Debug, Clone)]
    struct Addr(Vec<TraceId>);

    impl TracedMessage for Addr {
        fn call_trace(&self) -> &Vec<TraceId> { &self.0 }
    }

    #[test]
    fn test_tree_shape() {
        assert_eq!(canonical_shape(&vec![Addr(vec![0])]),
                   vec![1,0]);
        assert_eq!(canonical_shape(&vec![Addr(vec![1])]),
                   vec![2,0,0]);
        assert_eq!(canonical_shape(&vec![Addr(vec![0, 1])]),
                   vec![1,2,0,0]);
        assert_eq!(canonical_shape(&vec![Addr(vec![2, 1, 3]), Addr(vec![3])]),
                   vec![4,0,0,2,0,0,4,0,0,0,0]);
    }
}
