// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use super::SpanPosition;

/// Tree-structured representation of a client session
#[derive(Debug, Clone, Abomonation)]
pub struct TraceTree<M> {
    roots: Vec<Node<M>>,
}

/// A single node in a transaction tree
#[derive(Debug, Clone, Abomonation)]
pub struct Node<M> {
    messages: Vec<M>,
    children: Vec<Node<M>>,
    depth: u32,
}

impl<M> TraceTree<M> {
    // Creates the transaction trees for the session
    pub fn construct(messages: &mut Vec<M>) -> TraceTree<M>
        where M: SpanPosition
    {
        let mut trxtree = TraceTree { roots: Vec::new() };
        for msg in messages.drain(..) {
            let trx = msg.get_span_id().0.clone();
            let digit = trx[0] as usize;
            while digit >= trxtree.roots.len() {
                // Insert empty node
                trxtree.roots.push(Node { messages: Vec::new(), children: Vec::new(), depth: 0 });
            }
            trxtree.roots[digit].attach_message(msg, trx);
        }
        trxtree
    }
}

impl<M> Node<M> {
    // Returns the total number of nodes in the tree ("virtual" nodes are also counted)
    pub fn count_all_children(&self) -> u32 {
        let mut num = 0;
        num += self.children.len() as u32;
        for child in &self.children {
            num += child.count_all_children();
        }
        if self.depth == 0 { num += 1; }
        num
    }

    // Returns the height of the tree
    pub fn get_height(&self) -> u32 {
        if self.children.is_empty() { return self.depth; }

        let mut height = 0;
        for child in &self.children {
            let h = child.get_height();
            if height < h {
                height = h;
            }
        }
        height
    }

    fn attach_message(&mut self, m: M, trxnb: Vec<u32>) {
        if self.depth as usize + 1 == trxnb.len() {
            self.messages.push(m);
        } else {
            let digit = trxnb[(self.depth as usize) + 1] as usize;
            while digit >= self.children.len() {
                // Insert empty node
                self.children.push(Node { messages: Vec::new(), children: Vec::new(), depth: self.depth + 1 });
            }
            self.children[digit].attach_message(m, trxnb);
        }
    }
}
