use super::SpanPosition;

/// Tree-structured representation of a client session
#[derive(Debug, Clone, Abomonation)]
pub struct TrxTree<M> {
    roots: Vec<Node<M>>,
}

/// A single node in a transaction tree
#[derive(Debug, Clone, Abomonation)]
pub struct Node<M> {
    messages: Vec<M>,
    children: Vec<Node<M>>,
    depth: u32,
}

fn insert<M>(m: M, trxnb: Vec<u32>, n: &mut Node<M>) {
    if n.depth as usize + 1 == trxnb.len() {
        n.messages.push(m);
    } else {
        let digit = trxnb[(n.depth as usize) + 1] as usize;
        while digit >= n.children.len() {
            // Insert empty node
            n.children.push(Node { messages: Vec::new(), children: Vec::new(), depth: n.depth + 1 });
        }
        insert(m, trxnb, &mut n.children[digit]);
    }
}

// Creates the transaction trees for the session
pub fn create_trees<M>(messages: &mut Vec<M>) -> TrxTree<M>
    where M: SpanPosition
{
    let mut trxtree = TrxTree { roots: Vec::new() };
    for msg in messages.drain(..) {
        let trx = msg.get_span_id().0.clone();
        let digit = trx[0] as usize;
        while digit >= trxtree.roots.len() {
            // Insert empty node
            trxtree.roots.push(Node { messages: Vec::new(), children: Vec::new(), depth: 0 });
        }
        insert(msg, trx, &mut trxtree.roots[digit]);
    }
    trxtree
}

// Returns the total number of nodes in the tree ("virtual" nodes are also counted)
pub fn get_nodes_number<M>(n: Node<M>) -> u32 {
    let mut num = 0;
    num += n.children.len() as u32;
    for child in n.children {
        num += get_nodes_number(child);
    }
    if n.depth == 0 { num += 1; }
    num
}

// Returns the height of the tree
pub fn get_height<M>(n: Node<M>) -> u32 {
    if n.children.is_empty() { return n.depth; }

    let mut height = 0;
    for child in n.children {
        let h = get_height(child);
        if height < h {
            height = h;
        }
    }
    height
}
