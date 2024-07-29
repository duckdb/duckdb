#include "duckdb/execution/index/art/node7_leaf.hpp"

namespace duckdb {

Node7Leaf &Node7Leaf::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NType::NODE_7_LEAF).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_7_LEAF));
	auto &n7 = Node::RefMutable<Node7Leaf>(art, node, NType::NODE_7_LEAF);

	n7.count = 0;
	return n7;
}

Node7Leaf &Node7Leaf::ShrinkNode15Leaf(ART &art, Node &node7_leaf, Node &node15_leaf) {
	auto &n7 = New(art, node7_leaf);
	if (node15_leaf.IsGate()) {
		node7_leaf.SetGate();
	}
	auto &n15 = Node::RefMutable<Node15Leaf>(art, node15_leaf, NType::NODE_15_LEAF);

	n7.count = n15.count;
	for (idx_t i = 0; i < n15.count; i++) {
		n7.key[i] = n15.key[i];
	}

	n15.count = 0;
	Node::Free(art, node15_leaf);
	return n7;
}

void Node7Leaf::InsertByte(ART &art, Node &node, const uint8_t byte) {
	D_ASSERT(node.HasMetadata());
	auto &n7 = Node::RefMutable<Node7Leaf>(art, node, NType::NODE_7_LEAF);

	// The node is full. Grow to Node15.
	if (n7.count == Node::NODE_7_LEAF_CAPACITY) {
		auto node7 = node;
		Node15Leaf::GrowNode7Leaf(art, node, node7);
		Node15Leaf::InsertByte(art, node, byte);
		return;
	}

	// Still space. Insert the child.
	idx_t child_pos = 0;
	while (child_pos < n7.count && n7.key[child_pos] < byte) {
		child_pos++;
	}

	// Move children backwards to make space.
	for (idx_t i = n7.count; i > child_pos; i--) {
		n7.key[i] = n7.key[i - 1];
	}

	n7.key[child_pos] = byte;
	n7.count++;
}

void Node7Leaf::DeleteByte(ART &art, Node &node, Node &prefix, const uint8_t byte) {
	D_ASSERT(node.HasMetadata());
	auto &n7 = Node::RefMutable<Node7Leaf>(art, node, NType::NODE_7_LEAF);

	idx_t child_pos = 0;
	for (; child_pos < n7.count; child_pos++) {
		if (n7.key[child_pos] == byte) {
			break;
		}
	}

	D_ASSERT(child_pos < n7.count);
	D_ASSERT(n7.count > 1);
	n7.count--;

	// Possibly move children backwards.
	for (idx_t i = child_pos; i < n7.count; i++) {
		n7.key[i] = n7.key[i + 1];
	}

	// Compress one-way nodes.
	if (n7.count == 1) {
		// We track the old node pointer because Concatenate() might overwrite it.
		auto old_n7_node = node;

		// Concatenate the byte to the prefix.
		Prefix::Concat(art, prefix, n7.key[0], node.IsGate());
		n7.count--;
		Node::Free(art, old_n7_node);
	}
}

bool Node7Leaf::GetNextByte(uint8_t &byte) const {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] >= byte) {
			byte = key[i];
			return true;
		}
	}
	return false;
}

} // namespace duckdb
