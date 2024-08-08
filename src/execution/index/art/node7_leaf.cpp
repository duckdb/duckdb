#include "duckdb/execution/index/art/node7_leaf.hpp"

#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node15_leaf.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

Node7Leaf &Node7Leaf::New(ART &art, Node &node) {
	return Node4::New<Node7Leaf>(art, node, NODE_7_LEAF);
}

void Node7Leaf::InsertByte(ART &art, Node &node, const uint8_t byte) {
	// The node is full. Grow to Node15.
	auto &n7 = Node::Ref<Node7Leaf>(art, node, NODE_7_LEAF);
	if (n7.count == CAPACITY) {
		auto node7 = node;
		Node15Leaf::GrowNode7Leaf(art, node, node7);
		Node15Leaf::InsertByte(art, node, byte);
		return;
	}

	// Still space. Insert the child.
	uint8_t child_pos = 0;
	while (child_pos < n7.count && n7.key[child_pos] < byte) {
		child_pos++;
	}

	InsertByteInternal(art, n7, byte);
}

void Node7Leaf::DeleteByte(ART &art, Node &node, Node &prefix, const uint8_t byte) {
	auto &n7 = DeleteByteInternal<Node7Leaf>(art, node, byte);

	// Compress one-way nodes.
	if (n7.count == 1) {
		// Inline the leaf.
		auto row_id = Prefix::CanInline(art, prefix, node, n7.key[0]);
		if (row_id != ART::INVALID_ROW_ID) {
			Node::Free(art, prefix);
			Leaf::New(prefix, row_id);
			return;
		}

		// Concatenate the byte to the prefix.
		auto old_n7_node = node;
		Prefix::Concat(art, prefix, n7.key[0], node.IsGate());
		n7.count--;
		Node::Free(art, old_n7_node);
	}
}

Node7Leaf &Node7Leaf::ShrinkNode15Leaf(ART &art, Node &node7_leaf, Node &node15_leaf) {
	auto &n7 = New(art, node7_leaf);
	auto &n15 = Node::Ref<Node15Leaf>(art, node15_leaf, NType::NODE_15_LEAF);
	if (node15_leaf.IsGate()) {
		node7_leaf.SetGate();
	}

	n7.count = n15.count;
	for (uint8_t i = 0; i < n15.count; i++) {
		n7.key[i] = n15.key[i];
	}

	n15.count = 0;
	Node::Free(art, node15_leaf);
	return n7;
}

} // namespace duckdb
