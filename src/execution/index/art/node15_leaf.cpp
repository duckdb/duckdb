#include "duckdb/execution/index/art/node15_leaf.hpp"

#include "duckdb/execution/index/art/node256_leaf.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node7_leaf.hpp"

namespace duckdb {

Node15Leaf &Node15Leaf::New(ART &art, Node &node) {
	return Node4::New<Node15Leaf>(art, node, NODE_15_LEAF);
}

Node15Leaf &Node15Leaf::GrowNode7Leaf(ART &art, Node &node15_leaf, Node &node7_leaf) {
	auto &n7 = Node::Ref<Node7Leaf>(art, node7_leaf, NType::NODE_7_LEAF);
	auto &n15 = New(art, node15_leaf);
	if (node7_leaf.IsGate()) {
		node15_leaf.SetGate();
	}

	n15.count = n7.count;
	for (uint8_t i = 0; i < n7.count; i++) {
		n15.key[i] = n7.key[i];
	}

	n7.count = 0;
	Node::Free(art, node7_leaf);
	return n15;
}

Node15Leaf &Node15Leaf::ShrinkNode256Leaf(ART &art, Node &node15_leaf, Node &node256_leaf) {
	auto &n15 = New(art, node15_leaf);
	auto &n256 = Node::Ref<Node256Leaf>(art, node256_leaf, NType::NODE_256_LEAF);
	if (node256_leaf.IsGate()) {
		node15_leaf.SetGate();
	}

	ValidityMask mask(&n256.mask[0]);
	for (uint16_t i = 0; i < Node::NODE_256_CAPACITY; i++) {
		if (mask.RowIsValid(i)) {
			n15.key[n15.count] = UnsafeNumericCast<uint8_t>(i);
			n15.count++;
		}
	}

	Node::Free(art, node256_leaf);
	return n15;
}

void Node15Leaf::InsertByte(ART &art, Node &node, const uint8_t byte) {
	// The node is full. Grow to Node256Leaf.
	auto &n15 = Node::Ref<Node15Leaf>(art, node, NODE_15_LEAF);
	if (n15.count == CAPACITY) {
		auto node15 = node;
		Node256Leaf::GrowNode15Leaf(art, node, node15);
		Node256Leaf::InsertByte(art, node, byte);
		return;
	}

	Node7Leaf::InsertByteInternal(art, n15, byte);
}

void Node15Leaf::DeleteByte(ART &art, Node &node, const uint8_t byte) {
	auto &n15 = Node7Leaf::DeleteByteInternal<Node15Leaf>(art, node, byte);

	// Shrink node to Node7.
	if (n15.count < Node::NODE_7_LEAF_CAPACITY) {
		auto node15 = node;
		Node7Leaf::ShrinkNode15Leaf(art, node, node15);
	}
}

bool Node15Leaf::GetNextByte(uint8_t &byte) const {
	return Node7Leaf::GetNextByte<const Node15Leaf>(*this, byte);
}

} // namespace duckdb
