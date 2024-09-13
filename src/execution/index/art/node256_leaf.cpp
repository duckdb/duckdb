#include "duckdb/execution/index/art/node256_leaf.hpp"

#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

Node256Leaf &Node256Leaf::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NODE_256_LEAF).New();
	node.SetMetadata(static_cast<uint8_t>(NODE_256_LEAF));
	auto &n256 = Node::Ref<Node256Leaf>(art, node, NODE_256_LEAF);

	n256.count = 0;
	ValidityMask mask(&n256.mask[0]);
	mask.SetAllInvalid(CAPACITY);
	return n256;
}

void Node256Leaf::InsertByte(ART &art, Node &node, const uint8_t byte) {
	auto &n256 = Node::Ref<Node256Leaf>(art, node, NODE_256_LEAF);
	n256.count++;
	ValidityMask mask(&n256.mask[0]);
	mask.SetValid(byte);
}

void Node256Leaf::DeleteByte(ART &art, Node &node, const uint8_t byte) {
	auto &n256 = Node::Ref<Node256Leaf>(art, node, NODE_256_LEAF);
	n256.count--;
	ValidityMask mask(&n256.mask[0]);
	mask.SetInvalid(byte);

	// Shrink node to Node15
	if (n256.count <= Node48::SHRINK_THRESHOLD) {
		auto node256 = node;
		Node15Leaf::ShrinkNode256Leaf(art, node, node256);
	}
}

bool Node256Leaf::HasByte(uint8_t &byte) {
	ValidityMask v_mask(&mask[0]);
	return v_mask.RowIsValid(byte);
}

bool Node256Leaf::GetNextByte(uint8_t &byte) {
	ValidityMask v_mask(&mask[0]);
	for (uint16_t i = byte; i < CAPACITY; i++) {
		if (v_mask.RowIsValid(i)) {
			byte = UnsafeNumericCast<uint8_t>(i);
			return true;
		}
	}
	return false;
}

Node256Leaf &Node256Leaf::GrowNode15Leaf(ART &art, Node &node256_leaf, Node &node15_leaf) {
	auto &n15 = Node::Ref<Node15Leaf>(art, node15_leaf, NType::NODE_15_LEAF);
	auto &n256 = New(art, node256_leaf);
	node256_leaf.SetGateStatus(node15_leaf.GetGateStatus());

	n256.count = n15.count;
	ValidityMask mask(&n256.mask[0]);
	for (uint8_t i = 0; i < n15.count; i++) {
		mask.SetValid(n15.key[i]);
	}

	n15.count = 0;
	Node::Free(art, node15_leaf);
	return n256;
}

} // namespace duckdb
