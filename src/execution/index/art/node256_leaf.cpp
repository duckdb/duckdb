#include "duckdb/execution/index/art/node256_leaf.hpp"

namespace duckdb {

Node256Leaf &Node256Leaf::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NType::NODE_256_LEAF).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_256_LEAF));
	auto &n256 = Node::RefMutable<Node256Leaf>(art, node, NType::NODE_256_LEAF);

	n256.count = 0;
	ValidityMask mask(&n256.mask[0]);
	mask.SetAllInvalid(Node::NODE_256_CAPACITY);
	return n256;
}

Node256Leaf &Node256Leaf::GrowNode15Leaf(ART &art, Node &node256_leaf, Node &node15_leaf) {
	auto &n15 = Node::RefMutable<Node15Leaf>(art, node15_leaf, NType::NODE_15_LEAF);
	auto &n256 = New(art, node256_leaf);
	if (node15_leaf.IsGate()) {
		node256_leaf.SetGate();
	}

	n256.count = n15.count;
	ValidityMask mask(&n256.mask[0]);
	for (idx_t i = 0; i < n15.count; i++) {
		mask.SetValid(n15.key[i]);
	}

	n15.count = 0;
	Node::Free(art, node15_leaf);
	return n256;
}

void Node256Leaf::InsertByte(ART &art, Node &node, const uint8_t byte) {
	D_ASSERT(node.HasMetadata());
	auto &n256 = Node::RefMutable<Node256Leaf>(art, node, NType::NODE_256_LEAF);

	n256.count++;
	ValidityMask mask(&n256.mask[0]);
	mask.SetValid(byte);
}

void Node256Leaf::DeleteByte(ART &art, Node &node, const uint8_t byte) {
	D_ASSERT(node.HasMetadata());
	auto &n256 = Node::RefMutable<Node256Leaf>(art, node, NType::NODE_256_LEAF);
	n256.count--;

	// Shrink node to Node15
	if (n256.count <= Node::NODE_48_SHRINK_THRESHOLD) {
		auto node256 = node;
		Node15Leaf::ShrinkNode256Leaf(art, node, node256);
	}
}

bool Node256Leaf::GetNextByte(uint8_t &byte) {
	// FIXME: Implement const validity masks.
	ValidityMask v_mask(&mask[0]);
	auto max = NumericLimits<uint8_t>().Maximum();
	for (auto i = byte; i <= max; i++) {
		if (v_mask.RowIsValid(i)) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
