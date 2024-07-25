#include "duckdb/execution/index/art/node256_leaf.hpp"

namespace duckdb {

Node256Leaf &Node256Leaf::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NType::NODE_256_LEAF).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_256_LEAF));
	auto &n256 = Node::RefMutable<Node256Leaf>(art, node, NType::NODE_256_LEAF);

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

	ValidityMask mask(&n256.mask[0]);
	for (idx_t i = 0; i < n15.count; i++) {
		mask.SetValid(n15.key[i]);
	}

	n15.count = 0;
	Node::Free(art, node15_leaf);
	return n256;
}

void Node256Leaf::InsertByte(ART &art, Node &node, const uint8_t byte) {
	// TODO
}

void Node256Leaf::DeleteByte(ART &art, Node &node, Node &prefix, const uint8_t byte) {
	// TODO
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

string Node256Leaf::VerifyAndToString(ART &art, const bool only_verify) const {
	// TODO
	return "";
}

} // namespace duckdb
