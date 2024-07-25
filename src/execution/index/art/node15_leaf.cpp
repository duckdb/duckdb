#include "duckdb/execution/index/art/node15_leaf.hpp"

namespace duckdb {

Node15Leaf &Node15Leaf::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NType::NODE_15_LEAF).New();
	node.SetMetadata(static_cast<uint8_t>(NType::NODE_15_LEAF));
	auto &n15 = Node::RefMutable<Node15Leaf>(art, node, NType::NODE_15_LEAF);

	n15.count = 0;
	return n15;
}

Node15Leaf &Node15Leaf::GrowNode7Leaf(ART &art, Node &node15_leaf, Node &node7_leaf) {
	auto &n7 = Node::RefMutable<Node7Leaf>(art, node7_leaf, NType::NODE_7_LEAF);
	auto &n15 = New(art, node15_leaf);
	if (node7_leaf.IsGate()) {
		node15_leaf.SetGate();
	}

	n15.count = n7.count;
	for (idx_t i = 0; i < n7.count; i++) {
		n15.key[i] = n7.key[i];
	}

	n7.count = 0;
	Node::Free(art, node7_leaf);
	return n15;
}

Node15Leaf &Node15Leaf::ShrinkNode256Leaf(ART &art, Node &node15_leaf, Node &node256_leaf) {
	auto &n15 = New(art, node15_leaf);
	if (node256_leaf.IsGate()) {
		node15_leaf.SetGate();
	}
	auto &n256 = Node::RefMutable<Node256Leaf>(art, node256_leaf, NType::NODE_256_LEAF);
	ValidityMask mask(&n256.mask[0]);

	auto max = NumericLimits<uint8_t>().Maximum();
	for (uint8_t i = 0; i <= max; i++) {
		if (mask.RowIsValid(i)) {
			n15.key[n15.count] = i;
			n15.count++;
		}
	}

	Node::Free(art, node256_leaf);
	return n15;
}

void Node15Leaf::InsertByte(ART &art, Node &node, const uint8_t byte) {
	// TODO
}

void Node15Leaf::DeleteByte(ART &art, Node &node, Node &prefix, const uint8_t byte) {
	// TODO
}

bool Node15Leaf::GetNextByte(uint8_t &byte) const {
	for (idx_t i = 0; i < count; i++) {
		if (key[i] >= byte) {
			byte = key[i];
			return true;
		}
	}
	return false;
}

string Node15Leaf::VerifyAndToString(ART &art, const bool only_verify) const {
	// TODO
	return "";
}

} // namespace duckdb
