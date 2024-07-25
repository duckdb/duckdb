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
	// TODO
}

void Node7Leaf::DeleteByte(ART &art, Node &node, Node &prefix, const uint8_t byte) {
	// TODO
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

string Node7Leaf::VerifyAndToString(ART &art, const bool only_verify) const {
	// TODO
	return "";
}

} // namespace duckdb
