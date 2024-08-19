#include "duckdb/execution/index/art/node4.hpp"

#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/leaf.hpp"

namespace duckdb {

void Node4::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	// The node is full. Grow to Node16.
	auto &n4 = Node::Ref<Node4>(art, node, NODE_4);
	if (n4.count == CAPACITY) {
		auto node4 = node;
		Node16::GrowNode4(art, node, node4);
		Node16::InsertChild(art, node, byte, child);
		return;
	}

	InsertChildInternal<Node4>(art, n4, byte, child);
}

void Node4::DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte, const bool in_gate) {
	auto &n4 = DeleteChildInternal<Node4>(art, node, byte);

	// Compress one-way nodes.
	if (n4.count == 1) {
		n4.count--;

		auto child = n4.children[0];
		auto remainder = n4.key[0];
		auto is_gate = node.IsGate();

		Node::Free(art, node);
		Prefix::Concat(art, prefix, remainder, is_gate, child, in_gate);
	}
}

Node4 &Node4::ShrinkNode16(ART &art, Node &node4, Node &node16) {
	auto &n4 = New<Node4>(art, node4, NODE_4);
	auto &n16 = Node::Ref<Node16>(art, node16, NType::NODE_16);
	if (node16.IsGate()) {
		node4.SetGate();
	}

	n4.count = n16.count;
	for (uint8_t i = 0; i < n16.count; i++) {
		n4.key[i] = n16.key[i];
		n4.children[i] = n16.children[i];
	}

	n16.count = 0;
	Node::Free(art, node16);
	return n4;
}

} // namespace duckdb
