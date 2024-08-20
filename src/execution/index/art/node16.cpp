#include "duckdb/execution/index/art/node16.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

Node16 &Node16::New(ART &art, Node &node) {
	return Node4::New<Node16>(art, node, NODE_16);
}

void Node16::Free(ART &art, Node &node) {
	Node4::Free<Node16>(art, node);
}

void Node16::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	// The node is full. Grow to Node48.
	auto &n16 = Node::Ref<Node16>(art, node, NODE_16);
	if (n16.count == CAPACITY) {
		auto node16 = node;
		Node48::GrowNode16(art, node, node16);
		Node48::InsertChild(art, node, byte, child);
		return;
	}

	Node4::InsertChildInternal<Node16>(art, n16, byte, child);
}

void Node16::DeleteChild(ART &art, Node &node, const uint8_t byte) {
	auto &n16 = Node4::DeleteChildInternal<Node16>(art, node, byte);

	// Shrink node to Node4.
	if (n16.count < Node4::CAPACITY) {
		auto node16 = node;
		Node4::ShrinkNode16(art, node, node16);
	}
}

void Node16::ReplaceChild(const uint8_t byte, const Node child) {
	D_ASSERT(count >= Node4::CAPACITY);
	Node4::ReplaceChild(*this, byte, child);
}

Node16 &Node16::GrowNode4(ART &art, Node &node16, Node &node4) {
	auto &n4 = Node::Ref<Node4>(art, node4, NType::NODE_4);
	auto &n16 = New(art, node16);
	if (node4.IsGate()) {
		node16.SetGate();
	}

	n16.count = n4.count;
	for (uint8_t i = 0; i < n4.count; i++) {
		n16.key[i] = n4.key[i];
		n16.children[i] = n4.children[i];
	}

	n4.count = 0;
	Node::Free(art, node4);
	return n16;
}

Node16 &Node16::ShrinkNode48(ART &art, Node &node16, Node &node48) {
	auto &n16 = New(art, node16);
	auto &n48 = Node::Ref<Node48>(art, node48, NType::NODE_48);
	if (node48.IsGate()) {
		node16.SetGate();
	}

	n16.count = 0;
	for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
		if (n48.child_index[i] != Node48::EMPTY_MARKER) {
			n16.key[n16.count] = UnsafeNumericCast<uint8_t>(i);
			n16.children[n16.count] = n48.children[n48.child_index[i]];
			n16.count++;
		}
	}

	n48.count = 0;
	Node::Free(art, node48);
	return n16;
}

} // namespace duckdb
