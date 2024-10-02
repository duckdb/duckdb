#include "duckdb/execution/index/art/node256.hpp"

#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

Node256 &Node256::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NODE_256).New();
	node.SetMetadata(static_cast<uint8_t>(NODE_256));
	auto &n256 = Node::Ref<Node256>(art, node, NODE_256);

	n256.count = 0;
	for (uint16_t i = 0; i < CAPACITY; i++) {
		n256.children[i].Clear();
	}

	return n256;
}

void Node256::Free(ART &art, Node &node) {
	auto &n256 = Node::Ref<Node256>(art, node, NODE_256);
	if (!n256.count) {
		return;
	}

	Iterator(n256, [&](Node &child) { Node::Free(art, child); });
}

void Node256::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	auto &n256 = Node::Ref<Node256>(art, node, NODE_256);
	n256.count++;
	n256.children[byte] = child;
}

void Node256::DeleteChild(ART &art, Node &node, const uint8_t byte) {
	auto &n256 = Node::Ref<Node256>(art, node, NODE_256);

	// Free the child and decrease the count.
	Node::Free(art, n256.children[byte]);
	n256.count--;

	// Shrink to Node48.
	if (n256.count <= SHRINK_THRESHOLD) {
		auto node256 = node;
		Node48::ShrinkNode256(art, node, node256);
	}
}

void Node256::ReplaceChild(const uint8_t byte, const Node child) {
	D_ASSERT(count > SHRINK_THRESHOLD);

	auto status = children[byte].GetGateStatus();
	children[byte] = child;
	if (status == GateStatus::GATE_SET && child.HasMetadata()) {
		children[byte].SetGateStatus(status);
	}
}

Node256 &Node256::GrowNode48(ART &art, Node &node256, Node &node48) {
	auto &n48 = Node::Ref<Node48>(art, node48, NType::NODE_48);
	auto &n256 = New(art, node256);
	node256.SetGateStatus(node48.GetGateStatus());

	n256.count = n48.count;
	for (uint16_t i = 0; i < CAPACITY; i++) {
		if (n48.child_index[i] != Node48::EMPTY_MARKER) {
			n256.children[i] = n48.children[n48.child_index[i]];
		} else {
			n256.children[i].Clear();
		}
	}

	n48.count = 0;
	Node::Free(art, node48);
	return n256;
}

} // namespace duckdb
