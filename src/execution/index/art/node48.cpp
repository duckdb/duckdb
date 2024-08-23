#include "duckdb/execution/index/art/node48.hpp"

#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {

Node48 &Node48::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NODE_48).New();
	node.SetMetadata(static_cast<uint8_t>(NODE_48));
	auto &n48 = Node::Ref<Node48>(art, node, NODE_48);

	n48.count = 0;
	for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
		n48.child_index[i] = EMPTY_MARKER;
	}
	for (uint8_t i = 0; i < CAPACITY; i++) {
		n48.children[i].Clear();
	}

	return n48;
}

void Node48::Free(ART &art, Node &node) {
	auto &n48 = Node::Ref<Node48>(art, node, NODE_48);
	if (!n48.count) {
		return;
	}

	Iterator(n48, [&](Node &child) { Node::Free(art, child); });
}

void Node48::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	auto &n48 = Node::Ref<Node48>(art, node, NODE_48);

	// The node is full. Grow to Node256.
	if (n48.count == CAPACITY) {
		auto node48 = node;
		Node256::GrowNode48(art, node, node48);
		Node256::InsertChild(art, node, byte, child);
		return;
	}

	// Still space. Insert the child.
	uint8_t child_pos = n48.count;
	if (n48.children[child_pos].HasMetadata()) {
		// Find an empty position in the node list.
		child_pos = 0;
		while (n48.children[child_pos].HasMetadata()) {
			child_pos++;
		}
	}

	n48.children[child_pos] = child;
	n48.child_index[byte] = child_pos;
	n48.count++;
}

void Node48::DeleteChild(ART &art, Node &node, const uint8_t byte) {
	auto &n48 = Node::Ref<Node48>(art, node, NODE_48);

	// Free the child and decrease the count.
	Node::Free(art, n48.children[n48.child_index[byte]]);
	n48.child_index[byte] = EMPTY_MARKER;
	n48.count--;

	// Shrink to Node16.
	if (n48.count < SHRINK_THRESHOLD) {
		auto node48 = node;
		Node16::ShrinkNode48(art, node, node48);
	}
}

void Node48::ReplaceChild(const uint8_t byte, const Node child) {
	D_ASSERT(count >= SHRINK_THRESHOLD);

	auto status = children[child_index[byte]].GetGateStatus();
	children[child_index[byte]] = child;
	if (status == GateStatus::GATE_SET && child.HasMetadata()) {
		children[child_index[byte]].SetGateStatus(status);
	}
}

Node48 &Node48::GrowNode16(ART &art, Node &node48, Node &node16) {
	auto &n16 = Node::Ref<Node16>(art, node16, NType::NODE_16);
	auto &n48 = New(art, node48);
	node48.SetGateStatus(node16.GetGateStatus());

	n48.count = n16.count;
	for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
		n48.child_index[i] = EMPTY_MARKER;
	}
	for (uint8_t i = 0; i < n16.count; i++) {
		n48.child_index[n16.key[i]] = i;
		n48.children[i] = n16.children[i];
	}
	for (uint8_t i = n16.count; i < CAPACITY; i++) {
		n48.children[i].Clear();
	}

	n16.count = 0;
	Node::Free(art, node16);
	return n48;
}

Node48 &Node48::ShrinkNode256(ART &art, Node &node48, Node &node256) {
	auto &n48 = New(art, node48);
	auto &n256 = Node::Ref<Node256>(art, node256, NType::NODE_256);
	node48.SetGateStatus(node256.GetGateStatus());

	n48.count = 0;
	for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
		if (!n256.children[i].HasMetadata()) {
			n48.child_index[i] = EMPTY_MARKER;
			continue;
		}
		n48.child_index[i] = n48.count;
		n48.children[n48.count] = n256.children[i];
		n48.count++;
	}
	for (uint8_t i = n48.count; i < CAPACITY; i++) {
		n48.children[i].Clear();
	}

	n256.count = 0;
	Node::Free(art, node256);
	return n48;
}

} // namespace duckdb
