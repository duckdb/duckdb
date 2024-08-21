#include "duckdb/execution/index/art/base_node.hpp"

#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// BaseNode
//===--------------------------------------------------------------------===//

template <uint8_t CAPACITY, NType TYPE>
void BaseNode<CAPACITY, TYPE>::InsertChildInternal(BaseNode &n, const uint8_t byte, const Node child) {
	// Still space. Insert the child.
	uint8_t child_pos = 0;
	while (child_pos < n.count && n.key[child_pos] < byte) {
		child_pos++;
	}

	// Move children backwards to make space.
	for (uint8_t i = n.count; i > child_pos; i--) {
		n.key[i] = n.key[i - 1];
		n.children[i] = n.children[i - 1];
	}

	n.key[child_pos] = byte;
	n.children[child_pos] = child;
	n.count++;
}

template <uint8_t CAPACITY, NType TYPE>
BaseNode<CAPACITY, TYPE> &BaseNode<CAPACITY, TYPE>::DeleteChildInternal(ART &art, Node &node, const uint8_t byte) {
	auto &n = Node::Ref<BaseNode>(art, node, TYPE);

	uint8_t child_pos = 0;
	for (; child_pos < n.count; child_pos++) {
		if (n.key[child_pos] == byte) {
			break;
		}
	}

	// Free the child and decrease the count.
	Node::Free(art, n.children[child_pos]);
	n.count--;

	// Possibly move children backwards.
	for (uint8_t i = child_pos; i < n.count; i++) {
		n.key[i] = n.key[i + 1];
		n.children[i] = n.children[i + 1];
	}
	return n;
}

//===--------------------------------------------------------------------===//
// Node4
//===--------------------------------------------------------------------===//

void Node4::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	// The node is full. Grow to Node16.
	auto &n = Node::Ref<Node4>(art, node, NODE_4);
	if (n.count == CAPACITY) {
		auto node4 = node;
		Node16::GrowNode4(art, node, node4);
		Node16::InsertChild(art, node, byte, child);
		return;
	}

	InsertChildInternal(n, byte, child);
}

void Node4::DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte, const GateStatus status) {
	auto &n = DeleteChildInternal(art, node, byte);

	// Compress one-way nodes.
	if (n.count == 1) {
		n.count--;

		auto child = n.children[0];
		auto remainder = n.key[0];
		auto old_status = node.GetGateStatus();

		Node::Free(art, node);
		Prefix::Concat(art, prefix, remainder, old_status, child, status);
	}
}

void Node4::ShrinkNode16(ART &art, Node &node4, Node &node16) {
	auto &n4 = New(art, node4);
	auto &n16 = Node::Ref<Node16>(art, node16, NType::NODE_16);
	node4.SetGateStatus(node16.GetGateStatus());

	n4.count = n16.count;
	for (uint8_t i = 0; i < n16.count; i++) {
		n4.key[i] = n16.key[i];
		n4.children[i] = n16.children[i];
	}

	n16.count = 0;
	Node::Free(art, node16);
}

//===--------------------------------------------------------------------===//
// Node16
//===--------------------------------------------------------------------===//

void Node16::DeleteChild(ART &art, Node &node, const uint8_t byte) {
	auto &n = DeleteChildInternal(art, node, byte);

	// Shrink node to Node4.
	if (n.count < Node4::CAPACITY) {
		auto node16 = node;
		Node4::ShrinkNode16(art, node, node16);
	}
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

	InsertChildInternal(n16, byte, child);
}

void Node16::GrowNode4(ART &art, Node &node16, Node &node4) {
	auto &n4 = Node::Ref<Node4>(art, node4, NType::NODE_4);
	auto &n16 = New(art, node16);
	node16.SetGateStatus(node4.GetGateStatus());

	n16.count = n4.count;
	for (uint8_t i = 0; i < n4.count; i++) {
		n16.key[i] = n4.key[i];
		n16.children[i] = n4.children[i];
	}

	n4.count = 0;
	Node::Free(art, node4);
}

void Node16::ShrinkNode48(ART &art, Node &node16, Node &node48) {
	auto &n16 = New(art, node16);
	auto &n48 = Node::Ref<Node48>(art, node48, NType::NODE_48);
	node16.SetGateStatus(node48.GetGateStatus());

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
}

} // namespace duckdb
