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
NodeHandle<BaseNode<CAPACITY, TYPE>> BaseNode<CAPACITY, TYPE>::DeleteChildInternal(ART &art, Node &node,
                                                                                   const uint8_t byte) {
	NodeHandle<BaseNode<CAPACITY, TYPE>> handle(art, node);
	auto &n = handle.Get();

	uint8_t child_pos = 0;
	for (; child_pos < n.count; child_pos++) {
		if (n.key[child_pos] == byte) {
			break;
		}
	}

	// Free the child and decrease the count.
	Node::FreeTree(art, n.children[child_pos]);
	n.count--;

	// Possibly move children backwards.
	for (uint8_t i = child_pos; i < n.count; i++) {
		n.key[i] = n.key[i + 1];
		n.children[i] = n.children[i + 1];
	}

	return handle;
}

//===--------------------------------------------------------------------===//
// Node4
//===--------------------------------------------------------------------===//

void Node4::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	{
		NodeHandle<Node4> handle(art, node);
		auto &n = handle.Get();

		if (n.count != CAPACITY) {
			InsertChildInternal(n, byte, child);
			return;
		}
	}
	// The node is full.
	// Grow to Node16.
	auto node4 = node;
	Node16::GrowNode4(art, node, node4);
	Node16::InsertChild(art, node, byte, child);
}

void Node4::DeleteChild(ART &art, Node &node, Node &parent, const uint8_t byte, const GateStatus status) {
	Node child;
	uint8_t remaining_byte;

	{
		auto handle = DeleteChildInternal(art, node, byte);
		auto &n = handle.Get();

		if (n.count != 1) {
			return;
		}

		// Compress one-way nodes.
		child = n.children[0];
		remaining_byte = n.key[0];
	}

	auto prev_node4_status = node.GetGateStatus();
	Node::FreeNode(art, node);
	Prefix::Concat(art, parent, node, child, remaining_byte, prev_node4_status);
}

void Node4::ShrinkNode16(ART &art, Node &node4, Node &node16) {
	{
		auto n4_handle = New(art, node4);
		auto &n4 = n4_handle.Get();
		node4.SetGateStatus(node16.GetGateStatus());

		NodeHandle<Node16> n16_handle(art, node16);
		auto &n16 = n16_handle.Get();

		n4.count = n16.count;
		for (uint8_t i = 0; i < n16.count; i++) {
			n4.key[i] = n16.key[i];
			n4.children[i] = n16.children[i];
		}
	}
	Node::FreeNode(art, node16);
}

//===--------------------------------------------------------------------===//
// Node16
//===--------------------------------------------------------------------===//

void Node16::InsertChild(ART &art, Node &node, const uint8_t byte, const Node child) {
	{
		NodeHandle<Node16> handle(art, node);
		auto &n = handle.Get();
		if (n.count != CAPACITY) {
			InsertChildInternal(n, byte, child);
			return;
		}
	}
	// The node is full.
	// Grow to Node48.
	auto node16 = node;
	Node48::GrowNode16(art, node, node16);
	Node48::InsertChild(art, node, byte, child);
}

void Node16::DeleteChild(ART &art, Node &node, const uint8_t byte) {
	{
		auto handle = DeleteChildInternal(art, node, byte);
		auto &n = handle.Get();
		if (n.count >= Node4::CAPACITY) {
			return;
		}
	}
	// Shrink node to Node4.
	auto node16 = node;
	Node4::ShrinkNode16(art, node, node16);
}

void Node16::GrowNode4(ART &art, Node &node16, Node &node4) {
	{
		NodeHandle<Node4> n4_handle(art, node4);
		auto &n4 = n4_handle.Get();

		auto n16_handle = New(art, node16);
		auto &n16 = n16_handle.Get();
		node16.SetGateStatus(node4.GetGateStatus());

		n16.count = n4.count;
		for (uint8_t i = 0; i < n4.count; i++) {
			n16.key[i] = n4.key[i];
			n16.children[i] = n4.children[i];
		}
	}
	Node::FreeNode(art, node4);
}

void Node16::ShrinkNode48(ART &art, Node &node16, Node &node48) {
	{
		auto n16_handle = New(art, node16);
		auto &n16 = n16_handle.Get();
		node16.SetGateStatus(node48.GetGateStatus());

		NodeHandle<Node48> n48_handle(art, node48);
		auto &n48 = n48_handle.Get();

		n16.count = 0;
		for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
			if (n48.child_index[i] != Node48::EMPTY_MARKER) {
				n16.key[n16.count] = UnsafeNumericCast<uint8_t>(i);
				n16.children[n16.count] = n48.children[n48.child_index[i]];
				n16.count++;
			}
		}
	}
	Node::FreeNode(art, node48);
}

} // namespace duckdb
