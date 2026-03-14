#include "duckdb/execution/index/art/base_leaf.hpp"

#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/base_node.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/node256_leaf.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// BaseLeaf
//===--------------------------------------------------------------------===//

template <uint8_t CAPACITY, NType TYPE>
void BaseLeaf<CAPACITY, TYPE>::InsertByteInternal(BaseLeaf &n, const uint8_t byte) {
	// Still space. Insert the child.
	uint8_t child_pos = 0;
	while (child_pos < n.count && n.key[child_pos] < byte) {
		child_pos++;
	}

	// Move children backwards to make space.
	for (uint8_t i = n.count; i > child_pos; i--) {
		n.key[i] = n.key[i - 1];
	}

	n.key[child_pos] = byte;
	n.count++;
}

template <uint8_t CAPACITY, NType TYPE>
NodeHandle BaseLeaf<CAPACITY, TYPE>::DeleteByteInternal(ART &art, NodePointer &node, const uint8_t byte) {
	NodeHandle handle(art, node);
	auto &n = handle.Get<BaseLeaf<CAPACITY, TYPE>>();
	uint8_t child_pos = 0;

	for (; child_pos < n.count; child_pos++) {
		if (n.key[child_pos] == byte) {
			break;
		}
	}
	n.count--;

	// Possibly move children backwards.
	for (uint8_t i = child_pos; i < n.count; i++) {
		n.key[i] = n.key[i + 1];
	}
	return handle;
}

//===--------------------------------------------------------------------===//
// Node7Leaf
//===--------------------------------------------------------------------===//

void Node7Leaf::InsertByte(ART &art, NodePointer &node, const uint8_t byte) {
	{
		NodeHandle handle(art, node);
		auto &n7 = handle.Get<Node7Leaf>();

		if (n7.count != CAPACITY) {
			InsertByteInternal(n7, byte);
			return;
		}
	}
	// The node is full. Grow to Node15.
	auto node7 = node;
	Node15Leaf::GrowNode7Leaf(art, node, node7);
	Node15Leaf::InsertByte(art, node, byte);
}

void Node7Leaf::DeleteByte(ART &art, NodePointer &node, NodePointer &prefix, const uint8_t byte, const ARTKey &row_id) {
	idx_t remainder;
	{
		auto n7_handle = DeleteByteInternal(art, node, byte);
		auto &n7 = n7_handle.Get<Node7Leaf>();

		if (n7.count != 1) {
			return;
		}

		// Compress one-way nodes.
		D_ASSERT(node.GetGateStatus() == GateStatus::GATE_NOT_SET);

		// Get the remaining row ID.
		remainder = UnsafeNumericCast<idx_t>(row_id.GetRowId()) & AND_LAST_BYTE;
		remainder |= UnsafeNumericCast<idx_t>(n7.key[0]);

		// Free the prefix (nodes) and inline the remainder.
		if (prefix.GetType() == NType::PREFIX) {
			NodePointer::FreeTree(art, prefix);
			Leaf::New(prefix, UnsafeNumericCast<row_t>(remainder));
			return;
		}
	}
	// Free the Node7Leaf and inline the remainder.
	NodePointer::FreeNode(art, node);
	Leaf::New(node, UnsafeNumericCast<row_t>(remainder));
}

void Node7Leaf::ShrinkNode15Leaf(ART &art, NodePointer &node7_leaf, NodePointer &node15_leaf) {
	{
		auto n7_handle = New(art, node7_leaf);
		auto &n7 = n7_handle.Get<Node7Leaf>();

		NodeHandle n15_handle(art, node15_leaf);
		auto &n15 = n15_handle.Get<Node15Leaf>();

		node7_leaf.SetGateStatus(node15_leaf.GetGateStatus());

		n7.count = n15.count;
		for (uint8_t i = 0; i < n15.count; i++) {
			n7.key[i] = n15.key[i];
		}
	}
	NodePointer::FreeNode(art, node15_leaf);
}

//===--------------------------------------------------------------------===//
// Node15Leaf
//===--------------------------------------------------------------------===//

void Node15Leaf::InsertByte(ART &art, NodePointer &node, const uint8_t byte) {
	{
		NodeHandle n15_handle(art, node);
		auto &n15 = n15_handle.Get<Node15Leaf>();
		if (n15.count != CAPACITY) {
			InsertByteInternal(n15, byte);
			return;
		}
	}
	auto node15 = node;
	Node256Leaf::GrowNode15Leaf(art, node, node15);
	Node256Leaf::InsertByte(art, node, byte);
}

void Node15Leaf::DeleteByte(ART &art, NodePointer &node, const uint8_t byte) {
	{
		auto n15_handle = DeleteByteInternal(art, node, byte);
		auto &n15 = n15_handle.Get<Node15Leaf>();
		if (n15.count >= Node7Leaf::CAPACITY) {
			return;
		}
	}
	auto node15 = node;
	Node7Leaf::ShrinkNode15Leaf(art, node, node15);
}

void Node15Leaf::GrowNode7Leaf(ART &art, NodePointer &node15_leaf, NodePointer &node7_leaf) {
	{
		NodeHandle n7_handle(art, node7_leaf);
		auto &n7 = n7_handle.Get<Node7Leaf>();

		auto n15_handle = New(art, node15_leaf);
		auto &n15 = n15_handle.Get<Node15Leaf>();
		node15_leaf.SetGateStatus(node7_leaf.GetGateStatus());

		n15.count = n7.count;
		for (uint8_t i = 0; i < n7.count; i++) {
			n15.key[i] = n7.key[i];
		}
	}
	NodePointer::FreeNode(art, node7_leaf);
}

void Node15Leaf::ShrinkNode256Leaf(ART &art, NodePointer &node15_leaf, NodePointer &node256_leaf) {
	{
		auto n15_handle = New(art, node15_leaf);
		auto &n15 = n15_handle.Get<Node15Leaf>();

		NodeHandle n256_handle(art, node256_leaf);
		auto &n256 = n256_handle.Get<Node256Leaf>();

		node15_leaf.SetGateStatus(node256_leaf.GetGateStatus());

		ValidityMask mask(&n256.mask[0], Node256::CAPACITY);
		for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
			if (mask.RowIsValid(i)) {
				n15.key[n15.count] = UnsafeNumericCast<uint8_t>(i);
				n15.count++;
			}
		}
	}
	NodePointer::FreeNode(art, node256_leaf);
}

} // namespace duckdb
