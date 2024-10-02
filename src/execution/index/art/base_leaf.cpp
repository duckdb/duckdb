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
BaseLeaf<CAPACITY, TYPE> &BaseLeaf<CAPACITY, TYPE>::DeleteByteInternal(ART &art, Node &node, const uint8_t byte) {
	auto &n = Node::Ref<BaseLeaf>(art, node, node.GetType());
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
	return n;
}

//===--------------------------------------------------------------------===//
// Node7Leaf
//===--------------------------------------------------------------------===//

void Node7Leaf::InsertByte(ART &art, Node &node, const uint8_t byte) {
	// The node is full. Grow to Node15.
	auto &n7 = Node::Ref<Node7Leaf>(art, node, NODE_7_LEAF);
	if (n7.count == CAPACITY) {
		auto node7 = node;
		Node15Leaf::GrowNode7Leaf(art, node, node7);
		Node15Leaf::InsertByte(art, node, byte);
		return;
	}

	// Still space. Insert the child.
	uint8_t child_pos = 0;
	while (child_pos < n7.count && n7.key[child_pos] < byte) {
		child_pos++;
	}

	InsertByteInternal(n7, byte);
}

void Node7Leaf::DeleteByte(ART &art, Node &node, Node &prefix, const uint8_t byte, const ARTKey &row_id) {
	auto &n7 = DeleteByteInternal(art, node, byte);

	// Compress one-way nodes.
	if (n7.count == 1) {
		D_ASSERT(node.GetGateStatus() == GateStatus::GATE_NOT_SET);

		// Get the remaining row ID.
		auto remainder = UnsafeNumericCast<idx_t>(row_id.GetRowId()) & AND_LAST_BYTE;
		remainder |= UnsafeNumericCast<idx_t>(n7.key[0]);

		n7.count--;
		Node::Free(art, node);

		if (prefix.GetType() == NType::PREFIX) {
			Node::Free(art, prefix);
			Leaf::New(prefix, UnsafeNumericCast<row_t>(remainder));
		} else {
			Leaf::New(node, UnsafeNumericCast<row_t>(remainder));
		}
	}
}

void Node7Leaf::ShrinkNode15Leaf(ART &art, Node &node7_leaf, Node &node15_leaf) {
	auto &n7 = New(art, node7_leaf);
	auto &n15 = Node::Ref<Node15Leaf>(art, node15_leaf, NType::NODE_15_LEAF);
	node7_leaf.SetGateStatus(node15_leaf.GetGateStatus());

	n7.count = n15.count;
	for (uint8_t i = 0; i < n15.count; i++) {
		n7.key[i] = n15.key[i];
	}

	n15.count = 0;
	Node::Free(art, node15_leaf);
}

//===--------------------------------------------------------------------===//
// Node15Leaf
//===--------------------------------------------------------------------===//

void Node15Leaf::InsertByte(ART &art, Node &node, const uint8_t byte) {
	// The node is full. Grow to Node256Leaf.
	auto &n15 = Node::Ref<Node15Leaf>(art, node, NODE_15_LEAF);
	if (n15.count == CAPACITY) {
		auto node15 = node;
		Node256Leaf::GrowNode15Leaf(art, node, node15);
		Node256Leaf::InsertByte(art, node, byte);
		return;
	}

	InsertByteInternal(n15, byte);
}

void Node15Leaf::DeleteByte(ART &art, Node &node, const uint8_t byte) {
	auto &n15 = DeleteByteInternal(art, node, byte);

	// Shrink node to Node7.
	if (n15.count < Node7Leaf::CAPACITY) {
		auto node15 = node;
		Node7Leaf::ShrinkNode15Leaf(art, node, node15);
	}
}

void Node15Leaf::GrowNode7Leaf(ART &art, Node &node15_leaf, Node &node7_leaf) {
	auto &n7 = Node::Ref<Node7Leaf>(art, node7_leaf, NType::NODE_7_LEAF);
	auto &n15 = New(art, node15_leaf);
	node15_leaf.SetGateStatus(node7_leaf.GetGateStatus());

	n15.count = n7.count;
	for (uint8_t i = 0; i < n7.count; i++) {
		n15.key[i] = n7.key[i];
	}

	n7.count = 0;
	Node::Free(art, node7_leaf);
}

void Node15Leaf::ShrinkNode256Leaf(ART &art, Node &node15_leaf, Node &node256_leaf) {
	auto &n15 = New(art, node15_leaf);
	auto &n256 = Node::Ref<Node256Leaf>(art, node256_leaf, NType::NODE_256_LEAF);
	node15_leaf.SetGateStatus(node256_leaf.GetGateStatus());

	ValidityMask mask(&n256.mask[0]);
	for (uint16_t i = 0; i < Node256::CAPACITY; i++) {
		if (mask.RowIsValid(i)) {
			n15.key[n15.count] = UnsafeNumericCast<uint8_t>(i);
			n15.count++;
		}
	}

	Node::Free(art, node256_leaf);
}

} // namespace duckdb
