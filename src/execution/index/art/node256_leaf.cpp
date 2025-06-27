#include "duckdb/execution/index/art/node256_leaf.hpp"

#include "duckdb/execution/index/art/base_leaf.hpp"
#include "duckdb/execution/index/art/node48.hpp"

namespace duckdb {

NodeHandle<Node256Leaf> Node256Leaf::New(ART &art, Node &node) {
	node = Node::GetAllocator(art, NODE_256_LEAF).New();
	node.SetMetadata(static_cast<uint8_t>(NODE_256_LEAF));

	NodeHandle<Node256Leaf> handle(art, node);
	auto &n = handle.Get();

	n.count = 0;
	ValidityMask mask(&n.mask[0], Node256::CAPACITY);
	mask.SetAllInvalid(CAPACITY);

	return handle;
}

void Node256Leaf::InsertByte(ART &art, Node &node, const uint8_t byte) {
	NodeHandle<Node256Leaf> handle(art, node);
	auto &n = handle.Get();

	n.count++;
	ValidityMask mask(&n.mask[0], Node256::CAPACITY);
	mask.SetValid(byte);
}

void Node256Leaf::DeleteByte(ART &art, Node &node, const uint8_t byte) {
	{
		NodeHandle<Node256Leaf> handle(art, node);
		auto &n = handle.Get();

		n.count--;
		ValidityMask mask(&n.mask[0], Node256::CAPACITY);
		mask.SetInvalid(byte);

		if (n.count > Node48::SHRINK_THRESHOLD) {
			return;
		}
	}
	// Shrink node to Node15.
	auto node256 = node;
	Node15Leaf::ShrinkNode256Leaf(art, node, node256);
}

bool Node256Leaf::HasByte(uint8_t &byte) {
	ValidityMask v_mask(&mask[0], Node256::CAPACITY);
	return v_mask.RowIsValid(byte);
}

array_ptr<uint8_t> Node256Leaf::GetBytes(ArenaAllocator &arena) {
	auto mem = arena.AllocateAligned(sizeof(uint8_t) * count);
	array_ptr<uint8_t> bytes(mem, count);

	ValidityMask v_mask(&mask[0], Node256::CAPACITY);
	uint16_t ptr_idx = 0;
	for (uint16_t i = 0; i < CAPACITY; i++) {
		if (v_mask.RowIsValid(i)) {
			bytes[ptr_idx++] = UnsafeNumericCast<uint8_t>(i);
		}
	}

	return bytes;
}

bool Node256Leaf::GetNextByte(uint8_t &byte) {
	ValidityMask v_mask(&mask[0], Node256::CAPACITY);
	for (uint16_t i = byte; i < CAPACITY; i++) {
		if (v_mask.RowIsValid(i)) {
			byte = UnsafeNumericCast<uint8_t>(i);
			return true;
		}
	}

	return false;
}

void Node256Leaf::GrowNode15Leaf(ART &art, Node &node256_leaf, Node &node15_leaf) {
	{
		NodeHandle<Node15Leaf> n15_handle(art, node15_leaf);
		auto &n15 = n15_handle.Get();

		auto n256_handle = New(art, node256_leaf);
		auto &n256 = n256_handle.Get();
		node256_leaf.SetGateStatus(node15_leaf.GetGateStatus());

		n256.count = n15.count;
		ValidityMask mask(&n256.mask[0], Node256::CAPACITY);
		for (uint8_t i = 0; i < n15.count; i++) {
			mask.SetValid(n15.key[i]);
		}
		n15.count = 0;
	}
	Node::Free(art, node15_leaf);
}

} // namespace duckdb
