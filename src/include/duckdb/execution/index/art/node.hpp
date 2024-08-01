//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/execution/index/index_pointer.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"

namespace duckdb {

enum class NType : uint8_t {
	PREFIX = 1,
	LEAF = 2,
	NODE_4 = 3,
	NODE_16 = 4,
	NODE_48 = 5,
	NODE_256 = 6,
	LEAF_INLINED = 7,
	PREFIX_INLINED = 8,
	NODE_7_LEAF = 9,
	NODE_15_LEAF = 10,
	NODE_256_LEAF = 11,
};

class ART;
class Prefix;
struct ARTFlags;

//! The Node is the pointer class of the ART index.
//! It inherits from the IndexPointer, and adds ART-specific functionality.
class Node : public IndexPointer {
public:
	static constexpr uint8_t NODE_48_SHRINK_THRESHOLD = 12;
	static constexpr uint8_t NODE_256_SHRINK_THRESHOLD = 36;

	static constexpr uint8_t NODE_4_CAPACITY = 4;
	static constexpr uint8_t NODE_7_LEAF_CAPACITY = 7;
	static constexpr uint8_t NODE_15_LEAF_CAPACITY = 15;
	static constexpr uint8_t NODE_16_CAPACITY = 16;
	static constexpr uint8_t NODE_48_CAPACITY = 48;
	static constexpr uint16_t NODE_256_CAPACITY = 256;

	static constexpr uint8_t EMPTY_MARKER = 48;
	static constexpr uint8_t LEAF_SIZE = 4; // Deprecated.
	static constexpr uint8_t PREFIX_SIZE = 15;
	static constexpr idx_t AND_ROW_ID = 0x00FFFFFFFFFFFFFF;

	//! A gate sets the leftmost bit of the metadata, binary: 1000-0000.
	static constexpr uint8_t AND_GATE = 0x80;

public:
	//! Get a new pointer to a node, might cause a new buffer allocation, and initialize it.
	static void New(ART &art, Node &node, const NType type);
	//! Free the node (and its subtree).
	static void Free(ART &art, Node &node);

	//! Get a reference to the allocator.
	static FixedSizeAllocator &GetAllocator(const ART &art, const NType type);
	//! Get the index of the matching allocator.
	static uint8_t GetAllocatorIdx(const NType type);

	//! Get an immutable reference to the node.
	template <class NODE>
	static inline const NODE &Ref(const ART &art, const Node ptr, const NType type) {
		return *(GetAllocator(art, type).Get<const NODE>(ptr, false));
	}
	//! Get a mutable reference to the node.
	template <class NODE>
	static inline NODE &RefMutable(const ART &art, const Node ptr, const NType type) {
		return *(GetAllocator(art, type).Get<NODE>(ptr));
	}
	//! Get a node pointer, if the node is in memory, else nullptr.
	template <class NODE>
	static inline NODE *GetInMemoryPtr(const ART &art, const Node ptr, const NType type) {
		return GetAllocator(art, type).GetInMemoryPtr<NODE>(ptr);
	}

	//! Replace the child at byte.
	void ReplaceChild(const ART &art, const uint8_t byte, const Node child) const;
	//! Insert the child at byte.
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child at byte.
	static void DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte);

	//! Get the immutable child at byte.
	const Node *GetChild(ART &art, const uint8_t byte) const;
	//! Get the child at byte.
	Node *GetChildMutable(ART &art, const uint8_t byte) const;
	//! Get the first immutable child greater or equal to the byte.
	const Node *GetNextChild(ART &art, uint8_t &byte) const;
	//! Get the first child greater or equal to the byte.
	Node *GetNextChildMutable(ART &art, uint8_t &byte) const;
	//! Get the next byte greater or equal to the byte.
	bool GetNextByte(ART &art, uint8_t &byte) const;

	//! Returns the string representation of the node, if only_verify is false.
	//! Else, it traverses and verifies the node and its subtree.
	string VerifyAndToString(ART &art, const bool only_verify) const;
	//! Returns the node leaf type for a count.
	static NType GetNodeLeafType(const idx_t count);
	//! Returns the node type for a count.
	static NType GetNodeType(const idx_t count);

	//! Initialize a merge by incrementing the buffer IDs of a node.
	void InitializeMerge(ART &art, const ARTFlags &flags);
	//! Merge a node into this node.
	bool Merge(ART &art, Node &other, const bool in_gate);
	bool MergeInternal(ART &art, Node &other, const bool in_gate);

	//! Vacuum all nodes exceeding their vacuum threshold.
	void Vacuum(ART &art, const ARTFlags &flags);

	//! Transform the node storage to deprecated storage.
	static void TransformToDeprecated(ART &art, Node &node);

	//! Returns the node type.
	inline NType GetType() const {
		return NType(GetMetadata() & ~AND_GATE);
	}
	//! True, if the node is a Node4, Node16, Node48, or Node256.
	inline bool IsNode() const {
		switch (GetType()) {
		case NType::NODE_4:
		case NType::NODE_16:
		case NType::NODE_48:
		case NType::NODE_256:
			return true;
		default:
			return false;
		}
	}
	//! True, if the node is a Node7Leaf, Node15Leaf, or Node256Leaf.
	inline bool IsLeafNode() const {
		switch (GetType()) {
		case NType::NODE_7_LEAF:
		case NType::NODE_15_LEAF:
		case NType::NODE_256_LEAF:
			return true;
		default:
			return false;
		}
	}
	//! True, if the node is any leaf.
	inline bool IsAnyLeaf() const {
		if (IsLeafNode()) {
			return true;
		}
		switch (GetType()) {
		case NType::LEAF_INLINED:
		case NType::LEAF:
		case NType::PREFIX_INLINED:
			return true;
		default:
			return false;
		}
	}
	//! True, if the node is a Prefix or PrefixInlined.
	inline bool IsPrefix() const {
		return GetType() == NType::PREFIX || GetType() == NType::PREFIX_INLINED;
	}

	//! Get the row ID (8th to 63rd bit)
	inline row_t GetRowId() const {
		return UnsafeNumericCast<row_t>(Get() & AND_ROW_ID);
	}
	//! Set the row ID (8th to 63rd bit)
	inline void SetRowId(const row_t row_id) {
		Set((Get() & AND_METADATA) | UnsafeNumericCast<idx_t>(row_id));
	}

	//! Returns true, if the node is a gate node.
	inline bool IsGate() const {
		return GetMetadata() & AND_GATE;
	}
	//! Turns the node into a gate node.
	inline void SetGate() {
		SetMetadata(GetMetadata() | AND_GATE);
	}
	//! Removes the gate flag from a node.
	inline void ResetGate() {
		SetMetadata(GetMetadata() & ~AND_GATE);
	}

	//! Assign operator
	inline void operator=(const IndexPointer &ptr) {
		Set(ptr.Get());
	}

private:
	//! Merge two nodes.
	bool MergeNodes(ART &art, Node &other, const bool in_gate);
	//! Reduce r_node's prefix and insert it into l_node, or recurse.
	bool PrefixContainsOther(ART &art, Node &l_node, Node &r_node, uint8_t pos, const bool in_gate);
	//! Split l_node and reduce r_node, and insert them into a new Node4.
	void MergeIntoNode4(ART &art, Node &l_node, Node &r_node, uint8_t pos);
	//! Merges two prefixes.
	bool MergePrefixes(ART &art, Node &other, const bool in_gate);
};
} // namespace duckdb
