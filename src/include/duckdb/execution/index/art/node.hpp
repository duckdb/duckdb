//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/index_pointer.hpp"

namespace duckdb {

enum class NType : uint8_t {
	PREFIX = 1,
	LEAF = 2,
	NODE_4 = 3,
	NODE_16 = 4,
	NODE_48 = 5,
	NODE_256 = 6,
	LEAF_INLINED = 7,
	NODE_7_LEAF = 8,
	NODE_15_LEAF = 9,
	NODE_256_LEAF = 10,
};

enum class GateStatus : uint8_t {
	GATE_NOT_SET = 0,
	GATE_SET = 1,
};

class ART;
class Prefix;
class ARTKey;

//! The Node is the pointer class of the ART index.
//! It inherits from the IndexPointer, and adds ART-specific functionality.
class Node : public IndexPointer {
	friend class Prefix;

public:
	//! A gate sets the leftmost bit of the metadata, binary: 1000-0000.
	static constexpr uint8_t AND_GATE = 0x80;
	static constexpr idx_t AND_ROW_ID = 0x00FFFFFFFFFFFFFF;

public:
	//! Get a new pointer to a node and initialize it.
	static void New(ART &art, Node &node, const NType type);
	//! Free the node.
	static void FreeNode(ART &art, Node &node);
	//! Free the node and its children.
	static void FreeTree(ART &art, Node &node);

	//! Get a reference to the allocator.
	static FixedSizeAllocator &GetAllocator(const ART &art, const NType type);
	//! Get the index of a node type's allocator.
	static uint8_t GetAllocatorIdx(const NType type);

	//! Get a reference to a node.
	template <class NODE>
	static inline NODE &Ref(const ART &art, const Node ptr, const NType type) {
		D_ASSERT(ptr.GetType() != NType::PREFIX);
		return *(GetAllocator(art, type).Get<NODE>(ptr, !std::is_const<NODE>::value));
	}
	//! Get a node pointer, if the node is in memory, else nullptr.
	template <class NODE>
	static inline unsafe_optional_ptr<NODE> InMemoryRef(const ART &art, const Node ptr, const NType type) {
		D_ASSERT(ptr.GetType() != NType::PREFIX);
		return GetAllocator(art, type).GetIfLoaded<NODE>(ptr);
	}

	//! Replace the child at byte.
	void ReplaceChild(const ART &art, const uint8_t byte, const Node child = Node()) const;
	//! Insert the child at byte.
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child = Node());
	//! Delete the child at byte.
	static void DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte, const GateStatus status,
	                        const ARTKey &row_id);

	//! Get the immutable child at byte.
	const unsafe_optional_ptr<Node> GetChild(ART &art, const uint8_t byte) const;
	//! Get the child at byte.
	unsafe_optional_ptr<Node> GetChildMutable(ART &art, const uint8_t byte, const bool unsafe = false) const;
	//! Get the first immutable child greater than or equal to the byte.
	const unsafe_optional_ptr<Node> GetNextChild(ART &art, uint8_t &byte) const;
	//! Returns true, if the byte exists, else false.
	bool HasByte(ART &art, const uint8_t byte) const;
	//! Get the first byte greater than or equal to the byte.
	bool GetNextByte(ART &art, uint8_t &byte) const;

	//! Traverses and verifies the node.
	void Verify(ART &art) const;
	//! Counts each node type.
	void VerifyAllocations(ART &art, unordered_map<uint8_t, idx_t> &node_counts) const;

	//! Returns the node type for a count.
	static NType GetNodeType(const idx_t count);

	//! Transform the node storage to deprecated storage.
	static void TransformToDeprecated(ART &art, Node &node,
	                                  unsafe_unique_ptr<FixedSizeAllocator> &deprecated_prefix_allocator);

	//! Returns the string representation of the node at indentation level.
	string ToString(ART &art, idx_t indent_level, bool inside_gate = false, bool display_ascii = false) const;

	//! Returns the node type.
	inline NType GetType() const {
		return NType(GetMetadata() & ~AND_GATE);
	}

	//! True, if the node is a Node4, Node16, Node48, or Node256.
	bool IsNode() const;
	//! True, if the node is a Node7Leaf, Node15Leaf, or Node256Leaf.
	bool IsLeafNode() const;
	//! True, if the node is any leaf.
	bool IsAnyLeaf() const;

	//! Get the row ID (8th to 63rd bit).
	inline row_t GetRowId() const {
		return UnsafeNumericCast<row_t>(Get() & AND_ROW_ID);
	}
	//! Set the row ID (8th to 63rd bit).
	inline void SetRowId(const row_t row_id) {
		Set((Get() & AND_METADATA) | UnsafeNumericCast<idx_t>(row_id));
	}

	//! Returns the gate status of a node.
	inline GateStatus GetGateStatus() const {
		return (GetMetadata() & AND_GATE) == 0 ? GateStatus::GATE_NOT_SET : GateStatus::GATE_SET;
	}
	//! Sets the gate status of a node.
	inline void SetGateStatus(const GateStatus status) {
		switch (status) {
		case GateStatus::GATE_SET:
			D_ASSERT(GetType() != NType::LEAF_INLINED);
			SetMetadata(GetMetadata() | AND_GATE);
			break;
		case GateStatus::GATE_NOT_SET:
			SetMetadata(GetMetadata() & ~AND_GATE);
			break;
		}
	}

	//! Assign operator.
	inline void operator=(const IndexPointer &ptr) {
		Set(ptr.Get());
	}

private:
	template <class NODE>
	static void TransformToDeprecatedInternal(ART &art, unsafe_optional_ptr<NODE> ptr,
	                                          unsafe_unique_ptr<FixedSizeAllocator> &allocator) {
		if (ptr) {
			NODE::Iterator(*ptr, [&](Node &child) { Node::TransformToDeprecated(art, child, allocator); });
		}
	}
};

//! NodeChildren holds the extracted bytes of a node, and their respective children.
//! The bytes and children are valid as long as the arena is valid,
//! even if the original node has been freed.
struct NodeChildren {
	NodeChildren() = delete;
	NodeChildren(array_ptr<uint8_t> bytes, array_ptr<Node> children) : bytes(bytes), children(children) {};

	array_ptr<uint8_t> bytes;
	array_ptr<Node> children;
};

template <class T>
class NodeHandle {
public:
	NodeHandle(ART &art, const Node node)
	    : handle(Node::GetAllocator(art, node.GetType()).GetHandle(node)), n(handle.GetRef<T>()) {
		handle.MarkModified();
	}

	NodeHandle(const NodeHandle &) = delete;
	NodeHandle &operator=(const NodeHandle &) = delete;

	NodeHandle(NodeHandle &&other) noexcept : handle(std::move(other.handle)), n(handle.GetRef<T>()) {
	}
	NodeHandle &operator=(NodeHandle &&other) noexcept = delete;

public:
	T &Get() {
		return n;
	}

private:
	SegmentHandle handle;
	T &n;
};

} // namespace duckdb
