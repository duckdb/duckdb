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
#include "duckdb/common/unique_ptr.hpp"
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
class FixedSizeAllocator;

//! State for TransformToDeprecated operations
class TransformToDeprecatedState {
public:
	explicit TransformToDeprecatedState(unsafe_unique_ptr<FixedSizeAllocator> allocator_p)
	    : allocator(std::move(allocator_p)) {
	}

	TransformToDeprecatedState() = delete;
	TransformToDeprecatedState(const TransformToDeprecatedState &) = delete;
	TransformToDeprecatedState &operator=(const TransformToDeprecatedState &) = delete;
	TransformToDeprecatedState(TransformToDeprecatedState &&) = delete;
	TransformToDeprecatedState &operator=(TransformToDeprecatedState &&) = delete;

public:
	bool HasAllocator() const {
		return allocator != nullptr;
	}

	FixedSizeAllocator &GetAllocator() const {
		D_ASSERT(HasAllocator());
		return *allocator;
	}

	unsafe_unique_ptr<FixedSizeAllocator> TakeAllocator() {
		return std::move(allocator);
	}

private:
	//! Allocator for creating deprecated nodes.
	unsafe_unique_ptr<FixedSizeAllocator> allocator;
};

//! Options for ToString printing functions
struct ToStringOptions {
	// Indentation for root node.
	idx_t indent_level = 0;
	// Amount to increase idnentation when traversing to a child node.
	idx_t indent_amount = 4;
	bool inside_gate = false;
	bool display_ascii = false;
	// Optional key argument to only print the path along to a specific key.
	// This prints nodes along the path, as well as the child bytes, but doesn't traverse into children not on the path
	// to the optional key_path.
	// This works in conjunction with the depth_remaining and structure_only arguments.
	// Note that nested ARTs are printed in their entirety regardless.
	optional_ptr<const ARTKey> key_path = nullptr;
	idx_t key_depth = 0;
	// If we have a key_path argument, we only print along a certain path to a specified key. depth_remaining allows us
	// to short circuit that, and print the entire tree starting at a certain depth. So if we are traversing towards
	// the leaf for a key, we can start printing the entire tree again. This is useful to be able to see a region of the
	// ART around a specific leaf.
	idx_t depth_remaining = 0;
	bool print_deprecated_leaves = true;
	// Similar to key path, but don't print the other child bytes at each node along the path to the key, i.e. skip
	// printing node contents. This gives a very barebones skeleton of the node structure leading to a key, and this
	// can also be short circuited by depth_remaining.
	bool structure_only = false;

	ToStringOptions() = default;

	ToStringOptions(idx_t indent_level, bool inside_gate, bool display_ascii, optional_ptr<const ARTKey> key_path,
	                idx_t key_depth, idx_t depth_remaining, bool print_deprecated_leaves, bool structure_only,
	                idx_t indent_amount = 2)
	    : indent_level(indent_level), indent_amount(indent_amount), inside_gate(inside_gate),
	      display_ascii(display_ascii), key_path(key_path), key_depth(key_depth), depth_remaining(depth_remaining),
	      print_deprecated_leaves(print_deprecated_leaves), structure_only(structure_only) {
	}
};

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
	static void TransformToDeprecated(ART &art, Node &node, TransformToDeprecatedState &state);

	//! Returns the string representation of the node at indentation level.
	//!
	//! Parameters:
	//! - art: root node of tree being printed.
	//! - options: Printing options (see ToStringOptions struct for details).
	string ToString(ART &art, const ToStringOptions &options) const;

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

//! NodeHandle is a mutable wrapper to access and modify a node.
//! A segment handle is used for memory management and marks memory as modified.
//! For read-only access, use ConstNodeHandle instead.
template <class T>
class NodeHandle {
public:
	NodeHandle(ART &art, const Node node)
	    : handle(Node::GetAllocator(art, node.GetType()).GetHandle(node)), n(handle.GetRef<T>()) {
		handle.MarkModified();
	}
	NodeHandle() = delete;
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

//! ConstNodeHandle is a read-only wrapper to access a node.
//! A segment handle is used for memory management, but it is not marked as modified.
//! For mutable access, use NodeHandle instead.
template <class T>
class ConstNodeHandle {
public:
	ConstNodeHandle(const ART &art, const Node node)
	    : handle(Node::GetAllocator(art, node.GetType()).GetHandle(node)), n(handle.GetRef<T>()) {
	}
	ConstNodeHandle() = delete;
	ConstNodeHandle(const ConstNodeHandle &) = delete;
	ConstNodeHandle &operator=(const ConstNodeHandle &) = delete;
	ConstNodeHandle(ConstNodeHandle &&other) noexcept = delete;
	ConstNodeHandle &operator=(ConstNodeHandle &&other) noexcept = delete;

public:
	const T &Get() const {
		return n;
	}

private:
	SegmentHandle handle;
	const T &n;
};

} // namespace duckdb
