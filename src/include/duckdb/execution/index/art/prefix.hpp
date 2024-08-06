//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/prefix.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

class ARTKey;

//! Prefix is a wrapper class containing field to access a prefix.
//! On a segment, the prefix contains up to the ART's prefix size bytes
//! and an additional byte for the count.
//! If not inlined, then it also contains a Node pointer to a child node.
class Prefix {
public:
	static constexpr NType PREFIX = NType::PREFIX;
	static constexpr NType INLINED = NType::PREFIX_INLINED;

public:
	Prefix() = delete;
	Prefix(const ART &art, const Node ptr_p, const bool is_mutable = false, const bool set_in_memory = false);
	Prefix(unsafe_unique_ptr<FixedSizeAllocator> &allocator, const Node ptr_p, const idx_t count);

	data_ptr_t data;
	Node *ptr;
	bool in_memory;

public:
	static inline uint8_t Count(const ART &art) {
		return art.prefix_count;
	}
	static inline uint8_t Size(const ART &art) {
		return Count(art) + 1;
	}

public:
	//! Get a new inlined prefix.
	static void NewInlined(ART &art, Node &node, const ARTKey &key, idx_t depth, uint8_t count);
	//! Get a new chain of prefix nodes. The node parameter holds the tail of the chain.
	static void New(ART &art, reference<Node> &node, const ARTKey &key, idx_t depth, idx_t count);

	//! Free the node and its subtree.
	static void Free(ART &art, Node &node);

	//! Initializes a merge by incrementing the buffer ID of the prefix and its child node(s)
	static void InitializeMerge(ART &art, Node &node, const unsafe_vector<idx_t> &upper_bounds);

	//! Returns the row ID, if we can inline it, else -1.
	static row_t CanInline(ART &art, Node &parent, Node &node, uint8_t byte, const Node &child = Node());
	//! Concatenates parent_node -> byte -> child_node. Special-handling, if
	//! 1. the byte was in a gate node.
	//! 2. the byte was in PREFIX_INLINED.
	static void Concat(ART &art, Node &parent, const uint8_t byte, bool is_gate, const Node &child = Node());

	//! Traverse a prefix and a key until
	//! 1. a non-prefix node.
	//! 2. a mismatching byte.
	//! Early-out, if the next prefix is a gate node.
	static idx_t Traverse(ART &art, reference<const Node> &node, const ARTKey &key, idx_t &depth);
	static idx_t TraverseMutable(ART &art, reference<Node> &node, const ARTKey &key, idx_t &depth);

	//! Traverse two prefixes to find (1) that they match (so far), or (2) that they have a mismatching position,
	//! or (3) that one prefix contains the other prefix. This function aids in merging Nodes, and, therefore,
	//! the nodes are not const.
	static bool Traverse(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &pos, const bool in_gate);

	//! Returns the byte at position.
	static uint8_t GetByte(const ART &art, const Node &node, uint8_t pos);

	//! Removes the first n bytes from the prefix.
	//! Shifts all subsequent bytes by n. Frees empty prefix nodes.
	static void Reduce(ART &art, Node &node, const idx_t n);
	//! Splits the prefix at pos.
	//! prefix_node points to the node that replaces the split byte.
	//! child_node points to the remaining node after the split.
	//! Returns true, if a gate was freed.
	static bool Split(ART &art, reference<Node> &node, Node &child, uint8_t pos);

	//! Returns the string representation of the node, or only traverses and verifies the node and its subtree
	static string VerifyAndToString(ART &art, const Node &node, const bool only_verify);
	//! Count the number of prefixes.
	static void VerifyAllocations(ART &art, const Node &node, unordered_map<uint8_t, idx_t> &node_counts);

	//! Vacuum the child of the node.
	static void Vacuum(ART &art, Node &node, const unordered_set<uint8_t> &indexes);

	//! Transform the child of the node.
	static void TransformToDeprecated(ART &art, Node &node, unsafe_unique_ptr<FixedSizeAllocator> &allocator);

	//! Appends the other_prefix and all its subsequent prefix nodes to this prefix node.
	//! Also frees all copied/appended nodes
	void Append(ART &art, Node other);
	//! Appends the byte.
	Prefix Append(ART &art, uint8_t byte);

private:
	static Prefix NewInternal(ART &art, Node &node, const data_ptr_t data, uint8_t count, idx_t offset, NType type);
	static Prefix GetTail(ART &art, Node &node);
	static void PrependByte(ART &art, Node &node, uint8_t byte);
	static void ConcatGate(ART &art, Node &parent, uint8_t byte, const Node &child);
	static void ConcatChildIsGate(ART &art, Node &parent, uint8_t byte, const Node &child);
	static void ConcatInlinedPrefix(ART &art, Node &parent, uint8_t byte, const Node &child);
	static bool TraverseInlined(ART &art, Node &l_node, Node &r_node, idx_t &pos);
	static void ReduceInlinedPrefix(ART &art, Node &node, const idx_t n);
	static void ReducePrefix(ART &art, Node &node, const idx_t n);
	static bool SplitInlined(ART &art, reference<Node> &node, Node &child, uint8_t pos);
	Prefix TransformToDeprecatedAppend(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator, uint8_t byte);
};
} // namespace duckdb
