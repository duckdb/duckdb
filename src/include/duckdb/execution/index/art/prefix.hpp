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

//! PrefixInlined is a special node containing up to PREFIX_SIZE bytes and a byte for the count.
class PrefixInlined {
public:
	static constexpr NType PREFIX = NType::PREFIX;
	static constexpr NType INLINED = NType::PREFIX_INLINED;

public:
	PrefixInlined() = delete;
	PrefixInlined(const PrefixInlined &) = delete;
	PrefixInlined &operator=(const PrefixInlined &) = delete;

	uint8_t data[Node::PREFIX_SIZE + 1];

public:
	//! Get a new inlined prefix.
	static void New(ART &art, Node &node, const ARTKey &key, uint32_t depth, uint8_t count);
};

//! Prefix additionally contains a Node pointer.
class Prefix : public PrefixInlined {
public:
	Node ptr;

public:
	//! Get a new chain of prefix nodes. The node parameter holds the tail of the chain.
	static void New(ART &art, reference<Node> &node, const ARTKey &key, uint32_t depth, uint32_t count);

	//! Free the node and its subtree.
	static void Free(ART &art, Node &node);

	//! Initializes a merge by incrementing the buffer ID of the prefix and its child node(s)
	static void InitializeMerge(ART &art, Node &node, const ARTFlags &flags);

	//! Concatenates parent_node -> byte -> child_node. Special-handling, if
	//! 1. the byte was in a gate node.
	//! 2. the byte was in PREFIX_INLINED.
	static void Concat(ART &art, Node &parent, const uint8_t byte, bool is_gate, const Node &child = Node());

	//! Traverse a prefix and a key until
	//! 1. a non-prefix node.
	//! 2. a mismatching byte.
	//! Early-out, if the next prefix is a gate node.
	template <class PREFIX, class NODE>
	static idx_t Traverse(ART &art, reference<NODE> &node, const ARTKey &key, idx_t &depth,
	                      PREFIX &(*func)(const ART &art, const Node ptr, const NType type));

	//! Traverse two prefixes to find (1) that they match (so far), or (2) that they have a mismatching position,
	//! or (3) that one prefix contains the other prefix. This function aids in merging Nodes, and, therefore,
	//! the nodes are not const.
	static bool Traverse(ART &art, reference<Node> &l, reference<Node> &r, idx_t &mismatch_pos, bool inside_gate);

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

	//! Vacuum the child of the node
	static void Vacuum(ART &art, Node &node, const ARTFlags &flags);

	//! Transform the child of the node.
	static void TransformToDeprecated(ART &art, Node &node);

	//! Appends the other_prefix and all its subsequent prefix nodes to this prefix node.
	//! Also frees all copied/appended nodes
	void Append(ART &art, Node other);
	//! Appends the byte.
	Prefix &Append(ART &art, uint8_t byte);
};
} // namespace duckdb
