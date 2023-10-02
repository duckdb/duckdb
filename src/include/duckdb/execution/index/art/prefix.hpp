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

// classes
class ARTKey;

//! The Prefix is a special node type that contains up to PREFIX_SIZE bytes, and one byte for the count,
//! and a Node pointer. This pointer either points to a prefix node or another Node.
class Prefix {
public:
	//! Delete copy constructors, as any Prefix can never own its memory
	Prefix(const Prefix &) = delete;
	Prefix &operator=(const Prefix &) = delete;

	//! Up to PREFIX_SIZE bytes of prefix data and the count
	uint8_t data[Node::PREFIX_SIZE + 1];
	//! A pointer to the next Node
	Node ptr;

public:
	//! Get a new empty prefix node, might cause a new buffer allocation
	static Prefix &New(ART &art, Node &node);
	//! Create a new prefix node containing a single byte and a pointer to a next node
	static Prefix &New(ART &art, Node &node, uint8_t byte, const Node &next = Node());
	//! Get a new chain of prefix nodes, might cause new buffer allocations,
	//! with the node parameter holding the tail of the chain
	static void New(ART &art, reference<Node> &node, const ARTKey &key, const uint32_t depth, uint32_t count);
	//! Free the node (and its subtree)
	static void Free(ART &art, Node &node);

	//! Initializes a merge by incrementing the buffer ID of the prefix and its child node(s)
	static void InitializeMerge(ART &art, Node &node, const ARTFlags &flags);

	//! Appends a byte and a child_prefix to prefix. If there is no prefix, than it pushes the
	//! byte on top of child_prefix. If there is no child_prefix, then it creates a new
	//! prefix node containing that byte
	static void Concatenate(ART &art, Node &prefix_node, const uint8_t byte, Node &child_prefix_node);
	//! Traverse a prefix and a key until (1) encountering a non-prefix node, or (2) encountering
	//! a mismatching byte, in which case depth indexes the mismatching byte in the key
	static idx_t Traverse(ART &art, reference<const Node> &prefix_node, const ARTKey &key, idx_t &depth);
	//! Traverse a prefix and a key until (1) encountering a non-prefix node, or (2) encountering
	//! a mismatching byte, in which case depth indexes the mismatching byte in the key
	static idx_t TraverseMutable(ART &art, reference<Node> &prefix_node, const ARTKey &key, idx_t &depth);
	//! Traverse two prefixes to find (1) that they match (so far), or (2) that they have a mismatching position,
	//! or (3) that one prefix contains the other prefix. This function aids in merging Nodes, and, therefore,
	//! the nodes are not const
	static bool Traverse(ART &art, reference<Node> &l_node, reference<Node> &r_node, idx_t &mismatch_position);
	//! Returns the byte at position
	static inline uint8_t GetByte(const ART &art, const Node &prefix_node, const idx_t position) {
		auto &prefix = Node::Ref<const Prefix>(art, prefix_node, NType::PREFIX);
		D_ASSERT(position < Node::PREFIX_SIZE);
		D_ASSERT(position < prefix.data[Node::PREFIX_SIZE]);
		return prefix.data[position];
	}
	//! Removes the first n bytes from the prefix and shifts all subsequent bytes in the
	//! prefix node(s) by n. Frees empty prefix nodes
	static void Reduce(ART &art, Node &prefix_node, const idx_t n);
	//! Splits the prefix at position. prefix_node then references the ptr (if any bytes left before
	//! the split), or stays unchanged (no bytes left before the split). child_node references
	//! the node after the split, which is either a new prefix node, or ptr
	static void Split(ART &art, reference<Node> &prefix_node, Node &child_node, idx_t position);

	//! Returns the string representation of the node, or only traverses and verifies the node and its subtree
	static string VerifyAndToString(ART &art, const Node &node, const bool only_verify);

	//! Vacuum the child of the node
	static void Vacuum(ART &art, Node &node, const ARTFlags &flags);

private:
	//! Appends the byte to this prefix node, or creates a subsequent prefix node,
	//! if this node is full
	Prefix &Append(ART &art, const uint8_t byte);
	//! Appends the other_prefix and all its subsequent prefix nodes to this prefix node.
	//! Also frees all copied/appended nodes
	void Append(ART &art, Node other_prefix);
};
} // namespace duckdb
