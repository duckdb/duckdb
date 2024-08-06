//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node16.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Node16 holds up to 16 Node children sorted by their key byte.
class Node16 {
public:
	Node16() = delete;
	Node16(const Node16 &) = delete;
	Node16 &operator=(const Node16 &) = delete;

	//! Number of non-null children
	uint8_t count;
	//! Array containing all partial key bytes
	uint8_t key[Node::NODE_16_CAPACITY];
	//! Node pointers to the child nodes
	Node children[Node::NODE_16_CAPACITY];

public:
	//! Get a new Node16, might cause a new buffer allocation, and initialize it
	static Node16 &New(ART &art, Node &node);
	//! Free the node (and its subtree)
	static void Free(ART &art, Node &node);

	//! Initializes all the fields of the node while growing a Node4 to a Node16
	static Node16 &GrowNode4(ART &art, Node &node16, Node &node4);
	//! Initializes all fields of the node while shrinking a Node48 to a Node16
	static Node16 &ShrinkNode48(ART &art, Node &node16, Node &node48);

	//! Initializes a merge by incrementing the buffer IDs of the node
	void InitializeMerge(ART &art, const unsafe_vector<idx_t> &upper_bounds);

	//! Insert a child node at byte
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child node at byte
	static void DeleteChild(ART &art, Node &node, const uint8_t byte);

	//! Replace the child node at byte
	void ReplaceChild(const uint8_t byte, const Node child);

	//! Get the (immutable) child for the respective byte in the node
	const Node *GetChild(const uint8_t byte) const;
	//! Get the child for the respective byte in the node
	Node *GetChildMutable(const uint8_t byte);
	//! Get the first (immutable) child that is greater or equal to the specific byte
	const Node *GetNextChild(uint8_t &byte) const;
	//! Get the first child that is greater or equal to the specific byte
	Node *GetNextChildMutable(uint8_t &byte);

	//! Vacuum the children of the node
	void Vacuum(ART &art, const unordered_set<uint8_t> &indexes);
	//! Transform the children of the node.
	void TransformToDeprecated(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator);
	//! Recurse into the children to count each node type.
	void VerifyAllocations(ART &art, unordered_map<uint8_t, idx_t> &node_counts) const;
};
} // namespace duckdb
