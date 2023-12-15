//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node4.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Node4 holds up to four Node children sorted by their key byte
class Node4 {
public:
	//! Delete copy constructors, as any Node4 can never own its memory
	Node4(const Node4 &) = delete;
	Node4 &operator=(const Node4 &) = delete;

	//! Number of non-null children
	uint8_t count;
	//! Array containing all partial key bytes
	uint8_t key[Node::NODE_4_CAPACITY];
	//! Node pointers to the child nodes
	Node children[Node::NODE_4_CAPACITY];

public:
	//! Get a new Node4, might cause a new buffer allocation, and initialize it
	static Node4 &New(ART &art, Node &node);
	//! Free the node (and its subtree)
	static void Free(ART &art, Node &node);

	//! Initializes all fields of the node while shrinking a Node16 to a Node4
	static Node4 &ShrinkNode16(ART &art, Node &node4, Node &node16);

	//! Initializes a merge by incrementing the buffer IDs of the child nodes
	void InitializeMerge(ART &art, const ARTFlags &flags);

	//! Insert a child node at byte
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child node at byte
	static void DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte);

	//! Replace the child node at byte
	void ReplaceChild(const uint8_t byte, const Node child);

	//! Get the (immutable) child for the respective byte in the node
	optional_ptr<const Node> GetChild(const uint8_t byte) const;
	//! Get the child for the respective byte in the node
	optional_ptr<Node> GetChildMutable(const uint8_t byte);
	//! Get the first (immutable) child that is greater or equal to the specific byte
	optional_ptr<const Node> GetNextChild(uint8_t &byte) const;
	//! Get the first child that is greater or equal to the specific byte
	optional_ptr<Node> GetNextChildMutable(uint8_t &byte);

	//! Vacuum the children of the node
	void Vacuum(ART &art, const ARTFlags &flags);
};
} // namespace duckdb
