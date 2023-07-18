//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node4.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Node4 holds up to four ARTNode children sorted by their key byte
class Node4 {
public:
	//! Number of non-null children
	uint8_t count;
	//! Array containing all partial key bytes
	uint8_t key[Node::NODE_4_CAPACITY];
	//! ART node pointers to the child nodes
	Node children[Node::NODE_4_CAPACITY];

public:
	//! Get a new Node4 node, might cause a new buffer allocation, and initialize it
	static Node4 &New(ART &art, Node &node);
	//! Free the node (and its subtree)
	static void Free(ART &art, Node &node);
	//! Get a reference to the node
	static inline Node4 &Get(const ART &art, const Node ptr) {
		D_ASSERT(!ptr.IsSerialized());
		return *Node::GetAllocator(art, NType::NODE_4).Get<Node4>(ptr);
	}
	//! Initializes all fields of the node while shrinking a Node16 to a Node4
	static Node4 &ShrinkNode16(ART &art, Node &node4, Node &node16);

	//! Initializes a merge by incrementing the buffer IDs of the child nodes
	void InitializeMerge(ART &art, const ARTFlags &flags);

	//! Insert a child node at byte
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child node at the respective byte
	static void DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte);

	//! Replace the child node at the respective byte
	void ReplaceChild(const uint8_t byte, const Node child);

	//! Get the child for the respective byte in the node
	optional_ptr<Node> GetChild(const uint8_t byte);
	//! Get the first child that is greater or equal to the specific byte
	optional_ptr<Node> GetNextChild(uint8_t &byte);

	//! Serialize this node
	BlockPointer Serialize(ART &art, MetaBlockWriter &writer);
	//! Deserialize this node
	void Deserialize(MetaBlockReader &reader);

	//! Vacuum the children of the node
	void Vacuum(ART &art, const ARTFlags &flags);
};
} // namespace duckdb
