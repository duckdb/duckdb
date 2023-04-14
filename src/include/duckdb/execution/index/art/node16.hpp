//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node16.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/fixed_size_allocator.hpp"

namespace duckdb {

//! Node16 holds up to 16 ARTNode children sorted by their key byte
class Node16 {
public:
	//! Number of non-null children
	uint8_t count;
	//! Compressed path (prefix)
	Prefix prefix;
	//! Array containing all partial key bytes
	uint8_t key[ARTNode::NODE_16_CAPACITY];
	//! ART node pointers to the child nodes
	ARTNode children[ARTNode::NODE_16_CAPACITY];

public:
	//! Get a new pointer to a node, might cause a new buffer allocation, and initialize it
	static Node16 *New(ART &art, ARTNode &node);
	//! Free the node (and its subtree)
	static void Free(ART &art, ARTNode &node);
	//! Get a pointer to the node
	static inline Node16 *Get(const ART &art, const ARTNode ptr) {
		return art.n16_nodes->Get<Node16>(ptr);
	}
	//! Initializes all the fields of the node while growing a Node4 to a Node16
	static Node16 *GrowNode4(ART &art, ARTNode &node16, ARTNode &node4);
	//! Initializes all fields of the node while shrinking a Node48 to a Node16
	static Node16 *ShrinkNode48(ART &art, ARTNode &node16, ARTNode &node48);

	//! Initializes a merge by incrementing the buffer IDs of the node
	void InitializeMerge(ART &art, const ARTFlags &flags);

	//! Insert a child node at byte
	static void InsertChild(ART &art, ARTNode &node, const uint8_t byte, const ARTNode child);
	//! Delete the child node at the respective byte
	static void DeleteChild(ART &art, ARTNode &node, const uint8_t byte);

	//! Replace the child node at the respective byte
	void ReplaceChild(const uint8_t byte, const ARTNode child);

	//! Get the child for the respective byte in the node
	ARTNode *GetChild(const uint8_t byte);
	//! Get the first child that is greater or equal to the specific byte
	ARTNode *GetNextChild(uint8_t &byte);

	//! Serialize an ART node
	BlockPointer Serialize(ART &art, MetaBlockWriter &writer);
	//! Deserialize this node
	void Deserialize(ART &art, MetaBlockReader &reader);

	//! Vacuum the children of the node
	void Vacuum(ART &art, const ARTFlags &flags);
};
} // namespace duckdb
