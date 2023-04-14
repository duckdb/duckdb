//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node48.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"
#include "duckdb/execution/index/art/fixed_size_allocator.hpp"

namespace duckdb {

//! Node48 holds up to 48 ARTNode children. It contains a child_index array which can be directly indexed by the key
//! byte, and which contains the position of the child node in the children array
class Node48 {
public:
	//! Number of non-null children
	uint8_t count;
	//! Compressed path (prefix)
	Prefix prefix;
	//! Array containing all possible partial key bytes, those not set have an EMPTY_MARKER
	uint8_t child_index[ARTNode::NODE_256_CAPACITY];
	//! ART node pointers to the child nodes
	ARTNode children[ARTNode::NODE_48_CAPACITY];

public:
	//! Get a new pointer to a node, might cause a new buffer allocation, and initialize it
	static Node48 *New(ART &art, ARTNode &node);
	//! Free the node (and its subtree)
	static void Free(ART &art, ARTNode &node);
	//! Get a pointer to the node
	static inline Node48 *Get(const ART &art, const ARTNode ptr) {
		return art.n48_nodes->Get<Node48>(ptr);
	}
	//! Initializes all the fields of the node while growing a Node16 to a Node48
	static Node48 *GrowNode16(ART &art, ARTNode &node48, ARTNode &node16);
	//! Initializes all fields of the node while shrinking a Node256 to a Node48
	static Node48 *ShrinkNode256(ART &art, ARTNode &node48, ARTNode &node256);

	//! Initializes a merge by incrementing the buffer IDs of the node
	void InitializeMerge(ART &art, const ARTFlags &flags);

	//! Insert a child node at byte
	static void InsertChild(ART &art, ARTNode &node, const uint8_t byte, const ARTNode child);
	//! Delete the child node at the respective byte
	static void DeleteChild(ART &art, ARTNode &node, const uint8_t byte);

	//! Replace the child node at the respective byte
	inline void ReplaceChild(const uint8_t byte, const ARTNode child) {
		D_ASSERT(child_index[byte] != ARTNode::EMPTY_MARKER);
		children[child_index[byte]] = child;
	}

	//! Get the child for the respective byte in the node
	inline ARTNode *GetChild(const uint8_t byte) {
		if (child_index[byte] != ARTNode::EMPTY_MARKER) {
			return &children[child_index[byte]];
		}
		return nullptr;
	}
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
