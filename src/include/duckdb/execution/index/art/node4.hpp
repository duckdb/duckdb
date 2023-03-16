//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node4.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

// classes
class ART;

//! Node4 holds up to four ARTNode children sorted by their key byte
class Node4 {
public:
	//! Number of non-null children
	uint8_t count;
	//! Compressed path (prefix)
	Prefix prefix;
	//! Array containing all partial key bytes
	uint8_t key[ARTNode::NODE_4_CAPACITY];
	//! ART node pointers to the child nodes
	ARTNode children[ARTNode::NODE_4_CAPACITY];

public:
	//! Get a new pointer to a node, might cause a new buffer allocation, and initialize it
	static Node4 *New(ART &art, ARTNode &node);
	//! Free the node (and its subtree)
	static void Free(ART &art, ARTNode &node);
	//! Initializes all fields of the node while shrinking a Node16 to a Node4
	static Node4 *ShrinkNode16(ART &art, ARTNode &node4, ARTNode &node16);

	//! Initializes a merge by incrementing the buffer IDs of the node
	void InitializeMerge(ART &art, const vector<idx_t> &buffer_counts);

	//! Insert a child node at byte
	static void InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child);
	//! Delete the child node at pos
	static void DeleteChild(ART &art, ARTNode &node, idx_t position);

	//! Replace a child node at pos
	inline void ReplaceChild(const idx_t &position, ARTNode &child) {
		D_ASSERT(position < ARTNode::NODE_4_CAPACITY);
		children[position] = child;
	}

	//! Get the child at the specified position in the node. pos must be between [0, count)
	inline ARTNode *GetChild(const idx_t &position) {
		D_ASSERT(position < count);
		return &children[position];
	}
	//! Get the byte at the specified position
	inline uint8_t GetKeyByte(const idx_t &position) const {
		D_ASSERT(position < count);
		return key[position];
	}
	//! Get the position of a child corresponding exactly to the specific byte, returns DConstants::INVALID_INDEX if
	//! the child does not exist
	idx_t GetChildPosition(const uint8_t &byte) const;
	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	idx_t GetChildPositionGreaterEqual(const uint8_t &byte, bool &inclusive) const;
	//! Get the position of the minimum child node in the node
	inline idx_t GetMinPosition() const {
		return 0;
	}
	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position. If pos ==
	//! DConstants::INVALID_INDEX, then the first valid position in the node is returned
	idx_t GetNextPosition(idx_t position) const;
	//! Get the next position and byte in the node, or DConstants::INVALID_INDEX if there is no next position. If pos ==
	//! DConstants::INVALID_INDEX, then the first valid position and byte in the node are returned
	idx_t GetNextPositionAndByte(idx_t position, uint8_t &byte) const;

	//! Serialize an ART node
	BlockPointer Serialize(ART &art, MetaBlockWriter &writer);
	//! Deserialize this node
	void Deserialize(ART &art, MetaBlockReader &reader);

	//! Vacuum the children of the node
	void Vacuum(ART &art, const vector<bool> &vacuum_nodes);
};
} // namespace duckdb
