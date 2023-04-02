//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node256.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

// classes
class ART;

//! Node256 holds up to 256 ARTNode children which can be directly indexed by the key byte
class Node256 {
public:
	//! Number of non-null children
	uint16_t count;
	//! Compressed path (prefix)
	Prefix prefix;
	//! ART node pointers to the child nodes
	ARTNode children[ARTNode::NODE_256_CAPACITY];

public:
	//! Get a new pointer to a node, might cause a new buffer allocation, and initialize it
	static Node256 *New(ART &art, ARTNode &node);
	//! Free the node (and its subtree)
	static void Free(ART &art, ARTNode &node);
	//! Initializes all the fields of the node while growing a Node48 to a Node256
	static Node256 *GrowNode48(ART &art, ARTNode &node256, ARTNode &node48);

	//! Initializes a merge by incrementing the buffer IDs of the node
	void InitializeMerge(ART &art, const vector<idx_t> &buffer_counts);

	//! Insert a child node at byte
	static void InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child);
	//! Delete the child node at pos
	static void DeleteChild(ART &art, ARTNode &node, idx_t position);

	//! Replace a child node at pos
	inline void ReplaceChild(const idx_t &position, ARTNode &child) {
		D_ASSERT(position < ARTNode::NODE_256_CAPACITY);
		children[position] = child;
	}

	//! Get the child at the specified position in the node. pos must be between [0, count)
	inline ARTNode *GetChild(const idx_t &position) {
		D_ASSERT(position < ARTNode::NODE_256_CAPACITY);
		return &children[position];
	}
	//! Get the byte at the specified position
	inline uint8_t GetKeyByte(const idx_t &position) const {
		D_ASSERT(position < ARTNode::NODE_256_CAPACITY);
		return position;
	}
	//! Get the position of a child corresponding exactly to the specific byte, returns DConstants::INVALID_INDEX if
	//! the child does not exist
	inline idx_t GetChildPosition(const uint8_t &byte) const {
		if (children[byte]) {
			return byte;
		}
		return DConstants::INVALID_INDEX;
	}
	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	idx_t GetChildPositionGreaterEqual(const uint8_t &byte, bool &inclusive) const;
	//! Get the position of the minimum child node in the node
	idx_t GetMinPosition() const;
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
	void Vacuum(ART &art, const vector<bool> &vacuum_flags);
};
} // namespace duckdb
