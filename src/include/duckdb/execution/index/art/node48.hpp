//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node48.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/art_node.hpp"
#include "duckdb/execution/index/art/prefix.hpp"

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
	//! Free the node (and its subtree)
	static void Free(ART &art, ARTNode &node);
	//! Initializes all the fields of the node
	static Node48 *Initialize(ART &art, const ARTNode &node);
	//! Initializes all the fields of the node while growing a Node16 to a Node48
	static Node48 *GrowNode16(ART &art, ARTNode &node48, ARTNode &node16);
	//! Initializes all fields of the node while shrinking a Node256 to a Node48
	static Node48 *ShrinkNode256(ART &art, ARTNode &node48, ARTNode &node256);

	//! Initializes a merge by incrementing the buffer IDs of the node
	void InitializeMerge(ART &art, const vector<idx_t> &buffer_counts);

	//! Insert a child node at byte
	static void InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child);
	//! Delete the child node at pos
	static void DeleteChild(ART &art, ARTNode &node, idx_t position);

	//! Replace a child node at pos
	void ReplaceChild(const idx_t &position, ARTNode &child);

	//! Get the child at the specified position in the node. pos must be between [0, count)
	ARTNode *GetChild(const idx_t &position);
	//! Get the byte at the specified position
	uint8_t GetKeyByte(const idx_t &position) const;
	//! Get the position of a child corresponding exactly to the specific byte, returns DConstants::INVALID_INDEX if
	//! the child does not exist
	idx_t GetChildPosition(const uint8_t &byte) const;
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
};
} // namespace duckdb
