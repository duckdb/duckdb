//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node16.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/prefix.hpp"

namespace duckdb {

class Node16 {
public:
	//! Number of non-null children
	uint16_t count;
	//! Compressed path (prefix)
	Prefix prefix;
	//! Array containing all partial key bytes
	uint8_t key[ARTNode::NODE_16_CAPACITY];
	//! ART node pointers to the child nodes
	ARTNode children[ARTNode::NODE_16_CAPACITY];

public:
	//! Initializes all the fields of the node
	static Node16 *Initialize(ART &art, const ARTNode &node);

	//! Insert a child node at byte
	static void InsertChild(ART &art, ARTNode &node, const uint8_t &byte, ARTNode &child);
	//! Delete the child node at pos
	static void DeleteChild(ART &art, ARTNode &node, idx_t pos);
	//! Delete the ART node and all possible child nodes
	static void Delete(ART &art, ARTNode &node);

	//! Replace a child node at pos
	void ReplaceChild(const idx_t &pos, ARTNode &child);

	//! Get the child at the specified position in the node. pos must be between [0, count)
	ARTNode GetChild(const idx_t &pos) const;
	//! Get the byte at the specified position
	uint8_t GetKeyByte(const idx_t &pos) const;
	//! Get the position of a child corresponding exactly to the specific byte, returns DConstants::INVALID_INDEX if
	//! the child does not exist
	idx_t GetChildPos(const uint8_t &byte) const;
	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	idx_t GetChildPosGreaterEqual(const uint8_t &byte, bool &inclusive) const;
	//! Get the position of the minimum child node in the node
	idx_t GetMinPos() const;
	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position. If pos ==
	//! DConstants::INVALID_INDEX, then the first valid position in the node is returned
	idx_t GetNextPos(idx_t pos) const;
	//! Get the next position and byte in the node, or DConstants::INVALID_INDEX if there is no next position. If pos ==
	//! DConstants::INVALID_INDEX, then the first valid position and byte in the node are returned
	idx_t GetNextPosAndByte(idx_t pos, uint8_t &byte) const;

	//! Serialize an ART node
	BlockPointer Serialize(ART &art, MetaBlockWriter &writer);
	//! Deserialize this node
	void Deserialize(ART &art, MetaBlockReader &reader);

	//! Returns the in-memory size
	idx_t MemorySize();
	//! Returns whether the child at pos is in-memory
	bool ChildIsInMemory(const idx_t &pos);
};
} // namespace duckdb
