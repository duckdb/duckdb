//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node4.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/swizzleable_pointer.hpp"

namespace duckdb {

class Node4 : public Node {
public:
	//! Empty Node4
	explicit Node4();
	//! Array containing all partial key bytes
	uint8_t key[4];
	//! ART pointers to the child nodes
	ARTPointer children[4];

public:
	static Node4 *New();
	//! Returns the memory size of the Node4
	idx_t MemorySize(ART &art, const bool &recurse) override;
	//! Get position of a byte, returns DConstants::INVALID_INDEX if not exists
	idx_t GetChildPos(uint8_t k) override;
	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	idx_t GetChildGreaterEqual(uint8_t k, bool &equal) override;
	//! Get the position of the minimum element in the node
	idx_t GetMin() override;
	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position
	idx_t GetNextPos(idx_t pos) override;
	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position
	idx_t GetNextPosAndByte(idx_t pos, uint8_t &byte) override;
	//! Get Node4 child
	Node *GetChild(ART &art, idx_t pos) override;
	//! Replace child pointer
	void ReplaceChildPointer(idx_t pos, Node *node) override;
	//! Returns the ART pointer at pos
	bool GetARTPointer(idx_t pos) override;

	//! Insert a new child node at key_byte into the Node4
	static void InsertChild(ART &art, Node *&node, uint8_t key_byte, Node *new_child);
	//! Erase the child at pos and (if necessary) merge with last child
	static void EraseChild(ART &art, Node *&node, idx_t pos);
	//! Returns the size (maximum capacity) of the Node4
	static idx_t GetSize();
};
} // namespace duckdb
