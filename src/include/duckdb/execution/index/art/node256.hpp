//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node256.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/swizzleable_pointer.hpp"

namespace duckdb {

class Node256 : public Node {
public:
	//! Empty Node256
	explicit Node256();
	//! ART pointers to the child nodes
	ARTPointer children[256];

public:
	static Node256 *New();
	//! Returns the memory size of the Node256
	idx_t MemorySize(ART &art, const bool &recurse) override;
	//! Get position of a specific byte, returns DConstants::INVALID_INDEX if not exists
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
	//! Get Node256 child
	Node *GetChild(ART &art, idx_t pos) override;
	//! Replace child pointer
	void ReplaceChildPointer(idx_t pos, Node *node) override;
	//! Returns whether the child at pos is in memory
	bool ChildIsInMemory(idx_t pos) override;

	//! Insert a new child node at key_byte into the Node256
	static void InsertChild(ART &art, Node *&node, uint8_t key_byte, Node *new_child);
	//! Erase the child at pos and (if necessary) shrink to Node48
	static void EraseChild(ART &art, Node *&node, idx_t pos);
	//! Returns the size (maximum capacity) of the Node256
	static idx_t GetSize();
};
} // namespace duckdb
