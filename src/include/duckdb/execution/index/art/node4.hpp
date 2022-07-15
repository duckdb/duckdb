//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node4.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

class Node4 : public Node {
public:
	explicit Node4(size_t compression_length);
	uint8_t key[4];
	// Pointers to the child nodes
	SwizzleablePointer children[4];

public:
	//! Get position of a byte, returns -1 if not exists
	idx_t GetChildPos(uint8_t k) override;
	//! Get the position of the first child that is greater or equal to the specific byte, or DConstants::INVALID_INDEX
	//! if there are no children matching the criteria
	idx_t GetChildGreaterEqual(uint8_t k, bool &equal) override;
	//! Get the next position in the node, or DConstants::INVALID_INDEX if there is no next position
	idx_t GetNextPos(idx_t pos) override;
	//! Get Node4 Child
	Node *GetChild(ART &art, idx_t pos) override;
	//! Replace child pointer
	void ReplaceChildPointer(idx_t pos, Node *node) override;

	idx_t GetMin() override;

	//! Insert Leaf to the Node4
	static void Insert(Node *&node, uint8_t key_byte, Node *new_child);
	//! Remove Leaf from Node4
	static void Erase(Node *&node, int pos, ART &art);
};
} // namespace duckdb
