//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/node256.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "node.hpp"

namespace duckdb {

class Node256 : public Node {
public:
	Node256(ART &art);

	unique_ptr<Node> child[256];
public:
	//! Get Node256 Child
	unique_ptr<Node> *getChild(const uint8_t k);

	//! Get position of a byte, returns -1 if not exists
	int getPos(const uint8_t k);

	//! Get min value
	unique_ptr<Node> *getMin();

	//! Insert node From Node256
	static void insert(ART &art, unique_ptr<Node> &node, uint8_t keyByte, unique_ptr<Node> &child);

	//! Shrink to node 48
	static void erase(ART &art, unique_ptr<Node> &node, int pos);
};
} // namespace duckdb
