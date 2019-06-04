//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/node48.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "node.hpp"

namespace duckdb {

class Node48 : public Node {
public:
	Node48(ART &art);

	uint8_t childIndex[256];
	unique_ptr<Node> child[48];
public:
	//! Get Node48 Child
	unique_ptr<Node> *getChild(const uint8_t k);

	//! Get position of a byte, returns -1 if not exists
	int getPos(const uint8_t k);

	//! Get min value
	unique_ptr<Node> *getMin();

	//! Insert node in Node48
	static void insert(ART &art, unique_ptr<Node> &node, uint8_t keyByte, unique_ptr<Node> &child);

	//! Shrink to node 16
	static void erase(ART &art, unique_ptr<Node> &node, int pos);
};
} // namespace duckdb
