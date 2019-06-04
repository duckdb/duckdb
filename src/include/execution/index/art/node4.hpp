//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/node4.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "node.hpp"

namespace duckdb {

class Node4 : public Node {
public:
	Node4(ART &art);

	uint8_t key[4];
	unique_ptr<Node> child[4];
public:
	//! Get Node4 Child
	unique_ptr<Node> *getChild(const uint8_t k);

	//! Get position of a byte, returns -1 if not exists
	int getPos(const uint8_t k);

	//! Get min value in node
	unique_ptr<Node> *getMin();

	//! Insert Leaf to the Node4
	static void insert(ART &art, unique_ptr<Node> &node, uint8_t keyByte, unique_ptr<Node> &child);
	//! Remove Leaf from Node4
	static void erase(ART &art, unique_ptr<Node> &node, int pos);
};
} // namespace duckdb
