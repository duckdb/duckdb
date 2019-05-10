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
	uint8_t key[4];
	Node *child[4];

	Node4(uint8_t maxPrefixLength) : Node(NodeType::N4, maxPrefixLength) {
		memset(key, 0, sizeof(key));
		memset(child, 0, sizeof(child));
	}

	//! Get Node4 Child
	Node **getChild(const uint8_t k);
	//! Insert Leaf to the Node4
	static void insert(Node4 *node, Node **nodeRef, uint8_t keyByte, Node *child);
	//! Delete Leaf from Node4
	static void erase(Node4 *node, Node **nodeRef, Node **leafPlace);
};
} // namespace duckdb
