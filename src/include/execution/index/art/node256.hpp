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
    unique_ptr<Node> child[256];
	Node256(uint8_t maxPrefixLength) : Node(NodeType::N256, maxPrefixLength) {
		memset(child, 0, sizeof(child));
	}

	//! Get Node256 Child
	unique_ptr<Node>* getChild(const uint8_t k);

	//! Insert node From Node256
	static void insert(unique_ptr<Node>& node, uint8_t keyByte, unique_ptr<Node>& child);

	//! Delete node From Node256
	static void erase(Node256 *node, Node **nodeRef, uint8_t keyByte);
};
} // namespace duckdb
