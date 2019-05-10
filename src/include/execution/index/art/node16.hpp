//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/node16.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "node.hpp"

namespace duckdb {

class Node16 : public Node {
public:
	uint8_t key[16];
	unique_ptr<Node> child[16];

	Node16(uint8_t maxPrefixLength) : Node(NodeType::N16, maxPrefixLength) {
		memset(key, 0, sizeof(key));
		memset(child, 0, sizeof(child));
	}

	//! Get Node16 Child
    unique_ptr<Node>* getChild(const uint8_t k);

	//! Insert node into Node16
	void static insert(unique_ptr<Node>& node, uint8_t keyByte, unique_ptr<Node>& child);

	//! Delete node from Node16
	static void erase(Node16 *node, Node **nodeRef, Node **leafPlace);
};
} // namespace duckdb
