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
    unique_ptr<Node> child[4];

	Node4(uint8_t maxPrefixLength) : Node(NodeType::N4, maxPrefixLength) {
		memset(key, 0, sizeof(key));
	}

	//! Get Node4 Child
	unique_ptr<Node>* getChild(const uint8_t k);
    unique_ptr<Node>* getChild(const uint8_t k, int& pos);

	//! Get min value in node
	unique_ptr<Node>* getMin();
	//! Insert Leaf to the Node4
	static void insert(unique_ptr<Node>& node, uint8_t keyByte, unique_ptr<Node>& child);
};
} // namespace duckdb
