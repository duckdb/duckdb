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
	uint8_t childIndex[256];
    unique_ptr<Node> child[48];

	Node48(uint8_t maxPrefixLength) : Node(NodeType::N48, maxPrefixLength) {
	    for(uint64_t i = 0; i < 256; i++) {
	        childIndex[i] = 48;
	    }
	}

	//! Get Node48 Child
	unique_ptr<Node>* getChild(const uint8_t k);
    unique_ptr<Node>* getChild(const uint8_t k, int& pos);

	//! Get min value
	unique_ptr<Node>* getMin();

	//! Insert node in Node48
	static void insert(unique_ptr<Node>& node, uint8_t keyByte, unique_ptr<Node>&child);

	//! Shrink to node 16
	static void shrink (unique_ptr<Node>& node);
};
} // namespace duckdb
