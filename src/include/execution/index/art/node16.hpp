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

	Node16(uint8_t max_prefix_length) : Node(NodeType::N16, max_prefix_length) {
		memset(key, 16, sizeof(key));
	}

	//! Get Node16 Child
	unique_ptr<Node> *getChild(const uint8_t k);

	//! Get position of a byte, returns -1 if not exists
	int getPos(const uint8_t k);

	//! Get min value
	unique_ptr<Node> *getMin();

	//! Insert node into Node16
	void static insert(unique_ptr<Node> &node, uint8_t keyByte, unique_ptr<Node> &child);

	//! Shrink to node 4
	static void erase(unique_ptr<Node> &node, int pos);
};
} // namespace duckdb
