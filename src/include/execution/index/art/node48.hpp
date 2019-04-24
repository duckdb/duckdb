//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/node48.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#import "node.hpp"

namespace duckdb {

class Node48 : public Node {
public:
	uint8_t childIndex[256];
	Node *child[48];

	Node48() : Node(NodeType::N48) {
		memset(childIndex, 48, sizeof(childIndex));
		memset(child, 0, sizeof(child));
	}

	Node *getChild(const uint8_t k) const;
	static void insert(Node48 *node, Node **nodeRef, uint8_t keyByte, Node *child);
};
} // namespace duckdb
