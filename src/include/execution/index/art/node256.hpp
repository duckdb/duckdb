//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/node256.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#import "node.hpp"

namespace duckdb {

class Node256 : public Node {
public:
	Node *child[256];

	Node256() : Node(NodeType::N256,this->maxPrefixLength) {
		memset(child, 0, sizeof(child));
	}

	//! Get Node256 Child
	Node **getChild(const uint8_t k);

	//! Insert node From Node256
	static void insert(Node256 *node, uint8_t keyByte, Node *child);

	//! Delete node From Node256
	static void erase(Node256* node,Node** nodeRef,uint8_t keyByte);

};
} // namespace duckdb
