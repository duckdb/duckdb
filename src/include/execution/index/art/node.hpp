//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "art_key.hpp"
#include "common/common.hpp"

namespace duckdb {
enum class NodeType : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3, NLeaf = 4 };

class Node {
public:
	static const uint8_t EMPTY_MARKER = 48;
public:
	Node(ART &art, NodeType type);
	virtual ~Node() {}

	//! length of the compressed path (prefix)
	uint32_t prefix_length;
	//! number of non-null children
	uint16_t count;
	//! node type
	NodeType type;
	//! compressed path (prefix)
	unique_ptr<uint8_t[]> prefix;
public:
	//! Find the leaf with smallest element in the tree
	static unique_ptr<Node> *minimum(unique_ptr<Node> &node);
	//! Find the next child for the keyByte
	static unique_ptr<Node> *findChild(const uint8_t k, unique_ptr<Node> &node);
	static int findKeyPos(const uint8_t k, Node *node);
	static Node *findChild(const uint8_t k, Node *node);

	//! Compare the key with the prefix of the node, return the number matching bytes
	static uint32_t PrefixMismatch(ART &art, Node *node, Key &key, uint64_t depth, TypeId type);
	//! Insert leaf into inner node
	static void InsertLeaf(ART &art, unique_ptr<Node> &node, uint8_t key, unique_ptr<Node> &newNode);
protected:
	//! Copies the prefix from the source to the destination node
	static void CopyPrefix(ART &art, Node *src, Node *dst);

};

} // namespace duckdb
