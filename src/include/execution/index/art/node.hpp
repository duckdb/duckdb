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
	Node(NodeType type, unsigned max_prefix_length) : prefix_length(0), count(0), type(type) {
		this->prefix = unique_ptr<uint8_t[]>(new uint8_t[max_prefix_length]);
		this->max_prefix_length = max_prefix_length;
	}
	virtual ~Node() {}

	//! length of the compressed path (prefix)
	uint32_t prefix_length;
	//! number of non-null children
	uint16_t count;
	//! node type
	NodeType type;
	//! compressed path (prefix)
	unique_ptr<uint8_t[]> prefix;

	uint8_t max_prefix_length;
public:
	//! Copies the prefix from the source to the destination node
	static void copyPrefix(Node *src, Node *dst);
	//! Find the leaf with smallest element in the tree
	static unique_ptr<Node> *minimum(unique_ptr<Node> &node);
	//! Find the next child for the keyByte
	static unique_ptr<Node> *findChild(const uint8_t k, unique_ptr<Node> &node);
	static int findKeyPos(const uint8_t k, Node *node);
	static Node *findChild(const uint8_t k, Node *node);

	//! Compare the key with the prefix of the node, return the number matching bytes
	static unsigned prefixMismatch(bool isLittleEndian, Node *node, Key &key, uint64_t depth, unsigned maxKeyLength,
	                               TypeId type);
	//! Insert leaf into inner node
	static void insertLeaf(unique_ptr<Node> &node, uint8_t key, unique_ptr<Node> &newNode);
	//! Compare two elements and return the smaller
	static unsigned min(unsigned a, unsigned b);
};

} // namespace duckdb
