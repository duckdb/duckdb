//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "art_key.hpp"
#include <common/exception.hpp>
#include "common/common.hpp"

namespace duckdb {
enum class NodeType : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3, NLeaf = 4 };

class Node {
public:
    //! length of the compressed path (prefix)
	uint32_t prefixLength;
	//! number of non-null children
	uint16_t count;
	//! node type
	NodeType type;
	//! compressed path (prefix)
	uint8_t *prefix;

    uint8_t  maxPrefixLength;

	static const uint8_t emptyMarker = 48;
	Node(NodeType type, unsigned maxPrefixLength) : prefixLength(0), count(0), type(type) {
		this->prefix = (uint8_t*) malloc(maxPrefixLength * sizeof(uint8_t));
		this->maxPrefixLength = maxPrefixLength;
	}
	//! Copies the prefix from the source to the destination node
	static void copyPrefix(Node *src, Node *dst);
	//! Find the leaf with smallest element in the tree
	static Node *minimum(Node *node);
	//! Find the next child for the keyByte
	static Node **findChild(const uint8_t k, Node *node);
	//! Compare the key with the prefix of the node, return the number matching bytes
	static unsigned prefixMismatch(bool isLittleEndian, Node *node, Key &key, size_t depth, unsigned maxKeyLength, TypeId type);
	//! Insert leaf into inner node
	static void insertLeaf(Node *node, Node **nodeRef, uint8_t key, Node *newNode);
	//! Compare two elements and return the smaller
	static unsigned min(unsigned a, unsigned b);

};

class Leaf : public Node {
public:
	uint64_t value;
	uint64_t capacity;
	uint64_t num_elements;
	uint64_t *row_id;
	Leaf(uint64_t value, uint64_t row_id) : Node(NodeType::NLeaf,this->maxPrefixLength) {
		this->value = value;
		this->capacity = 1;
		this->row_id = (uint64_t*) malloc(this->capacity * sizeof(uint64_t));
		this->row_id[0] = row_id;
		this->num_elements =1;
	}
	static void insert(Leaf *leaf, uint64_t row_id){
		// Grow array
		if (leaf->num_elements == leaf->capacity ){
			leaf->capacity  *= 2;
			leaf->row_id = (uint64_t *)realloc(leaf->row_id, leaf->capacity * sizeof(uint64_t));
		}
		leaf->row_id[leaf->num_elements] = row_id;
		leaf->num_elements++;
	}
};

} // namespace duckdb
