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
	unique_ptr<uint8_t[]> prefix;

	uint8_t maxPrefixLength;

	static const uint8_t emptyMarker = 48;
	Node(NodeType type, unsigned maxPrefixLength) : prefixLength(0), count(0), type(type) {
		this->prefix = unique_ptr<uint8_t[]>(new uint8_t[maxPrefixLength]);
		this->maxPrefixLength = maxPrefixLength;
	}
	virtual ~Node() {
	}

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

class Leaf : public Node {
public:
	uint64_t value;
	uint64_t capacity;
	uint64_t num_elements;
	unique_ptr<uint64_t[]> row_id;

	Leaf(uint64_t value, uint64_t row_id, uint8_t maxPrefixLength) : Node(NodeType::NLeaf, maxPrefixLength) {
		this->value = value;
		this->capacity = 1;
		this->row_id = unique_ptr<uint64_t[]>(new uint64_t[this->capacity]);
		this->row_id[0] = row_id;
		this->num_elements = 1;
	}

	static void insert(Leaf *leaf, uint64_t row_id) {
		// Grow array
		if (leaf->num_elements == leaf->capacity) {
			auto new_row_id = unique_ptr<uint64_t[]>(new uint64_t[leaf->capacity * 2]);
			memcpy(new_row_id.get(), leaf->row_id.get(), leaf->capacity * sizeof(uint64_t));
			leaf->capacity *= 2;
			leaf->row_id = move(new_row_id);
		}
		leaf->row_id[leaf->num_elements] = row_id;
		leaf->num_elements++;
	}

	//! TODO: Maybe shrink array dynamically?
	static void remove(Leaf *leaf, uint64_t row_id) {
		uint64_t entry_offset = -1;
		for (uint64_t i = 0; i < leaf->num_elements; i++) {
			if (leaf->row_id[i] == row_id) {
				entry_offset = i;
				break;
			}
		}
		leaf->num_elements--;
		for (uint64_t j = entry_offset; j < leaf->num_elements; j++)
			leaf->row_id[j] = leaf->row_id[j + 1];
	}
};

} // namespace duckdb
