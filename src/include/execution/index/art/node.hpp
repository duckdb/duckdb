//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/index/art/node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <common/exception.hpp>
#include "common/common.hpp"

namespace duckdb {
enum class NodeType : uint8_t { N4 = 0, N16 = 1, N48 = 2, N256 = 3, NLeaf = 4 };
//! The maximum prefix length for compressed paths stored in the
//! header, if the path is longer it is loaded from the database on demand
static const unsigned maxPrefixLength = 8;

class Node {
public:
	//! length of the compressed path (prefix)
	uint32_t prefixLength;
	//! number of non-null children
	uint16_t count;
	//! node type
	NodeType type;
	//! compressed path (prefix)
	uint8_t prefix[maxPrefixLength];
	static const uint8_t emptyMarker = 48;
	Node(NodeType type) : prefixLength(0), count(0), type(type) {
	}
	//! Count trailing zeros, only defined for x>0 (from Hacker's Delight)
	static inline unsigned ctz(uint16_t x);
	//! Find the node with a matching key, optimistic version
	Node *lookup(Node *node, uint8_t key[], unsigned keyLength, unsigned depth, unsigned maxKeyLength, TypeId type);
	//! Insert the leaf value into the tree
	static void insert(Node *node, Node **nodeRef, uint8_t key[], unsigned depth, uintptr_t value,
	                   unsigned maxKeyLength, TypeId type, uint64_t row_id);
	//! Copies the prefix from the source to the destination node
	static void copyPrefix(Node *src, Node *dst);
	//! Flip the sign bit, enables signed SSE comparison of unsigned values
	static uint8_t flipSign(uint8_t keyByte);
	//! Returns the stored in the leaf
	static inline uint64_t getLeafValue(const Node *node);

	//! Performs convestion to binary comparable format (i.e., swap bits to big-endian, swap MSB for signed integers)
	static void convert_to_binary_comparable(TypeId type, uintptr_t tid, uint8_t key[]) {
		switch (type) {
		case TypeId::BOOLEAN:
			reinterpret_cast<uint8_t *>(key)[0] = ((tid & 0xf0) >> 4) | ((tid & 0x0f) << 4);
		case TypeId::TINYINT:
			reinterpret_cast<uint8_t *>(key)[0] = ((tid & 0xf0) >> 4) | ((tid & 0x0f) << 4);
			key[0] = flipSign(key[0]);
			break;
		case TypeId::SMALLINT:
			reinterpret_cast<uint16_t *>(key)[0] = __builtin_bswap16(tid);
			key[0] = flipSign(key[0]);
			break;
		case TypeId::INTEGER:
			reinterpret_cast<uint32_t *>(key)[0] = __builtin_bswap32(tid);
			key[0] = flipSign(key[0]);
			break;
		case TypeId::BIGINT:
			reinterpret_cast<uint64_t *>(key)[0] = __builtin_bswap64(tid);
			key[0] = flipSign(key[0]);
			break;
		default:
			throw NotImplementedException("Unimplemented type for ART index");
		}
	}

private:
	//! Compare two elements and return the smaller
	static unsigned min(unsigned a, unsigned b);
	//! Find the leaf with smallest element in the tree
	static Node *minimum(Node *node);
	//! Find the next child for the keyByte
	static Node *findChild(const uint8_t k, const Node *node);
	//! Compare the key with the prefix of the node, return the number matching bytes
	static unsigned prefixMismatch(Node *node, uint8_t key[], unsigned depth, unsigned maxKeyLength, TypeId type);
	//! Insert leaf into inner node
	static void insertLeaf(Node *node, Node **nodeRef, uint8_t key, Node *newNode);
};

//! TODO: Duplicates
class Leaf : public Node {
public:
	uint64_t row_id;
	uint64_t value;
	Leaf(uint64_t value, uint64_t row_id) : Node(NodeType::NLeaf) {
		this->value = value;
		this->row_id = row_id;
	}
};

} // namespace duckdb
