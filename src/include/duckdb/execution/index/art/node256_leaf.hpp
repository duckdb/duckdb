//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node256_leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/node256.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"

namespace duckdb {

//! Node256Leaf is a bitmask containing 256 bits.
class Node256Leaf {
	friend class Node15Leaf;

public:
	static constexpr NType NODE_256_LEAF = NType::NODE_256_LEAF;
	static constexpr uint16_t CAPACITY = Node256::CAPACITY;

public:
	Node256Leaf() = delete;
	Node256Leaf(const Node256Leaf &) = delete;
	Node256Leaf &operator=(const Node256Leaf &) = delete;

private:
	uint16_t count;
	validity_t mask[CAPACITY / sizeof(validity_t)];

public:
	//! Get a new Node256Leaf and initialize it.
	static Node256Leaf &New(ART &art, Node &node);

	//! Insert a byte.
	static void InsertByte(ART &art, Node &node, const uint8_t byte);
	//! Delete a byte.
	static void DeleteByte(ART &art, Node &node, const uint8_t byte);

	//! Returns true, if the byte exists, else false.
	bool HasByte(uint8_t &byte);
	//! Get the first byte greater or equal to the byte.
	bool GetNextByte(uint8_t &byte);

private:
	static Node256Leaf &GrowNode15Leaf(ART &art, Node &node256_leaf, Node &node15_leaf);
};

} // namespace duckdb
