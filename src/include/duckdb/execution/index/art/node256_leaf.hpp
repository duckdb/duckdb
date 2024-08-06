//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node256_leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Node256Leaf is a bitmask containing 256 bits.
class Node256Leaf {
public:
	static constexpr NType NODE_256_LEAF = NType::NODE_256_LEAF;
	static constexpr uint16_t CAPACITY = Node::NODE_256_CAPACITY;

public:
	Node256Leaf() = delete;
	Node256Leaf(const Node256Leaf &) = delete;
	Node256Leaf &operator=(const Node256Leaf &) = delete;

	uint16_t count;
	validity_t mask[CAPACITY / sizeof(validity_t)];

public:
	//! Get a new Node256Leaf and initialize it.
	static Node256Leaf &New(ART &art, Node &node);

	//! Initializes all fields of the node while growing a Node15 to a Node256Leaf.
	static Node256Leaf &GrowNode15Leaf(ART &art, Node &node256_leaf, Node &node15_leaf);

	//! Insert a byte.
	static void InsertByte(ART &art, Node &node, const uint8_t byte);
	//! Delete a byte.
	static void DeleteByte(ART &art, Node &node, const uint8_t byte);

	//! Get the first byte that is greater or equal to the byte parameter.
	bool GetNextByte(uint8_t &byte);
};

} // namespace duckdb
