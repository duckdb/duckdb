//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node15_leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Node15Leaf holds up to 15 sorted bytes.
class Node15Leaf {
	friend class Node4;
	friend class Node7Leaf;
	friend class Node256Leaf;

public:
	static constexpr NType NODE_15_LEAF = NType::NODE_15_LEAF;
	static constexpr uint8_t CAPACITY = 15;

public:
	Node15Leaf() = delete;
	Node15Leaf(const Node15Leaf &) = delete;
	Node15Leaf &operator=(const Node15Leaf &) = delete;

private:
	uint8_t count;
	uint8_t key[CAPACITY];

public:
	//! Get a new Node15Leaf and initialize it.
	static Node15Leaf &New(ART &art, Node &node);

	//! Insert a byte.
	static void InsertByte(ART &art, Node &node, const uint8_t byte);
	//! Delete a byte.
	static void DeleteByte(ART &art, Node &node, const uint8_t byte);

	//! Get the first byte greater than or equal to the byte.
	bool GetNextByte(uint8_t &byte) const;

private:
	static Node15Leaf &GrowNode7Leaf(ART &art, Node &node15_leaf, Node &node7_leaf);
	static Node15Leaf &ShrinkNode256Leaf(ART &art, Node &node15_leaf, Node &node256_leaf);
};

} // namespace duckdb
