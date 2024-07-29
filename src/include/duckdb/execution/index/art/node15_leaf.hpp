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
public:
	Node15Leaf() = delete;
	Node15Leaf(const Node15Leaf &) = delete;
	Node15Leaf &operator=(const Node15Leaf &) = delete;

	//! Byte count.
	uint8_t count;
	//! Byte array.
	uint8_t key[Node::NODE_15_LEAF_CAPACITY];

public:
	//! Get a new Node15Leaf, might cause a new buffer allocation, and initialize it.
	static Node15Leaf &New(ART &art, Node &node);
	//! Initializes all the fields of the node while growing a Node7 to a Node15.
	static Node15Leaf &GrowNode7Leaf(ART &art, Node &node15_leaf, Node &node7_leaf);
	//! Initializes all fields of the node while shrinking a Node256Leaf to a Node15Leaf.
	static Node15Leaf &ShrinkNode256Leaf(ART &art, Node &node15_leaf, Node &node256_leaf);
	//! Insert a byte.
	static void InsertByte(ART &art, Node &node, const uint8_t byte);
	//! Delete a byte.
	static void DeleteByte(ART &art, Node &node, const uint8_t byte);
	//! Get the first byte that is greater or equal to the byte parameter.
	bool GetNextByte(uint8_t &byte) const;
};

} // namespace duckdb
