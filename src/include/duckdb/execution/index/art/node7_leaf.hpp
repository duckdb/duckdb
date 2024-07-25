//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node7_leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Node7Leaf holds up to 7 sorted bytes.
class Node7Leaf {
public:
	Node7Leaf() = delete;
	Node7Leaf(const Node7Leaf &) = delete;
	Node7Leaf &operator=(const Node7Leaf &) = delete;

	//! Byte count.
	uint8_t count;
	//! Byte array.
	uint8_t key[Node::NODE_7_LEAF_CAPACITY];

public:
	//! Get a new Node7Leaf, might cause a new buffer allocation, and initialize it.
	static Node7Leaf &New(ART &art, Node &node);
	//! Initializes all fields of the node while shrinking a Node15Leaf to a Node7Leaf.
	static Node7Leaf &ShrinkNode15Leaf(ART &art, Node &node7_leaf, Node &node15_leaf);
	//! Insert a byte.
	static void InsertByte(ART &art, Node &node, const uint8_t byte);
	//! Delete a byte.
	static void DeleteByte(ART &art, Node &node, Node &prefix, const uint8_t byte);
	//! Get the first byte that is greater or equal to the byte parameter.
	bool GetNextByte(uint8_t &byte) const;
	//! Returns the string representation of the node, or early-outs.
	string VerifyAndToString(ART &art, const bool only_verify) const;
};

} // namespace duckdb
