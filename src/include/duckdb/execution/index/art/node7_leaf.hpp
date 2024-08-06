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

//! Node7Leaf holds up to seven sorted bytes.
class Node7Leaf {
public:
	static constexpr NType NODE_7_LEAF = NType::NODE_7_LEAF;
	static constexpr uint8_t CAPACITY = Node::NODE_7_LEAF_CAPACITY;

public:
	Node7Leaf() = delete;
	Node7Leaf(const Node7Leaf &) = delete;
	Node7Leaf &operator=(const Node7Leaf &) = delete;

	uint8_t count;
	uint8_t key[CAPACITY];

public:
	//! Get a new Node7Leaf and initialize it.
	static Node7Leaf &New(ART &art, Node &node);

	//! Initializes all fields of the node while shrinking a Node15Leaf to a Node7Leaf.
	static Node7Leaf &ShrinkNode15Leaf(ART &art, Node &node7_leaf, Node &node15_leaf);

	//! Insert a byte.
	static void InsertByte(ART &art, Node &node, const uint8_t byte);
	template <class NODE>
	static void InsertByteInternal(ART &art, NODE &n, const uint8_t byte) {
		// Still space. Insert the child.
		uint8_t child_pos = 0;
		while (child_pos < n.count && n.key[child_pos] < byte) {
			child_pos++;
		}

		// Move children backwards to make space.
		for (uint8_t i = n.count; i > child_pos; i--) {
			n.key[i] = n.key[i - 1];
		}

		n.key[child_pos] = byte;
		n.count++;
	}

	//! Delete a byte.
	static void DeleteByte(ART &art, Node &node, Node &prefix, const uint8_t byte);
	template <class NODE>
	static NODE &DeleteByteInternal(ART &art, Node &node, const uint8_t byte) {
		auto &n = Node::Ref<NODE>(art, node, node.GetType());
		uint8_t child_pos = 0;

		for (; child_pos < n.count; child_pos++) {
			if (n.key[child_pos] == byte) {
				break;
			}
		}
		n.count--;

		// Possibly move children backwards.
		for (uint8_t i = child_pos; i < n.count; i++) {
			n.key[i] = n.key[i + 1];
		}
		return n;
	}

	//! Get the first byte that is greater than or equal to the byte parameter.
	template <class NODE>
	static bool GetNextByte(NODE &n, uint8_t &byte) {
		for (uint8_t i = 0; i < n.count; i++) {
			if (n.key[i] >= byte) {
				byte = n.key[i];
				return true;
			}
		}
		return false;
	}
};

} // namespace duckdb
