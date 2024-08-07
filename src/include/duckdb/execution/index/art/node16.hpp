//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node16.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/node4.hpp"

namespace duckdb {

//! Node16 holds up to 16 children sorted by their key byte.
class Node16 {
public:
	static constexpr NType NODE_16 = NType::NODE_16;
	static constexpr uint8_t CAPACITY = Node::NODE_16_CAPACITY;

public:
	Node16() = delete;
	Node16(const Node16 &) = delete;
	Node16 &operator=(const Node16 &) = delete;

	uint8_t count;
	uint8_t key[CAPACITY];
	Node children[CAPACITY];

public:
	//! Get a new Node16 and initialize it.
	static Node16 &New(ART &art, Node &node);
	//! Free the node and its children.
	static void Free(ART &art, Node &node);

	//! Initializes all fields of the node while growing a Node4 to a Node16.
	static Node16 &GrowNode4(ART &art, Node &node16, Node &node4);
	//! Initializes all fields of the node while shrinking a Node48 to a Node16.
	static Node16 &ShrinkNode48(ART &art, Node &node16, Node &node48);

	//! Insert a child at byte.
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child at byte.
	static void DeleteChild(ART &art, Node &node, const uint8_t byte);
	//! Replace the child at byte.
	void ReplaceChild(const uint8_t byte, const Node child);

public:
	template <class F, class NODE>
	static void Iterator(NODE &n, F &&lambda) {
		Node4::Iterator(n, lambda);
	}

	template <class NODE>
	static Node *GetChild(NODE &n, const uint8_t byte) {
		return Node4::GetChild(n, byte);
	}

	template <class NODE>
	static Node *GetNextChild(NODE &n, uint8_t &byte) {
		return Node4::GetNextChild(n, byte);
	}
};
} // namespace duckdb
