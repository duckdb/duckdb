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
	friend class Node4;
	friend class Node48;

public:
	static constexpr NType NODE_16 = NType::NODE_16;
	static constexpr uint8_t CAPACITY = 16;

public:
	Node16() = delete;
	Node16(const Node16 &) = delete;
	Node16 &operator=(const Node16 &) = delete;

private:
	uint8_t count;
	uint8_t key[CAPACITY];
	Node children[CAPACITY];

public:
	//! Get a new Node16 and initialize it.
	static Node16 &New(ART &art, Node &node);
	//! Free the node and its children.
	static void Free(ART &art, Node &node);

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
	static unsafe_optional_ptr<Node> GetChild(NODE &n, const uint8_t byte) {
		return Node4::GetChild(n, byte);
	}

	template <class NODE>
	static unsafe_optional_ptr<Node> GetNextChild(NODE &n, uint8_t &byte) {
		return Node4::GetNextChild(n, byte);
	}

private:
	static Node16 &GrowNode4(ART &art, Node &node16, Node &node4);
	static Node16 &ShrinkNode48(ART &art, Node &node16, Node &node48);
};
} // namespace duckdb
