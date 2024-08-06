//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node256.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Node256 holds up to 256 children. They are indexed by their key byte.
class Node256 {
public:
	static constexpr NType NODE_256 = NType::NODE_256;
	static constexpr uint16_t CAPACITY = Node::NODE_256_CAPACITY;

public:
	Node256() = delete;
	Node256(const Node256 &) = delete;
	Node256 &operator=(const Node256 &) = delete;

	uint16_t count;
	Node children[Node::NODE_256_CAPACITY];

public:
	//! Get a new Node256 and initialize it.
	static Node256 &New(ART &art, Node &node);
	//! Free the node and its children.
	static void Free(ART &art, Node &node);

	//! Initializes all fields of the node while growing a Node48 to a Node256.
	static Node256 &GrowNode48(ART &art, Node &node256, Node &node48);

	//! Insert a child at byte.
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child at byte.
	static void DeleteChild(ART &art, Node &node, const uint8_t byte);
	//! Replace the child at byte.
	void ReplaceChild(const uint8_t byte, const Node child);

public:
	template <class F, class NODE>
	static void Iterator(NODE &n, F &&lambda) {
		for (idx_t i = 0; i < CAPACITY; i++) {
			if (n.children[i].HasMetadata()) {
				lambda(n.children[i]);
			}
		}
	}

	template <class OUT, class NODE>
	static OUT *GetChild(NODE &n, const uint8_t byte) {
		if (n.children[byte].HasMetadata()) {
			return &n.children[byte];
		}
		return nullptr;
	}

	template <class OUT, class NODE>
	static OUT *GetNextChild(NODE &n, uint8_t &byte) {
		for (idx_t i = byte; i < CAPACITY; i++) {
			if (n.children[i].HasMetadata()) {
				byte = UnsafeNumericCast<uint8_t>(i);
				return &n.children[i];
			}
		}
		return nullptr;
	}
};
} // namespace duckdb
