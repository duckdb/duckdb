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
	friend class Node48;

public:
	static constexpr NType NODE_256 = NType::NODE_256;
	static constexpr uint16_t CAPACITY = 256;
	static constexpr uint8_t SHRINK_THRESHOLD = 36;

public:
	Node256() = delete;
	Node256(const Node256 &) = delete;
	Node256 &operator=(const Node256 &) = delete;

private:
	uint16_t count;
	Node children[CAPACITY];

public:
	//! Get a new Node256 and initialize it.
	static Node256 &New(ART &art, Node &node);
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
		for (idx_t i = 0; i < CAPACITY; i++) {
			if (n.children[i].HasMetadata()) {
				lambda(n.children[i]);
			}
		}
	}

	template <class NODE>
	static unsafe_optional_ptr<Node> GetChild(NODE &n, const uint8_t byte) {
		if (n.children[byte].HasMetadata()) {
			return &n.children[byte];
		}
		return nullptr;
	}

	template <class NODE>
	static unsafe_optional_ptr<Node> GetNextChild(NODE &n, uint8_t &byte) {
		for (idx_t i = byte; i < CAPACITY; i++) {
			if (n.children[i].HasMetadata()) {
				byte = UnsafeNumericCast<uint8_t>(i);
				return &n.children[i];
			}
		}
		return nullptr;
	}

private:
	static Node256 &GrowNode48(ART &art, Node &node256, Node &node48);
};
} // namespace duckdb
