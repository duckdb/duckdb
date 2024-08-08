//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node4.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Node4 holds up to four children sorted by their key byte.
class Node4 {
	friend class Node16;

public:
	static constexpr NType NODE_4 = NType::NODE_4;
	static constexpr uint8_t CAPACITY = 4;

public:
	Node4() = delete;
	Node4(const Node4 &) = delete;
	Node4 &operator=(const Node4 &) = delete;

private:
	uint8_t count;
	uint8_t key[CAPACITY];
	Node children[CAPACITY];

public:
	//! Get a new Node4 and initialize it.
	template <class NODE>
	static NODE &New(ART &art, Node &node, NType type) {
		node = Node::GetAllocator(art, type).New();
		node.SetMetadata(static_cast<uint8_t>(type));

		auto &n = Node::Ref<NODE>(art, node, type);
		n.count = 0;
		return n;
	}
	//! Free the node and its children.
	template <class NODE>
	static void Free(ART &art, Node &node) {
		auto &n = Node::Ref<NODE>(art, node, node.GetType());
		for (uint8_t i = 0; i < n.count; i++) {
			Node::Free(art, n.children[i]);
		}
	}

	//! Insert a child at byte.
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child at byte.
	static void DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte);
	//! Replace the child at byte.
	template <class NODE>
	static void ReplaceChild(NODE &n, const uint8_t byte, const Node child) {
		for (uint8_t i = 0; i < n.count; i++) {
			if (n.key[i] == byte) {
				auto was_gate = n.children[i].IsGate();
				n.children[i] = child;

				if (was_gate && child.HasMetadata()) {
					n.children[i].SetGate();
				}
				return;
			}
		}
	}

public:
	template <class F, class NODE>
	static void Iterator(NODE &n, F &&lambda) {
		for (uint8_t i = 0; i < n.count; i++) {
			lambda(n.children[i]);
		}
	}

	template <class NODE>
	static Node *GetChild(NODE &n, const uint8_t byte) {
		for (uint8_t i = 0; i < n.count; i++) {
			if (n.key[i] == byte) {
				return &n.children[i];
			}
		}
		return nullptr;
	}

	template <class NODE>
	static Node *GetNextChild(NODE &n, uint8_t &byte) {
		for (uint8_t i = 0; i < n.count; i++) {
			if (n.key[i] >= byte) {
				byte = n.key[i];
				return &n.children[i];
			}
		}
		return nullptr;
	}

private:
	static Node4 &ShrinkNode16(ART &art, Node &node4, Node &node16);

	template <class NODE>
	static void InsertChildInternal(ART &art, NODE &n, const uint8_t byte, const Node child) {
		// Still space. Insert the child.
		uint8_t child_pos = 0;
		while (child_pos < n.count && n.key[child_pos] < byte) {
			child_pos++;
		}

		// Move children backwards to make space.
		for (uint8_t i = n.count; i > child_pos; i--) {
			n.key[i] = n.key[i - 1];
			n.children[i] = n.children[i - 1];
		}

		n.key[child_pos] = byte;
		n.children[child_pos] = child;
		n.count++;
	}

	template <class NODE>
	static NODE &DeleteChildInternal(ART &art, Node &node, const uint8_t byte) {
		auto &n = Node::Ref<NODE>(art, node, node.GetType());

		uint8_t child_pos = 0;
		for (; child_pos < n.count; child_pos++) {
			if (n.key[child_pos] == byte) {
				break;
			}
		}

		// Free the child and decrease the count.
		Node::Free(art, n.children[child_pos]);
		n.count--;

		// Possibly move children backwards.
		for (uint8_t i = child_pos; i < n.count; i++) {
			n.key[i] = n.key[i + 1];
			n.children[i] = n.children[i + 1];
		}
		return n;
	}
};
} // namespace duckdb
