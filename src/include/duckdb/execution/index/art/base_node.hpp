//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/base_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

template <uint8_t CAPACITY, NType TYPE>
class BaseNode {
	friend class Node4;
	friend class Node16;
	friend class Node48;

public:
	BaseNode() = delete;
	BaseNode(const BaseNode &) = delete;
	BaseNode &operator=(const BaseNode &) = delete;

private:
	uint8_t count;
	uint8_t key[CAPACITY];
	Node children[CAPACITY];

public:
	//! Get a new BaseNode handle and initialize the base node.
	static NodeHandle<BaseNode> New(ART &art, Node &node) {
		node = Node::GetAllocator(art, TYPE).New();
		node.SetMetadata(static_cast<uint8_t>(TYPE));

		NodeHandle<BaseNode> handle(art, node);
		auto &n = handle.Get();

		n.count = 0;
		return handle;
	}

	//! Free the children of the node.
	static void Free(ART &art, Node &node) {
		NodeHandle<BaseNode> handle(art, node);
		auto &n = handle.Get();

		for (uint8_t i = 0; i < n.count; i++) {
			Node::Free(art, n.children[i]);
		}
	}

	//! Replace the child at byte.
	static void ReplaceChild(BaseNode &n, const uint8_t byte, const Node child) {
		D_ASSERT(n.count != 0);
		for (uint8_t i = 0; i < n.count; i++) {
			if (n.key[i] == byte) {
				auto status = n.children[i].GetGateStatus();
				n.children[i] = child;

				if (status == GateStatus::GATE_SET && child.HasMetadata()) {
					n.children[i].SetGateStatus(status);
				}
				return;
			}
		}
	}

	//! Get the child at byte.
	static unsafe_optional_ptr<Node> GetChild(BaseNode &n, const uint8_t byte) {
		for (uint8_t i = 0; i < n.count; i++) {
			if (n.key[i] == byte) {
				D_ASSERT(n.children[i].HasMetadata());
				return &n.children[i];
			}
		}
		return nullptr;
	}

	//! Get the first child greater than or equal to the byte.
	static unsafe_optional_ptr<Node> GetNextChild(BaseNode &n, uint8_t &byte) {
		for (uint8_t i = 0; i < n.count; i++) {
			if (n.key[i] >= byte) {
				byte = n.key[i];
				return &n.children[i];
			}
		}
		return nullptr;
	}

	//! Extracts the bytes and their respective children.
	//! The return value is valid as long as the arena is valid.
	//! The node must be freed after calling into this function.
	NodeChildren ExtractChildren(ArenaAllocator &arena) {
		auto mem_bytes = arena.AllocateAligned(sizeof(uint8_t) * count);
		array_ptr<uint8_t> bytes(mem_bytes, count);
		auto mem_children = arena.AllocateAligned(sizeof(Node) * count);
		array_ptr<Node> children_ptr(reinterpret_cast<Node *>(mem_children), count);

		for (uint8_t i = 0; i < count; i++) {
			bytes[i] = key[i];
			children_ptr[i] = children[i];
		}

		count = 0;
		return NodeChildren(bytes, children_ptr);
	}

public:
	template <class F>
	static void Iterator(BaseNode<CAPACITY, TYPE> &n, F &&lambda) {
		for (uint8_t i = 0; i < n.count; i++) {
			lambda(n.children[i]);
		}
	}

private:
	static void InsertChildInternal(BaseNode &n, const uint8_t byte, const Node child);
	static NodeHandle<BaseNode> DeleteChildInternal(ART &art, Node &node, const uint8_t byte);
};

//! Node4 holds up to four children sorted by their key byte.
class Node4 : public BaseNode<4, NType::NODE_4> {
	friend class Node16;

public:
	static constexpr NType NODE_4 = NType::NODE_4;
	static constexpr uint8_t CAPACITY = 4;

public:
	//! Insert a child at byte.
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child at byte.
	static void DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte, const GateStatus status);

private:
	static void ShrinkNode16(ART &art, Node &node4, Node &node16);
};

class Node16 : public BaseNode<16, NType::NODE_16> {
	friend class Node4;
	friend class Node48;

public:
	static constexpr NType NODE_16 = NType::NODE_16;
	static constexpr uint8_t CAPACITY = 16;

public:
	//! Insert a child at byte.
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child at byte.
	static void DeleteChild(ART &art, Node &node, const uint8_t byte);

private:
	static void GrowNode4(ART &art, Node &node16, Node &node4);
	static void ShrinkNode48(ART &art, Node &node16, Node &node48);
};

} // namespace duckdb
