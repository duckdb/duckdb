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
	//! Get a new BaseNode and initialize it.
	static BaseNode &New(ART &art, Node &node);
	//! Free the node and its children.
	static void Free(ART &art, Node &node);
	//! Replace the child at byte.
	static void ReplaceChild(BaseNode &n, const uint8_t byte, const Node child);
	//! Get the child at byte.
	static unsafe_optional_ptr<Node> GetChild(BaseNode &n, const uint8_t byte);
	//! Get the first child greater than or equal to the byte.
	static unsafe_optional_ptr<Node> GetNextChild(BaseNode &n, uint8_t &byte);

public:
	template <class F>
	static void Iterator(BaseNode<CAPACITY, TYPE> &n, F &&lambda) {
		for (uint8_t i = 0; i < n.count; i++) {
			lambda(n.children[i]);
		}
	}

private:
	static void InsertChildInternal(BaseNode &n, const uint8_t byte, const Node child);
	static BaseNode &DeleteChildInternal(ART &art, Node &node, const uint8_t byte);
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
