//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node48.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Node48 holds up to 48 ARTNode children. It contains a child_index array which can be directly indexed by the key
//! byte, and which contains the position of the child node in the children array
class Node48 {
public:
	//! Number of non-null children
	uint8_t count;
	//! Array containing all possible partial key bytes, those not set have an EMPTY_MARKER
	uint8_t child_index[Node::NODE_256_CAPACITY];
	//! ART node pointers to the child nodes
	Node children[Node::NODE_48_CAPACITY];

public:
	//! Get a new Node48 node, might cause a new buffer allocation, and initialize it
	static Node48 &New(ART &art, Node &node);
	//! Free the node (and its subtree)
	static void Free(ART &art, Node &node);
	//! Get a reference to the node
	static inline Node48 &Get(const ART &art, const Node ptr) {
		D_ASSERT(!ptr.IsSerialized());
		return *Node::GetAllocator(art, NType::NODE_48).Get<Node48>(ptr);
	}
	//! Initializes all the fields of the node while growing a Node16 to a Node48
	static Node48 &GrowNode16(ART &art, Node &node48, Node &node16);
	//! Initializes all fields of the node while shrinking a Node256 to a Node48
	static Node48 &ShrinkNode256(ART &art, Node &node48, Node &node256);

	//! Initializes a merge by incrementing the buffer IDs of the node
	void InitializeMerge(ART &art, const ARTFlags &flags);

	//! Insert a child node at byte
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child node at the respective byte
	static void DeleteChild(ART &art, Node &node, const uint8_t byte);

	//! Replace the child node at the respective byte
	inline void ReplaceChild(const uint8_t byte, const Node child) {
		D_ASSERT(child_index[byte] != Node::EMPTY_MARKER);
		children[child_index[byte]] = child;
	}

	//! Get the child for the respective byte in the node
	inline optional_ptr<Node> GetChild(const uint8_t byte) {
		if (child_index[byte] != Node::EMPTY_MARKER) {
			D_ASSERT(children[child_index[byte]].IsSet());
			return &children[child_index[byte]];
		}
		return nullptr;
	}
	//! Get the first child that is greater or equal to the specific byte
	optional_ptr<Node> GetNextChild(uint8_t &byte);

	//! Serialize this node
	BlockPointer Serialize(ART &art, MetaBlockWriter &writer);
	//! Deserialize this node
	void Deserialize(MetaBlockReader &reader);

	//! Vacuum the children of the node
	void Vacuum(ART &art, const ARTFlags &flags);
};
} // namespace duckdb
