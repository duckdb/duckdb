//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node256.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Node256 holds up to 256 ARTNode children which can be directly indexed by the key byte
class Node256 {
public:
	//! Number of non-null children
	uint16_t count;
	//! ART node pointers to the child nodes
	Node children[Node::NODE_256_CAPACITY];

public:
	//! Get a new Node256 node, might cause a new buffer allocation, and initialize it
	static Node256 &New(ART &art, Node &node);
	//! Free the node (and its subtree)
	static void Free(ART &art, Node &node);
	//! Get a reference to the node
	static inline Node256 &Get(const ART &art, const Node ptr) {
		D_ASSERT(!ptr.IsSerialized());
		return *Node::GetAllocator(art, NType::NODE_256).Get<Node256>(ptr);
	}
	//! Initializes all the fields of the node while growing a Node48 to a Node256
	static Node256 &GrowNode48(ART &art, Node &node256, Node &node48);

	//! Initializes a merge by incrementing the buffer IDs of the node
	void InitializeMerge(ART &art, const ARTFlags &flags);

	//! Insert a child node at byte
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child node at the respective byte
	static void DeleteChild(ART &art, Node &node, const uint8_t byte);

	//! Replace the child node at the respective byte
	inline void ReplaceChild(const uint8_t byte, const Node child) {
		children[byte] = child;
	}

	//! Get the child for the respective byte in the node
	inline optional_ptr<Node> GetChild(const uint8_t byte) {
		if (children[byte].IsSet()) {
			return &children[byte];
		}
		return nullptr;
	}
	//! Get the first child that is greater or equal to the specific byte
	optional_ptr<Node> GetNextChild(uint8_t &byte);

	//! Serialize this node
	BlockPointer Serialize(ART &art, MetadataWriter &writer);
	//! Deserialize this node
	void Deserialize(MetadataReader &reader);

	//! Vacuum the children of the node
	void Vacuum(ART &art, const ARTFlags &flags);
};
} // namespace duckdb
