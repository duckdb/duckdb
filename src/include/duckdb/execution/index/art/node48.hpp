//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/node48.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

//! Node48 holds up to 48 Node children. It contains a child_index array, which is indexed by the key
//! byte, and which contains the position of the child node in the children array.
class Node48 {
public:
	Node48() = delete;
	Node48(const Node48 &) = delete;
	Node48 &operator=(const Node48 &) = delete;

	//! Number of non-null children
	uint8_t count;
	//! Array containing all possible partial key bytes, those not set have an EMPTY_MARKER
	uint8_t child_index[Node::NODE_256_CAPACITY];
	//! Node pointers to the child nodes
	Node children[Node::NODE_48_CAPACITY];

public:
	//! Get a new Node48, might cause a new buffer allocation, and initialize it
	static Node48 &New(ART &art, Node &node);
	//! Free the node (and its subtree)
	static void Free(ART &art, Node &node);

	//! Initializes all the fields of the node while growing a Node16 to a Node48
	static Node48 &GrowNode16(ART &art, Node &node48, Node &node16);
	//! Initializes all fields of the node while shrinking a Node256 to a Node48
	static Node48 &ShrinkNode256(ART &art, Node &node48, Node &node256);

	//! Initializes a merge by incrementing the buffer IDs of the node
	void InitializeMerge(ART &art, const unsafe_vector<idx_t> &upper_bounds);

	//! Insert a child node at byte
	static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);
	//! Delete the child node at byte
	static void DeleteChild(ART &art, Node &node, const uint8_t byte);

	//! Replace the child node at byte
	inline void ReplaceChild(const uint8_t byte, const Node child) {
		D_ASSERT(child_index[byte] != Node::EMPTY_MARKER);
		auto was_gate = children[child_index[byte]].IsGate();
		children[child_index[byte]] = child;
		if (was_gate && child.HasMetadata()) {
			children[child_index[byte]].SetGate();
		}
	}

	//! Get the (immutable) child for the respective byte in the node
	const Node *GetChild(const uint8_t byte) const;
	//! Get the child for the respective byte in the node
	Node *GetChildMutable(const uint8_t byte);
	//! Get the first (immutable) child that is greater or equal to the specific byte
	const Node *GetNextChild(uint8_t &byte) const;
	//! Get the first child that is greater or equal to the specific byte
	Node *GetNextChildMutable(uint8_t &byte);

	//! Vacuum the children of the node
	void Vacuum(ART &art, const unordered_set<uint8_t> &indexes);
	//! Transform the children of the node.
	void TransformToDeprecated(ART &art, unsafe_unique_ptr<FixedSizeAllocator> &allocator);
	//! Recurse into the children to count each node type.
	void VerifyAllocations(ART &art, unordered_map<uint8_t, idx_t> &node_counts) const;
};
} // namespace duckdb
