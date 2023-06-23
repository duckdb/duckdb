//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/art/fixed_size_allocator.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/art/node.hpp"

namespace duckdb {

// classes
class MetaBlockWriter;
class MetaBlockReader;

// structs
struct BlockPointer;

//! The LEAF is a special node type that contains a count, up to LEAF_SIZE row IDs,
//! and a Node pointer. If this pointer is set, then it must point to another LEAF,
//! creating a chain of leaf nodes storing row IDs.
//! This class also covers the case of LEAF_INLINED, in which case we store the
//! row ID directly in the node pointer.
class Leaf {
public:
	//! The number of row IDs in this leaf
	uint8_t count;
	//! Up to LEAF_SIZE row IDs
	row_t row_ids[Node::LEAF_SIZE];
	//! A pointer to the next LEAF node
	Node ptr;

public:
	//! Inline a row ID into a node pointer
	static void New(Node &node, const row_t row_id);
	//! Get a new chain of leaf nodes, might cause new buffer allocations,
	//! with the node parameter holding the tail of the chain
	static void New(ART &art, reference<Node> &node, const row_t *row_ids, idx_t count);
	//! Free the leaf (chain)
	static void Free(ART &art, Node &node);
	//! Get a reference to the leaf
	static inline Leaf &Get(const ART &art, const Node ptr) {
		return *Node::GetAllocator(art, NType::LEAF).Get<Leaf>(ptr);
	}

	//! Initializes a merge by incrementing the buffer IDs of the leaf
	static void InitializeMerge(ART &art, Node &node, const ARTFlags &flags);
	//! Merge leaves and free all copied leaf nodes
	static void Merge(ART &art, Node &l_node, Node &r_node);

	//! Insert a row ID into a leaf
	static void Insert(ART &art, Node &node, const row_t row_id);
	//! Remove a row ID from a leaf
	static void Remove(ART &art, reference<Node> &node, const row_t row_id);

	//! Get the row ID at the position
	static row_t GetRowId(ART &art, Node &node, const idx_t position);

	//! Returns the string representation of the leaf, or only traverses and verifies the leaf
	static string VerifyAndToString(ART &art, Node &node, const bool only_verify);

	//! Serialize the leaf
	static BlockPointer Serialize(const ART &art, Node &node, MetaBlockWriter &writer);
	//! Deserialize the leaf
	static void Deserialize(ART &art, Node &node, MetaBlockReader &reader);

	//! Vacuum the leaf
	inline void Vacuum(ART &art, const ARTFlags &flags) {
		ptr.Vacuum(art, flags);
	}

private:
	//! Moves the inlined row ID onto a leaf
	static void MoveInlinedToLeaf(ART &art, Node &node);
	//! Appends the row ID to this leaf, or creates a subsequent leaf, if this node is full
	Leaf &Append(ART &art, const row_t row_id);
};

} // namespace duckdb
