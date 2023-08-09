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
class MetadataWriter;
class MetadataReader;

// structs
struct BlockPointer;

//! The LEAF is a special node type that contains a count, up to LEAF_SIZE row IDs,
//! and a Node pointer. If this pointer is set, then it must point to another LEAF,
//! creating a chain of leaf nodes storing row IDs.
//! This class also contains functionality for nodes of type LEAF_INLINED, in which case we store the
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
		D_ASSERT(!ptr.IsSerialized());
		return *Node::GetAllocator(art, NType::LEAF).Get<Leaf>(ptr);
	}

	//! Initializes a merge by incrementing the buffer IDs of the leaf (chain)
	static void InitializeMerge(ART &art, Node &node, const ARTFlags &flags);
	//! Merge leaves (chains) and free all copied leaf nodes
	static void Merge(ART &art, Node &l_node, Node &r_node);

	//! Insert a row ID into a leaf
	static void Insert(ART &art, Node &node, const row_t row_id);
	//! Remove a row ID from a leaf. Returns true, if the leaf is empty after the removal
	static bool Remove(ART &art, reference<Node> &node, const row_t row_id);

	//! Get the total count of row IDs in the chain of leaves
	static idx_t TotalCount(ART &art, Node &node);
	//! Fill the result_ids vector with the row IDs of this leaf chain, if the total count does not exceed max_count
	static bool GetRowIds(ART &art, Node &node, vector<row_t> &result_ids, idx_t max_count);
	//! Returns whether the leaf contains the row ID
	static bool ContainsRowId(ART &art, Node &node, const row_t row_id);

	//! Returns the string representation of the leaf (chain), or only traverses and verifies the leaf (chain)
	static string VerifyAndToString(ART &art, Node &node);

	//! Serialize the leaf (chain)
	static BlockPointer Serialize(ART &art, Node &node, MetadataWriter &writer);
	//! Deserialize the leaf (chain)
	static void Deserialize(ART &art, Node &node, MetadataReader &reader);

	//! Vacuum the leaf (chain)
	static void Vacuum(ART &art, Node &node);

private:
	//! Moves the inlined row ID onto a leaf
	static void MoveInlinedToLeaf(ART &art, Node &node);
	//! Appends the row ID to this leaf, or creates a subsequent leaf, if this node is full
	Leaf &Append(ART &art, const row_t row_id);
};

} // namespace duckdb
