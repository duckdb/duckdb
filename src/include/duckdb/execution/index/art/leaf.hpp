//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/leaf.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/fixed_size_allocator.hpp"
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
	//! Delete copy constructors, as any Leaf can never own its memory
	Leaf(const Leaf &) = delete;
	Leaf &operator=(const Leaf &) = delete;

	//! The number of row IDs in this leaf
	uint8_t count;
	//! Up to LEAF_SIZE row IDs
	row_t row_ids[Node::LEAF_SIZE];
	//! A pointer to the next LEAF node
	Node ptr;

public:
	//! Inline a row ID into a node pointer.
	static void New(Node &node, const row_t row_id);
	//! Get a new leaf node without any data.
	static Leaf &New(ART &art, Node &node);
	//! Get a new non-inlined leaf node, might cause new buffer allocations.
	static void New(ART &art, reference<Node> &node, const vector<ARTKey> &row_ids, const idx_t start,
	                const idx_t count);
	//! Free the leaf.
	static void Free(ART &art, Node &node);

	//! Initializes a merge by incrementing the buffer IDs of the leaf.
	static void InitializeMerge(ART &art, Node &node, const ARTFlags &flags);
	//! Merges two leaves.
	static void Merge(ART &art, Node &l_node, Node &r_node);

	//! Insert a row ID into a leaf.
	static void Insert(ART &art, Node &node, const ARTKey &row_id);
	//! Remove a row ID from a leaf. Returns true, if the leaf is empty after the removal.
	static bool Remove(ART &art, reference<Node> &node, const ARTKey &row_id);

	//! Fill the row_ids vector with the row IDs of this leaf, if the total count does not exceed max_count.
	static bool GetRowIds(ART &art, const Node &node, vector<row_t> &row_ids, const idx_t max_count);
	//! Returns true, if the leaf contains the row ID.
	static bool ContainsRowId(ART &art, const Node &node, const ARTKey &row_id);

	//! Returns the string representation of the leaf, or only traverses and verifies the leaf.
	static string VerifyAndToString(ART &art, const Node &node, const bool only_verify);

	//! Vacuum the leaf.
	static void Vacuum(ART &art, Node &node, const ARTFlags &flags);

private:
	//! Unnests a non-inlined leaf node by returning its mutable root node.
	static Node &UnnestMutable(ART &art, Node &node);
	//! Unnests a non-inlined leaf node by returning its constant root node.
	static const Node &Unnest(ART &art, const Node &node);

	//! Merge an inlined row ID into a leaf.
	static void MergeInlinedIntoLeaf(ART &art, Node &l_node, Node &r_node);

public:
	//===--------------------------------------------------------------------===//
	// Deprecated code paths.
	//===--------------------------------------------------------------------===//

	//! Get a new chain of leaf nodes, might cause new buffer allocations,
	//! with the node parameter holding the tail of the chain.
	static void DeprecatedNew(ART &art, reference<Node> &node, const vector<ARTKey> &row_ids, const idx_t start,
	                          const idx_t count);
	//! Free the leaf (chain).
	static void DeprecatedFree(ART &art, Node &node);

	//! Initializes a merge by incrementing the buffer IDs of the leaf (chain).
	static void DeprecatedInitializeMerge(ART &art, Node &node, const ARTFlags &flags);
	//! Merge leaf (chains) and free all copied leaf nodes.
	static void DeprecatedMerge(ART &art, Node &l_node, Node &r_node);

	//! Insert a row ID into a leaf.
	static void DeprecatedInsert(ART &art, Node &node, const row_t row_id);
	//! Remove a row ID from a non-inlined leaf. Returns true, if the leaf is empty after the removal.
	static bool DeprecatedRemove(ART &art, reference<Node> &node, const row_t row_id);

	//! Get the total count of row IDs in the chain of leaves.
	static idx_t DeprecatedTotalCount(ART &art, const Node &node);
	//! Fill the row_ids vector with the row IDs of this non-inlined leaf chain, if the total count does not exceed
	//! max_count.
	static bool DeprecatedGetRowIds(ART &art, const Node &node, vector<row_t> &row_ids, const idx_t max_count);
	//! Returns true, if the non-inlined leaf contains the row ID.
	static bool DeprecatedContainsRowId(ART &art, const Node &node, const row_t row_id);

	//! Returns the string representation of the leaf (chain), or only traverses and verifies the leaf (chain).
	static string DeprecatedVerifyAndToString(ART &art, const Node &node, const bool only_verify);

	//! Vacuum the leaf (chain).
	static void DeprecatedVacuum(ART &art, Node &node);

private:
	//! Moves the inlined row ID onto a leaf.
	static void DeprecatedMoveInlinedToLeaf(ART &art, Node &node);
	//! Appends the row ID to this leaf, or creates a subsequent leaf, if this node is full.
	Leaf &DeprecatedAppend(ART &art, const row_t row_id);
};

} // namespace duckdb
