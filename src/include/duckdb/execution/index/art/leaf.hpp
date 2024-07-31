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

//! There are three types of leaves.
//! 1. LEAF_INLINED: Inlines a row ID in a Node pointer.
//! 2. LEAF: Deprecated. A list of Leaf nodes containing row IDs.
//! 3. Nested leaves indicated by gate nodes. If an ART key contains multiple row IDs, then we use the row IDs as keys
//! and create a nested ART behind the gate node. As row IDs are always unique, these nested ARTs never contain
//! duplicates themselves.
class Leaf {
public:
	Leaf() = delete;
	Leaf(const Leaf &) = delete;
	Leaf &operator=(const Leaf &) = delete;

	uint8_t count;                  // Deprecated.
	row_t row_ids[Node::LEAF_SIZE]; // Deprecated.
	Node ptr;                       // Deprecated.

public:
	//! Inline a row ID into a node pointer.
	static void New(Node &node, const row_t row_id);
	//! Get a new non-inlined nested leaf node. Might cause new buffer allocations.
	static void New(ART &art, reference<Node> &node, unsafe_vector<ARTKey> &row_ids, idx_t start, idx_t count);

	//! Merges two leaves.
	static void MergeInlined(ART &art, Node &l_node, Node &r_node);

	//! Inserts a row ID into an inlined leaf.
	static void InsertIntoInlined(ART &art, Node &node, reference<ARTKey> row_id);
	//! Erase a row ID from a nested leaf.
	static void EraseFromNested(ART &art, Node &node, const ARTKey &row_id);

	//! Transforms a deprecated leaf to a nested leaf.
	static void TransformToNested(ART &art, Node &node);
	//! Transforms a nested leaf to a deprecated leaf.
	static void TransformToDeprecated(ART &art, Node &node);

	//! Returns true, if the leaf contains the row ID.
	static bool ContainsRowId(ART &art, const Node &node, const ARTKey &row_id);

public:
	//! Frees the linked list of leaves.
	static void DeprecatedFree(ART &art, Node &node);
	//! Fills the row_ids vector with the row IDs of this linked list of leaves.
	//! Never pushes more than max_count row IDs.
	static bool DeprecatedGetRowIds(ART &art, const Node &node, unsafe_vector<row_t> &row_ids, const idx_t max_count);
	//! Vacuums the linked list of leaves.
	static void DeprecatedVacuum(ART &art, Node &node);
	//! Returns the string representation of the linked list of leaves, if only_verify is true.
	//! Else, it traverses and verifies the linked list of leaves.
	static string DeprecatedVerifyAndToString(ART &art, const Node &node, const bool only_verify);
};

} // namespace duckdb
