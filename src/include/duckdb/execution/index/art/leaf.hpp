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
//! 1. LEAF_INLINED: Inlines a row ID in a NodePtr.
//! 2. LEAF: Deprecated. A list of Leaf nodes containing row IDs.
//! 3. Nested leaves indicated by gate nodes. If an ART key contains multiple row IDs,
//! then we use the row IDs as keys and create a nested ART behind the gate node.
//! As row IDs are always unique, these nested ARTs never contain duplicates themselves.
class Leaf {
public:
	static constexpr NType LEAF = NType::LEAF;
	static constexpr NType INLINED = NType::LEAF_INLINED;

	static constexpr uint8_t LEAF_SIZE = 4; // Deprecated.

public:
	Leaf() = delete;
	Leaf(const Leaf &) = delete;
	Leaf &operator=(const Leaf &) = delete;

private:
	uint8_t count;            // Deprecated.
	row_t row_ids[LEAF_SIZE]; // Deprecated.
	NodePtr ptr;              // Deprecated.

public:
	//! Inline a row ID into a node pointer.
	static void New(NodePtr &node, const row_t row_id);

	//! Merge two inlined leaf nodes.
	static void MergeInlined(ArenaAllocator &arena, ART &art, NodePtr &left, NodePtr &right, GateStatus status,
	                         idx_t depth);

	//! Transforms a deprecated leaf to a nested leaf.
	static void TransformToNested(ART &art, NodePtr &node);
	//! Transforms a nested leaf to a deprecated leaf.
	static void TransformToDeprecated(ART &art, NodePtr &node);

public:
	//! Frees the linked list of leaves.
	static void DeprecatedFree(ART &art, NodePtr &node);
	//! Fills the row_ids vector with the row IDs of this linked list of leaves.
	//! Never pushes more than max_count row IDs.
	static bool DeprecatedGetRowIds(ART &art, const NodePtr &node, set<row_t> &row_ids, const idx_t max_count);
	//! Vacuums the internal links in the deprecated leaf list pointed to by node.
	//! The caller is responsible for vacuuming the slot that points to the list head.
	static void DeprecatedVacuum(ART &art, NodePtr node);

	//! Traverses and verifies the linked list of leaves.
	static void DeprecatedVerify(ART &art, const NodePtr &node);
	//! Count the number of leaves.
	static void DeprecatedVerifyAllocations(ART &art, const NodePtr &node, unordered_map<uint8_t, idx_t> &node_counts);

	//! Return string representation of the linked list of leaves.
	//! If print_deprecated_leaves is false, returns "[deprecated leaves]" with proper indentation.
	static string DeprecatedToString(ART &art, const NodePtr &node, const ToStringOptions &options);
};

} // namespace duckdb
