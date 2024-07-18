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

//! TODO: change description. Also, anything useful to do with the memory here?

//! The LEAF is a special node type that contains a count, up to LEAF_SIZE row IDs,
//! and a Node pointer. If this pointer is set, then it must point to another LEAF,
//! creating a chain of leaf nodes storing row IDs.
//! This class also contains functionality for nodes of type LEAF_INLINED, in which case we store the
//! row ID directly in the node pointer.
class Leaf {
public:
	Leaf() = delete;
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
	//! Get a new non-inlined nested leaf node, might cause new buffer allocations.
	static void New(ART &art, reference<Node> &node, const vector<ARTKey> &row_ids, const idx_t start,
	                const idx_t count);

	//! Merges two leaves.
	static void Merge(ART &art, Node &l_node, Node &r_node);

	//! Inserts a row ID into an inlined leaf.
	static void InsertIntoInlined(ART &art, Node &node, reference<const ARTKey> row_id);
	//! Removes a row ID from the leaf.
	//! Returns true, if the leaf is empty after the removal, else false.
	static bool Remove(ART &art, reference<Node> &node, const ARTKey &row_id);

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
	static bool DeprecatedGetRowIds(ART &art, const Node &node, vector<row_t> &row_ids, const idx_t max_count);

	//! Vacuums the linked list of leaves.
	static void DeprecatedVacuum(ART &art, Node &node);

	//! Returns the string representation of the linked list of leaves, if only_verify is true.
	//! Else, it traverses and verifies the linked list of leaves.
	static string DeprecatedVerifyAndToString(ART &art, const Node &node, const bool only_verify);
};

} // namespace duckdb
