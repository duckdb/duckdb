//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art_merger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/stack.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

//! ARTMerger merges two ART nodes and their subtrees, until
//! 1. triggering a constraint violation.
//! 2. all nodes have been processed.
//! Must be initialized via its Init function before calling Merge,
//! otherwise, the stack is empty and there is nothing to do.
class ARTMerger {
public:
	ARTMerger() = delete;
	ARTMerger(ArenaAllocator &arena, ART &art) : arena(arena), art(art) {
	}

public:
	//! Init the merge by setting the initial nodes.
	void Init(Node &left, Node &right);
	//! Merge until (1) triggering constraint violation or (2) all nodes have been processed.
	ARTConflictType Merge();

private:
	//! NodeEntry holds a node on the stack.
	//! The ARTMerger always merges into the left node.
	//! If inside a gate, status is always GATE_SET, otherwise, it is GATE_NOT_SET.
	//! The depth resets when entering a gate.
	struct NodeEntry {
		NodeEntry() = delete;
		NodeEntry(Node &left, Node &right, const GateStatus status, const idx_t depth)
		    : left(left), right(right), status(status), depth(depth) {};

		Node &left;
		Node &right;
		GateStatus status;
		idx_t depth;
	};

	//! The arena holds any temporary memory allocated during the Merge phase.
	ArenaAllocator &arena;
	//! The ART holding the node memory.
	ART &art;
	//! The stack. While merging, NodeEntry elements are pushed onto of the stack.
	stack<NodeEntry> s;

private:
	// When pushing anything on the stack, we ensure that:
	// - if left is LEAF_INLINED, then right is also LEAF_INLINED.
	// - if left is PREFIX, then right is also PREFIX (except for PREFIX + LEAF_INLINED).
	void Emplace(Node &left, Node &right, const GateStatus parent_status, const idx_t depth);

	ARTConflictType MergeNodeAndInlined(NodeEntry &entry);

	array_ptr<uint8_t> GetBytes(Node &leaf);
	void MergeLeaves(NodeEntry &entry);

	NodeChildren ExtractChildren(Node &node);
	void MergeNodes(NodeEntry &entry);

	//! Merges a node and a prefix.
	//! pos determines up to which position we need to reduce the prefix.
	//! If we call into this function via MergePrefixes (case: one prefix contains the other),
	//! then pos is the length of the shorter prefix.
	void MergeNodeAndPrefix(Node &node, Node &prefix, const GateStatus parent_status, const idx_t parent_depth,
	                        const uint8_t pos);
	void MergeNodeAndPrefix(Node &node, Node &prefix, const GateStatus parent_status, const idx_t parent_depth);
	void MergePrefixes(NodeEntry &entry);
};

} // namespace duckdb
