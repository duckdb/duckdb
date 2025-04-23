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

class ARTMerger {
public:
	ARTMerger() = delete;
	ARTMerger(Allocator &allocator, ART &art) : arena(allocator), art(art) {
	}

public:
	void Init(Node &left, Node &right);
	ARTConflictType Merge();

private:
	struct NodeEntry {
		NodeEntry() = delete;
		NodeEntry(Node &left, Node &right, const GateStatus status, const idx_t depth)
		    : left(left), right(right), status(status), depth(depth) {};

		Node &left;
		Node &right;
		GateStatus status;
		idx_t depth;
	};

	ArenaAllocator arena;
	ART &art;
	stack<NodeEntry> s;

private:
	// When pushing anything on the stack, we ensure that:
	// - if left is LEAF_INLINED, then right is also LEAF_INLINED.
	// - if left is PREFIX, then right is also PREFIX (except for PREFIX <-> LEAF_INLINED combinations).
	void Emplace(Node &left, Node &right, const GateStatus parent_status, const idx_t depth);

	void MergeInlined(NodeEntry &entry);
	ARTConflictType MergeNodeAndInlined(NodeEntry &entry);

	array_ptr<uint8_t> GetBytes(Node &node);
	void MergeLeaves(NodeEntry &entry);

	NodeChildren ExtractChildren(Node &node);
	void MergeNodes(NodeEntry &entry);

	void MergeNodeAndPrefix(Node &node, Node &prefix, const GateStatus parent_status, const idx_t parent_depth,
	                        uint8_t pos);
	void MergeNodeAndPrefix(Node &node, Node &prefix, const GateStatus parent_status, const idx_t parent_depth);
	void MergePrefixes(NodeEntry &entry);
};

} // namespace duckdb
