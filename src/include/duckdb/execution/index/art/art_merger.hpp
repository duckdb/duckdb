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

enum class ARTMergeResult : uint8_t {
	SUCCESS,
	DUPLICATE,
};

class ARTMerger {
public:
	ARTMerger() = delete;
	ARTMerger(Allocator &allocator, ART &art) : arena(allocator), art(art) {
	}

public:
	void Init(Node &left, Node &right);
	ARTMergeResult Merge();

private:
	struct NodeEntry {
		NodeEntry() = delete;
		NodeEntry(Node &left, Node &right) : left(left), right(right) {};

		Node &left;
		Node &right;
	};

	ArenaAllocator arena;
	ART &art;
	stack<NodeEntry> s;

private:
	// When pushing anything on the stack, we ensure that:
	// - if left is LEAF_INLINED, then right is also LEAF_INLINED.
	// - if left is PREFIX, then right is also PREFIX.
	void Emplace(Node &left, Node &right);

	void TransformInlinedToNested(Node &inlined);
	void MergeInlined(Node &left, Node &right);
	void MergeGateAndInlined(Node &gate, Node &inlined);

	array_ptr<uint8_t> GetBytes(Node &node);
	void MergeLeaves(Node &left, Node &right);

	NodeChildren ExtractChildren(Node &node);
	void MergeNodes(Node &left, Node &right);

	void MergeNodeAndPrefix(Node &node, Node &prefix, uint8_t pos);
	void MergeNodeAndPrefix(Node &node, Node &prefix);
	void MergePrefixes(Node &left, Node &right);
};

} // namespace duckdb
