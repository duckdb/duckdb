//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_index_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_merge_sort_tree.hpp"

namespace duckdb {

class WindowIndexTree;

class WindowIndexTreeLocalState : public WindowMergeSortTreeLocalState {
public:
	explicit WindowIndexTreeLocalState(WindowIndexTree &index_tree);

	//! Process sorted leaf data
	void BuildLeaves() override;

	//! The index tree we are building
	WindowIndexTree &index_tree;
};

class WindowIndexTree : public WindowMergeSortTree {
public:
	WindowIndexTree(ClientContext &context, const vector<BoundOrderByNode> &orders, const vector<column_t> &sort_idx,
	                const idx_t count);
	WindowIndexTree(ClientContext &context, const BoundOrderModifier &order_bys, const vector<column_t> &sort_idx,
	                const idx_t count);
	~WindowIndexTree() override = default;

	unique_ptr<WindowAggregatorState> GetLocalState() override;

	//! Find the Nth index in the set of subframes
	//! Returns {nth index, 0} or {nth offset, overflow}
	pair<idx_t, idx_t> SelectNth(const SubFrames &frames, idx_t n) const;
};

} // namespace duckdb
