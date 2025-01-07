//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_token_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/window/window_merge_sort_tree.hpp"

namespace duckdb {

// Builds a merge sort tree that uses integer tokens for the comparison values instead of the sort keys.
class WindowTokenTree : public WindowMergeSortTree {
public:
	WindowTokenTree(ClientContext &context, const vector<BoundOrderByNode> &orders, const vector<column_t> &sort_idx,
	                const idx_t count, bool unique = false)
	    : WindowMergeSortTree(context, orders, sort_idx, count, unique) {
	}
	WindowTokenTree(ClientContext &context, const BoundOrderModifier &order_bys, const vector<column_t> &sort_idx,
	                const idx_t count, bool unique = false)
	    : WindowTokenTree(context, order_bys.orders, sort_idx, count, unique) {
	}

	unique_ptr<WindowAggregatorState> GetLocalState() override;

	//! Thread-safe post-sort cleanup
	void CleanupSort() override;

	//! Find the rank of the row within the range
	idx_t Rank(const idx_t lower, const idx_t upper, const idx_t row_idx) const;

	//! Find the next peer after the row and within the range
	idx_t PeerEnd(const idx_t lower, const idx_t upper, const idx_t row_idx) const;

	//! Peer boundaries.
	vector<uint8_t> deltas;

protected:
	//! Find the starts of all the blocks
	idx_t MeasurePayloadBlocks() override;
};

} // namespace duckdb
