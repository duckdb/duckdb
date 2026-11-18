//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/order/top_n_boundary.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {
struct DynamicFilterData;

//! What the selection does with the rows whose keys are equal to the boundary
enum class TopNBoundaryTies : uint8_t {
	//! Only the rows that sort strictly before the boundary are selected. For an operator that already holds the
	//! boundary row itself, such as the heap of the Top-N: an equal row cannot replace it.
	EXCLUDE,
	//! The rows that are equal to the boundary are selected as well. For an operator that has to pass every row of
	//! the boundary key on, such as the top-key filter below an aggregate.
	INCLUDE
};

//! The order of the rows in the selection
enum class TopNBoundaryRowOrder : uint8_t {
	//! The rows are selected key by key: the selection is not in the order of the input. For a consumer that does not
	//! depend on it, such as a heap.
	ANY,
	//! The selection is in the order of the input rows. For a streaming operator that forwards the rows.
	INPUT_ORDER
};

//! A boundary in a sort order that is shared by the threads of an operator: a sort key that only moves towards the
//! start of the order. Every published boundary has to be valid on its own (no row that sorts after it is needed), so
//! that a thread may keep using an older one. The first key of the boundary is forwarded to a dynamic filter, if any.
struct TopNBoundaryValue {
	TopNBoundaryValue(const vector<BoundOrderByNode> &orders, optional_ptr<DynamicFilterData> dynamic_filter);

	//! Copies the boundary into current_boundary if it differs from it and the lock is free. Returns whether it was
	//! copied. A boundary that could not be refreshed without blocking stays valid.
	bool TryGetBoundaryValue(string &current_boundary);
	//! Publishes a boundary. It is ignored unless it sorts before the current one.
	void UpdateValue(string_t boundary_val);

private:
	optional_ptr<DynamicFilterData> dynamic_filter;
	mutex lock;
	string boundary_value;
	bool is_set = false;
	Vector boundary_vector;
	OrderModifiers boundary_modifiers;
};

//! The thread-local side of a boundary: its copy of the shared boundary, and the selection of the rows of a chunk of
//! keys that sort before a boundary in the lexicographic order of the keys
class TopNBoundarySelector {
public:
	TopNBoundarySelector(Allocator &allocator, const vector<BoundOrderByNode> &orders);

	//! Refreshes the local copy of the shared boundary. Returns whether there is a boundary.
	bool Refresh(TopNBoundaryValue &global_boundary);
	bool HasBoundary() const {
		return !boundary_val.empty();
	}
	//! The local copy of the shared boundary as constant vectors (only valid if there is a boundary)
	DataChunk &BoundaryValues() {
		return boundary_values;
	}
	//! Decodes a boundary in sort-key format into constant vectors
	void Decode(const string &sort_key, DataChunk &target) const;

	//! Selects the rows of "keys" (those of sel, or all of them) that sort before "bound" - constant vectors, one per
	//! key - and writes their indexes to result. Returns their number.
	idx_t Select(DataChunk &keys, DataChunk &bound, optional_ptr<const SelectionVector> sel, idx_t count,
	             TopNBoundaryTies ties, TopNBoundaryRowOrder row_order, SelectionVector &result);

private:
	const vector<BoundOrderByNode> &orders;
	vector<OrderModifiers> modifiers;
	//! The shared boundary as this thread last saw it: in sort-key format and as constant vectors
	string boundary_val;
	DataChunk boundary_values;
	DataChunk compare_chunk;
	SelectionVector true_sel;
	SelectionVector false_sel;
	SelectionVector new_remaining_sel;
	//! Marks the selected rows to restore the order of the input (allocated on first use)
	unsafe_unique_array<bool> selected_rows;
};

} // namespace duckdb
