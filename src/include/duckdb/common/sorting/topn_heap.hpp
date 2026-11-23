//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/topn_heap.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arena_containers/arena_vector.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Heaps
//===--------------------------------------------------------------------===//
class TopNHeap;

struct TopNEntry {
	string_t sort_key;
	idx_t index;

	bool operator<(const TopNEntry &other) const {
		return sort_key < other.sort_key;
	}
};

struct TopNScanState {
	TopNScanState() : pos(0), sel(STANDARD_VECTOR_SIZE) {
	}

	idx_t pos;
	vector<sel_t> scan_order;
	SelectionVector sel;
};

struct TopNBoundaryValue {
	TopNBoundaryValue(const BoundOrderByNode &order, const shared_ptr<DynamicFilterData> &dynamic_filter)
	    : dynamic_filter(dynamic_filter), boundary_vector(order.expression->GetReturnType()),
	      boundary_modifiers(order.type, order.null_order) {
	}

	const shared_ptr<DynamicFilterData> &dynamic_filter;
	mutex lock;
	string boundary_value;
	bool is_set = false;
	Vector boundary_vector;
	OrderModifiers boundary_modifiers;

	bool TryGetBoundaryValue(string &current_boundary) {
		unique_lock<mutex> l(lock, std::try_to_lock);
		if (!l.owns_lock() || boundary_value == current_boundary) {
			return false;
		}
		current_boundary = boundary_value;
		return true;
	}

	void UpdateValue(string_t boundary_val) {
		unique_lock<mutex> l(lock);
		if (!is_set || boundary_val < string_t(boundary_value)) {
			boundary_value = boundary_val.GetString();
			is_set = true;
			if (dynamic_filter) {
				CreateSortKeyHelpers::DecodeSortKey(boundary_val, boundary_vector, 0, boundary_modifiers);
				auto new_dynamic_value = boundary_vector.GetValue(0);
				l.unlock();
				dynamic_filter->SetValue(std::move(new_dynamic_value));
			}
		}
	}
};

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

//! The thread-local side of a boundary: its copy of the shared boundary, and the selection of the rows of a chunk of
//! keys that sort before a boundary in the lexicographic order of the keys
class TopNBoundarySelector {
public:
	//! not_distinct_from: the NOT DISTINCT FROM comparison of every key with its boundary value (created at plan time),
	//! over the input [keys..., boundary values...]
	TopNBoundarySelector(ClientContext &context, Allocator &allocator, const vector<BoundOrderByNode> &orders,
	                     const vector<unique_ptr<Expression>> &not_distinct_from);

	//! Refreshes the local copy of the shared boundary. Returns whether there is a boundary.
	bool Refresh(TopNBoundaryValue &global_boundary);
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
	//! NOT DISTINCT FROM comparison of every key column with its boundary value
	vector<unique_ptr<ExpressionExecutor>> not_distinct_from;
	//! The shared boundary as this thread last saw it: in sort-key format and as constant vectors
	string boundary_val;
	DataChunk boundary_values;
	DataChunk compare_chunk;
	//! The input of not_distinct_from: [keys..., boundary values...]
	DataChunk boundary_compare_chunk;
	SelectionVector true_sel;
	SelectionVector false_sel;
	SelectionVector new_remaining_sel;
	//! Marks the selected rows to restore the order of the input (allocated on first use)
	unsafe_unique_array<bool> selected_rows;
};

struct TopOneFunc {
	typedef idx_t (*topone_size_t)();
	typedef optional_idx (*topone_update_t)(data_ptr_t state_p, Vector &sort_key, const idx_t count);
	typedef bool (*topone_combine_t)(const_data_ptr_t source_p, data_ptr_t target_p);

	topone_size_t state_size;
	topone_update_t update;
	topone_combine_t combine;
};

class TopNHeap {
public:
	static bool CanOptimize(idx_t limit, idx_t cardinality);

	TopNHeap(ClientContext &context, const vector<LogicalType> &payload_types, const vector<BoundOrderByNode> &orders,
	         const vector<unique_ptr<Expression>> &not_distinct_from, idx_t limit, idx_t offset);
	TopNHeap(ExecutionContext &context, const vector<LogicalType> &payload_types,
	         const vector<BoundOrderByNode> &orders, const vector<unique_ptr<Expression>> &not_distinct_from,
	         idx_t limit, idx_t offset);
	TopNHeap(ClientContext &context, Allocator &allocator, const vector<LogicalType> &payload_types,
	         const vector<BoundOrderByNode> &orders, const vector<unique_ptr<Expression>> &not_distinct_from,
	         idx_t limit, idx_t offset);

	Allocator &allocator;
	BufferManager &buffer_manager;
	ArenaAllocator arena_allocator;
	unsafe_arena_vector<TopNEntry> heap;
	const vector<LogicalType> &payload_types;
	const vector<BoundOrderByNode> &orders;
	vector<OrderModifiers> modifiers;
	idx_t limit;
	idx_t offset;
	idx_t heap_size;
	ExpressionExecutor executor;
	DataChunk sort_chunk;
	DataChunk heap_data;
	DataChunk sort_keys;
	CreateSortKeyState sort_key_state;
	StringHeap sort_key_heap;

	SelectionVector matching_sel;

	//! The local copy of the global boundary value and the selection of the rows before it (heap only)
	unique_ptr<TopNBoundarySelector> boundary_selector;
	SelectionVector final_sel;

	unique_ptr<TopOneFunc> topone;
	vector<data_t> topone_state;

public:
	void Sink(DataChunk &input, optional_ptr<TopNBoundaryValue> boundary_value = nullptr);
	void Combine(TopNHeap &other);
	void Reduce();
	void Finalize();

	void InitializeScan(TopNScanState &state, bool exclude_offset);
	void Scan(TopNScanState &state, DataChunk &chunk, idx_t &pos);

	bool CheckBoundaryValues(DataChunk &sort_chunk, DataChunk &payload, TopNBoundaryValue &boundary_val);
	void AddSingleHeap(DataChunk &input, optional_ptr<TopNBoundaryValue> global_boundary);
	void AddSmallHeap(DataChunk &input, const Vector &sort_keys_vec);
	void AddLargeHeap(DataChunk &input, const Vector &sort_keys_vec);

public:
	idx_t ReduceThreshold() const {
		return MaxValue<idx_t>(STANDARD_VECTOR_SIZE * 5ULL, 2ULL * heap_size);
	}

	idx_t InitialHeapAllocSize() const {
		return MinValue<idx_t>(STANDARD_VECTOR_SIZE * 100ULL, ReduceThreshold()) + STANDARD_VECTOR_SIZE;
	}

private:
	inline bool EntryShouldBeAdded(const string_t &sort_key) {
		if (heap_size == 0) {
			// the heap has no capacity (LIMIT 0) - no entry can ever be added
			return false;
		}
		if (heap.size() < heap_size) {
			// heap is not full yet - the entry can be added
			return true;
		}
		if (sort_key < heap.front().sort_key) {
			// sort key is smaller than current max value
			return true;
		}
		// heap is full and there is no room for the entry
		return false;
	}

	inline void AddEntryToHeap(const TopNEntry &entry) {
		D_ASSERT(heap_size > 0);
		if (heap.size() >= heap_size) {
			D_ASSERT(!heap.empty());
			std::pop_heap(heap.begin(), heap.end());
			heap.pop_back();
		}
		heap.push_back(entry);
		std::push_heap(heap.begin(), heap.end());
	}
};

} // namespace duckdb
