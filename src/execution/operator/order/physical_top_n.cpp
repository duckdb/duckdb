#include "duckdb/execution/operator/order/physical_top_n.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/row/row_layout.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

PhysicalTopN::PhysicalTopN(vector<LogicalType> types, vector<BoundOrderByNode> orders, idx_t limit, idx_t offset,
                           idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::TOP_N, std::move(types), estimated_cardinality), orders(std::move(orders)),
      limit(limit), offset(offset) {
}

//===--------------------------------------------------------------------===//
// Heaps
//===--------------------------------------------------------------------===//
class TopNHeap;

struct TopNScanState {
	unique_ptr<PayloadScanner> scanner;
	idx_t pos;
	bool exclude_offset;
};

class TopNSortState {
public:
	explicit TopNSortState(TopNHeap &heap);

	TopNHeap &heap;
	unique_ptr<LocalSortState> local_state;
	unique_ptr<GlobalSortState> global_state;
	idx_t count;
	bool is_sorted;

public:
	void Initialize();
	void Append(DataChunk &sort_chunk, DataChunk &payload);

	void Sink(DataChunk &input);
	void Finalize();

	void Move(TopNSortState &other);

	void InitializeScan(TopNScanState &state, bool exclude_offset);
	void Scan(TopNScanState &state, DataChunk &chunk);
};

class TopNHeap {
public:
	TopNHeap(ClientContext &context, const vector<LogicalType> &payload_types, const vector<BoundOrderByNode> &orders,
	         idx_t limit, idx_t offset);
	TopNHeap(ExecutionContext &context, const vector<LogicalType> &payload_types,
	         const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset);
	TopNHeap(ClientContext &context, Allocator &allocator, const vector<LogicalType> &payload_types,
	         const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset);

	Allocator &allocator;
	BufferManager &buffer_manager;
	const vector<LogicalType> &payload_types;
	const vector<BoundOrderByNode> &orders;
	idx_t limit;
	idx_t offset;
	TopNSortState sort_state;
	ExpressionExecutor executor;
	DataChunk sort_chunk;
	DataChunk compare_chunk;
	DataChunk payload_chunk;
	//! A set of boundary values that determine either the minimum or the maximum value we have to consider for our
	//! top-n
	DataChunk boundary_values;
	//! Whether or not the boundary_values has been set. The boundary_values are only set after a reduce step
	bool has_boundary_values;

	SelectionVector final_sel;
	SelectionVector true_sel;
	SelectionVector false_sel;
	SelectionVector new_remaining_sel;

public:
	void Sink(DataChunk &input);
	void Combine(TopNHeap &other);
	void Reduce();
	void Finalize();

	void ExtractBoundaryValues(DataChunk &current_chunk, DataChunk &prev_chunk);

	void InitializeScan(TopNScanState &state, bool exclude_offset);
	void Scan(TopNScanState &state, DataChunk &chunk);

	bool CheckBoundaryValues(DataChunk &sort_chunk, DataChunk &payload);
};

//===--------------------------------------------------------------------===//
// TopNSortState
//===--------------------------------------------------------------------===//
TopNSortState::TopNSortState(TopNHeap &heap) : heap(heap), count(0), is_sorted(false) {
}

void TopNSortState::Initialize() {
	RowLayout layout;
	layout.Initialize(heap.payload_types);
	auto &buffer_manager = heap.buffer_manager;
	global_state = make_uniq<GlobalSortState>(buffer_manager, heap.orders, layout);
	local_state = make_uniq<LocalSortState>();
	local_state->Initialize(*global_state, buffer_manager);
}

void TopNSortState::Append(DataChunk &sort_chunk, DataChunk &payload) {
	D_ASSERT(!is_sorted);
	if (heap.has_boundary_values) {
		if (!heap.CheckBoundaryValues(sort_chunk, payload)) {
			return;
		}
	}

	local_state->SinkChunk(sort_chunk, payload);
	count += payload.size();
}

void TopNSortState::Sink(DataChunk &input) {
	// compute the ordering values for the new chunk
	heap.sort_chunk.Reset();
	heap.executor.Execute(input, heap.sort_chunk);

	// append the new chunk to what we have already
	Append(heap.sort_chunk, input);
}

void TopNSortState::Move(TopNSortState &other) {
	local_state = std::move(other.local_state);
	global_state = std::move(other.global_state);
	count = other.count;
	is_sorted = other.is_sorted;
}

void TopNSortState::Finalize() {
	D_ASSERT(!is_sorted);
	global_state->AddLocalState(*local_state);

	global_state->PrepareMergePhase();
	while (global_state->sorted_blocks.size() > 1) {
		MergeSorter merge_sorter(*global_state, heap.buffer_manager);
		merge_sorter.PerformInMergeRound();
		global_state->CompleteMergeRound();
	}
	is_sorted = true;
}

void TopNSortState::InitializeScan(TopNScanState &state, bool exclude_offset) {
	D_ASSERT(is_sorted);
	if (global_state->sorted_blocks.empty()) {
		state.scanner = nullptr;
	} else {
		D_ASSERT(global_state->sorted_blocks.size() == 1);
		state.scanner = make_uniq<PayloadScanner>(*global_state->sorted_blocks[0]->payload_data, *global_state);
	}
	state.pos = 0;
	state.exclude_offset = exclude_offset && heap.offset > 0;
}

void TopNSortState::Scan(TopNScanState &state, DataChunk &chunk) {
	if (!state.scanner) {
		return;
	}
	auto offset = heap.offset;
	auto limit = heap.limit;
	D_ASSERT(is_sorted);
	while (chunk.size() == 0) {
		state.scanner->Scan(chunk);
		if (chunk.size() == 0) {
			break;
		}
		idx_t start = state.pos;
		idx_t end = state.pos + chunk.size();
		state.pos = end;

		idx_t chunk_start = 0;
		idx_t chunk_end = chunk.size();
		if (state.exclude_offset) {
			// we need to exclude all tuples before the OFFSET
			// check if we should include anything
			if (end <= offset) {
				// end is smaller than offset: include nothing!
				chunk.Reset();
				continue;
			} else if (start < offset) {
				// we need to slice
				chunk_start = offset - start;
			}
		}
		// check if we need to truncate at the offset + limit mark
		if (start >= offset + limit) {
			// we are finished
			chunk_end = 0;
		} else if (end > offset + limit) {
			// the end extends past the offset + limit
			// truncate the current chunk
			chunk_end = offset + limit - start;
		}
		D_ASSERT(chunk_end - chunk_start <= STANDARD_VECTOR_SIZE);
		if (chunk_end == chunk_start) {
			chunk.Reset();
			break;
		} else if (chunk_start > 0) {
			SelectionVector sel(STANDARD_VECTOR_SIZE);
			for (idx_t i = chunk_start; i < chunk_end; i++) {
				sel.set_index(i - chunk_start, i);
			}
			chunk.Slice(sel, chunk_end - chunk_start);
		} else if (chunk_end != chunk.size()) {
			chunk.SetCardinality(chunk_end);
		}
	}
}

//===--------------------------------------------------------------------===//
// TopNHeap
//===--------------------------------------------------------------------===//
TopNHeap::TopNHeap(ClientContext &context, Allocator &allocator, const vector<LogicalType> &payload_types_p,
                   const vector<BoundOrderByNode> &orders_p, idx_t limit, idx_t offset)
    : allocator(allocator), buffer_manager(BufferManager::GetBufferManager(context)), payload_types(payload_types_p),
      orders(orders_p), limit(limit), offset(offset), sort_state(*this), executor(context), has_boundary_values(false),
      final_sel(STANDARD_VECTOR_SIZE), true_sel(STANDARD_VECTOR_SIZE), false_sel(STANDARD_VECTOR_SIZE),
      new_remaining_sel(STANDARD_VECTOR_SIZE) {
	// initialize the executor and the sort_chunk
	vector<LogicalType> sort_types;
	for (auto &order : orders) {
		auto &expr = order.expression;
		sort_types.push_back(expr->return_type);
		executor.AddExpression(*expr);
	}
	payload_chunk.Initialize(allocator, payload_types);
	sort_chunk.Initialize(allocator, sort_types);
	compare_chunk.Initialize(allocator, sort_types);
	boundary_values.Initialize(allocator, sort_types);
	sort_state.Initialize();
}

TopNHeap::TopNHeap(ClientContext &context, const vector<LogicalType> &payload_types,
                   const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
    : TopNHeap(context, BufferAllocator::Get(context), payload_types, orders, limit, offset) {
}

TopNHeap::TopNHeap(ExecutionContext &context, const vector<LogicalType> &payload_types,
                   const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
    : TopNHeap(context.client, Allocator::Get(context.client), payload_types, orders, limit, offset) {
}

void TopNHeap::Sink(DataChunk &input) {
	sort_state.Sink(input);
}

void TopNHeap::Combine(TopNHeap &other) {
	other.Finalize();

	TopNScanState state;
	other.InitializeScan(state, false);
	while (true) {
		payload_chunk.Reset();
		other.Scan(state, payload_chunk);
		if (payload_chunk.size() == 0) {
			break;
		}
		Sink(payload_chunk);
	}
	Reduce();
}

void TopNHeap::Finalize() {
	sort_state.Finalize();
}

void TopNHeap::Reduce() {
	idx_t min_sort_threshold = MaxValue<idx_t>(STANDARD_VECTOR_SIZE * 5ULL, 2ULL * (limit + offset));
	if (sort_state.count < min_sort_threshold) {
		// only reduce when we pass two times the limit + offset, or 5 vectors (whichever comes first)
		return;
	}
	sort_state.Finalize();
	TopNSortState new_state(*this);
	new_state.Initialize();

	TopNScanState state;
	sort_state.InitializeScan(state, false);

	DataChunk new_chunk;
	new_chunk.Initialize(allocator, payload_types);

	DataChunk *current_chunk = &new_chunk;
	DataChunk *prev_chunk = &payload_chunk;
	has_boundary_values = false;
	while (true) {
		current_chunk->Reset();
		Scan(state, *current_chunk);
		if (current_chunk->size() == 0) {
			ExtractBoundaryValues(*current_chunk, *prev_chunk);
			break;
		}
		new_state.Sink(*current_chunk);
		std::swap(current_chunk, prev_chunk);
	}

	sort_state.Move(new_state);
}

void TopNHeap::ExtractBoundaryValues(DataChunk &current_chunk, DataChunk &prev_chunk) {
	// extract the last entry of the prev_chunk and set as minimum value
	D_ASSERT(prev_chunk.size() > 0);
	for (idx_t col_idx = 0; col_idx < current_chunk.ColumnCount(); col_idx++) {
		ConstantVector::Reference(current_chunk.data[col_idx], prev_chunk.data[col_idx], prev_chunk.size() - 1,
		                          prev_chunk.size());
	}
	current_chunk.SetCardinality(1);
	sort_chunk.Reset();
	executor.Execute(&current_chunk, sort_chunk);

	boundary_values.Reset();
	boundary_values.Append(sort_chunk);
	boundary_values.SetCardinality(1);
	for (idx_t i = 0; i < boundary_values.ColumnCount(); i++) {
		boundary_values.data[i].SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	has_boundary_values = true;
}

bool TopNHeap::CheckBoundaryValues(DataChunk &sort_chunk, DataChunk &payload) {
	// we have boundary values
	// from these boundary values, determine which values we should insert (if any)
	idx_t final_count = 0;

	SelectionVector remaining_sel(nullptr);
	idx_t remaining_count = sort_chunk.size();
	for (idx_t i = 0; i < orders.size(); i++) {
		if (remaining_sel.data()) {
			compare_chunk.data[i].Slice(sort_chunk.data[i], remaining_sel, remaining_count);
		} else {
			compare_chunk.data[i].Reference(sort_chunk.data[i]);
		}
		bool is_last = i + 1 == orders.size();
		idx_t true_count;
		if (orders[i].null_order == OrderByNullType::NULLS_LAST) {
			if (orders[i].type == OrderType::ASCENDING) {
				true_count = VectorOperations::DistinctLessThan(compare_chunk.data[i], boundary_values.data[i],
				                                                &remaining_sel, remaining_count, &true_sel, &false_sel);
			} else {
				true_count = VectorOperations::DistinctGreaterThanNullsFirst(compare_chunk.data[i],
				                                                             boundary_values.data[i], &remaining_sel,
				                                                             remaining_count, &true_sel, &false_sel);
			}
		} else {
			D_ASSERT(orders[i].null_order == OrderByNullType::NULLS_FIRST);
			if (orders[i].type == OrderType::ASCENDING) {
				true_count = VectorOperations::DistinctLessThanNullsFirst(compare_chunk.data[i],
				                                                          boundary_values.data[i], &remaining_sel,
				                                                          remaining_count, &true_sel, &false_sel);
			} else {
				true_count =
				    VectorOperations::DistinctGreaterThan(compare_chunk.data[i], boundary_values.data[i],
				                                          &remaining_sel, remaining_count, &true_sel, &false_sel);
			}
		}

		if (true_count > 0) {
			memcpy(final_sel.data() + final_count, true_sel.data(), true_count * sizeof(sel_t));
			final_count += true_count;
		}
		idx_t false_count = remaining_count - true_count;
		if (!is_last && false_count > 0) {
			// check what we should continue to check
			compare_chunk.data[i].Slice(sort_chunk.data[i], false_sel, false_count);
			remaining_count = VectorOperations::NotDistinctFrom(compare_chunk.data[i], boundary_values.data[i],
			                                                    &false_sel, false_count, &new_remaining_sel, nullptr);
			remaining_sel.Initialize(new_remaining_sel);
		} else {
			break;
		}
	}
	if (final_count == 0) {
		return false;
	}
	if (final_count < sort_chunk.size()) {
		sort_chunk.Slice(final_sel, final_count);
		payload.Slice(final_sel, final_count);
	}
	return true;
}

void TopNHeap::InitializeScan(TopNScanState &state, bool exclude_offset) {
	sort_state.InitializeScan(state, exclude_offset);
}

void TopNHeap::Scan(TopNScanState &state, DataChunk &chunk) {
	sort_state.Scan(state, chunk);
}

class TopNGlobalState : public GlobalSinkState {
public:
	TopNGlobalState(ClientContext &context, const vector<LogicalType> &payload_types,
	                const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
	    : heap(context, payload_types, orders, limit, offset) {
	}

	mutex lock;
	TopNHeap heap;
};

class TopNLocalState : public LocalSinkState {
public:
	TopNLocalState(ExecutionContext &context, const vector<LogicalType> &payload_types,
	               const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
	    : heap(context, payload_types, orders, limit, offset) {
	}

	TopNHeap heap;
};

unique_ptr<LocalSinkState> PhysicalTopN::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<TopNLocalState>(context, types, orders, limit, offset);
}

unique_ptr<GlobalSinkState> PhysicalTopN::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<TopNGlobalState>(context, types, orders, limit, offset);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalTopN::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	// append to the local sink state
	auto &sink = input.local_state.Cast<TopNLocalState>();
	sink.heap.Sink(chunk);
	sink.heap.Reduce();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType PhysicalTopN::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<TopNGlobalState>();
	auto &lstate = input.local_state.Cast<TopNLocalState>();

	// scan the local top N and append it to the global heap
	lock_guard<mutex> glock(gstate.lock);
	gstate.heap.Combine(lstate.heap);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalTopN::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                        OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<TopNGlobalState>();
	// global finalize: compute the final top N
	gstate.heap.Finalize();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class TopNOperatorState : public GlobalSourceState {
public:
	TopNScanState state;
	bool initialized = false;
};

unique_ptr<GlobalSourceState> PhysicalTopN::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<TopNOperatorState>();
}

SourceResultType PhysicalTopN::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	if (limit == 0) {
		return SourceResultType::FINISHED;
	}
	auto &state = input.global_state.Cast<TopNOperatorState>();
	auto &gstate = sink_state->Cast<TopNGlobalState>();

	if (!state.initialized) {
		gstate.heap.InitializeScan(state.state, true);
		state.initialized = true;
	}
	gstate.heap.Scan(state.state, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

string PhysicalTopN::ParamsToString() const {
	string result;
	result += "Top " + to_string(limit);
	if (offset > 0) {
		result += "\n";
		result += "Offset " + to_string(offset);
	}
	result += "\n[INFOSEPARATOR]";
	for (idx_t i = 0; i < orders.size(); i++) {
		result += "\n";
		result += orders[i].expression->ToString() + " ";
		result += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	return result;
}

} // namespace duckdb
