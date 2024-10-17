#include "duckdb/execution/operator/order/physical_top_n.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/function/create_sort_key.hpp"

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

struct TopNEntry {
	string_t sort_key;
	idx_t index;

	bool operator<(const TopNEntry &other) const {
		return other.sort_key < sort_key;
	}
};

struct TopNScanState {
	TopNScanState() : sel(STANDARD_VECTOR_SIZE) {}

	vector<TopNEntry> heap;
	SelectionVector sel;
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
	vector<TopNEntry> heap;
	const vector<LogicalType> &payload_types;
	const vector<BoundOrderByNode> &orders;
	idx_t limit;
	idx_t offset;
	idx_t heap_size;
	ExpressionExecutor executor;
	DataChunk sort_chunk;
	DataChunk heap_data;
	DataChunk payload_chunk;
	Vector sort_keys;
	StringHeap sort_key_heap;

	SelectionVector final_sel;
	SelectionVector true_sel;
	SelectionVector false_sel;
	SelectionVector new_remaining_sel;

public:
	void Sink(DataChunk &input);
	void Combine(TopNHeap &other);
	void Reduce();
	void Finalize();

	void InitializeScan(TopNScanState &state, bool exclude_offset);
	void Scan(TopNScanState &state, DataChunk &chunk);
};

//===--------------------------------------------------------------------===//
// TopNHeap
//===--------------------------------------------------------------------===//
TopNHeap::TopNHeap(ClientContext &context, Allocator &allocator, const vector<LogicalType> &payload_types_p,
                   const vector<BoundOrderByNode> &orders_p, idx_t limit, idx_t offset)
    : allocator(allocator), buffer_manager(BufferManager::GetBufferManager(context)), payload_types(payload_types_p),
      orders(orders_p), limit(limit), offset(offset), executor(context), sort_keys(LogicalType::BLOB),
      final_sel(STANDARD_VECTOR_SIZE), true_sel(STANDARD_VECTOR_SIZE), false_sel(STANDARD_VECTOR_SIZE),
      new_remaining_sel(STANDARD_VECTOR_SIZE) {
	// initialize the executor and the sort_chunk
	vector<LogicalType> sort_types;
	for (auto &order : orders) {
		auto &expr = order.expression;
		sort_types.push_back(expr->return_type);
		executor.AddExpression(*expr);
	}
	heap_data.Initialize(allocator, payload_types);
	payload_chunk.Initialize(allocator, payload_types);
	sort_chunk.Initialize(allocator, sort_types);
	heap_size = limit + offset;
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
	// compute the ordering values for the new chunk
	sort_chunk.Reset();
	executor.Execute(input, sort_chunk);

	// construct the sort key from the sort chunk
	vector<OrderModifiers> modifiers;
	for(auto &order : orders) {
		modifiers.emplace_back(order.type, order.null_order);
	}
	CreateSortKeyHelpers::CreateSortKey(sort_chunk, modifiers, sort_keys);

	// insert the sort keys into the priority queue
	constexpr idx_t BASE_INDEX = NumericLimits<uint32_t>::Maximum();

	bool any_added = false;
	auto sort_key_values = FlatVector::GetData<string_t>(sort_keys);
	for(idx_t r = 0; r < input.size(); r++) {
		auto &sort_key = sort_key_values[r];
		if (heap.size() >= heap_size) {
			// heap is full - check the latest entry
			if (sort_key < heap.front().sort_key) {
				// current max in the heap is larger than the new key - skip this entry
				continue;
			}
		}
		// replace the previous top entry with the new entry
		TopNEntry entry;
		entry.sort_key = sort_key;
		entry.index = BASE_INDEX + r;
		if (heap.size() >= heap_size) {
			heap.pop_back();
		}
		heap.push_back(entry);
		std::push_heap(heap.begin(), heap.end());
		any_added = true;
	}
	if (!any_added) {
		// early-out: no matches
		return;
	}

	// for all matching entries we need to copy over the corresponding payload values
	idx_t match_count = 0;
	for(auto &entry : heap) {
		if (entry.index < BASE_INDEX) {
			continue;
		}
		// this entry was added in this chunk
		// if not inlined - copy over the string to the string heap
		if (!entry.sort_key.IsInlined()) {
			entry.sort_key = sort_key_heap.AddString(entry.sort_key);
		}
		// to finalize the addition of this entry we need to move over the payload data
		true_sel.set_index(match_count, entry.index - BASE_INDEX);
		entry.index = heap_data.size() + match_count;
		match_count++;
	}

	// copy over the input rows to the payload chunk
	heap_data.Append(input, true, &true_sel, match_count);
}

void TopNHeap::Combine(TopNHeap &other) {
	other.Finalize();

	// FIXME: heaps can be merged directly instead of doing it like this
	// that only really speeds things up if heaps are very large, however
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
}

void TopNHeap::Reduce() {
	// FIXME: reduce payload by scanning the heap and only keeping rows that are required
}

void TopNHeap::InitializeScan(TopNScanState &state, bool exclude_offset) {
	state.heap = heap;
	if (exclude_offset) {
		// pop the first "offset" entries
		for(idx_t i = 0; !state.heap.empty() && i < offset; i++) {
			std::pop_heap(state.heap.begin(), state.heap.end());
			state.heap.pop_back();
		}
	}
}

void TopNHeap::Scan(TopNScanState &state, DataChunk &chunk) {
	// scan until either the heap is empty or we have gathered a full chunk
	idx_t count;
	for(count = 0; !state.heap.empty() && count < STANDARD_VECTOR_SIZE; count++) {
		std::pop_heap(state.heap.begin(), state.heap.end());
		state.sel.set_index(count, state.heap.back().index);
		state.heap.pop_back();
	}
	chunk.Reset();
	chunk.Slice(heap_data, state.sel, count);
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

InsertionOrderPreservingMap<string> PhysicalTopN::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Top"] = to_string(limit);
	if (offset > 0) {
		result["Offset"] = to_string(offset);
	}

	string orders_info;
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			orders_info += "\n";
		}
		orders_info += orders[i].expression->ToString() + " ";
		orders_info += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	result["Order By"] = orders_info;
	return result;
}

} // namespace duckdb
