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
	mutex lock;
	string boundary_value;
	bool is_set = false;

	string GetBoundaryValue() {
		lock_guard<mutex> l(lock);
		return boundary_value;
	}

	void UpdateValue(string_t boundary_val) {
		lock_guard<mutex> l(lock);
		if (!is_set || boundary_val < string_t(boundary_value)) {
			boundary_value = boundary_val.GetString();
			is_set = true;
		}
	}
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
	unsafe_vector<TopNEntry> heap;
	const vector<LogicalType> &payload_types;
	const vector<BoundOrderByNode> &orders;
	idx_t limit;
	idx_t offset;
	idx_t heap_size;
	ExpressionExecutor executor;
	DataChunk sort_chunk;
	DataChunk heap_data;
	DataChunk payload_chunk;
	DataChunk sort_keys;
	StringHeap sort_key_heap;

	SelectionVector matching_sel;

public:
	void Sink(DataChunk &input, optional_ptr<TopNBoundaryValue> boundary_value = nullptr);
	void Combine(TopNHeap &other);
	void Reduce();
	void Finalize();

	void InitializeScan(TopNScanState &state, bool exclude_offset);
	void Scan(TopNScanState &state, DataChunk &chunk);

	void AddSmallHeap(DataChunk &input, Vector &sort_keys_vec, const string &boundary_val,
	                  const string_t &global_boundary_val);
	void AddLargeHeap(DataChunk &input, Vector &sort_keys_vec, const string &boundary_val,
	                  const string_t &global_boundary_val);

public:
	idx_t ReduceThreshold() const {
		return MaxValue<idx_t>(STANDARD_VECTOR_SIZE * 5ULL, 2ULL * heap_size);
	}

	idx_t InitialHeapAllocSize() const {
		return MinValue<idx_t>(STANDARD_VECTOR_SIZE * 100ULL, ReduceThreshold()) + STANDARD_VECTOR_SIZE;
	}

private:
	inline bool EntryShouldBeAdded(const string_t &sort_key) {
		if (heap.size() < heap_size) {
			// heap is full - check the latest entry
			return true;
		}
		if (sort_key < heap.front().sort_key) {
			// sort key is smaller than current max value
			return true;
		}
		// heap is full and there is no room for the entry
		return false;
	}

	inline bool EntryShouldBeAdded(const string_t &sort_key, const string &boundary_val,
	                               const string_t &global_boundary_val) {
		// first compare against the global boundary value (if there is any)
		if (!boundary_val.empty() && sort_key > global_boundary_val) {
			// this entry is out-of-range for the global boundary val
			// it will never be in the final result even if it fits in this heap
			return false;
		}
		return EntryShouldBeAdded(sort_key);
	}

	inline void AddEntryToHeap(const TopNEntry &entry) {
		if (heap.size() >= heap_size) {
			std::pop_heap(heap.begin(), heap.end());
			heap.pop_back();
		}
		heap.push_back(entry);
		std::push_heap(heap.begin(), heap.end());
	}
};

//===--------------------------------------------------------------------===//
// TopNHeap
//===--------------------------------------------------------------------===//
TopNHeap::TopNHeap(ClientContext &context, Allocator &allocator, const vector<LogicalType> &payload_types_p,
                   const vector<BoundOrderByNode> &orders_p, idx_t limit, idx_t offset)
    : allocator(allocator), buffer_manager(BufferManager::GetBufferManager(context)), payload_types(payload_types_p),
      orders(orders_p), limit(limit), offset(offset), heap_size(limit + offset), executor(context),
      matching_sel(STANDARD_VECTOR_SIZE) {
	// initialize the executor and the sort_chunk
	vector<LogicalType> sort_types;
	for (auto &order : orders) {
		auto &expr = order.expression;
		sort_types.push_back(expr->return_type);
		executor.AddExpression(*expr);
	}
	heap.reserve(InitialHeapAllocSize());
	vector<LogicalType> sort_keys_type {LogicalType::BLOB};
	sort_keys.Initialize(allocator, sort_keys_type);
	heap_data.Initialize(allocator, payload_types, InitialHeapAllocSize());
	payload_chunk.Initialize(allocator, payload_types);
	sort_chunk.Initialize(allocator, sort_types);
}

TopNHeap::TopNHeap(ClientContext &context, const vector<LogicalType> &payload_types,
                   const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
    : TopNHeap(context, BufferAllocator::Get(context), payload_types, orders, limit, offset) {
}

TopNHeap::TopNHeap(ExecutionContext &context, const vector<LogicalType> &payload_types,
                   const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
    : TopNHeap(context.client, Allocator::Get(context.client), payload_types, orders, limit, offset) {
}

void TopNHeap::AddSmallHeap(DataChunk &input, Vector &sort_keys_vec, const string &boundary_val,
                            const string_t &global_boundary_val) {
	// insert the sort keys into the priority queue
	constexpr idx_t BASE_INDEX = NumericLimits<uint32_t>::Maximum();

	bool any_added = false;
	auto sort_key_values = FlatVector::GetData<string_t>(sort_keys_vec);
	for (idx_t r = 0; r < input.size(); r++) {
		auto &sort_key = sort_key_values[r];
		if (!EntryShouldBeAdded(sort_key, boundary_val, global_boundary_val)) {
			continue;
		}
		// replace the previous top entry with the new entry
		TopNEntry entry;
		entry.sort_key = sort_key;
		entry.index = BASE_INDEX + r;
		AddEntryToHeap(entry);
		any_added = true;
	}
	if (!any_added) {
		// early-out: no matches
		return;
	}

	// for all matching entries we need to copy over the corresponding payload values
	idx_t match_count = 0;
	for (auto &entry : heap) {
		if (entry.index < BASE_INDEX) {
			continue;
		}
		// this entry was added in this chunk
		// if not inlined - copy over the string to the string heap
		if (!entry.sort_key.IsInlined()) {
			entry.sort_key = sort_key_heap.AddBlob(entry.sort_key);
		}
		// to finalize the addition of this entry we need to move over the payload data
		matching_sel.set_index(match_count, entry.index - BASE_INDEX);
		entry.index = heap_data.size() + match_count;
		match_count++;
	}

	// copy over the input rows to the payload chunk
	heap_data.Append(input, true, &matching_sel, match_count);
}

void TopNHeap::AddLargeHeap(DataChunk &input, Vector &sort_keys_vec, const string &boundary_val,
                            const string_t &global_boundary_val) {
	auto sort_key_values = FlatVector::GetData<string_t>(sort_keys_vec);
	idx_t base_index = heap_data.size();
	idx_t match_count = 0;
	for (idx_t r = 0; r < input.size(); r++) {
		auto &sort_key = sort_key_values[r];
		if (!EntryShouldBeAdded(sort_key, boundary_val, global_boundary_val)) {
			continue;
		}
		// replace the previous top entry with the new entry
		TopNEntry entry;
		entry.sort_key = sort_key.IsInlined() ? sort_key : sort_key_heap.AddBlob(sort_key);
		entry.index = base_index + match_count;
		AddEntryToHeap(entry);
		matching_sel.set_index(match_count++, r);
	}
	if (match_count == 0) {
		// early-out: no matches
		return;
	}

	// copy over the input rows to the payload chunk
	heap_data.Append(input, true, &matching_sel, match_count);
}

void TopNHeap::Sink(DataChunk &input, optional_ptr<TopNBoundaryValue> global_boundary) {
	static constexpr idx_t SMALL_HEAP_THRESHOLD = 100;

	// compute the ordering values for the new chunk
	sort_chunk.Reset();
	executor.Execute(input, sort_chunk);

	// construct the sort key from the sort chunk
	vector<OrderModifiers> modifiers;
	for (auto &order : orders) {
		modifiers.emplace_back(order.type, order.null_order);
	}
	sort_keys.Reset();
	auto &sort_keys_vec = sort_keys.data[0];
	CreateSortKeyHelpers::CreateSortKey(sort_chunk, modifiers, sort_keys_vec);

	// fetch the current global boundary (if any)
	string boundary_val;
	string_t global_boundary_val;
	if (global_boundary) {
		boundary_val = global_boundary->GetBoundaryValue();
		global_boundary_val = string_t(boundary_val);
	}

	if (heap_size <= SMALL_HEAP_THRESHOLD) {
		AddSmallHeap(input, sort_keys_vec, boundary_val, global_boundary_val);
	} else {
		AddLargeHeap(input, sort_keys_vec, boundary_val, global_boundary_val);
	}

	// if we modified the heap we might be able to update the global boundary
	// note that the global boundary only applies to FULL heaps
	if (heap.size() >= heap_size && global_boundary) {
		global_boundary->UpdateValue(heap.front().sort_key);
	}
}

void TopNHeap::Combine(TopNHeap &other) {
	other.Finalize();

	idx_t match_count = 0;
	// merge the heap of other into this
	for (idx_t i = 0; i < other.heap.size(); i++) {
		// heap is full - check the latest entry
		auto &other_entry = other.heap[i];
		auto &sort_key = other_entry.sort_key;
		if (!EntryShouldBeAdded(sort_key)) {
			continue;
		}
		// add this entry
		TopNEntry new_entry;
		new_entry.sort_key = sort_key.IsInlined() ? sort_key : sort_key_heap.AddBlob(sort_key);
		new_entry.index = heap_data.size() + match_count;
		AddEntryToHeap(new_entry);

		matching_sel.set_index(match_count++, other_entry.index);
		if (match_count >= STANDARD_VECTOR_SIZE) {
			// flush
			heap_data.Append(other.heap_data, true, &matching_sel, match_count);
			match_count = 0;
		}
	}
	if (match_count > 0) {
		// flush
		heap_data.Append(other.heap_data, true, &matching_sel, match_count);
		match_count = 0;
	}
	Reduce();
}

void TopNHeap::Finalize() {
}

void TopNHeap::Reduce() {
	if (heap_data.size() < ReduceThreshold()) {
		// only reduce when we pass the reduce threshold
		return;
	}
	// we have too many values in the heap - reduce them
	StringHeap new_sort_heap;
	DataChunk new_heap_data;
	new_heap_data.Initialize(allocator, payload_types, heap.size());

	SelectionVector new_payload_sel(heap.size());
	for (idx_t i = 0; i < heap.size(); i++) {
		auto &entry = heap[i];
		// the entry is not inlined - move the sort key to the new sort heap
		if (!entry.sort_key.IsInlined()) {
			entry.sort_key = new_sort_heap.AddBlob(entry.sort_key);
		}
		// move this heap entry to position X in the payload chunk
		new_payload_sel.set_index(i, entry.index);
		entry.index = i;
	}

	// copy over the data from the current payload chunk to the new payload chunk
	new_heap_data.Slice(heap_data, new_payload_sel, heap.size());
	new_heap_data.Flatten();

	sort_key_heap.Move(new_sort_heap);
	heap_data.Reference(new_heap_data);
}

void TopNHeap::InitializeScan(TopNScanState &state, bool exclude_offset) {
	auto heap_copy = heap;
	// traverse the rest of the heap
	state.scan_order.resize(heap_copy.size());
	while (!heap_copy.empty()) {
		std::pop_heap(heap_copy.begin(), heap_copy.end());
		state.scan_order[heap_copy.size() - 1] = UnsafeNumericCast<sel_t>(heap_copy.back().index);
		heap_copy.pop_back();
	}
	state.pos = exclude_offset ? offset : 0;
}

void TopNHeap::Scan(TopNScanState &state, DataChunk &chunk) {
	if (state.pos >= state.scan_order.size()) {
		return;
	}
	SelectionVector sel(state.scan_order.data() + state.pos);
	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.scan_order.size() - state.pos);
	state.pos += STANDARD_VECTOR_SIZE;

	chunk.Reset();
	chunk.Slice(heap_data, sel, count);
}

class TopNGlobalState : public GlobalSinkState {
public:
	TopNGlobalState(ClientContext &context, const vector<LogicalType> &payload_types,
	                const vector<BoundOrderByNode> &orders, idx_t limit, idx_t offset)
	    : heap(context, payload_types, orders, limit, offset) {
	}

	mutex lock;
	TopNHeap heap;
	TopNBoundaryValue boundary_value;
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
	auto &gstate = input.global_state.Cast<TopNGlobalState>();
	auto &sink = input.local_state.Cast<TopNLocalState>();
	sink.heap.Sink(chunk, &gstate.boundary_value);
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
