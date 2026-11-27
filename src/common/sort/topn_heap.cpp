#include "duckdb/common/sorting/topn_heap.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// TopNBoundarySelector
//===--------------------------------------------------------------------===//
TopNBoundarySelector::TopNBoundarySelector(ClientContext &context, Allocator &allocator,
                                           const vector<BoundOrderByNode> &orders_p,
                                           const vector<unique_ptr<Expression>> &not_distinct_from_p)
    : orders(orders_p), true_sel(STANDARD_VECTOR_SIZE), false_sel(STANDARD_VECTOR_SIZE),
      new_remaining_sel(STANDARD_VECTOR_SIZE) {
	D_ASSERT(not_distinct_from_p.size() == orders.size());
	vector<LogicalType> key_types;
	for (auto &order : orders) {
		key_types.push_back(order.expression->GetReturnType());
		not_distinct_from.push_back(
		    make_uniq<ExpressionExecutor>(context, *not_distinct_from_p[not_distinct_from.size()]));
		modifiers.emplace_back(order.type, order.null_order);
	}
	compare_chunk.Initialize(allocator, key_types);
	boundary_values.Initialize(allocator, key_types);
	auto boundary_compare_types = key_types;
	boundary_compare_types.insert(boundary_compare_types.end(), key_types.begin(), key_types.end());
	boundary_compare_chunk.InitializeEmpty(boundary_compare_types);
}

void TopNBoundarySelector::Decode(const string &sort_key, DataChunk &target) const {
	target.Reset();
	CreateSortKeyHelpers::DecodeSortKey(string_t(sort_key), target, 0, modifiers);
	for (auto &col : target.data) {
		col.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

bool TopNBoundarySelector::Refresh(TopNBoundaryValue &global_boundary) {
	// An older local boundary remains valid if refreshing it would block.
	const auto boundary_updated = global_boundary.TryGetBoundaryValue(boundary_val);
	if (boundary_val.empty()) {
		// no boundary value (yet)
		return false;
	}
	if (boundary_updated) {
		// new boundary value - decode
		Decode(boundary_val, boundary_values);
	}
	return true;
}

//! Selects the rows whose left value sorts strictly before the right value in the given order
static idx_t SelectBefore(const BoundOrderByNode &order, Vector &left, Vector &right, SelectionVector &sel, idx_t count,
                          SelectionVector &true_sel, SelectionVector &false_sel) {
	if (order.null_order == OrderByNullType::NULLS_LAST) {
		if (order.type == OrderType::ASCENDING) {
			return VectorOperations::DistinctLessThan(left, right, &sel, count, &true_sel, &false_sel);
		}
		return VectorOperations::DistinctGreaterThanNullsFirst(left, right, &sel, count, &true_sel, &false_sel);
	}
	D_ASSERT(order.null_order == OrderByNullType::NULLS_FIRST);
	if (order.type == OrderType::ASCENDING) {
		return VectorOperations::DistinctLessThanNullsFirst(left, right, &sel, count, &true_sel, &false_sel);
	}
	return VectorOperations::DistinctGreaterThan(left, right, &sel, count, &true_sel, &false_sel);
}

idx_t TopNBoundarySelector::Select(DataChunk &keys, DataChunk &bound, optional_ptr<const SelectionVector> sel,
                                   idx_t count, TopNBoundaryTies ties, TopNBoundaryRowOrder row_order,
                                   SelectionVector &result) {
	const auto key_count = orders.size();
	const auto row_count = keys.size();
	bound.SetChildCardinality(row_count);
	for (idx_t i = 0; i < key_count; i++) {
		boundary_compare_chunk.data[i].Reference(keys.data[i]);
		boundary_compare_chunk.data[key_count + i].Reference(bound.data[i]);
	}
	boundary_compare_chunk.CheckCardinality(row_count);

	idx_t final_count = 0;
	// the number of runs of rows that were appended to the result: each run is in the order of the input
	idx_t appended_runs = 0;
	SelectionVector remaining_sel(nullptr);
	if (sel) {
		remaining_sel.Initialize(*sel);
	}
	idx_t remaining_count = count;
	for (idx_t i = 0; i < key_count && remaining_count > 0; i++) {
		auto &order = orders[i];
		if (remaining_sel.data()) {
			compare_chunk.data[i].Slice(keys.data[i], remaining_sel, remaining_count);
		} else {
			compare_chunk.data[i].Reference(keys.data[i]);
		}
		const auto is_last = i + 1 == key_count;
		if (is_last && ties == TopNBoundaryTies::INCLUDE) {
			// before or equal to the boundary: the rows that the boundary does not sort before
			const auto after_count = SelectBefore(order, bound.data[i], compare_chunk.data[i], remaining_sel,
			                                      remaining_count, false_sel, true_sel);
			const auto pass_count = remaining_count - after_count;
			if (pass_count > 0) {
				memcpy(result.data() + final_count, true_sel.data(), pass_count * sizeof(sel_t));
				final_count += pass_count;
				appended_runs++;
			}
			break;
		}
		const auto true_count = SelectBefore(order, compare_chunk.data[i], bound.data[i], remaining_sel,
		                                     remaining_count, true_sel, false_sel);
		if (true_count > 0) {
			memcpy(result.data() + final_count, true_sel.data(), true_count * sizeof(sel_t));
			final_count += true_count;
			appended_runs++;
		}
		const auto false_count = remaining_count - true_count;
		if (is_last || false_count == 0) {
			break;
		}
		// the rows that are equal to the boundary on this key continue with the next key
		remaining_count = not_distinct_from[i]->SelectExpression(boundary_compare_chunk, &new_remaining_sel, nullptr,
		                                                         &false_sel, false_count);
		remaining_sel.Initialize(new_remaining_sel);
	}
	if (row_order == TopNBoundaryRowOrder::INPUT_ORDER && appended_runs > 1) {
		// the rows were selected key by key: restore the order of the input
		if (!selected_rows) {
			selected_rows = make_unsafe_uniq_array<bool>(STANDARD_VECTOR_SIZE);
		}
		memset(selected_rows.get(), 0, row_count * sizeof(bool));
		for (idx_t i = 0; i < final_count; i++) {
			selected_rows[result.get_index(i)] = true;
		}
		idx_t result_idx = 0;
		for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
			result.set_index(result_idx, row_idx);
			result_idx += selected_rows[row_idx];
		}
		D_ASSERT(result_idx == final_count);
	}
	return final_count;
}

//===--------------------------------------------------------------------===//
// TopOneFunc
//===--------------------------------------------------------------------===//
//	Simple scalar min/max aggregation
template <typename T>
struct TopOneState {
	using value_type = T;
	bool is_set = false;
	bool is_null;
	value_type value;
};

template <typename OP, typename T>
struct TopOneOp {
	using STATE = TopOneState<T>;

	static idx_t Size() {
		return sizeof(STATE);
	}

	static optional_idx Update(data_ptr_t state_p, Vector &sort_key, const idx_t count) {
		auto &state = *reinterpret_cast<STATE *>(state_p);
		optional_idx result;
		UnifiedVectorFormat format;
		sort_key.ToUnifiedFormat(format);
		const auto data = format.GetData<typename STATE::value_type>();
		for (idx_t r = 0; r < count; ++r) {
			const auto idx = format.sel->get_index(r);
			STATE left;
			left.is_null = !format.validity.RowIsValid(idx);
			left.value = data[idx];
			auto &right = state;
			if (!state.is_set || OP::Operation(left.value, right.value, left.is_null, right.is_null)) {
				state = left;
				state.is_set = true;
				result = r;
			}
		}

		return result;
	}

	static bool Combine(const_data_ptr_t source_p, data_ptr_t target_p) {
		const auto &source = *reinterpret_cast<const STATE *>(source_p);
		auto &target = *reinterpret_cast<STATE *>(target_p);
		if (!source.is_set) {
			return false;
		} else if (!target.is_set || OP::Operation(source.value, target.value, source.is_null, target.is_null)) {
			target = source;
			return true;
		}
		return false;
	}
};

template <typename T, typename REDUCE_OP>
static unique_ptr<TopOneFunc> MakeTopOneFunc() {
	using OP = TopOneOp<REDUCE_OP, T>;
	auto result = make_uniq<TopOneFunc>();
	result->state_size = OP::Size;
	result->update = OP::Update;
	result->combine = OP::Combine;

	return result;
}

template <typename OP>
static unique_ptr<TopOneFunc> GetTopOneFunc(const BoundOrderByNode &order) {
	auto &expr = *order.expression;
	switch (expr.GetReturnType().InternalType()) {
	case PhysicalType::BOOL:
		return MakeTopOneFunc<int8_t, OP>();
	case PhysicalType::INT8:
		return MakeTopOneFunc<int8_t, OP>();
	case PhysicalType::INT16:
		return MakeTopOneFunc<int16_t, OP>();
	case PhysicalType::INT32:
		return MakeTopOneFunc<int32_t, OP>();
	case PhysicalType::INT64:
		return MakeTopOneFunc<int64_t, OP>();
	case PhysicalType::UINT8:
		return MakeTopOneFunc<uint8_t, OP>();
	case PhysicalType::UINT16:
		return MakeTopOneFunc<uint16_t, OP>();
	case PhysicalType::UINT32:
		return MakeTopOneFunc<uint32_t, OP>();
	case PhysicalType::UINT64:
		return MakeTopOneFunc<uint64_t, OP>();
	case PhysicalType::INT128:
		return MakeTopOneFunc<hugeint_t, OP>();
	case PhysicalType::UINT128:
		return MakeTopOneFunc<uhugeint_t, OP>();
	case PhysicalType::FLOAT:
		return MakeTopOneFunc<float, OP>();
	case PhysicalType::DOUBLE:
		return MakeTopOneFunc<double, OP>();
	case PhysicalType::INTERVAL:
		return MakeTopOneFunc<interval_t, OP>();
	default:
		return nullptr;
	}
}

static unique_ptr<TopOneFunc> TopOneFactory(const TopNHeap &heap) {
	if (heap.offset > 0 || heap.limit != 1) {
		return nullptr;
	}

	const auto &orders = heap.orders;
	if (orders.size() != 1) {
		return nullptr;
	}

	const auto &order = orders.front();
	switch (order.type) {
	case OrderType::ASCENDING:
		switch (order.null_order) {
		case OrderByNullType::NULLS_LAST:
			return GetTopOneFunc<DistinctLessThan>(order);
		case OrderByNullType::NULLS_FIRST:
			return GetTopOneFunc<DistinctLessThanNullsFirst>(order);
		default:
			break;
		}
		break;
	case OrderType::DESCENDING:
		switch (order.null_order) {
		case OrderByNullType::NULLS_LAST:
			return GetTopOneFunc<DistinctGreaterThanNullsFirst>(order);
		case OrderByNullType::NULLS_FIRST:
			return GetTopOneFunc<DistinctGreaterThan>(order);
		default:
			break;
		}
		break;
	default:
		break;
	}

	return nullptr;
}

//===--------------------------------------------------------------------===//
// TopNHeap
//===--------------------------------------------------------------------===//
TopNHeap::TopNHeap(ClientContext &context, Allocator &allocator, const vector<LogicalType> &payload_types_p,
                   const vector<BoundOrderByNode> &orders_p, const vector<unique_ptr<Expression>> &not_distinct_from_p,
                   idx_t limit, idx_t offset)
    : allocator(allocator), buffer_manager(BufferManager::GetBufferManager(context)), arena_allocator(allocator),
      heap(arena_allocator), payload_types(payload_types_p), orders(orders_p), limit(limit), offset(offset),
      heap_size(limit + offset), executor(context), sort_key_heap(allocator), matching_sel(STANDARD_VECTOR_SIZE),
      final_sel(STANDARD_VECTOR_SIZE) {
	// initialize the executor and the sort_chunk
	vector<LogicalType> sort_types;
	for (auto &order : orders) {
		auto &expr = order.expression;
		sort_types.push_back(expr->GetReturnType());
		executor.AddExpression(*expr);
		modifiers.emplace_back(order.type, order.null_order);
	}
	vector<LogicalType> sort_keys_type {LogicalType::BLOB};
	sort_keys.Initialize(allocator, sort_keys_type);
	sort_chunk.Initialize(allocator, sort_types);
	heap_data.Initialize(allocator, payload_types, InitialHeapAllocSize());

	//	Can we use a fast aggregate?
	topone = TopOneFactory(*this);
	if (topone) {
		topone_state.resize(topone->state_size());
	} else {
		//	Set up heap-only data structures
		heap.reserve(InitialHeapAllocSize());
		boundary_selector = make_uniq<TopNBoundarySelector>(context, allocator, orders, not_distinct_from_p);
	}
}

TopNHeap::TopNHeap(ClientContext &context, const vector<LogicalType> &payload_types,
                   const vector<BoundOrderByNode> &orders, const vector<unique_ptr<Expression>> &not_distinct_from,
                   idx_t limit, idx_t offset)
    : TopNHeap(context, BufferAllocator::Get(context), payload_types, orders, not_distinct_from, limit, offset) {
}

TopNHeap::TopNHeap(ExecutionContext &context, const vector<LogicalType> &payload_types,
                   const vector<BoundOrderByNode> &orders, const vector<unique_ptr<Expression>> &not_distinct_from,
                   idx_t limit, idx_t offset)
    : TopNHeap(context.client, BufferAllocator::Get(context.client), payload_types, orders, not_distinct_from, limit,
               offset) {
}

bool TopNHeap::CanOptimize(idx_t limit, idx_t cardinality) {
	// only check if we should switch to full sorting if we have estimated cardinality
	auto constant_limit = static_cast<double>(limit);
	auto child_card = static_cast<double>(cardinality);

	// if the limit is > 0.7% of the child cardinality, sorting the whole table is faster
	bool limit_is_large = constant_limit > 5000;
	if (constant_limit > child_card * 0.007 && limit_is_large) {
		return false;
	}

	return true;
}

void TopNHeap::AddSingleHeap(DataChunk &input, optional_ptr<TopNBoundaryValue> global_boundary) {
	const auto count = input.size();
	const auto best = topone->update(topone_state.data(), sort_chunk.data[0], count);
	if (!best.IsValid()) {
		return;
	}

	// copy over the input row to the payload chunk
	idx_t match_count = 0;
	matching_sel.set_index(match_count++, best.GetIndex());
	heap_data.Reset();
	heap_data.Append(input, matching_sel, match_count, VectorAppendMode::ALLOW_RESIZE);

	// the new best row tightens the dynamic filter
	if (global_boundary && global_boundary->dynamic_filter) {
		sort_chunk.Slice(matching_sel, match_count);
		sort_keys.Reset();
		auto &sort_keys_vec = sort_keys.data[0];
		CreateSortKeyHelpers::CreateSortKey(sort_chunk, modifiers, sort_keys_vec, sort_key_state);
		global_boundary->UpdateValue(FlatVector::GetData<string_t>(sort_keys_vec)[0]);
	}
}

void TopNHeap::AddSmallHeap(DataChunk &input, const Vector &sort_keys_vec) {
	// insert the sort keys into the priority queue
	constexpr idx_t BASE_INDEX = NumericLimits<uint32_t>::Maximum();

	bool any_added = false;
	auto sort_key_values = FlatVector::GetData<string_t>(sort_keys_vec);
	for (idx_t r = 0; r < input.size(); r++) {
		auto &sort_key = sort_key_values[r];
		if (!EntryShouldBeAdded(sort_key)) {
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
		// copy over the string to the string heap
		entry.sort_key = sort_key_heap.AddBlob(entry.sort_key);
		// to finalize the addition of this entry we need to move over the payload data
		matching_sel.set_index(match_count, entry.index - BASE_INDEX);
		entry.index = heap_data.size() + match_count;
		match_count++;
	}

	// copy over the input rows to the payload chunk
	heap_data.Append(input, matching_sel, match_count, VectorAppendMode::ALLOW_RESIZE);
}

void TopNHeap::AddLargeHeap(DataChunk &input, const Vector &sort_keys_vec) {
	auto sort_key_values = FlatVector::GetData<string_t>(sort_keys_vec);
	idx_t base_index = heap_data.size();
	idx_t match_count = 0;
	for (idx_t r = 0; r < input.size(); r++) {
		auto &sort_key = sort_key_values[r];
		if (!EntryShouldBeAdded(sort_key)) {
			continue;
		}
		// replace the previous top entry with the new entry
		TopNEntry entry;
		entry.sort_key = sort_key_heap.AddBlob(sort_key);
		entry.index = base_index + match_count;
		AddEntryToHeap(entry);
		matching_sel.set_index(match_count++, r);
	}
	if (match_count == 0) {
		// early-out: no matches
		return;
	}

	// copy over the input rows to the payload chunk
	heap_data.Append(input, matching_sel, match_count, VectorAppendMode::ALLOW_RESIZE);
}

bool TopNHeap::CheckBoundaryValues(DataChunk &sort_chunk, DataChunk &payload, TopNBoundaryValue &global_boundary) {
	D_ASSERT(boundary_selector);
	if (!boundary_selector->Refresh(global_boundary)) {
		// no boundary value (yet) - don't do anything
		return true;
	}
	// we have boundary values
	// from these boundary values, determine which values we should insert (if any): the rows strictly before the
	// boundary (the heap holds the boundary row itself), in any order
	const auto final_count =
	    boundary_selector->Select(sort_chunk, boundary_selector->BoundaryValues(), nullptr, sort_chunk.size(),
	                              TopNBoundaryTies::EXCLUDE, TopNBoundaryRowOrder::ANY, final_sel);
	if (final_count == 0) {
		return false;
	}
	if (final_count < sort_chunk.size()) {
		sort_chunk.Slice(final_sel, final_count);
		payload.Slice(final_sel, final_count);
	}
	return true;
}

void TopNHeap::Sink(DataChunk &input, optional_ptr<TopNBoundaryValue> global_boundary) {
	static constexpr idx_t SMALL_HEAP_THRESHOLD = 100;

	if (heap_size == 0) {
		// LIMIT 0 without OFFSET - the heap can never hold any entry, so there is nothing to do
		return;
	}

	// compute the ordering values for the new chunk
	sort_chunk.Reset();
	executor.Execute(input, sort_chunk);

	//	LIMIT 1 without OFFSET and a single scalar column with NULLS LAST
	if (topone) {
		AddSingleHeap(input, global_boundary);
		return;
	}

	if (global_boundary) {
		// if we have a global boundary value check which rows pass before doing anything
		if (!CheckBoundaryValues(sort_chunk, input, *global_boundary)) {
			// nothing in this chunk can be in the final result
			return;
		}
	}

	// construct the sort key from the sort chunk
	sort_keys.Reset();
	auto &sort_keys_vec = sort_keys.data[0];
	CreateSortKeyHelpers::CreateSortKey(sort_chunk, modifiers, sort_keys_vec, sort_key_state);

	if (heap_size <= SMALL_HEAP_THRESHOLD) {
		AddSmallHeap(input, sort_keys_vec);
	} else {
		AddLargeHeap(input, sort_keys_vec);
	}

	// if we modified the heap we might be able to update the global boundary
	// note that the global boundary only applies to FULL heaps
	if (!heap.empty() && heap.size() >= heap_size && global_boundary) {
		global_boundary->UpdateValue(heap.front().sort_key);
	}
}

void TopNHeap::Combine(TopNHeap &other) {
	// "other" is sorted at this point
	idx_t match_count = 0;
	if (topone) {
		if (topone->combine(other.topone_state.data(), topone_state.data())) {
			matching_sel.set_index(match_count++, 0);
			heap_data.Reset();
			heap_data.Append(other.heap_data, matching_sel, match_count, VectorAppendMode::ALLOW_RESIZE);
		}
		return;
	}

	// merge the heap of other into this
	for (idx_t i = 0; i < other.heap.size(); i++) {
		// heap is full - check the latest entry
		auto &other_entry = other.heap[i];
		auto &sort_key = other_entry.sort_key;
		if (!EntryShouldBeAdded(sort_key)) {
			break; // We can break out of the loop here since we sorted the heap
		}
		// add this entry
		TopNEntry new_entry;
		new_entry.sort_key = sort_key_heap.AddBlob(sort_key);
		new_entry.index = heap_data.size() + match_count;
		AddEntryToHeap(new_entry);

		matching_sel.set_index(match_count++, other_entry.index);
		if (match_count >= STANDARD_VECTOR_SIZE) {
			// flush
			heap_data.Append(other.heap_data, matching_sel, match_count, VectorAppendMode::ALLOW_RESIZE);
			match_count = 0;
		}
	}
	if (match_count > 0) {
		// flush
		heap_data.Append(other.heap_data, matching_sel, match_count, VectorAppendMode::ALLOW_RESIZE);
		match_count = 0;
	}
	Reduce();
}

void TopNHeap::Finalize() {
	//	For TopOne there is at most one entry.
	if (topone && heap_data.size()) {
		TopNEntry entry;
		entry.index = 0;
		AddEntryToHeap(entry);
	}

	std::sort(heap.begin(), heap.end());
}

void TopNHeap::Reduce() {
	if (heap_data.size() < ReduceThreshold()) {
		// only reduce when we pass the reduce threshold
		return;
	}
	// we have too many values in the heap - reduce them
	StringHeap new_sort_heap(allocator);
	DataChunk new_heap_data;
	new_heap_data.Initialize(allocator, payload_types, heap.size());

	SelectionVector new_payload_sel(heap.size());
	for (idx_t i = 0; i < heap.size(); i++) {
		auto &entry = heap[i];
		// move the sort key to the new sort heap
		entry.sort_key = new_sort_heap.AddBlob(entry.sort_key);
		// move this heap entry to position X in the payload chunk
		new_payload_sel.set_index(i, entry.index);
		entry.index = i;
	}

	// copy over the data from the current payload chunk to the new payload chunk
	new_heap_data.Slice(heap_data, new_payload_sel, heap.size());
	new_heap_data.Flatten();

	sort_key_heap.Destroy();
	sort_key_heap.Move(new_sort_heap);
	heap_data.Reference(new_heap_data);
}

void TopNHeap::InitializeScan(TopNScanState &state, bool exclude_offset) {
	auto heap_copy = heap;
	state.scan_order.resize(heap_copy.size());

	// we sorted the heap in Finalize(), so we can sequentially here
	// we do this because sorting the heap is much more efficient than popping one by one
	for (idx_t i = 0; i < heap_copy.size(); i++) {
		state.scan_order[i] = UnsafeNumericCast<sel_t>(heap_copy[i].index);
	}

	state.pos = exclude_offset ? offset : 0;
}

void TopNHeap::Scan(TopNScanState &state, DataChunk &chunk, idx_t &pos) {
	if (pos >= state.scan_order.size()) {
		return;
	}
	SelectionVector sel(state.scan_order.data() + pos, state.scan_order.size() - pos);
	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.scan_order.size() - pos);
	pos += STANDARD_VECTOR_SIZE;

	chunk.Reset();
	chunk.Slice(heap_data, sel, count);
}

} // namespace duckdb
