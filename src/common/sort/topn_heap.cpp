#include "duckdb/common/sorting/topn_heap.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

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
      final_sel(STANDARD_VECTOR_SIZE), true_sel(STANDARD_VECTOR_SIZE), false_sel(STANDARD_VECTOR_SIZE),
      new_remaining_sel(STANDARD_VECTOR_SIZE) {
	// initialize the executor and the sort_chunk
	vector<LogicalType> sort_types;
	for (auto &order : orders) {
		auto &expr = order.expression;
		sort_types.push_back(expr->GetReturnType());
		executor.AddExpression(*expr);
		not_distinct_from.push_back(
		    make_uniq<ExpressionExecutor>(executor.GetContext(), *not_distinct_from_p[not_distinct_from.size()]));
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
		compare_chunk.Initialize(allocator, sort_types);
		boundary_values.Initialize(allocator, sort_types);
		auto boundary_compare_types = sort_types;
		boundary_compare_types.insert(boundary_compare_types.end(), sort_types.begin(), sort_types.end());
		boundary_compare_chunk.InitializeEmpty(boundary_compare_types);
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
	// An older local boundary remains valid if refreshing it would block.
	const auto boundary_updated = global_boundary.TryGetBoundaryValue(boundary_val);
	if (boundary_val.empty()) {
		// no boundary value (yet) - don't do anything
		return true;
	}
	if (boundary_updated) {
		// new boundary value - decode
		boundary_values.Reset();
		CreateSortKeyHelpers::DecodeSortKey(string_t(boundary_val), boundary_values, 0, modifiers);
		for (auto &col : boundary_values.data) {
			col.SetVectorType(VectorType::CONSTANT_VECTOR);
		}
	}
	boundary_values.SetChildCardinality(sort_chunk.size());
	for (idx_t i = 0; i < orders.size(); i++) {
		boundary_compare_chunk.data[i].Reference(sort_chunk.data[i]);
		boundary_compare_chunk.data[orders.size() + i].Reference(boundary_values.data[i]);
	}
	boundary_compare_chunk.CheckCardinality(sort_chunk.size());

	// we have boundary values
	// from these boundary values, determine which values we should insert (if any)
	idx_t final_count = 0;

	SelectionVector remaining_sel(nullptr);
	idx_t remaining_count = sort_chunk.size();
	for (idx_t i = 0; i < orders.size() && remaining_count > 0; i++) {
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
			remaining_count = not_distinct_from[i]->SelectExpression(boundary_compare_chunk, &new_remaining_sel,
			                                                         nullptr, &false_sel, false_count);
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
