#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// For basic types
template <class T>
struct HeapEntry {
	T value;

	void Assign(ArenaAllocator &allocator, const T &val) {
		value = val;
	}
};

// For strings that require arena allocation
template <>
struct HeapEntry<string_t> {
	string_t value;
	uint32_t capacity;
	data_ptr_t allocated_data;

	HeapEntry() : value(), capacity(0), allocated_data(nullptr) {
	}

	// Not copyable
	HeapEntry(const HeapEntry &other) = delete;
	HeapEntry &operator=(const HeapEntry &other) = delete;

	// But movable
	HeapEntry(HeapEntry &&other) noexcept {
		if (other.value.IsInlined()) {
			value = other.value;
			capacity = 0;
			allocated_data = nullptr;
		} else {
			capacity = other.capacity;
			allocated_data = other.allocated_data;
			value = string_t(const_char_ptr_cast(allocated_data), UnsafeNumericCast<uint32_t>(other.value.GetSize()));
			other.allocated_data = nullptr;
		}
	}

	HeapEntry &operator=(HeapEntry &&other) noexcept {
		if (other.value.IsInlined()) {
			value = other.value;
		} else {
			capacity = other.capacity;
			allocated_data = other.allocated_data;
			value = string_t(const_char_ptr_cast(allocated_data), UnsafeNumericCast<uint32_t>(other.value.GetSize()));
			other.allocated_data = nullptr;
		}
		return *this;
	}

	void Assign(ArenaAllocator &allocator, const string_t &new_val) {
		if (new_val.IsInlined()) {
			value = new_val;
			return;
		}

		// Short path for first assignment
		if (allocated_data == nullptr) {
			auto new_size = UnsafeNumericCast<uint32_t>(new_val.GetSize());
			auto new_capacity = NextPowerOfTwo(new_size);
			if (new_capacity > string_t::MAX_STRING_SIZE) {
				throw InvalidInputException("Resulting string/blob too large!");
			}
			capacity = UnsafeNumericCast<uint32_t>(new_capacity);
			allocated_data = allocator.Allocate(capacity);
			memcpy(allocated_data, new_val.GetData(), new_size);
			value = string_t(const_char_ptr_cast(allocated_data), new_size);
			return;
		}

		// double allocation until value fits
		if (capacity < new_val.GetSize()) {
			auto old_size = capacity;
			capacity *= 2;
			while (capacity < new_val.GetSize()) {
				capacity *= 2;
			}
			allocated_data = allocator.Reallocate(allocated_data, old_size, capacity);
		}
		auto new_size = UnsafeNumericCast<uint32_t>(new_val.GetSize());
		memcpy(allocated_data, new_val.GetData(), new_size);
		value = string_t(const_char_ptr_cast(allocated_data), new_size);
	}
};

template <class T, class T_COMPARATOR>
class UnaryAggregateHeap {
public:
	UnaryAggregateHeap() = default;

	UnaryAggregateHeap(ArenaAllocator &allocator, idx_t capacity_p) {
		Initialize(allocator, capacity_p);
	}

	void Initialize(ArenaAllocator &allocator, const idx_t capacity_p) {
		capacity = capacity_p;
		auto ptr = allocator.AllocateAligned(capacity * sizeof(HeapEntry<T>));
		memset(ptr, 0, capacity * sizeof(HeapEntry<T>));
		heap = reinterpret_cast<HeapEntry<T> *>(ptr);
		size = 0;
	}

	bool IsEmpty() const {
		return size == 0;
	}
	idx_t Size() const {
		return size;
	}
	idx_t Capacity() const {
		return capacity;
	}

	void Insert(ArenaAllocator &allocator, const T &value) {
		D_ASSERT(capacity != 0); // must be initialized

		// If the heap is not full, insert the value into a new slot
		if (size < capacity) {
			heap[size++].Assign(allocator, value);
			std::push_heap(heap, heap + size, Compare);
		}
		// If the heap is full, check if the value is greater than the smallest value in the heap
		// If it is, assign the new value to the slot and re-heapify
		else if (T_COMPARATOR::Operation(value, heap[0].value)) {
			std::pop_heap(heap, heap + size, Compare);
			heap[size - 1].Assign(allocator, value);
			std::push_heap(heap, heap + size, Compare);
		}
		D_ASSERT(std::is_heap(heap, heap + size, Compare));
	}

	void Insert(ArenaAllocator &allocator, const UnaryAggregateHeap &other) {
		for (idx_t slot = 0; slot < other.Size(); slot++) {
			Insert(allocator, other.heap[slot].value);
		}
	}

	HeapEntry<T> *SortAndGetHeap() {
		std::sort_heap(heap, heap + size, Compare);
		return heap;
	}

	static const T &GetValue(const HeapEntry<T> &slot) {
		return slot.value;
	}

private:
	static bool Compare(const HeapEntry<T> &left, const HeapEntry<T> &right) {
		return T_COMPARATOR::Operation(left.value, right.value);
	}

	idx_t capacity;
	HeapEntry<T> *heap;
	idx_t size;
};

template <class K, class V, class K_COMPARATOR>
class BinaryAggregateHeap {
	using STORAGE_TYPE = pair<HeapEntry<K>, HeapEntry<V>>;

public:
	BinaryAggregateHeap() = default;

	BinaryAggregateHeap(ArenaAllocator &allocator, idx_t capacity_p) {
		Initialize(allocator, capacity_p);
	}

	void Initialize(ArenaAllocator &allocator, const idx_t capacity_p) {
		capacity = capacity_p;
		auto ptr = allocator.AllocateAligned(capacity * sizeof(STORAGE_TYPE));
		memset(ptr, 0, capacity * sizeof(STORAGE_TYPE));
		heap = reinterpret_cast<STORAGE_TYPE *>(ptr);
		size = 0;
	}

	bool IsEmpty() const {
		return size == 0;
	}
	idx_t Size() const {
		return size;
	}
	idx_t Capacity() const {
		return capacity;
	}

	void Insert(ArenaAllocator &allocator, const K &key, const V &value) {
		D_ASSERT(capacity != 0); // must be initialized

		// If the heap is not full, insert the value into a new slot
		if (size < capacity) {
			heap[size].first.Assign(allocator, key);
			heap[size].second.Assign(allocator, value);
			size++;
			std::push_heap(heap, heap + size, Compare);
		}
		// If the heap is full, check if the value is greater than the smallest value in the heap
		// If it is, assign the new value to the slot and re-heapify
		else if (K_COMPARATOR::Operation(key, heap[0].first.value)) {
			std::pop_heap(heap, heap + size, Compare);
			heap[size - 1].first.Assign(allocator, key);
			heap[size - 1].second.Assign(allocator, value);
			std::push_heap(heap, heap + size, Compare);
		}
		D_ASSERT(std::is_heap(heap, heap + size, Compare));
	}

	void Insert(ArenaAllocator &allocator, const BinaryAggregateHeap &other) {
		for (idx_t slot = 0; slot < other.Size(); slot++) {
			Insert(allocator, other.heap[slot].first.value, other.heap[slot].second.value);
		}
	}

	STORAGE_TYPE *SortAndGetHeap() {
		std::sort_heap(heap, heap + size, Compare);
		return heap;
	}

	static const V &GetValue(const STORAGE_TYPE &slot) {
		return slot.second.value;
	}

private:
	static bool Compare(const STORAGE_TYPE &left, const STORAGE_TYPE &right) {
		return K_COMPARATOR::Operation(left.first.value, right.first.value);
	}

	idx_t capacity;
	STORAGE_TYPE *heap;
	idx_t size;
};

//------------------------------------------------------------------------------
// BinaryAggregateHeapWithTies: extends BinaryAggregateHeap to track ties at the boundary
//------------------------------------------------------------------------------
enum class RankType : uint8_t { RANK };

template <class K, class V, class K_COMPARATOR>
class BinaryAggregateHeapWithTies {
	using STORAGE_TYPE = pair<HeapEntry<K>, HeapEntry<V>>;

public:
	BinaryAggregateHeapWithTies() = default;

	void Initialize(ArenaAllocator &allocator, const idx_t capacity_p) {
		capacity = capacity_p;
		auto ptr = allocator.AllocateAligned(capacity * sizeof(STORAGE_TYPE));
		memset(ptr, 0, capacity * sizeof(STORAGE_TYPE));
		heap = reinterpret_cast<STORAGE_TYPE *>(ptr);
		heap_size = 0;
	}

	bool IsEmpty() const {
		return heap_size == 0;
	}

	idx_t GetHeapSize() const {
		return heap_size;
	}

	idx_t Size() const {
		return heap_size + ties.size();
	}

	idx_t Capacity() const {
		return capacity;
	}

	void Insert(ArenaAllocator &allocator, const K &key, const V &value) {
		D_ASSERT(capacity != 0);

		if (heap_size < capacity) {
			// Heap not full yet, just insert
			heap[heap_size].first.Assign(allocator, key);
			heap[heap_size].second.Assign(allocator, value);
			heap_size++;
			std::push_heap(heap, heap + heap_size, Compare);

		} else if (KeyEquals(key, heap[0].first.value)) {
			// Key equals boundary
			STORAGE_TYPE entry;
			entry.first.Assign(allocator, key);
			entry.second.Assign(allocator, value);
			ties.push_back(std::move(entry));
		} else if (K_COMPARATOR::Operation(key, heap[0].first.value)) {
			// Key is better than boundary
			std::pop_heap(heap, heap + heap_size, Compare);
			STORAGE_TYPE evicted;
			evicted.first.Assign(allocator, heap[heap_size - 1].first.value);
			evicted.second.Assign(allocator, heap[heap_size - 1].second.value);

			heap[heap_size - 1].first.Assign(allocator, key);
			heap[heap_size - 1].second.Assign(allocator, value);
			std::push_heap(heap, heap + heap_size, Compare);

			if (KeyEquals(evicted.first.value, heap[0].first.value)) {
				// Boundary unchanged, evicted element is a tie
				ties.push_back(std::move(evicted));
			} else {
				// Boundary changed, old ties no longer match
				ties.clear();
			}
		}
		// Otherwise key is worse than boundary, ignore
	}

	void Insert(ArenaAllocator &allocator, const BinaryAggregateHeapWithTies &other) {
		for (idx_t slot = other.heap_size; slot > 0; slot--) {
			Insert(allocator, other.heap[slot - 1].first.value, other.heap[slot - 1].second.value);
		}
		for (idx_t slot = 0; slot < other.ties.size(); slot++) {
			Insert(allocator, other.ties[slot].first.value, other.ties[slot].second.value);
		}
	}

	STORAGE_TYPE *SortAndGetHeap() {
		std::sort_heap(heap, heap + heap_size, Compare);
		return heap;
	}

	const vector<STORAGE_TYPE> &GetTies() const {
		return ties;
	}

	static const V &GetValue(const STORAGE_TYPE &slot) {
		return slot.second.value;
	}

	static const K &GetKey(const STORAGE_TYPE &slot) {
		return slot.first.value;
	}

private:
	static bool Compare(const STORAGE_TYPE &left, const STORAGE_TYPE &right) {
		return K_COMPARATOR::Operation(left.first.value, right.first.value);
	}

	static bool KeyEquals(const K &a, const K &b) {
		return !K_COMPARATOR::Operation(a, b) && !K_COMPARATOR::Operation(b, a);
	}

	idx_t capacity;
	STORAGE_TYPE *heap;
	idx_t heap_size;
	vector<STORAGE_TYPE> ties;
};

enum class ArgMinMaxNullHandling { IGNORE_ANY_NULL, HANDLE_ARG_NULL, HANDLE_ANY_NULL };

struct ArgMinMaxFunctionData : FunctionData {
	explicit ArgMinMaxFunctionData(ArgMinMaxNullHandling null_handling_p = ArgMinMaxNullHandling::IGNORE_ANY_NULL,
	                               bool nulls_last_p = true)
	    : null_handling(null_handling_p), nulls_last(nulls_last_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<ArgMinMaxFunctionData>();
		copy->null_handling = null_handling;
		copy->nulls_last = nulls_last;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ArgMinMaxFunctionData>();
		return other.null_handling == null_handling && other.nulls_last == nulls_last;
	}

	ArgMinMaxNullHandling null_handling;
	bool nulls_last;
};

//------------------------------------------------------------------------------
// Specializations for fixed size types, strings, and anything else (using sortkey)
//------------------------------------------------------------------------------
template <class T>
struct MinMaxFixedValue {
	using TYPE = T;
	using EXTRA_STATE = bool;

	static TYPE Create(const UnifiedVectorFormat &format, const idx_t idx) {
		return UnifiedVectorFormat::GetData<T>(format)[idx];
	}

	static void Assign(Vector &vector, const idx_t idx, const TYPE &value, const bool nulls_last) {
		FlatVector::GetData<T>(vector)[idx] = value;
	}

	// Nothing to do here
	static EXTRA_STATE CreateExtraState(Vector &input, idx_t count) {
		return false;
	}

	static void PrepareData(Vector &input, const idx_t count, EXTRA_STATE &, UnifiedVectorFormat &format,
	                        const bool nulls_last) {
		input.ToUnifiedFormat(count, format);
	}
};

struct MinMaxStringValue {
	using TYPE = string_t;
	using EXTRA_STATE = bool;

	static TYPE Create(const UnifiedVectorFormat &format, const idx_t idx) {
		return UnifiedVectorFormat::GetData<string_t>(format)[idx];
	}

	static void Assign(Vector &vector, const idx_t idx, const TYPE &value, const bool nulls_last) {
		FlatVector::GetData<string_t>(vector)[idx] = StringVector::AddStringOrBlob(vector, value);
	}

	// Nothing to do here
	static EXTRA_STATE CreateExtraState(Vector &input, idx_t count) {
		return false;
	}

	static void PrepareData(Vector &input, const idx_t count, EXTRA_STATE &, UnifiedVectorFormat &format,
	                        const bool nulls_last) {
		input.ToUnifiedFormat(count, format);
	}
};

// Use sort key to serialize/deserialize values
struct MinMaxFallbackValue {
	using TYPE = string_t;
	using EXTRA_STATE = Vector;

	static TYPE Create(const UnifiedVectorFormat &format, const idx_t idx) {
		return UnifiedVectorFormat::GetData<string_t>(format)[idx];
	}

	static void Assign(Vector &vector, const idx_t idx, const TYPE &value, const bool nulls_last) {
		auto order_by_null_type = nulls_last ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
		OrderModifiers modifiers(OrderType::ASCENDING, order_by_null_type);
		CreateSortKeyHelpers::DecodeSortKey(value, vector, idx, modifiers);
	}

	static EXTRA_STATE CreateExtraState(Vector &input, idx_t count) {
		return Vector(LogicalTypeId::BLOB);
	}

	static void PrepareData(Vector &input, const idx_t count, EXTRA_STATE &extra_state, UnifiedVectorFormat &format,
	                        const bool nulls_last) {
		auto order_by_null_type = nulls_last ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
		const OrderModifiers modifiers(OrderType::ASCENDING, order_by_null_type);
		CreateSortKeyHelpers::CreateSortKeyWithValidity(input, extra_state, modifiers, count);
		input.Flatten(count);
		extra_state.ToUnifiedFormat(count, format);
	}
};

template <class T, bool NULLS_LAST>
struct ValueOrNull {
	T value;
	bool is_valid;

	bool operator==(const ValueOrNull &other) const {
		return is_valid == other.is_valid && value == other.value;
	}

	bool operator>(const ValueOrNull &other) const {
		if (is_valid && other.is_valid) {
			return value > other.value;
		}
		if (!is_valid && !other.is_valid) {
			return false;
		}

		return is_valid ^ NULLS_LAST;
	}
};

template <class T, bool NULLS_LAST>
struct MinMaxFixedValueOrNull {
	using TYPE = ValueOrNull<T, NULLS_LAST>;
	using EXTRA_STATE = bool;

	static TYPE Create(const UnifiedVectorFormat &format, const idx_t idx) {
		return TYPE {UnifiedVectorFormat::GetData<T>(format)[idx], format.validity.RowIsValid(idx)};
	}

	static void Assign(Vector &vector, const idx_t idx, const TYPE &value, const bool nulls_last) {
		FlatVector::Validity(vector).Set(idx, value.is_valid);
		FlatVector::GetData<T>(vector)[idx] = value.value;
	}

	static EXTRA_STATE CreateExtraState(Vector &input, idx_t count) {
		return false;
	}

	static void PrepareData(Vector &input, const idx_t count, EXTRA_STATE &extra_state, UnifiedVectorFormat &format,
	                        const bool nulls_last) {
		input.ToUnifiedFormat(count, format);
	}
};

//------------------------------------------------------------------------------
// MinMaxN Operation (common for both ArgMinMaxN and MinMaxN)
//------------------------------------------------------------------------------
struct MinMaxNOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input) {
		if (!source.is_initialized) {
			// source is empty, nothing to do
			return;
		}

		if (!target.is_initialized) {
			target.Initialize(aggr_input.allocator, source.heap.Capacity());
		} else if (source.heap.Capacity() != target.heap.Capacity()) {
			throw InvalidInputException("Mismatched n values in min/max/arg_min/arg_max");
		}

		// Merge the heaps
		target.heap.Insert(aggr_input.allocator, source.heap);
	}

	template <class STATE>
	static void Finalize(Vector &state_vector, AggregateInputData &input_data, Vector &result, idx_t count,
	                     idx_t offset) {
		// We only expect bind data from arg_max, otherwise nulls last is the default
		const bool nulls_last =
		    input_data.bind_data ? input_data.bind_data->Cast<ArgMinMaxFunctionData>().nulls_last : true;

		UnifiedVectorFormat state_format;
		state_vector.ToUnifiedFormat(count, state_format);

		const auto states = UnifiedVectorFormat::GetData<STATE *>(state_format);
		auto &mask = FlatVector::Validity(result);

		const auto old_len = ListVector::GetListSize(result);

		// Count the number of new entries
		idx_t new_entries = 0;
		for (idx_t i = 0; i < count; i++) {
			const auto state_idx = state_format.sel->get_index(i);
			auto &state = *states[state_idx];
			new_entries += state.heap.Size();
		}

		// Resize the list vector to fit the new entries
		ListVector::Reserve(result, old_len + new_entries);

		const auto list_entries = FlatVector::GetData<list_entry_t>(result);
		auto &child_data = ListVector::GetEntry(result);

		idx_t current_offset = old_len;
		for (idx_t i = 0; i < count; i++) {
			const auto rid = i + offset;
			const auto state_idx = state_format.sel->get_index(i);
			auto &state = *states[state_idx];

			if (!state.is_initialized || state.heap.IsEmpty()) {
				mask.SetInvalid(rid);
				continue;
			}

			// Add the entries to the list vector
			auto &list_entry = list_entries[rid];
			list_entry.offset = current_offset;
			list_entry.length = state.heap.Size();

			// Turn the heap into a sorted list, invalidating the heap property
			auto heap = state.heap.SortAndGetHeap();

			for (idx_t slot = 0; slot < state.heap.Size(); slot++) {
				STATE::VAL_TYPE::Assign(child_data, current_offset++, state.heap.GetValue(heap[slot]), nulls_last);
			}
		}

		D_ASSERT(current_offset == old_len + new_entries);
		ListVector::SetListSize(result, current_offset);
		result.Verify(count);
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	static bool IgnoreNull() {
		return true;
	}
};

//------------------------------------------------------------------------------
// MinMaxNWithTiesOperation: Finalize produces LIST<STRUCT(arg, rank)>
//------------------------------------------------------------------------------
struct MinMaxNWithTiesOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input) {
		if (!source.is_initialized) {
			return;
		}

		if (!target.is_initialized) {
			target.Initialize(aggr_input.allocator, source.heap.Capacity());
		} else if (source.heap.Capacity() != target.heap.Capacity()) {
			throw InvalidInputException("Mismatched n values in __internal_arg_min_rank/__internal_arg_max_rank");
		}

		target.heap.Insert(aggr_input.allocator, source.heap);
	}

	template <class STATE>
	static void Finalize(Vector &state_vector, AggregateInputData &input_data, Vector &result, idx_t count,
	                     idx_t offset) {
		const bool nulls_last = !input_data.bind_data || input_data.bind_data->Cast<ArgMinMaxFunctionData>().nulls_last;

		UnifiedVectorFormat state_format;
		state_vector.ToUnifiedFormat(count, state_format);

		const auto states = UnifiedVectorFormat::GetData<STATE *>(state_format);
		auto &mask = FlatVector::Validity(result);

		const auto old_len = ListVector::GetListSize(result);

		// Count the total number of entries (heap + ties)
		idx_t new_entries = 0;
		for (idx_t i = 0; i < count; i++) {
			const auto state_idx = state_format.sel->get_index(i);
			auto &state = *states[state_idx];
			new_entries += state.heap.Size();
		}

		ListVector::Reserve(result, old_len + new_entries);

		const auto list_entries = FlatVector::GetData<list_entry_t>(result);
		auto &child_data = ListVector::GetEntry(result);

		// child_data is a STRUCT vector with children: "arg" and "rank"
		auto &struct_entries = StructVector::GetEntries(child_data);
		D_ASSERT(struct_entries.size() == 2);
		auto &arg_vector = *struct_entries[0];
		auto &rank_vector = *struct_entries[1];

		idx_t current_offset = old_len;
		for (idx_t i = 0; i < count; i++) {
			const auto rid = i + offset;
			const auto state_idx = state_format.sel->get_index(i);
			auto &state = *states[state_idx];

			if (!state.is_initialized || state.heap.IsEmpty()) {
				mask.SetInvalid(rid);
				continue;
			}

			auto heap_size = state.heap.GetHeapSize();
			auto &ties = state.heap.GetTies();
			idx_t total_entries = state.heap.Size();

			auto &list_entry = list_entries[rid];
			list_entry.offset = current_offset;
			list_entry.length = total_entries;

			// Sort the heap entries
			auto heap = state.heap.SortAndGetHeap();

			// First: assign heap entries
			for (idx_t slot = 0; slot < heap_size; slot++) {
				STATE::VAL_TYPE::Assign(arg_vector, current_offset + slot, state.heap.GetValue(heap[slot]), nulls_last);
			}
			// Then: assign ties entries
			for (idx_t slot = 0; slot < ties.size(); slot++) {
				STATE::VAL_TYPE::Assign(arg_vector, current_offset + heap_size + slot, state.heap.GetValue(ties[slot]),
				                        nulls_last);
			}

			// Now compute rank values
			auto rank_data = FlatVector::GetData<int64_t>(rank_vector);
			int64_t current_rank = 1;
			int64_t row_number = 1;
			bool first = true;

			auto get_key = [&](idx_t idx) -> const typename STATE::ARG_TYPE::TYPE & {
				if (idx < heap_size) {
					return state.heap.GetKey(heap[idx]);
				}
				return state.heap.GetKey(ties[idx - heap_size]);
			};

			for (idx_t slot = 0; slot < total_entries; slot++) {
				if (first) {
					rank_data[current_offset + slot] = 1;
					first = false;
				} else {
					auto &prev_key = get_key(slot - 1);
					auto &curr_key = get_key(slot);
					bool is_tie = STATE::KeyEquals(prev_key, curr_key);

					if (is_tie) {
						rank_data[current_offset + slot] = current_rank;
					} else {
						current_rank = row_number;
						rank_data[current_offset + slot] = current_rank;
					}
				}
				row_number++;
			}

			current_offset += total_entries;
		}

		D_ASSERT(current_offset == old_len + new_entries);
		ListVector::SetListSize(result, current_offset);
		result.Verify(count);
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	static bool IgnoreNull() {
		return true;
	}
};

//------------------------------------------------------------------------------
// ArgMinMaxWithTies: FunctionData, State, Update, and Specialize
// Used by TopNWindowElimination optimizer to directly construct BoundAggregateExpression
//------------------------------------------------------------------------------

struct ArgMinMaxWithTiesFunctionData : public ArgMinMaxFunctionData {
	explicit ArgMinMaxWithTiesFunctionData(
	    RankType rank_type_p = RankType::RANK,
	    ArgMinMaxNullHandling null_handling_p = ArgMinMaxNullHandling::IGNORE_ANY_NULL, bool nulls_last_p = true)
	    : ArgMinMaxFunctionData(null_handling_p, nulls_last_p), rank_type(rank_type_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<ArgMinMaxWithTiesFunctionData>(rank_type, null_handling, nulls_last);
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ArgMinMaxWithTiesFunctionData>();
		return other.null_handling == null_handling && other.nulls_last == nulls_last && other.rank_type == rank_type;
	}

	RankType rank_type;
};

template <class A, class B, class COMPARATOR>
class ArgMinMaxNWithTiesState {
public:
	using VAL_TYPE = A;
	using ARG_TYPE = B;

	using V = typename VAL_TYPE::TYPE;
	using K = typename ARG_TYPE::TYPE;

	BinaryAggregateHeapWithTies<K, V, COMPARATOR> heap;

	bool is_initialized = false;
	void Initialize(ArenaAllocator &allocator, idx_t nval) {
		heap.Initialize(allocator, nval);
		is_initialized = true;
	}

	static bool KeyEquals(const K &a, const K &b) {
		return !COMPARATOR::Operation(a, b) && !COMPARATOR::Operation(b, a);
	}
};

template <class STATE>
inline void ArgMinMaxNWithTiesUpdate(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count,
                                     Vector &state_vector, idx_t count) {
	D_ASSERT(aggr_input.bind_data);
	const auto &bind_data = aggr_input.bind_data->Cast<ArgMinMaxWithTiesFunctionData>();

	auto &val_vector = inputs[0];
	auto &arg_vector = inputs[1];
	auto &n_vector = inputs[2];

	UnifiedVectorFormat val_format;
	UnifiedVectorFormat arg_format;
	UnifiedVectorFormat n_format;
	UnifiedVectorFormat state_format;

	auto val_extra_state = STATE::VAL_TYPE::CreateExtraState(val_vector, count);
	auto arg_extra_state = STATE::ARG_TYPE::CreateExtraState(arg_vector, count);

	STATE::VAL_TYPE::PrepareData(val_vector, count, val_extra_state, val_format, bind_data.nulls_last);
	STATE::ARG_TYPE::PrepareData(arg_vector, count, arg_extra_state, arg_format, bind_data.nulls_last);

	n_vector.ToUnifiedFormat(count, n_format);
	state_vector.ToUnifiedFormat(count, state_format);

	auto states = UnifiedVectorFormat::GetData<STATE *>(state_format);

	for (idx_t i = 0; i < count; i++) {
		const auto arg_idx = arg_format.sel->get_index(i);
		const auto val_idx = val_format.sel->get_index(i);

		const auto state_idx = state_format.sel->get_index(i);
		auto &state = *states[state_idx];

		if (!state.is_initialized) {
			static constexpr int64_t MAX_N = 1000000;
			const auto nidx = n_format.sel->get_index(i);
			if (!n_format.validity.RowIsValid(nidx)) {
				throw InvalidInputException(
				    "Invalid input for __internal_arg_min/__internal_arg_max_rank: n value cannot be NULL");
			}
			const auto nval = UnifiedVectorFormat::GetData<int64_t>(n_format)[nidx];
			if (nval <= 0) {
				throw InvalidInputException(
				    "Invalid input for __internal_arg_min/__internal_arg_max_rank: n value must be > 0");
			}
			if (nval >= MAX_N) {
				throw InvalidInputException(
				    "Invalid input for __internal_arg_min/__internal_arg_max_rank: n value must be < %d", MAX_N);
			}
			state.Initialize(aggr_input.allocator, UnsafeNumericCast<idx_t>(nval));
		}

		auto arg_val = STATE::ARG_TYPE::Create(arg_format, arg_idx);
		auto val_val = STATE::VAL_TYPE::Create(val_format, val_idx);

		state.heap.Insert(aggr_input.allocator, arg_val, val_val);
	}
}

template <class VAL_TYPE, class ARG_TYPE, class COMPARATOR>
inline void SpecializeArgMinMaxNWithTiesNullFunction(AggregateFunction &function) {
	using STATE = ArgMinMaxNWithTiesState<VAL_TYPE, ARG_TYPE, COMPARATOR>;
	using OP = MinMaxNWithTiesOperation;
	function.SetStateSizeCallback(AggregateFunction::StateSize<STATE>);
	function.SetStateInitCallback(AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>);
	function.SetStateCombineCallback(AggregateFunction::StateCombine<STATE, OP>);
	function.SetStateDestructorCallback(AggregateFunction::StateDestroy<STATE, OP>);
	function.SetStateFinalizeCallback(MinMaxNWithTiesOperation::Finalize<STATE>);
	function.SetStateUpdateCallback(ArgMinMaxNWithTiesUpdate<STATE>);
}

template <class VAL_TYPE, bool NULLS_LAST, class COMPARATOR>
inline void SpecializeArgMinMaxNWithTiesNullFunction(PhysicalType arg_type, AggregateFunction &function) {
	switch (arg_type) {
#ifndef DUCKDB_SMALLER_BINARY
	case PhysicalType::VARCHAR:
		SpecializeArgMinMaxNWithTiesNullFunction<VAL_TYPE, MinMaxFallbackValue, COMPARATOR>(function);
		break;
	case PhysicalType::INT32:
		SpecializeArgMinMaxNWithTiesNullFunction<VAL_TYPE, MinMaxFixedValueOrNull<int32_t, NULLS_LAST>, COMPARATOR>(
		    function);
		break;
	case PhysicalType::INT64:
		SpecializeArgMinMaxNWithTiesNullFunction<VAL_TYPE, MinMaxFixedValueOrNull<int64_t, NULLS_LAST>, COMPARATOR>(
		    function);
		break;
	case PhysicalType::FLOAT:
		SpecializeArgMinMaxNWithTiesNullFunction<VAL_TYPE, MinMaxFixedValueOrNull<float, NULLS_LAST>, COMPARATOR>(
		    function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeArgMinMaxNWithTiesNullFunction<VAL_TYPE, MinMaxFixedValueOrNull<double, NULLS_LAST>, COMPARATOR>(
		    function);
		break;
#endif
	default:
		SpecializeArgMinMaxNWithTiesNullFunction<VAL_TYPE, MinMaxFallbackValue, COMPARATOR>(function);
		break;
	}
}

template <bool NULLS_LAST, class COMPARATOR>
inline void SpecializeArgMinMaxNWithTiesNullFunction(PhysicalType val_type, PhysicalType arg_type,
                                                     AggregateFunction &function) {
	switch (val_type) {
#ifndef DUCKDB_SMALLER_BINARY
	case PhysicalType::VARCHAR:
		SpecializeArgMinMaxNWithTiesNullFunction<MinMaxFallbackValue, NULLS_LAST, COMPARATOR>(arg_type, function);
		break;
	case PhysicalType::INT32:
		SpecializeArgMinMaxNWithTiesNullFunction<MinMaxFixedValueOrNull<int32_t, NULLS_LAST>, NULLS_LAST, COMPARATOR>(
		    arg_type, function);
		break;
	case PhysicalType::INT64:
		SpecializeArgMinMaxNWithTiesNullFunction<MinMaxFixedValueOrNull<int64_t, NULLS_LAST>, NULLS_LAST, COMPARATOR>(
		    arg_type, function);
		break;
	case PhysicalType::FLOAT:
		SpecializeArgMinMaxNWithTiesNullFunction<MinMaxFixedValueOrNull<float, NULLS_LAST>, NULLS_LAST, COMPARATOR>(
		    arg_type, function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeArgMinMaxNWithTiesNullFunction<MinMaxFixedValueOrNull<double, NULLS_LAST>, NULLS_LAST, COMPARATOR>(
		    arg_type, function);
		break;
#endif
	default:
		SpecializeArgMinMaxNWithTiesNullFunction<MinMaxFallbackValue, NULLS_LAST, COMPARATOR>(arg_type, function);
		break;
	}
}

//------------------------------------------------------------------------------
// ArgMinMaxRankHelper: directly construct bound aggregate functions for internal use
//------------------------------------------------------------------------------
struct ArgMinMaxRankHelper {
	static inline void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
	                             const AggregateFunction &function) {
		serializer.WriteProperty(100, "arguments", function.arguments);
		serializer.WriteProperty(101, "return_type", function.GetReturnType());
		auto &data = bind_data->Cast<ArgMinMaxWithTiesFunctionData>();
		serializer.WriteProperty(102, "rank_type", static_cast<uint8_t>(data.rank_type));
	}

	static inline unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		function.arguments = deserializer.ReadProperty<vector<LogicalType>>(100, "arguments");
		auto return_type = deserializer.ReadProperty<LogicalType>(101, "return_type");
		auto rank_type_val = deserializer.ReadProperty<uint8_t>(102, "rank_type");
		auto rank_type = static_cast<RankType>(rank_type_val);

		function.SetReturnType(std::move(return_type));

		auto type = function.arguments[0].InternalType();
		auto by_type = function.arguments[1].InternalType();

		if (StringUtil::StartsWith(function.name, "__internal_arg_min")) {
			SpecializeArgMinMaxNWithTiesNullFunction<true, LessThan>(type, by_type, function);
			return make_uniq<ArgMinMaxWithTiesFunctionData>(rank_type, ArgMinMaxNullHandling::HANDLE_ANY_NULL, true);
		} else {
			SpecializeArgMinMaxNWithTiesNullFunction<false, GreaterThan>(type, by_type, function);
			return make_uniq<ArgMinMaxWithTiesFunctionData>(rank_type, ArgMinMaxNullHandling::HANDLE_ANY_NULL, false);
		}
	}

	template <bool NULLS_LAST, class COMPARATOR>
	static inline AggregateFunction GetBoundFunctionInternal(const string &name, const LogicalType &type,
	                                                         const LogicalType &by_type, RankType rank_type,
	                                                         unique_ptr<FunctionData> &bind_data) {
		// Return type: LIST<STRUCT(arg: type, rank: BIGINT)>
		child_list_t<LogicalType> struct_children;
		struct_children.emplace_back("arg", type);
		struct_children.emplace_back("rank", LogicalType::BIGINT);
		auto struct_type = LogicalType::STRUCT(std::move(struct_children));
		auto return_type = LogicalType::LIST(struct_type);

		AggregateFunction func(name, {type, by_type, LogicalType::BIGINT}, return_type, nullptr, nullptr, nullptr,
		                       nullptr, nullptr, FunctionNullHandling::DEFAULT_NULL_HANDLING);

		// Specialize with null-aware callbacks: MinMaxFixedValueOrNull handles NULL ORDER BY values correctly.
		SpecializeArgMinMaxNWithTiesNullFunction<NULLS_LAST, COMPARATOR>(type.InternalType(), by_type.InternalType(),
		                                                                 func);

		// Set serialization callbacks to bypass bind during deserialization
		func.SetSerializeCallback(ArgMinMaxRankHelper::Serialize);
		func.SetDeserializeCallback(ArgMinMaxRankHelper::Deserialize);

		// Create bind data with null-aware handling
		bind_data =
		    make_uniq<ArgMinMaxWithTiesFunctionData>(rank_type, ArgMinMaxNullHandling::HANDLE_ANY_NULL, NULLS_LAST);

		return func;
	}

	static inline AggregateFunction GetInternalArgMinRank(const LogicalType &type, const LogicalType &by_type,
	                                                      unique_ptr<FunctionData> &bind_data) {
		return GetBoundFunctionInternal<true, LessThan>("__internal_arg_min_rank_nulls_last", type, by_type,
		                                                RankType::RANK, bind_data);
	}

	static inline AggregateFunction GetInternalArgMaxRank(const LogicalType &type, const LogicalType &by_type,
	                                                      unique_ptr<FunctionData> &bind_data) {
		return GetBoundFunctionInternal<false, GreaterThan>("__internal_arg_max_rank_nulls_last", type, by_type,
		                                                    RankType::RANK, bind_data);
	}
};

} // namespace duckdb
