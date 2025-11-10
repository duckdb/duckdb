#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/create_sort_key.hpp"

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

} // namespace duckdb
