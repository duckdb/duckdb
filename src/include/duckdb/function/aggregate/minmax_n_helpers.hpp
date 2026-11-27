#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/arena_allocator.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include <new>

namespace duckdb {

//! The min/max/arg_min/arg_max "n" aggregates require the requested n to be strictly smaller than this value
static constexpr int64_t MIN_MAX_N_MAX_VALUE = 1000000;

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
			auto new_size = UnsafeNumericCast<uint32_t>(new_val.GetSize());
			auto new_capacity = NextPowerOfTwo(new_size);
			if (new_capacity > string_t::MAX_STRING_SIZE) {
				throw InvalidInputException("Resulting string/blob too large!");
			}
			do {
				capacity *= 2;
			} while (capacity < new_val.GetSize());
			allocated_data = allocator.Reallocate(allocated_data, old_size, capacity);
		}
		auto new_size = UnsafeNumericCast<uint32_t>(new_val.GetSize());
		memcpy(allocated_data, new_val.GetData(), new_size);
		value = string_t(const_char_ptr_cast(allocated_data), new_size);
	}
};

//! Grows a heap's slot array towards its capacity: doubling, then straight to the capacity once it exceeds
//! capacity / grow_to_capacity_divisor. A larger divisor abandons fewer arrays in heaps that fill up, but
//! over-allocates more in heaps that stay small.
template <class ENTRY>
ENTRY *GrowHeap(ArenaAllocator &allocator, ENTRY *heap, idx_t &allocated_capacity, const idx_t capacity,
                const idx_t required, const idx_t grow_to_capacity_divisor) {
	D_ASSERT(allocated_capacity < required && required <= capacity);
	const auto old_allocated_capacity = allocated_capacity;
	if (allocated_capacity == 0) {
		allocated_capacity = 1;
	} else if (allocated_capacity > capacity / grow_to_capacity_divisor) {
		allocated_capacity = capacity;
	} else {
		if (!TryMultiplyOperator::Operation(allocated_capacity, static_cast<idx_t>(2), allocated_capacity)) {
			throw OutOfRangeException("Overflow of 'allocated_capacity' in GrowHeap");
		}
	}
	allocated_capacity = MaxValue(allocated_capacity, required);

	const auto old_size = old_allocated_capacity * sizeof(ENTRY);
	idx_t new_size;
	if (!TryMultiplyOperator::Operation(allocated_capacity, static_cast<idx_t>(sizeof(ENTRY)), new_size)) {
		throw OutOfRangeException("Overflow of 'new_size' in GrowHeap");
	}
	auto ptr = heap ? allocator.ReallocateAligned(reinterpret_cast<data_ptr_t>(heap), old_size, new_size)
	                : allocator.AllocateAligned(new_size);
	memset(ptr + old_size, 0, new_size - old_size);
	return reinterpret_cast<ENTRY *>(ptr);
}

template <class T, class T_COMPARATOR>
class UnaryAggregateHeap {
public:
	UnaryAggregateHeap() = default;

	UnaryAggregateHeap(ArenaAllocator &allocator, idx_t capacity_p) {
		Initialize(allocator, capacity_p);
	}

	void Initialize(ArenaAllocator &allocator, const idx_t capacity_p) {
		capacity = capacity_p;
		allocated_capacity = 0;
		heap = nullptr;
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
			if (size == allocated_capacity) {
				heap = GrowHeap(allocator, heap, allocated_capacity, capacity, size + 1, GROW_TO_CAPACITY_DIVISOR);
			}
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

	//! Appends a slot without restoring the heap order
	void AppendSlot(ArenaAllocator &allocator, const T &value) {
		D_ASSERT(size < capacity);
		if (size == allocated_capacity) {
			heap = GrowHeap(allocator, heap, allocated_capacity, capacity, size + 1, GROW_TO_CAPACITY_DIVISOR);
		}
		heap[size++].Assign(allocator, value);
	}

	//! Pre-allocates exactly n slots (the state import knows the final size up front)
	void Reserve(ArenaAllocator &allocator, idx_t n) {
		if (n > allocated_capacity) {
			heap = GrowHeap(allocator, heap, allocated_capacity, capacity, n, GROW_TO_CAPACITY_DIVISOR);
		}
	}

	//! Restores the heap order after AppendSlot - a valid heap keeps its slot order
	void RestoreHeap() {
		if (!std::is_heap(heap, heap + size, Compare)) {
			std::make_heap(heap, heap + size, Compare);
		}
	}

	//! Slot indexes in the order the heap would sort to, leaving the heap intact for later reads and inserts. The
	//! indexes in heap order form the same heap, so sorting them as a heap yields the same order, ties included.
	void SortedSlots(vector<idx_t> &order) const {
		order.resize(size);
		for (idx_t slot = 0; slot < size; slot++) {
			order[slot] = slot;
		}
		std::sort_heap(order.begin(), order.end(),
		               [this](idx_t left, idx_t right) { return Compare(heap[left], heap[right]); });
	}

	const HeapEntry<T> &GetSlot(idx_t slot) const {
		return heap[slot];
	}

	static const T &GetValue(const HeapEntry<T> &slot) {
		return slot.value;
	}

private:
	static bool Compare(const HeapEntry<T> &left, const HeapEntry<T> &right) {
		return T_COMPARATOR::Operation(left.value, right.value);
	}

	//! Take all n slots once the heap holds more than n / 32: fewer abandoned arrays in heaps that fill up
	static constexpr idx_t GROW_TO_CAPACITY_DIVISOR = 32;

	idx_t capacity = 0;
	idx_t allocated_capacity = 0;
	HeapEntry<T> *heap = nullptr;
	idx_t size = 0;
};

struct List {
	uint32_t capacity;
	uint32_t size;
	List *next;
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
		allocated_capacity = 0;
		heap = nullptr;
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
			if (size == allocated_capacity) {
				heap = GrowHeap(allocator, heap, allocated_capacity, capacity, size + 1, GROW_TO_CAPACITY_DIVISOR);
			}
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

	//! Appends a slot without restoring the heap order
	void AppendSlot(ArenaAllocator &allocator, const K &key, const V &value) {
		D_ASSERT(size < capacity);
		if (size == allocated_capacity) {
			heap = GrowHeap(allocator, heap, allocated_capacity, capacity, size + 1, GROW_TO_CAPACITY_DIVISOR);
		}
		heap[size].first.Assign(allocator, key);
		heap[size].second.Assign(allocator, value);
		size++;
	}

	//! Pre-allocates exactly n slots (the state import knows the final size up front)
	void Reserve(ArenaAllocator &allocator, idx_t n) {
		if (n > allocated_capacity) {
			heap = GrowHeap(allocator, heap, allocated_capacity, capacity, n, GROW_TO_CAPACITY_DIVISOR);
		}
	}

	//! Restores the heap order after AppendSlot - a valid heap keeps its slot order
	void RestoreHeap() {
		if (!std::is_heap(heap, heap + size, Compare)) {
			std::make_heap(heap, heap + size, Compare);
		}
	}

	//! Slot indexes in the order the heap would sort to, leaving the heap intact for later reads and inserts. The
	//! indexes in heap order form the same heap, so sorting them as a heap yields the same order, ties included.
	void SortedSlots(vector<idx_t> &order) const {
		order.resize(size);
		for (idx_t slot = 0; slot < size; slot++) {
			order[slot] = slot;
		}
		std::sort_heap(order.begin(), order.end(),
		               [this](idx_t left, idx_t right) { return Compare(heap[left], heap[right]); });
	}

	const STORAGE_TYPE &GetSlot(idx_t slot) const {
		return heap[slot];
	}

	static const V &GetValue(const STORAGE_TYPE &slot) {
		return slot.second.value;
	}

private:
	static bool Compare(const STORAGE_TYPE &left, const STORAGE_TYPE &right) {
		return K_COMPARATOR::Operation(left.first.value, right.first.value);
	}

	//! Take all n slots past n / 2; an earlier jump makes sparse arg_max heaps several times larger
	static constexpr idx_t GROW_TO_CAPACITY_DIVISOR = 2;

	idx_t capacity = 0;
	idx_t allocated_capacity = 0;
	STORAGE_TYPE *heap = nullptr;
	idx_t size = 0;
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
		FlatVector::GetDataMutable<T>(vector)[idx] = value;
	}

	// Nothing to do here
	static EXTRA_STATE CreateExtraState(idx_t count) {
		return false;
	}

	static void PrepareData(const Vector &input, EXTRA_STATE &, UnifiedVectorFormat &format, const bool nulls_last) {
		input.ToUnifiedFormat(format);
	}
};

struct MinMaxStringValue {
	using TYPE = string_t;
	using EXTRA_STATE = bool;

	static TYPE Create(const UnifiedVectorFormat &format, const idx_t idx) {
		return UnifiedVectorFormat::GetData<string_t>(format)[idx];
	}

	static void Assign(Vector &vector, const idx_t idx, const TYPE &value, const bool nulls_last) {
		FlatVector::GetDataMutable<string_t>(vector)[idx] = StringVector::AddStringOrBlob(vector, value);
	}

	// Nothing to do here
	static EXTRA_STATE CreateExtraState(idx_t count) {
		return false;
	}

	static void PrepareData(const Vector &input, EXTRA_STATE &, UnifiedVectorFormat &format, const bool nulls_last) {
		input.ToUnifiedFormat(format);
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

	static EXTRA_STATE CreateExtraState(idx_t count) {
		return Vector(LogicalType::BLOB, MaxValue<idx_t>(count, 1));
	}

	static void PrepareData(const Vector &input, EXTRA_STATE &extra_state, UnifiedVectorFormat &format,
	                        const bool nulls_last) {
		auto order_by_null_type = nulls_last ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
		const OrderModifiers modifiers(OrderType::ASCENDING, order_by_null_type);
		CreateSortKeyHelpers::CreateSortKeyWithValidity(input, extra_state, modifiers);
		extra_state.ToUnifiedFormat(format);
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
			return GreaterThan::Operation(value, other.value);
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
		FlatVector::ValidityMutable(vector).Set(idx, value.is_valid);
		FlatVector::GetDataMutable<T>(vector)[idx] = value.value;
	}

	static EXTRA_STATE CreateExtraState(idx_t count) {
		return false;
	}

	static void PrepareData(const Vector &input, EXTRA_STATE &extra_state, UnifiedVectorFormat &format,
	                        const bool nulls_last) {
		input.ToUnifiedFormat(format);
	}
};

//------------------------------------------------------------------------------
// MinMaxN Operation (common for both ArgMinMaxN and MinMaxN)
//------------------------------------------------------------------------------
struct MinMaxNOperation {
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
	static void Finalize(Vector &state_vector, AggregateFinalizeInputData &input_data, Vector &result, idx_t count,
	                     idx_t offset) {
		// We only expect bind data from arg_max, otherwise nulls last is the default
		const bool nulls_last =
		    input_data.bind_data ? input_data.bind_data->Cast<ArgMinMaxFunctionData>().nulls_last : true;

		UnifiedVectorFormat state_format;
		state_vector.ToUnifiedFormat(state_format);

		const auto states = UnifiedVectorFormat::GetData<STATE *>(state_format);

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

		auto result_data = FlatVector::Writer<list_entry_t>(result, count, offset);
		auto &child_data = ListVector::GetChildMutable(result);

		idx_t current_offset = old_len;
		vector<idx_t> sorted_slots;
		for (idx_t i = 0; i < count; i++) {
			const auto state_idx = state_format.sel->get_index(i);
			auto &state = *states[state_idx];

			if (!state.is_initialized || state.heap.IsEmpty()) {
				result_data.WriteNull();
				continue;
			}

			// Add the entries to the list vector
			list_entry_t list_entry;
			list_entry.offset = current_offset;
			list_entry.length = state.heap.Size();
			result_data.WriteValue(list_entry);

			// Read the heap in sorted order without disturbing it: a state can be read again and updated afterwards
			state.heap.SortedSlots(sorted_slots);
			for (const auto slot : sorted_slots) {
				STATE::VAL_TYPE::Assign(child_data, current_offset++, state.heap.GetValue(state.heap.GetSlot(slot)),
				                        nulls_last);
			}
		}

		D_ASSERT(current_offset == old_len + new_entries);
		ListVector::SetListSize(result, current_offset);
		result.Verify();
	}

	static bool IgnoreNull() {
		return true;
	}

	//! Exported state: STRUCT(n, entries LIST(entry)) - the heap slots in heap order, NULL if no value was added
	static AggregateStateLayout GetStateType(AggregateLayoutInput &input, const LogicalType &entry_type) {
		child_list_t<LogicalType> children;
		children.emplace_back("n", LogicalType::BIGINT);
		children.emplace_back("entries", LogicalType::LIST(entry_type));
		AggregateStateLayout layout;
		layout.type = LogicalType::STRUCT(std::move(children));
		layout.total_state_size = AlignValue(input.function.GetStateSize(input.bind_data.get()));
		layout.spill_opaque_arena_memory = true;
		return layout;
	}

	//! States with different n values cannot be combined, so a per-row n cannot export its states. It also cannot
	//! stream: the streaming window (unlike WINDOW) would never check the n of later rows.
	static void BindPerRowN(BoundAggregateFunction &function) {
		function.GetCallbacks().get_state_type = nullptr;
		function.SetCanStream(false);
	}

	template <class STATE>
	static void ExportState(Vector &state_vector, AggregateFinalizeInputData &input_data, Vector &result, idx_t count,
	                        idx_t offset) {
		const bool nulls_last =
		    input_data.bind_data ? input_data.bind_data->Cast<ArgMinMaxFunctionData>().nulls_last : true;
		auto states = state_vector.Values<STATE *>();
		auto &fields = StructVector::GetEntries(result);
		auto &entry_lists = fields[1];

		idx_t entry_count = ListVector::GetListSize(entry_lists);
		for (idx_t i = 0; i < count; i++) {
			auto &state = *states[i].GetValue();
			entry_count += state.is_initialized ? state.heap.Size() : 0;
		}
		ListVector::Reserve(entry_lists, entry_count);
		auto &entries = ListVector::GetChildMutable(entry_lists);

		auto n_writer = FlatVector::Writer<int64_t>(fields[0], count, offset);
		auto list_writer = FlatVector::Writer<list_entry_t>(entry_lists, count, offset);
		idx_t entry_idx = ListVector::GetListSize(entry_lists);
		for (idx_t i = 0; i < count; i++) {
			auto &state = *states[i].GetValue();
			if (!state.is_initialized || state.heap.IsEmpty()) {
				// children of a NULL struct must also be NULL
				n_writer.WriteNull();
				list_writer.WriteNull();
				FlatVector::SetNull(result, offset + i, true);
				continue;
			}
			n_writer.WriteValue(UnsafeNumericCast<int64_t>(state.heap.Capacity()));
			list_writer.WriteValue(list_entry_t(entry_idx, state.heap.Size()));
			for (idx_t slot = 0; slot < state.heap.Size(); slot++) {
				state.ExportSlot(entries, entry_idx++, slot, nulls_last);
			}
		}
		ListVector::SetListSize(entry_lists, entry_idx);
		FlatVector::SetSize(result, offset + count);
	}

	template <class STATE>
	static void ImportState(AggregateImportInputData &input) {
		bool nulls_last = true;
		bool reject_nulls = true;
		if (input.bind_data) {
			auto &bind_data = input.bind_data->Cast<ArgMinMaxFunctionData>();
			nulls_last = bind_data.nulls_last;
			reject_nulls = bind_data.null_handling == ArgMinMaxNullHandling::IGNORE_ANY_NULL;
		}
		const auto count = input.input_vec.size();
		// flatten the input, so the struct and list children can be read by position
		Vector input_vec(input.input_vec, 0, count);
		input_vec.Flatten();
		auto &fields = StructVector::GetEntries(input_vec);
		auto n_values = fields[0].Values<int64_t>();
		auto entry_lists = fields[1].Values<list_entry_t>();
		const auto child_count = ListVector::GetListSize(fields[1]);
		typename STATE::ImportData entries(ListVector::GetChild(fields[1]), child_count, nulls_last);

		auto input_validity = input_vec.Validity();
		for (idx_t i = 0; i < count; i++) {
			auto &state = *reinterpret_cast<STATE *>(input.dest_buffer + i * input.layout.total_state_size);
			if (!input_validity.IsValid(i)) {
				continue;
			}
			const auto n = n_values[i];
			const auto entry_list = entry_lists[i];
			if (!n.IsValid() || !entry_list.IsValid()) {
				throw InvalidInputException("Invalid min/max/arg_min/arg_max state - n and entries cannot be NULL");
			}
			if (n.GetValue() <= 0 || n.GetValue() >= MIN_MAX_N_MAX_VALUE) {
				throw InvalidInputException("Invalid min/max/arg_min/arg_max state - n must be > 0 and < %d",
				                            MIN_MAX_N_MAX_VALUE);
			}
			const auto &list = entry_list.GetValue();
			if (list.length > UnsafeNumericCast<idx_t>(n.GetValue())) {
				throw InvalidInputException("Invalid min/max/arg_min/arg_max state - more entries than n");
			}
			if (list.length == 0) {
				// an empty heap exports as NULL, so it imports as an empty state too
				continue;
			}
			state.Initialize(input.allocator, UnsafeNumericCast<idx_t>(n.GetValue()));
			state.heap.Reserve(input.allocator, list.length);
			for (idx_t entry_idx = list.offset; entry_idx < list.offset + list.length; entry_idx++) {
				state.ImportSlot(input.allocator, entries, entry_idx, reject_nulls);
			}
			state.heap.RestoreHeap();
		}
	}
};

} // namespace duckdb
