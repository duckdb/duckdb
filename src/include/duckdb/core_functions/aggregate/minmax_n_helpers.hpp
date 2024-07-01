#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

template <class T, class COMPARATOR>
class BoundedHeap {
public:
	BoundedHeap() = default;

	explicit BoundedHeap(const idx_t capacity) {
		SetSize(capacity);
	}

	void SetSize(const idx_t capacity) {
		k = capacity;
		heap.reserve(k);
	}

	bool IsEmpty() const {
		return heap.empty();
	}
	idx_t Size() const {
		return heap.size();
	}
	idx_t Capacity() const {
		return k;
	}

	void Insert(const T &item) {
		if (heap.size() < k) {
			heap.push_back(item);
			std::push_heap(heap.begin(), heap.end(), COMPARATOR::Operation);
		} else if (COMPARATOR::Operation(item, heap.front())) {
			std::pop_heap(heap.begin(), heap.end(), COMPARATOR::Operation);
			heap.back() = item;
			std::push_heap(heap.begin(), heap.end(), COMPARATOR::Operation);
		}
		D_ASSERT(std::is_heap(heap.begin(), heap.end(), COMPARATOR::Operation));
	}

	// Merges the other heap into this
	void Insert(const BoundedHeap &other) {
		for (const auto &elem : other.heap) {
			Insert(elem);
		}
	}

	// Returns the elements in sorted order, destroying the heap
	vector<T> &SortAndGetHeap() {
		std::sort_heap(heap.begin(), heap.end(), COMPARATOR::Operation);
		return heap;
	}

private:
	idx_t k = 0;
	vector<T> heap;
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

	static void Assign(Vector &vector, const idx_t idx, const TYPE &value) {
		FlatVector::GetData<T>(vector)[idx] = value;
	}

	// Nothing to do here
	static EXTRA_STATE CreateExtraState(Vector &input, idx_t count) {
		return false;
	}

	static void PrepareData(Vector &input, const idx_t count, EXTRA_STATE &, UnifiedVectorFormat &format) {
		input.ToUnifiedFormat(count, format);
	}
};

struct MinMaxStringValue {
	using TYPE = string;
	using EXTRA_STATE = bool;

	static TYPE Create(const UnifiedVectorFormat &format, const idx_t idx) {
		return UnifiedVectorFormat::GetData<string_t>(format)[idx].GetString();
	}

	static void Assign(Vector &vector, const idx_t idx, const TYPE &value) {
		FlatVector::GetData<string_t>(vector)[idx] = StringVector::AddString(vector, value);
	}

	// Nothing to do here
	static EXTRA_STATE CreateExtraState(Vector &input, idx_t count) {
		return false;
	}

	static void PrepareData(Vector &input, const idx_t count, EXTRA_STATE &, UnifiedVectorFormat &format) {
		input.ToUnifiedFormat(count, format);
	}
};

// Use sort key to serialize/deserialize values
struct MinMaxFallbackValue {
	using TYPE = string;
	using EXTRA_STATE = Vector;

	static TYPE Create(const UnifiedVectorFormat &format, const idx_t idx) {
		return UnifiedVectorFormat::GetData<string_t>(format)[idx].GetString();
	}

	static void Assign(Vector &vector, const idx_t idx, const TYPE &value) {
		OrderModifiers modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
		CreateSortKeyHelpers::DecodeSortKey(string_t(value), vector, idx, modifiers);
	}

	static EXTRA_STATE CreateExtraState(Vector &input, idx_t count) {
		return Vector(LogicalTypeId::BLOB);
	}

	static void PrepareData(Vector &input, const idx_t count, EXTRA_STATE &extra_state, UnifiedVectorFormat &format) {
		const OrderModifiers modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);
		CreateSortKeyHelpers::CreateSortKey(input, count, modifiers, extra_state);
		extra_state.ToUnifiedFormat(count, format);
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
			target.Initialize(source.heap.Capacity());
		} else if (source.heap.Capacity() != target.heap.Capacity()) {
			throw InvalidInputException("Mismatched k values in arg_top_k");
		}

		// Merge the heaps
		target.heap.Insert(source.heap);
	}

	template <class STATE>
	static void Finalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {

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
			auto &heap = state.heap.SortAndGetHeap();

			// Loop over the heap backwards to add the entries in sorted order
			for (auto rit = heap.rbegin(); rit != heap.rend(); ++rit) {
				STATE::VAL_TYPE::Assign(child_data, current_offset++, STATE::GetValue(*rit));
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
