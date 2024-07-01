#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/core_functions/aggregate/sort_key_helpers.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

//------------------------------------------------------------------------------
// Specializations
//------------------------------------------------------------------------------

template <class T>
struct FixedTopKValue {
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

struct StringTopKValue {
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
struct FallbackTopKValue {
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
// State
//------------------------------------------------------------------------------

template <class A_T, class B_T, bool ASCENDING>
class ArgTopKState {
public:
	using A = A_T;
	using B = B_T;

	using K = typename A::TYPE;
	using V = typename B::TYPE;

	struct TopKSlot {
		K key;
		V value;

		TopKSlot(K key, V value) : key(key), value(value) {
		}

		// Used by std::push_heap and std::pop_heap
		bool operator<(const TopKSlot &rhs) const {
			return ASCENDING ? (key < rhs.key) : (key > rhs.key);
		}
	};

	void Initialize(idx_t kval) {
		k = kval;
		heap.reserve(kval);
		is_initialized = true;
	}

	bool IsEmpty() const {
		return heap.empty();
	}
	bool IsInitialized() const {
		return is_initialized;
	}
	idx_t Size() const {
		return heap.size();
	}
	idx_t Capacity() const {
		return k;
	}

	// Try to insert a key into the heap, returning a pointer to the value if the key belongs in the top k
	optional_ptr<V> TryInsert(const K &key) {
		if (heap.size() < k) {
			heap.emplace_back(key, V());
			return &heap.back().value;
		}
		if (ASCENDING ? heap.front().key > key : heap.front().key < key) {
			std::pop_heap(heap.begin(), heap.end());
			heap.back() = TopKSlot(key, V());
			return &heap.back().value;
		}
		return nullptr;
	}

	void CommitInsert() {
		// Reorder the vector so that the heap property is restored
		std::push_heap(heap.begin(), heap.end());
		D_ASSERT(std::is_heap(heap.begin(), heap.end()));
	}

	void Merge(const ArgTopKState &other) {
		for (auto &entry : other.heap) {
			auto slot = TryInsert(entry.key);
			if (slot) {
				*slot = entry.value;
				CommitInsert();
			}
		}
		D_ASSERT(std::is_heap(heap.begin(), heap.end()));
	}

	vector<TopKSlot> &SortAndGetHeap() {
		std::sort_heap(heap.begin(), heap.end());
		return heap;
	}

private:
	vector<TopKSlot> heap;
	idx_t k = 0;
	bool is_initialized = false;
};

//------------------------------------------------------------------------------
// Operation
//------------------------------------------------------------------------------
struct ArgTopKOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class STATE>
	static void Update(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count, Vector &state_vector,
	                   idx_t count) {

		auto &arg_vector = inputs[0];
		auto &val_vector = inputs[1];
		auto &top_k_vector = inputs[2];

		UnifiedVectorFormat arg_format;
		UnifiedVectorFormat val_format;
		UnifiedVectorFormat top_k_format;
		UnifiedVectorFormat state_format;

		auto arg_extra_state = STATE::A::CreateExtraState(arg_vector, count);
		auto val_extra_state = STATE::B::CreateExtraState(val_vector, count);

		STATE::A::PrepareData(arg_vector, count, arg_extra_state, arg_format);
		STATE::B::PrepareData(val_vector, count, val_extra_state, val_format);

		top_k_vector.ToUnifiedFormat(count, top_k_format);
		state_vector.ToUnifiedFormat(count, state_format);

		auto states = UnifiedVectorFormat::GetData<STATE *>(state_format);

		for (idx_t i = 0; i < count; i++) {
			const auto arg_idx = arg_format.sel->get_index(i);
			const auto val_idx = val_format.sel->get_index(i);
			if (!arg_format.validity.RowIsValid(arg_idx) || !val_format.validity.RowIsValid(val_idx)) {
				continue;
			}
			const auto state_idx = state_format.sel->get_index(i);
			auto &state = *states[state_idx];

			// Initialize the heap if necessary and add the input to the heap
			if (!state.IsInitialized()) {
				static constexpr int64_t MAX_K = 1000000;
				const auto kidx = top_k_format.sel->get_index(i);
				if (!top_k_format.validity.RowIsValid(kidx)) {
					throw InvalidInputException("Invalid input for approx_top_k: k value cannot be NULL");
				}
				const auto kval = UnifiedVectorFormat::GetData<int64_t>(top_k_format)[kidx];
				if (kval <= 0) {
					throw InvalidInputException("Invalid input for approx_top_k: k value must be > 0");
				}
				if (kval >= MAX_K) {
					throw InvalidInputException("Invalid input for approx_top_k: k value must be < %d", MAX_K);
				}
				state.Initialize(UnsafeNumericCast<idx_t>(kval));
			}

			// Now add the input to the heap
			auto arg_val = STATE::A::Create(arg_format, arg_idx);

			// Does this key belong in the top k?
			auto slot = state.TryInsert(arg_val);
			if (slot) {
				auto val_val = STATE::B::Create(val_format, val_idx);
				*slot = val_val;
				state.CommitInsert();
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input) {
		if (source.IsEmpty()) {
			// source is empty, nothing to do
			return;
		}

		if (!target.IsInitialized()) {
			target.Initialize(source.Capacity());
		} else if (source.Capacity() != target.Capacity()) {
			throw InvalidInputException("Mismatched k values in arg_top_k");
		}

		// Merge the heaps
		target.Merge(source);
	}

	template <class STATE>
	static void Finalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
		// using STATE = ArgTopKState<string, string, false>;

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
			new_entries += state.Size();
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

			// TODO: Set null if empty? or if uninitialized?
			if (state.IsEmpty()) {
				mask.SetInvalid(rid);
				continue;
			}

			// Add the entries to the list vector
			auto &list_entry = list_entries[rid];
			list_entry.offset = current_offset;
			list_entry.length = state.Size();

			// Turn the heap into a sorted list, invalidating the heap property
			auto &heap = state.SortAndGetHeap();

			// Loop over the heap backwards to add the entries in sorted order
			for (auto rit = heap.rbegin(); rit != heap.rend(); ++rit) {
				STATE::B::Assign(child_data, current_offset++, rit->value);
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
// Bind
//------------------------------------------------------------------------------

template <class K, bool ASCENDING>
static void SpecializeFunction(PhysicalType val_type, AggregateFunction &function) {
	switch (val_type) {
	case PhysicalType::VARCHAR:
		function.update = ArgTopKOperation::Update<ArgTopKState<K, StringTopKValue, ASCENDING>>;
		function.finalize = ArgTopKOperation::Finalize<ArgTopKState<K, StringTopKValue, ASCENDING>>;
		break;
	case PhysicalType::INT32:
		function.update = ArgTopKOperation::Update<ArgTopKState<K, FixedTopKValue<int32_t>, ASCENDING>>;
		function.finalize = ArgTopKOperation::Finalize<ArgTopKState<K, FixedTopKValue<int32_t>, ASCENDING>>;
		break;
	case PhysicalType::INT64:
		function.update = ArgTopKOperation::Update<ArgTopKState<K, FixedTopKValue<int64_t>, ASCENDING>>;
		function.finalize = ArgTopKOperation::Finalize<ArgTopKState<K, FixedTopKValue<int64_t>, ASCENDING>>;
		break;
	case PhysicalType::FLOAT:
		function.update = ArgTopKOperation::Update<ArgTopKState<K, FixedTopKValue<float>, ASCENDING>>;
		function.finalize = ArgTopKOperation::Finalize<ArgTopKState<K, FixedTopKValue<float>, ASCENDING>>;
		break;
	case PhysicalType::DOUBLE:
		function.update = ArgTopKOperation::Update<ArgTopKState<K, FixedTopKValue<double>, ASCENDING>>;
		function.finalize = ArgTopKOperation::Finalize<ArgTopKState<K, FixedTopKValue<double>, ASCENDING>>;
		break;
	default:
		function.update = ArgTopKOperation::Update<ArgTopKState<K, FallbackTopKValue, ASCENDING>>;
		function.finalize = ArgTopKOperation::Finalize<ArgTopKState<K, FallbackTopKValue, ASCENDING>>;
		break;
	}
}

template <bool ASCENDING>
static void SpecializeFunction(PhysicalType arg_type, PhysicalType val_type, AggregateFunction &function) {
	switch (arg_type) {
	case PhysicalType::VARCHAR:
		SpecializeFunction<StringTopKValue, ASCENDING>(val_type, function);
		break;
	case PhysicalType::INT32:
		SpecializeFunction<FixedTopKValue<int32_t>, ASCENDING>(val_type, function);
		break;
	case PhysicalType::INT64:
		SpecializeFunction<FixedTopKValue<int64_t>, ASCENDING>(val_type, function);
		break;
	case PhysicalType::FLOAT:
		SpecializeFunction<FixedTopKValue<float>, ASCENDING>(val_type, function);
		break;
	case PhysicalType::DOUBLE:
		SpecializeFunction<FixedTopKValue<double>, ASCENDING>(val_type, function);
		break;
	default:
		SpecializeFunction<FallbackTopKValue, ASCENDING>(val_type, function);
		break;
	}
}

unique_ptr<FunctionData> ArgTopKBind(ClientContext &context, AggregateFunction &function,
                                     vector<unique_ptr<Expression>> &arguments) {
	for (auto &arg : arguments) {
		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}

	const auto arg_type = arguments[0]->return_type.InternalType();
	const auto val_type = arguments[1]->return_type.InternalType();

	// Attempt to specialize the function based on the input types
	SpecializeFunction<false>(arg_type, val_type, function);

	function.return_type = LogicalType::LIST(arguments[0]->return_type);
	return nullptr;
}

AggregateFunction ArgTopKFun::GetFunction() {
	using STATE = ArgTopKState<StringTopKValue, StringTopKValue, false>;
	using OP = ArgTopKOperation;
	return AggregateFunction("arg_top_k", {LogicalTypeId::ANY, LogicalTypeId::ANY, LogicalType::BIGINT},
	                         LogicalType::LIST(LogicalType::ANY), AggregateFunction::StateSize<STATE>,
	                         AggregateFunction::StateInitialize<STATE, OP>, OP::Update<STATE>,
	                         AggregateFunction::StateCombine<STATE, OP>, OP::Finalize<STATE>, nullptr, ArgTopKBind,
	                         AggregateFunction::StateDestroy<STATE, OP>);
}

} // namespace duckdb
