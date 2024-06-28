#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/core_functions/aggregate/sort_key_helpers.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include <duckdb/common/value_operations/value_operations.hpp>

namespace duckdb {

//------------------------------------------------------------------------------
// State
//------------------------------------------------------------------------------

struct ArgTopKValue {
	string key;
	string value;

	ArgTopKValue(const string &key, const string &value) : key(key), value(value) {
	}

	// operator overloads
	// clang-format off
	bool operator>(const ArgTopKValue &rhs) const { return key < rhs.key; }
	bool operator<(const ArgTopKValue &rhs) const { return key > rhs.key; }
	// clang-format on
};

struct ArgTopKState {

	vector<ArgTopKValue> heap;
	idx_t k;
	bool is_initialized = false;

	void Initialize(idx_t kval) {
		k = kval;
		heap.reserve(kval);
		is_initialized = true;
	}

	void Insert(const ArgTopKValue &input) {
		// If theres still space in the heap, add the input
		if (heap.size() < k) {
			heap.push_back(input);
			std::push_heap(heap.begin(), heap.end());
		} else if (input < heap.front()) {
			// The input is larger than the smallest element in the heap, replace the smallest element
			std::pop_heap(heap.begin(), heap.end());
			heap.back() = input;
			std::push_heap(heap.begin(), heap.end());
		}
		// Otherwise, ignore the input, it is not in the top k
		D_ASSERT(std::is_heap(heap.begin(), heap.end()));
	}
};

//------------------------------------------------------------------------------
// Operation
//------------------------------------------------------------------------------
struct ArgTopKOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class TYPE, class STATE = ArgTopKState>
	static void Operation(STATE &state, const TYPE &arg_input, const TYPE &val_input, AggregateInputData &aggr_input,
	                      Vector &top_k_vector, idx_t offset, idx_t count) {

		// Is the heap initialized?
		if (!state.is_initialized) {
			static constexpr int64_t MAX_APPROX_K = 1000000;
			UnifiedVectorFormat kdata;
			top_k_vector.ToUnifiedFormat(count, kdata);
			const auto kidx = kdata.sel->get_index(offset);
			if (!kdata.validity.RowIsValid(kidx)) {
				throw InvalidInputException("Invalid input for approx_top_k: k value cannot be NULL");
			}
			const auto kval = UnifiedVectorFormat::GetData<int64_t>(kdata)[kidx];
			if (kval <= 0) {
				throw InvalidInputException("Invalid input for approx_top_k: k value must be > 0");
			}
			if (kval >= MAX_APPROX_K) {
				throw InvalidInputException("Invalid input for approx_top_k: k value must be < %d", MAX_APPROX_K);
			}
			state.Initialize(UnsafeNumericCast<idx_t>(kval));
		}

		// Add the input to the heap
		string arg_str = arg_input.GetString();
		string val_str = val_input.GetString();
		state.Insert(ArgTopKValue(arg_str, val_str));
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input) {
		if (source.heap.empty()) {
			// source is empty, nothing to do
			return;
		}

		if (!target.is_initialized) {
			target.Initialize(source.k);
		} else if (source.k != target.k) {
			throw InvalidInputException("Mismatched k values in arg_top_k");
		}

		// Move the source heap into the target heap
		for (auto &entry : source.heap) {
			target.Insert(entry);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct StringTopK {
	using RESULT_TYPE = string_t;
	using ARG_TYPE = string;

	static void Assign(Vector &vector, idx_t result_idx, const ARG_TYPE &entry) {
		FlatVector::GetData<string_t>(vector)[result_idx] = StringVector::AddString(vector, entry);
	}
};

struct SortKeyTopK {
	using TYPE = string_t;
	using ARG_TYPE = string;

	static void Assign(Vector &vector, idx_t result_idx, const ARG_TYPE &entry) {
		CreateSortKeyHelpers::DecodeSortKey(string_t(entry), vector, result_idx,
		                                    OrderModifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST));
	}
};

template <class TYPE>
struct FixedTopK {
	using TYPE = TYPE;
	using ARG_TYPE = TYPE;

	static void Assign(Vector &vector, idx_t result_idx, const ARG_TYPE &entry) {
		FlatVector::GetData<TYPE>(vector)[result_idx] = entry;
	}
};

//------------------------------------------------------------------------------
// Update
//------------------------------------------------------------------------------
template <class T = string_t>
static void ArgTopKUpdate(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count, Vector &state_vector,
                          idx_t count) {
	using STATE = ArgTopKState;

	auto &arg_vector = inputs[0];
	auto &val_vector = inputs[1];
	auto &top_k_vector = inputs[2];

	UnifiedVectorFormat arg_format;
	UnifiedVectorFormat val_format;
	UnifiedVectorFormat state_format;

	arg_vector.ToUnifiedFormat(count, arg_format);
	val_vector.ToUnifiedFormat(count, val_format);
	state_vector.ToUnifiedFormat(count, state_format);

	auto states = UnifiedVectorFormat::GetData<STATE *>(state_format);
	auto arg_data = UnifiedVectorFormat::GetData<T>(arg_format);
	auto val_data = UnifiedVectorFormat::GetData<T>(val_format);
	for (idx_t i = 0; i < count; i++) {
		const auto arg_idx = arg_format.sel->get_index(i);
		const auto val_idx = val_format.sel->get_index(i);
		if (!arg_format.validity.RowIsValid(arg_idx) || !val_format.validity.RowIsValid(val_idx)) {
			continue;
		}
		const auto state_idx = state_format.sel->get_index(i);
		auto &state = *states[state_idx];
		ArgTopKOperation::Operation<T, STATE>(state, arg_data[arg_idx], val_data[val_idx], aggr_input, top_k_vector, i,
		                                      count);
	}
}

static void ArgTopKSortKeyUpdate(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count,
                                 Vector &state_vector, idx_t count) {
	using STATE = ArgTopKState;

	auto &arg_vector = inputs[0];
	auto &val_vector = inputs[1];
	auto &top_k_vector = inputs[2];

	Vector val_keys(LogicalType::BLOB);
	Vector val_values(LogicalType::BLOB);
	OrderModifiers modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);

	CreateSortKeyHelpers::CreateSortKey(val_vector, count, modifiers, val_keys);
	CreateSortKeyHelpers::CreateSortKey(arg_vector, count, modifiers, val_values);

	UnifiedVectorFormat state_format;
	state_vector.ToUnifiedFormat(count, state_format);

	const auto states = UnifiedVectorFormat::GetData<STATE *>(state_format);
	const auto arg_data = FlatVector::GetData<string_t>(val_values);
	const auto val_data = FlatVector::GetData<string_t>(val_keys);

	for (idx_t i = 0; i < count; i++) {
		if (FlatVector::IsNull(val_values, i) || FlatVector::IsNull(val_keys, i)) {
			continue;
		}
		const auto state_idx = state_format.sel->get_index(i);
		auto &state = *states[state_idx];
		ArgTopKOperation::Operation<string_t, STATE>(state, arg_data[i], val_data[i], aggr_input, top_k_vector, i,
		                                             count);
	}
}

//------------------------------------------------------------------------------
// Finalize
//------------------------------------------------------------------------------

static void ArgTopKFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {

	UnifiedVectorFormat state_format;
	state_vector.ToUnifiedFormat(count, state_format);

	const auto states = UnifiedVectorFormat::GetData<ArgTopKState *>(state_format);
	auto &mask = FlatVector::Validity(result);

	const auto old_len = ListVector::GetListSize(result);

	// Count the number of new entries
	idx_t new_entries = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto state_idx = state_format.sel->get_index(i);
		auto &state = *states[state_idx];
		new_entries += state.heap.size();
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
		if (state.heap.empty()) {
			mask.SetInvalid(rid);
			continue;
		}

		// Add the entries to the list vector
		auto &list_entry = list_entries[rid];
		list_entry.offset = current_offset;
		list_entry.length = state.heap.size();

		// Turn the heap into a sorted list, invalidating the heap property
		std::sort_heap(state.heap.begin(), state.heap.end());

		const auto child_data_ptr = FlatVector::GetData<string_t>(child_data);

		// Loop over the heap backwards to add the entries in sorted order
		for (idx_t val_idx = state.heap.size(); val_idx > 0; val_idx--) {
			auto &entry = state.heap[val_idx - 1];
			child_data_ptr[current_offset++] = StringVector::AddString(child_data, entry.value);
		}
	}

	D_ASSERT(current_offset == old_len + new_entries);
	ListVector::SetListSize(result, current_offset);
	result.Verify(count);
}

static void ArgTopKSortKeyFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count,
                                   idx_t offset) {

	UnifiedVectorFormat state_format;
	state_vector.ToUnifiedFormat(count, state_format);

	const auto states = UnifiedVectorFormat::GetData<ArgTopKState *>(state_format);
	auto &mask = FlatVector::Validity(result);

	const auto old_len = ListVector::GetListSize(result);

	// Count the number of new entries
	idx_t new_entries = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto state_idx = state_format.sel->get_index(i);
		auto &state = *states[state_idx];
		new_entries += state.heap.size();
	}

	// Resize the list vector to fit the new entries
	ListVector::Reserve(result, old_len + new_entries);

	const auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &child_data = ListVector::GetEntry(result);

	OrderModifiers modifiers(OrderType::ASCENDING, OrderByNullType::NULLS_LAST);

	idx_t current_offset = old_len;
	for (idx_t i = 0; i < count; i++) {
		const auto rid = i + offset;
		const auto state_idx = state_format.sel->get_index(i);
		auto &state = *states[state_idx];

		// TODO: Set null if empty? or if uninitialized?
		if (state.heap.empty()) {
			mask.SetInvalid(rid);
			continue;
		}

		// Add the entries to the list vector
		auto &list_entry = list_entries[rid];
		list_entry.offset = current_offset;
		list_entry.length = state.heap.size();

		// Turn the heap into a sorted list, invalidating the heap property
		std::sort_heap(state.heap.begin(), state.heap.end());

		// Loop over the heap backwards to add the entries in sorted order
		for (idx_t val_idx = state.heap.size(); val_idx > 0; val_idx--) {
			auto &entry = state.heap[val_idx - 1];
			CreateSortKeyHelpers::DecodeSortKey(string_t(entry.value), child_data, current_offset++, modifiers);
		}
	}

	D_ASSERT(current_offset == old_len + new_entries);
	ListVector::SetListSize(result, current_offset);
	result.Verify(count);
}

//------------------------------------------------------------------------------
// Bind
//------------------------------------------------------------------------------
unique_ptr<FunctionData> ArgTopKBind(ClientContext &context, AggregateFunction &function,
                                     vector<unique_ptr<Expression>> &arguments) {
	for (auto &arg : arguments) {
		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}
	/*
	if (arguments[0]->return_type.id() == LogicalTypeId::VARCHAR) {
	    function.update = ArgTopKUpdate<string_t, HistogramStringFunctor>;
	    function.finalize = ArgTopKFinalize<HistogramStringFunctor>;
	}
	*/
	function.return_type = LogicalType::LIST(arguments[0]->return_type);
	return nullptr;
}

AggregateFunction ArgTopKFun::GetFunction() {
	using STATE = ArgTopKState;
	using OP = ArgTopKOperation;
	return AggregateFunction("arg_top_k", {LogicalTypeId::ANY, LogicalTypeId::ANY, LogicalType::BIGINT},
	                         LogicalType::LIST(LogicalType::ANY), AggregateFunction::StateSize<STATE>,
	                         AggregateFunction::StateInitialize<STATE, OP>, ArgTopKSortKeyUpdate,
	                         AggregateFunction::StateCombine<STATE, OP>, ArgTopKSortKeyFinalize, nullptr, ArgTopKBind,
	                         AggregateFunction::StateDestroy<STATE, OP>);
}

} // namespace duckdb
