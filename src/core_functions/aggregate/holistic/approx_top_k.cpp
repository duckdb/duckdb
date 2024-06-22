#include "duckdb/core_functions/aggregate/histogram_helpers.hpp"
#include "duckdb/core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/core_functions/aggregate/sort_key_helpers.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

// approx top k algorithm based on "A parallel space saving algorithm for frequent items and the Hurwitz zeta
// distribution" arxiv link -  https://arxiv.org/pdf/1401.0702
struct ApproxTopKValue {
	//! The counter
	idx_t count = 0;
	//! Index in the values array
	idx_t index = 0;
	//! The string value
	string_t str_value;
	//! Allocated data
	char *dataptr = nullptr;
	uint32_t size = 0;
	uint32_t capacity = 0;
};

struct ApproxTopKState {
	// the top-k data structure has two components
	// a list of k values sorted on "count" (i.e. values[0] has the lowest count)
	// a lookup map: string_t -> idx in "values" array
	unsafe_unique_array<ApproxTopKValue> stored_values;
	unsafe_vector<reference<ApproxTopKValue>> values;
	string_map_t<reference<ApproxTopKValue>> lookup_map;
	idx_t k = 0;

	void Initialize(idx_t kval) {
		D_ASSERT(values.empty());
		D_ASSERT(lookup_map.empty());
		k = kval;
		idx_t capacity = kval * 3;
		stored_values = make_unsafe_uniq_array<ApproxTopKValue>(capacity);
		values.reserve(capacity);
		for (idx_t i = 0; i < capacity; i++) {
			auto &val = stored_values[i];
			val.index = i;
			values.push_back(val);
		}
	}

	static void CopyValue(ApproxTopKValue &value, const string_t &input, AggregateInputData &input_data) {
		if (input.IsInlined()) {
			// no need to copy
			value.str_value = input;
			return;
		}
		value.size = UnsafeNumericCast<uint32_t>(input.GetSize());
		if (value.size > value.capacity) {
			// need to re-allocate for this value
			value.capacity = UnsafeNumericCast<uint32_t>(NextPowerOfTwo(value.size));
			value.dataptr = char_ptr_cast(input_data.allocator.Allocate(value.capacity));
		}
		// copy over the data
		memcpy(value.dataptr, input.GetData(), value.size);
		value.str_value = string_t(value.dataptr, value.size);
	}

	void InsertOrReplaceEntry(const string_t &input, AggregateInputData &aggr_input, idx_t increment = 1) {
		Verify();
		auto &value = values[0].get();
		if (value.count > 0) {
			// there is an existing entry - we need to erase it from the map
			lookup_map.erase(value.str_value);
		}
		CopyValue(value, input, aggr_input);
		lookup_map.insert(make_pair(value.str_value, reference<ApproxTopKValue>(value)));
		IncrementCount(value, increment);
		Verify();
	}

	void IncrementCount(ApproxTopKValue &value, idx_t increment = 1) {
		value.count += increment;
		// maintain sortedness of "values"
		// swap while we have a higher count than the next entry
		while (value.index + 1 < values.size() &&
		       values[value.index].get().count > values[value.index + 1].get().count) {
			// swap the elements around
			auto &left = values[value.index];
			auto &right = values[value.index + 1];
			std::swap(left.get().index, right.get().index);
			std::swap(left, right);
		}
	}

	void Verify() {
#ifdef DEBUG
		if (values.empty()) {
			D_ASSERT(lookup_map.empty());
			return;
		}
		D_ASSERT(values.size() >= k);
		idx_t non_zero_entries = 0;
		for (idx_t k = 0; k < values.size(); k++) {
			auto &val = values[k].get();
			if (val.count > 0) {
				non_zero_entries++;
				// verify map exists
				auto entry = lookup_map.find(val.str_value);
				D_ASSERT(entry != lookup_map.end());
				// verify the index is correct
				D_ASSERT(val.index == k);
			}
			if (k > 0) {
				// sortedness
				D_ASSERT(val.count >= values[k - 1].get().count);
			}
		}
		// verify lookup map does not contain extra entries
		D_ASSERT(lookup_map.size() == non_zero_entries);
#endif
	}
};

struct ApproxTopKOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class TYPE, class STATE>
	static void Operation(STATE &state, const TYPE &input, AggregateInputData &aggr_input, Vector &top_k_vector,
	                      idx_t offset, idx_t count) {
		if (state.values.empty()) {
			// not initialized yet - initialize the K value and set all counters to 0
			UnifiedVectorFormat kdata;
			top_k_vector.ToUnifiedFormat(count, kdata);
			auto kidx = kdata.sel->get_index(offset);
			if (!kdata.validity.RowIsValid(kidx)) {
				throw InvalidInputException("Invalid input for approx_top_k: k value cannot be NULL");
			}
			auto kval = UnifiedVectorFormat::GetData<int64_t>(kdata)[kidx];
			if (kval <= 0) {
				throw InvalidInputException("Invalid input for approx_top_k: k value must be >= 0");
			}
			if (kval >= NumericLimits<uint32_t>::Maximum()) {
				throw InvalidInputException("Invalid input for approx_top_k: k value must be < %d",
				                            NumericLimits<uint32_t>::Maximum());
			}
			state.Initialize(UnsafeNumericCast<idx_t>(kval));
		}
		auto entry = state.lookup_map.find(input);
		if (entry != state.lookup_map.end()) {
			// the input is monitored - increment the count
			state.IncrementCount(entry->second.get());
		} else {
			// the input is not monitored - replace the first entry with the current entry and increment
			state.InsertOrReplaceEntry(input, aggr_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input) {
		if (source.lookup_map.empty()) {
			// source is empty
			return;
		}
		auto min_source = source.values[0].get().count;
		idx_t min_target;
		if (target.values.empty()) {
			min_target = 0;
			target.Initialize(source.k);
		} else {
			if (source.k != target.k) {
				throw NotImplementedException("Approx Top K - cannot combine approx_top_K with different k values. "
				                              "K values must be the same for all entries within the same group");
			}
			min_target = target.values[0].get().count;
		}
		// for all entries in target
		// check if they are tracked in source
		//     if they do - add the tracked count
		//     if they do not - add the minimum count
		for (idx_t target_idx = target.values.size(); target_idx > 0; --target_idx) {
			auto &val = target.values[target_idx - 1].get();
			if (val.count == 0) {
				continue;
			}
			auto source_entry = source.lookup_map.find(val.str_value);
			idx_t increment = min_source;
			if (source_entry != source.lookup_map.end()) {
				increment = source_entry->second.get().count;
			}
			if (increment == 0) {
				continue;
			}
			target.IncrementCount(val, min_source);
		}
		// now for each entry in source, if it is not tracked by the target, at the target minimum
		for (auto &source_entry : source.values) {
			auto &source_val = source_entry.get();
			auto target_entry = target.lookup_map.find(source_val.str_value);
			if (target_entry != target.lookup_map.end()) {
				// already tracked - no need to add anything
				continue;
			}
			auto new_count = source_val.count + min_target;
			// check if we exceed the current minimum of the target
			if (new_count <= target.values[0].get().count) {
				// if we do not we can skip this entry
				continue;
			}
			// otherwise we should add it to the target
			idx_t diff = new_count - target.values[0].get().count;
			target.InsertOrReplaceEntry(source_val.str_value, aggr_input, diff);
		}
		target.Verify();
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class T = string_t, class OP = HistogramGenericFunctor>
static void ApproxTopKUpdate(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count, Vector &state_vector,
                             idx_t count) {
	using STATE = ApproxTopKState;
	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);

	auto &top_k_vector = inputs[1];

	auto extra_state = OP::CreateExtraState();
	UnifiedVectorFormat input_data;
	OP::PrepareData(input, count, extra_state, input_data);

	auto states = UnifiedVectorFormat::GetData<STATE *>(sdata);
	auto data = UnifiedVectorFormat::GetData<T>(input_data);
	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			continue;
		}
		auto &state = *states[sdata.sel->get_index(i)];
		ApproxTopKOperation::Operation<T, STATE>(state, data[idx], aggr_input, top_k_vector, i, count);
	}
}

template <class OP = HistogramGenericFunctor>
static void ApproxTopKFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count, idx_t offset) {
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = UnifiedVectorFormat::GetData<ApproxTopKState *>(sdata);

	auto &mask = FlatVector::Validity(result);
	auto old_len = ListVector::GetListSize(result);
	idx_t new_entries = 0;
	// figure out how much space we need
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		if (state.values.empty()) {
			continue;
		}
		// get up to k values for each state
		// this can be less of fewer unique values were found
		for (auto &val : state.values) {
			if (val.get().count == 0) {
				continue;
			}
			new_entries++;
			if (new_entries >= state.k) {
				break;
			}
		}
	}
	// reserve space in the list vector
	ListVector::Reserve(result, old_len + new_entries);
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &child_data = ListVector::GetEntry(result);

	idx_t current_offset = old_len;
	for (idx_t i = 0; i < count; i++) {
		const auto rid = i + offset;
		auto &state = *states[sdata.sel->get_index(i)];
		if (state.values.empty()) {
			mask.SetInvalid(rid);
			continue;
		}
		auto &list_entry = list_entries[rid];
		list_entry.offset = current_offset;
		for (idx_t val_idx = state.values.size(); val_idx > state.values.size() - state.k; val_idx--) {
			auto &val = state.values[val_idx - 1].get();
			if (val.count == 0) {
				break;
			}
			OP::template HistogramFinalize<string_t>(val.str_value, child_data, current_offset);
			current_offset++;
		}
		list_entry.length = current_offset - list_entry.offset;
	}
	D_ASSERT(current_offset == old_len + new_entries);
	ListVector::SetListSize(result, current_offset);
	result.Verify(count);
}

unique_ptr<FunctionData> ApproxTopKBind(ClientContext &context, AggregateFunction &function,
                                        vector<unique_ptr<Expression>> &arguments) {
	for (auto &arg : arguments) {
		if (arg->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}
	if (arguments[0]->return_type.id() == LogicalTypeId::VARCHAR) {
		function.update = ApproxTopKUpdate<string_t, HistogramStringFunctor>;
		function.finalize = ApproxTopKFinalize<HistogramStringFunctor>;
	}
	function.return_type = LogicalType::LIST(arguments[0]->return_type);
	return nullptr;
}

AggregateFunction ApproxTopKFun::GetFunction() {
	using STATE = ApproxTopKState;
	using OP = ApproxTopKOperation;
	return AggregateFunction("approx_top_k", {LogicalTypeId::ANY, LogicalType::BIGINT},
	                         LogicalType::LIST(LogicalType::ANY), AggregateFunction::StateSize<STATE>,
	                         AggregateFunction::StateInitialize<STATE, OP>, ApproxTopKUpdate,
	                         AggregateFunction::StateCombine<STATE, OP>, ApproxTopKFinalize, nullptr, ApproxTopKBind);
}

} // namespace duckdb
