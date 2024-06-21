#include "duckdb/core_functions/aggregate/histogram_helpers.hpp"
#include "duckdb/core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/core_functions/aggregate/sort_key_helpers.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/string_map_set.hpp"

namespace duckdb {

// approx top k algorithm based on "A parallel space saving algorithm for frequent items and the Hurwitz zeta distribution"
// arxiv link -  https://arxiv.org/pdf/1401.0702
struct ApproxTopKValue {
	idx_t count = 0;
	string_t value = string_t(UINT32_C(0));
};

struct ApproxTopKState {
	// the top-k data structure has two components
	// a list of k values sorted on "count" (i.e. values[0] has the lowest count)
	// a lookup map: string_t -> idx in "values" array
	vector<ApproxTopKValue> values;
	string_map_t<idx_t> lookup_map;
	idx_t k = 0;

	~ApproxTopKState() {
		for(auto &entry : values) {
			if (entry.count == 0) {
				continue;
			}
			DestroyString(entry.value);
		}
	}

	void Initialize(idx_t kval) {
		D_ASSERT(values.empty());
		D_ASSERT(lookup_map.empty());
		k = kval;
		values.resize(k);
	}

	static void DestroyString(string_t input) {
		if (input.IsInlined()) {
			return;
		}
		delete [] input.GetDataWriteable();
	}

	static string_t CopyValue(string_t input) {
		if (input.IsInlined()) {
			// inlined - no copy required
			return input;
		}
		// have to make a copy of the string data prior to inserting
		auto data = new char[input.GetSize()];
		memcpy(data, input.GetData(), input.GetSize());
		return string_t(data, UnsafeNumericCast<uint32_t>(input.GetSize()));
	}

	static string_t ReplaceValue(string_t input, string_t old_value) {
		if (input.IsInlined()) {
			// inlined - no copy required
			DestroyString(old_value);
			return input;
		}
		// have to make a copy
		// check if we can re-use the space of the other string
		char *data;
		if (old_value.GetSize() >= input.GetSize()) {
			// we can!
			data = old_value.GetDataWriteable();
		} else {
			// we cannot! destroy the old string and allocate new memory
			DestroyString(old_value);
			data = new char[input.GetSize()];
		}
		memcpy(data, input.GetData(), input.GetSize());
		return string_t(data, UnsafeNumericCast<uint32_t>(input.GetSize()));
	}

	void InsertOrReplaceEntry(string_t input, idx_t increment = 1) {
		Verify();
		if (values[0].count == 0) {
			// no entry yet at the first position - insert a new one
			// this happens if we have found < K elements so far
			values[0].value = CopyValue(input);
		} else {
			// there is an existing entry - we need to erase it from the map
			lookup_map.erase(values[0].value);
			// replace the string
			values[0].value = ReplaceValue(input, values[0].value);
		}
		auto entry = lookup_map.insert(make_pair(values[0].value, 0));
		D_ASSERT(entry.second);
		IncrementCount(entry.first, increment);
		Verify();
	}

	void IncrementCount(unordered_map<string_t, idx_t>::iterator &it, idx_t increment = 1) {
		auto &index = it->second;
		values[index].count += increment;
		// maintain sortedness of "values"
		// swap while we have a higher count than the next entry
		while(index + 1 < values.size() && values[index].count > values[index + 1].count) {
			// decrement the position of the other value, if it is not empty
			if (values[index + 1].count > 0) {
				auto other_entry = lookup_map.find(values[index + 1].value);
				D_ASSERT(other_entry != lookup_map.end());
				D_ASSERT(other_entry->second == index + 1);
				other_entry->second--;
			}
			std::swap(values[index], values[index + 1]);
			// increment the position of the current value
			index++;
		}
	}

	void Verify() {
#ifdef DEBUG
		if (values.empty()) {
			D_ASSERT(lookup_map.empty());
			return;
		}
		D_ASSERT(values.size() == k);
		idx_t non_zero_entries = 0;
		for(idx_t k = 0; k < values.size(); k++) {
			if (values[k].count > 0) {
				non_zero_entries++;
				// verify map exists and points to correct index
				auto entry = lookup_map.find(values[k].value) ;
				D_ASSERT(entry != lookup_map.end());
				D_ASSERT(entry->second == k);
			}
			if (k > 0) {
				// sortedness
				D_ASSERT(values[k].count >= values[k - 1].count);
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
	static void Operation(STATE &state, const TYPE &input, Vector &top_k_vector, idx_t offset, idx_t count) {
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
				throw InvalidInputException("Invalid input for approx_top_k: k value must be < %d", NumericLimits<uint32_t>::Maximum());
			}
			state.Initialize(UnsafeNumericCast<idx_t>(kval));
		}
		auto entry = state.lookup_map.find(input);
		if (entry != state.lookup_map.end()) {
			// the input is monitored - increment the count
			state.IncrementCount(entry);
		} else {
			// the input is not monitored - replace the first entry with the current entry and increment
			state.InsertOrReplaceEntry(input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.lookup_map.empty()) {
			// source is empty
			return;
		}
		auto min_source = source.values[0].count;
		idx_t min_target;
		if (target.values.empty()) {
			min_target = 0;
			target.Initialize(source.k);
		} else {
			if (source.k != target.k) {
				throw NotImplementedException(
				    "Approx Top K - cannot combine approx_top_K with different k values. "
				    "K values must be the same for all entries within the same group");
			}
			min_target = target.values[0].count;
		}
		// for all entries in target
		// check if they are tracked in source
		//     if they do - add the tracked count
		//     if they do not - add the minimum count
		for(idx_t target_idx = target.values.size(); target_idx > 0; --target_idx) {
			auto &val = target.values[target_idx - 1];
			if (val.count == 0) {
				continue;
			}
			auto source_entry = source.lookup_map.find(val.value);
			idx_t increment = min_source;
			if (source_entry != source.lookup_map.end()) {
				increment = source.values[source_entry->second].count;
			}
			if (increment == 0) {
				continue;
			}
			auto entry = target.lookup_map.find(val.value);
			D_ASSERT(entry != target.lookup_map.end());
			target.IncrementCount(entry, min_source);
		}
		// now for each entry in source, if it is not tracked by the target, at the target minimum
		for(auto &source_val : source.values) {
			auto target_entry = target.lookup_map.find(source_val.value);
			if (target_entry != target.lookup_map.end()) {
				// already tracked - no need to add anything
				continue;
			}
			auto new_count = source_val.count + min_target;
			// check if we exceed the current minimum of the target
			if (new_count <= target.values[0].count) {
				// if we do not we can skip this entry
				continue;
			}
			// otherwise we should add it to the target
			idx_t diff =  new_count - target.values[0].count;
			target.InsertOrReplaceEntry(source_val.value, diff);
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

template<class T = string_t, class OP = HistogramGenericFunctor>
static void ApproxTopKUpdate(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count,
                                       Vector &state_vector, idx_t count) {
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
		ApproxTopKOperation::Operation<T, STATE>(state, data[idx], top_k_vector, i, count);
	}
}

static void ApproxTopKFinalize(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count,
                                         idx_t offset) {
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
		for(auto &val : state.values) {
			if (val.count > 0) {
				new_entries++;
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
		for(idx_t val_idx = state.values.size(); val_idx > 0; val_idx--) {
			auto &val = state.values[val_idx - 1];
			if (val.count == 0) {
				break;
			}
			HistogramGenericFunctor::template HistogramFinalize<string_t>(val.value, child_data, current_offset);
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
	function.return_type = LogicalType::LIST(arguments[0]->return_type);
	return nullptr;
}

AggregateFunction ApproxTopKFun::GetFunction() {
	using STATE = ApproxTopKState;
	using OP = ApproxTopKOperation;
	return AggregateFunction("approx_top_k", {LogicalTypeId::ANY, LogicalType::BIGINT}, LogicalType::LIST(LogicalType::ANY),
	                                              AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>, ApproxTopKUpdate, AggregateFunction::StateCombine<STATE, OP>, ApproxTopKFinalize, nullptr,
	                                              ApproxTopKBind);
}

} // namespace duckdb
