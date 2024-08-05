#include "duckdb/core_functions/aggregate/histogram_helpers.hpp"
#include "duckdb/core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/core_functions/aggregate/sort_key_helpers.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

struct ApproxTopKString {
	ApproxTopKString() : str(UINT32_C(0)), hash(0) {
	}
	ApproxTopKString(string_t str_p, hash_t hash_p) : str(str_p), hash(hash_p) {
	}

	string_t str;
	hash_t hash;
};

struct ApproxTopKHash {
	std::size_t operator()(const ApproxTopKString &k) const {
		return k.hash;
	}
};

struct ApproxTopKEquality {
	bool operator()(const ApproxTopKString &a, const ApproxTopKString &b) const {
		return Equals::Operation(a.str, b.str);
	}
};

template <typename T>
using approx_topk_map_t = unordered_map<ApproxTopKString, T, ApproxTopKHash, ApproxTopKEquality>;

// approx top k algorithm based on "A parallel space saving algorithm for frequent items and the Hurwitz zeta
// distribution" arxiv link -  https://arxiv.org/pdf/1401.0702
// together with the filter extension (Filtered Space-Saving) from "Estimating Top-k Destinations in Data Streams"
struct ApproxTopKValue {
	//! The counter
	idx_t count = 0;
	//! Index in the values array
	idx_t index = 0;
	//! The string value
	ApproxTopKString str_val;
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
	approx_topk_map_t<reference<ApproxTopKValue>> lookup_map;
	unsafe_vector<idx_t> filter;
	idx_t k = 0;
	idx_t capacity = 0;
	idx_t filter_mask;

	void Initialize(idx_t kval) {
		static constexpr idx_t MONITORED_VALUES_RATIO = 3;
		static constexpr idx_t FILTER_RATIO = 8;

		D_ASSERT(values.empty());
		D_ASSERT(lookup_map.empty());
		k = kval;
		capacity = kval * MONITORED_VALUES_RATIO;
		stored_values = make_unsafe_uniq_array_uninitialized<ApproxTopKValue>(capacity);
		values.reserve(capacity);

		// we scale the filter based on the amount of values we are monitoring
		idx_t filter_size = NextPowerOfTwo(capacity * FILTER_RATIO);
		filter_mask = filter_size - 1;
		filter.resize(filter_size);
	}

	static void CopyValue(ApproxTopKValue &value, const ApproxTopKString &input, AggregateInputData &input_data) {
		value.str_val.hash = input.hash;
		if (input.str.IsInlined()) {
			// no need to copy
			value.str_val = input;
			return;
		}
		value.size = UnsafeNumericCast<uint32_t>(input.str.GetSize());
		if (value.size > value.capacity) {
			// need to re-allocate for this value
			value.capacity = UnsafeNumericCast<uint32_t>(NextPowerOfTwo(value.size));
			value.dataptr = char_ptr_cast(input_data.allocator.Allocate(value.capacity));
		}
		// copy over the data
		memcpy(value.dataptr, input.str.GetData(), value.size);
		value.str_val.str = string_t(value.dataptr, value.size);
	}

	void InsertOrReplaceEntry(const ApproxTopKString &input, AggregateInputData &aggr_input, idx_t increment = 1) {
		if (values.size() < capacity) {
			D_ASSERT(increment > 0);
			// we can always add this entry
			auto &val = stored_values[values.size()];
			val.index = values.size();
			values.push_back(val);
		}
		auto &value = values.back().get();
		if (value.count > 0) {
			// the capacity is reached - we need to replace an entry

			// we use the filter as an early out
			// based on the hash - we find a slot in the filter
			// instead of monitoring the value immediately, we add to the slot in the filter
			// ONLY when the value in the filter exceeds the current min value, we start monitoring the value
			// this speeds up the algorithm as switching monitor values means we need to erase/insert in the hash table
			auto &filter_value = filter[input.hash & filter_mask];
			if (filter_value + increment < value.count) {
				// if the filter has a lower count than the current min count
				// we can skip adding this entry (for now)
				filter_value += increment;
				return;
			}
			// the filter exceeds the min value - start monitoring this value
			// erase the existing entry from the map
			// and set the filter for the minimum value back to the current minimum value
			filter[value.str_val.hash & filter_mask] = value.count;
			lookup_map.erase(value.str_val);
		}
		CopyValue(value, input, aggr_input);
		lookup_map.insert(make_pair(value.str_val, reference<ApproxTopKValue>(value)));
		IncrementCount(value, increment);
	}

	void IncrementCount(ApproxTopKValue &value, idx_t increment = 1) {
		value.count += increment;
		// maintain sortedness of "values"
		// swap while we have a higher count than the next entry
		while (value.index > 0 && values[value.index].get().count > values[value.index - 1].get().count) {
			// swap the elements around
			auto &left = values[value.index];
			auto &right = values[value.index - 1];
			std::swap(left.get().index, right.get().index);
			std::swap(left, right);
		}
	}

	void Verify() const {
#ifdef DEBUG
		if (values.empty()) {
			D_ASSERT(lookup_map.empty());
			return;
		}
		D_ASSERT(values.size() <= capacity);
		for (idx_t k = 0; k < values.size(); k++) {
			auto &val = values[k].get();
			D_ASSERT(val.count > 0);
			// verify map exists
			auto entry = lookup_map.find(val.str_val);
			D_ASSERT(entry != lookup_map.end());
			// verify the index is correct
			D_ASSERT(val.index == k);
			if (k > 0) {
				// sortedness
				D_ASSERT(val.count <= values[k - 1].get().count);
			}
		}
		// verify lookup map does not contain extra entries
		D_ASSERT(lookup_map.size() == values.size());
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
			static constexpr int64_t MAX_APPROX_K = 1000000;
			// not initialized yet - initialize the K value and set all counters to 0
			UnifiedVectorFormat kdata;
			top_k_vector.ToUnifiedFormat(count, kdata);
			auto kidx = kdata.sel->get_index(offset);
			if (!kdata.validity.RowIsValid(kidx)) {
				throw InvalidInputException("Invalid input for approx_top_k: k value cannot be NULL");
			}
			auto kval = UnifiedVectorFormat::GetData<int64_t>(kdata)[kidx];
			if (kval <= 0) {
				throw InvalidInputException("Invalid input for approx_top_k: k value must be > 0");
			}
			if (kval >= MAX_APPROX_K) {
				throw InvalidInputException("Invalid input for approx_top_k: k value must be < %d", MAX_APPROX_K);
			}
			state.Initialize(UnsafeNumericCast<idx_t>(kval));
		}
		ApproxTopKString topk_string(input, Hash(input));
		auto entry = state.lookup_map.find(topk_string);
		if (entry != state.lookup_map.end()) {
			// the input is monitored - increment the count
			state.IncrementCount(entry->second.get());
		} else {
			// the input is not monitored - replace the first entry with the current entry and increment
			state.InsertOrReplaceEntry(topk_string, aggr_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input) {
		if (source.values.empty()) {
			// source is empty
			return;
		}
		source.Verify();
		auto min_source = source.values.back().get().count;
		idx_t min_target;
		if (target.values.empty()) {
			min_target = 0;
			target.Initialize(source.k);
		} else {
			if (source.k != target.k) {
				throw NotImplementedException("Approx Top K - cannot combine approx_top_K with different k values. "
				                              "K values must be the same for all entries within the same group");
			}
			min_target = target.values.back().get().count;
		}
		// for all entries in target
		// check if they are tracked in source
		//     if they do - add the tracked count
		//     if they do not - add the minimum count
		for (idx_t target_idx = 0; target_idx < target.values.size(); target_idx++) {
			auto &val = target.values[target_idx].get();
			auto source_entry = source.lookup_map.find(val.str_val);
			idx_t increment = min_source;
			if (source_entry != source.lookup_map.end()) {
				increment = source_entry->second.get().count;
			}
			if (increment == 0) {
				continue;
			}
			target.IncrementCount(val, increment);
		}
		// now for each entry in source, if it is not tracked by the target, at the target minimum
		for (auto &source_entry : source.values) {
			auto &source_val = source_entry.get();
			auto target_entry = target.lookup_map.find(source_val.str_val);
			if (target_entry != target.lookup_map.end()) {
				// already tracked - no need to add anything
				continue;
			}
			auto new_count = source_val.count + min_target;
			idx_t increment;
			if (target.values.size() >= target.capacity) {
				idx_t current_min = target.values.empty() ? 0 : target.values.back().get().count;
				D_ASSERT(target.values.size() == target.capacity);
				// target already has capacity values
				// check if we should insert this entry
				if (new_count <= current_min) {
					// if we do not we can skip this entry
					continue;
				}
				increment = new_count - current_min;
			} else {
				// target does not have capacity entries yet
				// just add this entry with the full count
				increment = new_count;
			}
			target.InsertOrReplaceEntry(source_val.str_val, aggr_input, increment);
		}
		// copy over the filter
		D_ASSERT(source.filter.size() == target.filter.size());
		for (idx_t filter_idx = 0; filter_idx < source.filter.size(); filter_idx++) {
			target.filter[filter_idx] += source.filter[filter_idx];
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

	auto extra_state = OP::CreateExtraState(count);
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
		new_entries += MinValue<idx_t>(state.values.size(), state.k);
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
		for (idx_t val_idx = 0; val_idx < MinValue<idx_t>(state.values.size(), state.k); val_idx++) {
			auto &val = state.values[val_idx].get();
			D_ASSERT(val.count > 0);
			OP::template HistogramFinalize<string_t>(val.str_val.str, child_data, current_offset);
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
	                         AggregateFunction::StateCombine<STATE, OP>, ApproxTopKFinalize, nullptr, ApproxTopKBind,
	                         AggregateFunction::StateDestroy<STATE, OP>);
}

} // namespace duckdb
