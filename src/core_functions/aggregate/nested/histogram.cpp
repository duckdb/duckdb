#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/core_functions/aggregate/nested_functions.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/core_functions/aggregate/histogram_helpers.hpp"
#include "duckdb/common/owning_string_map.hpp"

namespace duckdb {

template <class MAP_TYPE>
struct HistogramFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.hist = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		if (state.hist) {
			delete state.hist;
		}
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (!source.hist) {
			return;
		}
		if (!target.hist) {
			target.hist = MAP_TYPE::CreateEmpty(input_data.allocator);
		}
		for (auto &entry : *source.hist) {
			(*target.hist)[entry.first] += entry.second;
		}
	}
};

template <class TYPE>
struct DefaultMapType {
	using MAP_TYPE = TYPE;

	static TYPE *CreateEmpty(ArenaAllocator &) {
		return new TYPE();
	}
};

template <class TYPE>
struct StringMapType {
	using MAP_TYPE = TYPE;

	static TYPE *CreateEmpty(ArenaAllocator &allocator) {
		return new TYPE(allocator);
	}
};

template <class OP, class T, class MAP_TYPE>
static void HistogramUpdateFunction(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count,
                                    Vector &state_vector, idx_t count) {

	D_ASSERT(input_count == 1);

	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);

	auto extra_state = OP::CreateExtraState(count);
	UnifiedVectorFormat input_data;
	OP::PrepareData(input, count, extra_state, input_data);

	auto states = UnifiedVectorFormat::GetData<HistogramAggState<T, typename MAP_TYPE::MAP_TYPE> *>(sdata);
	auto input_values = UnifiedVectorFormat::GetData<T>(input_data);
	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			continue;
		}
		auto &state = *states[sdata.sel->get_index(i)];
		if (!state.hist) {
			state.hist = MAP_TYPE::CreateEmpty(aggr_input.allocator);
		}
		auto &input_value = input_values[idx];
		++(*state.hist)[input_value];
	}
}

template <class OP, class T, class MAP_TYPE>
static void HistogramFinalizeFunction(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count,
                                      idx_t offset) {
	using HIST_STATE = HistogramAggState<T, typename MAP_TYPE::MAP_TYPE>;

	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = UnifiedVectorFormat::GetData<HIST_STATE *>(sdata);

	auto &mask = FlatVector::Validity(result);
	auto old_len = ListVector::GetListSize(result);
	idx_t new_entries = 0;
	// figure out how much space we need
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[sdata.sel->get_index(i)];
		if (!state.hist) {
			continue;
		}
		new_entries += state.hist->size();
	}
	// reserve space in the list vector
	ListVector::Reserve(result, old_len + new_entries);
	auto &keys = MapVector::GetKeys(result);
	auto &values = MapVector::GetValues(result);
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto count_entries = FlatVector::GetData<uint64_t>(values);

	idx_t current_offset = old_len;
	for (idx_t i = 0; i < count; i++) {
		const auto rid = i + offset;
		auto &state = *states[sdata.sel->get_index(i)];
		if (!state.hist) {
			mask.SetInvalid(rid);
			continue;
		}

		auto &list_entry = list_entries[rid];
		list_entry.offset = current_offset;
		for (auto &entry : *state.hist) {
			OP::template HistogramFinalize<T>(entry.first, keys, current_offset);
			count_entries[current_offset] = entry.second;
			current_offset++;
		}
		list_entry.length = current_offset - list_entry.offset;
	}
	D_ASSERT(current_offset == old_len + new_entries);
	ListVector::SetListSize(result, current_offset);
	result.Verify(count);
}

template <class OP, class T, class MAP_TYPE>
static AggregateFunction GetHistogramFunction(const LogicalType &type) {
	using STATE_TYPE = HistogramAggState<T, typename MAP_TYPE::MAP_TYPE>;
	using HIST_FUNC = HistogramFunction<MAP_TYPE>;

	auto struct_type = LogicalType::MAP(type, LogicalType::UBIGINT);
	return AggregateFunction(
	    "histogram", {type}, struct_type, AggregateFunction::StateSize<STATE_TYPE>,
	    AggregateFunction::StateInitialize<STATE_TYPE, HIST_FUNC>, HistogramUpdateFunction<OP, T, MAP_TYPE>,
	    AggregateFunction::StateCombine<STATE_TYPE, HIST_FUNC>, HistogramFinalizeFunction<OP, T, MAP_TYPE>, nullptr,
	    nullptr, AggregateFunction::StateDestroy<STATE_TYPE, HIST_FUNC>);
}

template <class OP, class T, class MAP_TYPE>
AggregateFunction GetMapTypeInternal(const LogicalType &type) {
	return GetHistogramFunction<OP, T, MAP_TYPE>(type);
}

template <class OP, class T, bool IS_ORDERED>
AggregateFunction GetMapType(const LogicalType &type) {
	if (IS_ORDERED) {
		return GetMapTypeInternal<OP, T, DefaultMapType<map<T, idx_t>>>(type);
	}
	return GetMapTypeInternal<OP, T, DefaultMapType<unordered_map<T, idx_t>>>(type);
}

template <class OP, bool IS_ORDERED>
AggregateFunction GetStringMapType(const LogicalType &type) {
	if (IS_ORDERED) {
		return GetMapTypeInternal<OP, string_t, StringMapType<OrderedOwningStringMap<idx_t>>>(type);
	} else {
		return GetMapTypeInternal<OP, string_t, StringMapType<OwningStringMap<idx_t>>>(type);
	}
}

template <bool IS_ORDERED = true>
AggregateFunction GetHistogramFunction(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return GetMapType<HistogramFunctor, bool, IS_ORDERED>(type);
	case PhysicalType::UINT8:
		return GetMapType<HistogramFunctor, uint8_t, IS_ORDERED>(type);
	case PhysicalType::UINT16:
		return GetMapType<HistogramFunctor, uint16_t, IS_ORDERED>(type);
	case PhysicalType::UINT32:
		return GetMapType<HistogramFunctor, uint32_t, IS_ORDERED>(type);
	case PhysicalType::UINT64:
		return GetMapType<HistogramFunctor, uint64_t, IS_ORDERED>(type);
	case PhysicalType::INT8:
		return GetMapType<HistogramFunctor, int8_t, IS_ORDERED>(type);
	case PhysicalType::INT16:
		return GetMapType<HistogramFunctor, int16_t, IS_ORDERED>(type);
	case PhysicalType::INT32:
		return GetMapType<HistogramFunctor, int32_t, IS_ORDERED>(type);
	case PhysicalType::INT64:
		return GetMapType<HistogramFunctor, int64_t, IS_ORDERED>(type);
	case PhysicalType::FLOAT:
		return GetMapType<HistogramFunctor, float, IS_ORDERED>(type);
	case PhysicalType::DOUBLE:
		return GetMapType<HistogramFunctor, double, IS_ORDERED>(type);
	case PhysicalType::VARCHAR:
		return GetStringMapType<HistogramStringFunctor, IS_ORDERED>(type);
	default:
		return GetStringMapType<HistogramGenericFunctor, IS_ORDERED>(type);
	}
}

template <bool IS_ORDERED = true>
unique_ptr<FunctionData> HistogramBindFunction(ClientContext &context, AggregateFunction &function,
                                               vector<unique_ptr<Expression>> &arguments) {

	D_ASSERT(arguments.size() == 1);

	if (arguments[0]->return_type.id() == LogicalTypeId::UNKNOWN) {
		throw ParameterNotResolvedException();
	}
	function = GetHistogramFunction<IS_ORDERED>(arguments[0]->return_type);
	return make_uniq<VariableReturnBindData>(function.return_type);
}

AggregateFunctionSet HistogramFun::GetFunctions() {
	AggregateFunctionSet fun;
	AggregateFunction histogram_function("histogram", {LogicalType::ANY}, LogicalTypeId::MAP, nullptr, nullptr, nullptr,
	                                     nullptr, nullptr, nullptr, HistogramBindFunction, nullptr);
	fun.AddFunction(HistogramFun::BinnedHistogramFunction());
	fun.AddFunction(histogram_function);
	return fun;
}

AggregateFunction HistogramFun::GetHistogramUnorderedMap(LogicalType &type) {
	return AggregateFunction("histogram", {LogicalType::ANY}, LogicalTypeId::MAP, nullptr, nullptr, nullptr, nullptr,
	                         nullptr, nullptr, HistogramBindFunction<false>, nullptr);
}

} // namespace duckdb
