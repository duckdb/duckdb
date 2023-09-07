#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/core_functions/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct HistogramFunctor {
	template <class T, class MAP_TYPE = map<T, idx_t>>
	static void HistogramUpdate(UnifiedVectorFormat &sdata, UnifiedVectorFormat &input_data, idx_t count) {
		auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;
		for (idx_t i = 0; i < count; i++) {
			if (input_data.validity.RowIsValid(input_data.sel->get_index(i))) {
				auto &state = *states[sdata.sel->get_index(i)];
				if (!state.hist) {
					state.hist = new MAP_TYPE();
				}
				auto value = UnifiedVectorFormat::GetData<T>(input_data);
				(*state.hist)[value[input_data.sel->get_index(i)]]++;
			}
		}
	}

	template <class T>
	static Value HistogramFinalize(T first) {
		return Value::CreateValue(first);
	}
};

struct HistogramStringFunctor {
	template <class T, class MAP_TYPE = map<T, idx_t>>
	static void HistogramUpdate(UnifiedVectorFormat &sdata, UnifiedVectorFormat &input_data, idx_t count) {
		auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;
		auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_data);
		for (idx_t i = 0; i < count; i++) {
			if (input_data.validity.RowIsValid(input_data.sel->get_index(i))) {
				auto &state = *states[sdata.sel->get_index(i)];
				if (!state.hist) {
					state.hist = new MAP_TYPE();
				}
				(*state.hist)[input_strings[input_data.sel->get_index(i)].GetString()]++;
			}
		}
	}

	template <class T>
	static Value HistogramFinalize(T first) {
		string_t value = first;
		return Value::CreateValue(value);
	}
};

struct HistogramFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.hist = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.hist) {
			delete state.hist;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class OP, class T, class MAP_TYPE>
static void HistogramUpdateFunction(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector,
                                    idx_t count) {

	D_ASSERT(input_count == 1);

	auto &input = inputs[0];
	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);

	OP::template HistogramUpdate<T, MAP_TYPE>(sdata, input_data, count);
}

template <class T, class MAP_TYPE>
static void HistogramCombineFunction(Vector &state_vector, Vector &combined, AggregateInputData &, idx_t count) {

	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states_ptr = (HistogramAggState<T, MAP_TYPE> **)sdata.data;

	auto combined_ptr = FlatVector::GetData<HistogramAggState<T, MAP_TYPE> *>(combined);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states_ptr[sdata.sel->get_index(i)];
		if (!state.hist) {
			continue;
		}
		if (!combined_ptr[i]->hist) {
			combined_ptr[i]->hist = new MAP_TYPE();
		}
		D_ASSERT(combined_ptr[i]->hist);
		D_ASSERT(state.hist);
		for (auto &entry : *state.hist) {
			(*combined_ptr[i]->hist)[entry.first] += entry.second;
		}
	}
}

template <class OP, class T, class MAP_TYPE>
static void HistogramFinalizeFunction(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count,
                                      idx_t offset) {

	UnifiedVectorFormat sdata;
	state_vector.ToUnifiedFormat(count, sdata);
	auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;

	auto &mask = FlatVector::Validity(result);
	auto old_len = ListVector::GetListSize(result);

	for (idx_t i = 0; i < count; i++) {
		const auto rid = i + offset;
		auto &state = *states[sdata.sel->get_index(i)];
		if (!state.hist) {
			mask.SetInvalid(rid);
			continue;
		}

		for (auto &entry : *state.hist) {
			Value bucket_value = OP::template HistogramFinalize<T>(entry.first);
			auto count_value = Value::CreateValue(entry.second);
			auto struct_value =
			    Value::STRUCT({std::make_pair("key", bucket_value), std::make_pair("value", count_value)});
			ListVector::PushBack(result, struct_value);
		}

		auto list_struct_data = ListVector::GetData(result);
		list_struct_data[rid].length = ListVector::GetListSize(result) - old_len;
		list_struct_data[rid].offset = old_len;
		old_len += list_struct_data[rid].length;
	}
	result.Verify(count);
}

unique_ptr<FunctionData> HistogramBindFunction(ClientContext &context, AggregateFunction &function,
                                               vector<unique_ptr<Expression>> &arguments) {

	D_ASSERT(arguments.size() == 1);

	if (arguments[0]->return_type.id() == LogicalTypeId::LIST ||
	    arguments[0]->return_type.id() == LogicalTypeId::STRUCT ||
	    arguments[0]->return_type.id() == LogicalTypeId::MAP) {
		throw NotImplementedException("Unimplemented type for histogram %s", arguments[0]->return_type.ToString());
	}

	auto struct_type = LogicalType::MAP(arguments[0]->return_type, LogicalType::UBIGINT);

	function.return_type = struct_type;
	return make_uniq<VariableReturnBindData>(function.return_type);
}

template <class OP, class T, class MAP_TYPE = map<T, idx_t>>
static AggregateFunction GetHistogramFunction(const LogicalType &type) {

	using STATE_TYPE = HistogramAggState<T, MAP_TYPE>;

	return AggregateFunction("histogram", {type}, LogicalTypeId::MAP, AggregateFunction::StateSize<STATE_TYPE>,
	                         AggregateFunction::StateInitialize<STATE_TYPE, HistogramFunction>,
	                         HistogramUpdateFunction<OP, T, MAP_TYPE>, HistogramCombineFunction<T, MAP_TYPE>,
	                         HistogramFinalizeFunction<OP, T, MAP_TYPE>, nullptr, HistogramBindFunction,
	                         AggregateFunction::StateDestroy<STATE_TYPE, HistogramFunction>);
}

template <class OP, class T, bool IS_ORDERED>
AggregateFunction GetMapType(const LogicalType &type) {

	if (IS_ORDERED) {
		return GetHistogramFunction<OP, T>(type);
	}
	return GetHistogramFunction<OP, T, unordered_map<T, idx_t>>(type);
}

template <bool IS_ORDERED = true>
AggregateFunction GetHistogramFunction(const LogicalType &type) {

	switch (type.id()) {
	case LogicalType::BOOLEAN:
		return GetMapType<HistogramFunctor, bool, IS_ORDERED>(type);
	case LogicalType::UTINYINT:
		return GetMapType<HistogramFunctor, uint8_t, IS_ORDERED>(type);
	case LogicalType::USMALLINT:
		return GetMapType<HistogramFunctor, uint16_t, IS_ORDERED>(type);
	case LogicalType::UINTEGER:
		return GetMapType<HistogramFunctor, uint32_t, IS_ORDERED>(type);
	case LogicalType::UBIGINT:
		return GetMapType<HistogramFunctor, uint64_t, IS_ORDERED>(type);
	case LogicalType::TINYINT:
		return GetMapType<HistogramFunctor, int8_t, IS_ORDERED>(type);
	case LogicalType::SMALLINT:
		return GetMapType<HistogramFunctor, int16_t, IS_ORDERED>(type);
	case LogicalType::INTEGER:
		return GetMapType<HistogramFunctor, int32_t, IS_ORDERED>(type);
	case LogicalType::BIGINT:
		return GetMapType<HistogramFunctor, int64_t, IS_ORDERED>(type);
	case LogicalType::FLOAT:
		return GetMapType<HistogramFunctor, float, IS_ORDERED>(type);
	case LogicalType::DOUBLE:
		return GetMapType<HistogramFunctor, double, IS_ORDERED>(type);
	case LogicalType::VARCHAR:
		return GetMapType<HistogramStringFunctor, string, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP:
		return GetMapType<HistogramFunctor, timestamp_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_TZ:
		return GetMapType<HistogramFunctor, timestamp_tz_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_S:
		return GetMapType<HistogramFunctor, timestamp_sec_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_MS:
		return GetMapType<HistogramFunctor, timestamp_ms_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_NS:
		return GetMapType<HistogramFunctor, timestamp_ns_t, IS_ORDERED>(type);
	case LogicalType::TIME:
		return GetMapType<HistogramFunctor, dtime_t, IS_ORDERED>(type);
	case LogicalType::TIME_TZ:
		return GetMapType<HistogramFunctor, dtime_tz_t, IS_ORDERED>(type);
	case LogicalType::DATE:
		return GetMapType<HistogramFunctor, date_t, IS_ORDERED>(type);
	default:
		throw InternalException("Unimplemented histogram aggregate");
	}
}

AggregateFunctionSet HistogramFun::GetFunctions() {
	AggregateFunctionSet fun;
	fun.AddFunction(GetHistogramFunction<>(LogicalType::BOOLEAN));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::UTINYINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::USMALLINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::UINTEGER));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::UBIGINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TINYINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::SMALLINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::INTEGER));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::BIGINT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::FLOAT));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::DOUBLE));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::VARCHAR));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIMESTAMP));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIMESTAMP_TZ));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIMESTAMP_S));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIMESTAMP_MS));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIMESTAMP_NS));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIME));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::TIME_TZ));
	fun.AddFunction(GetHistogramFunction<>(LogicalType::DATE));
	return fun;
}

AggregateFunction HistogramFun::GetHistogramUnorderedMap(LogicalType &type) {
	const auto &const_type = type;
	return GetHistogramFunction<false>(const_type);
}

} // namespace duckdb
