#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

struct HistogramUpdateFunctor {
	template <class T, class MAP_TYPE = map<T, idx_t>>
	static void HistogramUpdate(VectorData &sdata, VectorData &input_data, idx_t count) {

		auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;
		for (idx_t i = 0; i < count; i++) {
			if (input_data.validity.RowIsValid(input_data.sel->get_index(i))) {
				auto state = states[sdata.sel->get_index(i)];
				if (!state->hist) {
					state->hist = new MAP_TYPE();
				}
				auto value = (T *)input_data.data;
				(*state->hist)[value[input_data.sel->get_index(i)]]++;
			}
		}
	}
};

struct HistogramStringUpdateFunctor {
	template <class T, class MAP_TYPE = map<T, idx_t>>
	static void HistogramUpdate(VectorData &sdata, VectorData &input_data, idx_t count) {

		auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;
		for (idx_t i = 0; i < count; i++) {
			if (input_data.validity.RowIsValid(input_data.sel->get_index(i))) {
				auto state = states[sdata.sel->get_index(i)];
				if (!state->hist) {
					state->hist = new MAP_TYPE();
				}
				auto value = (string_t *)input_data.data;
				(*state->hist)[value[input_data.sel->get_index(i)].GetString()]++;
			}
		}
	}
};

struct HistogramFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->hist = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->hist) {
			delete state->hist;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class OP, class T, class MAP_TYPE>
static void HistogramUpdateFunction(Vector inputs[], FunctionData *, idx_t input_count, Vector &state_vector,
                                    idx_t count) {

	D_ASSERT(input_count == 1);

	auto &input = inputs[0];
	VectorData sdata;
	state_vector.Orrify(count, sdata);
	VectorData input_data;
	input.Orrify(count, input_data);

	OP::template HistogramUpdate<T, MAP_TYPE>(sdata, input_data, count);
}

template <class T, class MAP_TYPE>
static void HistogramCombineFunction(Vector &state, Vector &combined, FunctionData *bind_data, idx_t count) {

	VectorData sdata;
	state.Orrify(count, sdata);
	auto states_ptr = (HistogramAggState<T, MAP_TYPE> **)sdata.data;

	auto combined_ptr = FlatVector::GetData<HistogramAggState<T, MAP_TYPE> *>(combined);

	for (idx_t i = 0; i < count; i++) {
		auto state = states_ptr[sdata.sel->get_index(i)];
		if (!state->hist) {
			continue;
		}
		if (!combined_ptr[i]->hist) {
			combined_ptr[i]->hist = new MAP_TYPE();
		}
		D_ASSERT(combined_ptr[i]->hist);
		D_ASSERT(state->hist);
		for (auto &entry : *state->hist) {
			(*combined_ptr[i]->hist)[entry.first] += entry.second;
		}
	}
}

template <class T, class MAP_TYPE>
static void HistogramFinalize(Vector &state_vector, FunctionData *, Vector &result, idx_t count, idx_t offset) {

	VectorData sdata;
	state_vector.Orrify(count, sdata);
	auto states = (HistogramAggState<T, MAP_TYPE> **)sdata.data;

	auto &mask = FlatVector::Validity(result);

	auto &child_entries = StructVector::GetEntries(result);
	auto &bucket_list = child_entries[0];
	auto &count_list = child_entries[1];

	auto old_len = ListVector::GetListSize(*bucket_list);

	auto &bucket_validity = FlatVector::Validity(*bucket_list);
	auto &count_validity = FlatVector::Validity(*count_list);
	for (idx_t i = 0; i < count; i++) {
		const auto rid = i + offset;
		auto state = states[sdata.sel->get_index(i)];
		if (!state->hist) {
			mask.SetInvalid(rid);
			bucket_validity.SetInvalid(rid);
			count_validity.SetInvalid(rid);
			continue;
		}
		for (auto &entry : *state->hist) {
			auto bucket_value = Value::CreateValue(entry.first);
			ListVector::PushBack(*bucket_list, bucket_value);
			auto count_value = Value::CreateValue(entry.second);
			ListVector::PushBack(*count_list, count_value);
		}
		auto list_struct_data = FlatVector::GetData<list_entry_t>(*bucket_list);
		list_struct_data[rid].length = ListVector::GetListSize(*bucket_list) - old_len;
		list_struct_data[rid].offset = old_len;

		list_struct_data = FlatVector::GetData<list_entry_t>(*count_list);
		list_struct_data[rid].length = ListVector::GetListSize(*count_list) - old_len;
		list_struct_data[rid].offset = old_len;
		old_len += list_struct_data[rid].length;
	}
}

unique_ptr<FunctionData> HistogramBindFunction(ClientContext &context, AggregateFunction &function,
                                               vector<unique_ptr<Expression>> &arguments) {

	D_ASSERT(arguments.size() == 1);
	child_list_t<LogicalType> struct_children;
	struct_children.push_back({"bucket", LogicalType::LIST(arguments[0]->return_type)});
	struct_children.push_back({"count", LogicalType::LIST(LogicalType::UBIGINT)});
	auto struct_type = LogicalType::MAP(move(struct_children));

	function.return_type = struct_type;
	return make_unique<VariableReturnBindData>(function.return_type);
}

template <class OP, class T, class MAP_TYPE = map<T, idx_t>>
static AggregateFunction GetHistogramFunction(const LogicalType &type) {

	using STATE_TYPE = HistogramAggState<T, MAP_TYPE>;

	return AggregateFunction("histogram", {type}, LogicalTypeId::MAP, AggregateFunction::StateSize<STATE_TYPE>,
	                         AggregateFunction::StateInitialize<STATE_TYPE, HistogramFunction>,
	                         HistogramUpdateFunction<OP, T, MAP_TYPE>, HistogramCombineFunction<T, MAP_TYPE>,
	                         HistogramFinalize<T, MAP_TYPE>, nullptr, HistogramBindFunction,
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
		return GetMapType<HistogramUpdateFunctor, bool, IS_ORDERED>(type);
	case LogicalType::UTINYINT:
		return GetMapType<HistogramUpdateFunctor, uint8_t, IS_ORDERED>(type);
	case LogicalType::USMALLINT:
		return GetMapType<HistogramUpdateFunctor, uint16_t, IS_ORDERED>(type);
	case LogicalType::UINTEGER:
		return GetMapType<HistogramUpdateFunctor, uint32_t, IS_ORDERED>(type);
	case LogicalType::UBIGINT:
		return GetMapType<HistogramUpdateFunctor, uint64_t, IS_ORDERED>(type);
	case LogicalType::TINYINT:
		return GetMapType<HistogramUpdateFunctor, int8_t, IS_ORDERED>(type);
	case LogicalType::SMALLINT:
		return GetMapType<HistogramUpdateFunctor, int16_t, IS_ORDERED>(type);
	case LogicalType::INTEGER:
		return GetMapType<HistogramUpdateFunctor, int32_t, IS_ORDERED>(type);
	case LogicalType::BIGINT:
		return GetMapType<HistogramUpdateFunctor, int64_t, IS_ORDERED>(type);
	case LogicalType::FLOAT:
		return GetMapType<HistogramUpdateFunctor, float, IS_ORDERED>(type);
	case LogicalType::DOUBLE:
		return GetMapType<HistogramUpdateFunctor, double, IS_ORDERED>(type);
	case LogicalType::VARCHAR:
		return GetMapType<HistogramStringUpdateFunctor, string, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP:
		return GetMapType<HistogramUpdateFunctor, int64_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_TZ:
		return GetMapType<HistogramUpdateFunctor, int64_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_S:
		return GetMapType<HistogramUpdateFunctor, int64_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_MS:
		return GetMapType<HistogramUpdateFunctor, int64_t, IS_ORDERED>(type);
	case LogicalType::TIMESTAMP_NS:
		return GetMapType<HistogramUpdateFunctor, int64_t, IS_ORDERED>(type);
	case LogicalType::TIME:
		return GetMapType<HistogramUpdateFunctor, int64_t, IS_ORDERED>(type);
	case LogicalType::TIME_TZ:
		return GetMapType<HistogramUpdateFunctor, int64_t, IS_ORDERED>(type);
	case LogicalType::DATE:
		return GetMapType<HistogramUpdateFunctor, int32_t, IS_ORDERED>(type);
	default:
		throw InternalException("Unimplemented histogram aggregate");
	}
}

void HistogramFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet fun("histogram");
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
	set.AddFunction(fun);
}

AggregateFunction HistogramFun::GetHistogramUnorderedMap(LogicalType &type) {
	const auto &const_type = type;
	return GetHistogramFunction<false>(const_type);
}

} // namespace duckdb
