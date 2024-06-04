#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/reservoir_sample.hpp"
#include "duckdb/core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include <algorithm>
#include <stdlib.h>

namespace duckdb {

template <typename T>
struct ReservoirQuantileState {
	T *v;
	idx_t len;
	idx_t pos;
	BaseReservoirSampling *r_samp;

	void Resize(idx_t new_len) {
		if (new_len <= len) {
			return;
		}
		T *old_v = v;
		v = (T *)realloc(v, new_len * sizeof(T));
		if (!v) {
			free(old_v);
			throw InternalException("Memory allocation failure");
		}
		len = new_len;
	}

	void ReplaceElement(T &input) {
		v[r_samp->min_weighted_entry_index] = input;
		r_samp->ReplaceElement();
	}

	void FillReservoir(idx_t sample_size, T element) {
		if (pos < sample_size) {
			v[pos++] = element;
			r_samp->InitializeReservoir(pos, len);
		} else {
			D_ASSERT(r_samp->next_index_to_sample >= r_samp->num_entries_to_skip_b4_next_sample);
			if (r_samp->next_index_to_sample == r_samp->num_entries_to_skip_b4_next_sample) {
				ReplaceElement(element);
			}
		}
	}
};

struct ReservoirQuantileBindData : public FunctionData {
	ReservoirQuantileBindData() {
	}
	ReservoirQuantileBindData(double quantile_p, idx_t sample_size_p)
	    : quantiles(1, quantile_p), sample_size(sample_size_p) {
	}

	ReservoirQuantileBindData(vector<double> quantiles_p, idx_t sample_size_p)
	    : quantiles(std::move(quantiles_p)), sample_size(sample_size_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ReservoirQuantileBindData>(quantiles, sample_size);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ReservoirQuantileBindData>();
		return quantiles == other.quantiles && sample_size == other.sample_size;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const AggregateFunction &function) {
		auto &bind_data = bind_data_p->Cast<ReservoirQuantileBindData>();
		serializer.WriteProperty(100, "quantiles", bind_data.quantiles);
		serializer.WriteProperty(101, "sample_size", bind_data.sample_size);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, AggregateFunction &function) {
		auto result = make_uniq<ReservoirQuantileBindData>();
		deserializer.ReadProperty(100, "quantiles", result->quantiles);
		deserializer.ReadProperty(101, "sample_size", result->sample_size);
		return std::move(result);
	}

	vector<double> quantiles;
	idx_t sample_size;
};

struct ReservoirQuantileOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.v = nullptr;
		state.len = 0;
		state.pos = 0;
		state.r_samp = nullptr;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		auto &bind_data = unary_input.input.bind_data->template Cast<ReservoirQuantileBindData>();
		if (state.pos == 0) {
			state.Resize(bind_data.sample_size);
		}
		if (!state.r_samp) {
			state.r_samp = new BaseReservoirSampling();
		}
		D_ASSERT(state.v);
		state.FillReservoir(bind_data.sample_size, input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.pos == 0) {
			return;
		}
		if (target.pos == 0) {
			target.Resize(source.len);
		}
		if (!target.r_samp) {
			target.r_samp = new BaseReservoirSampling();
		}
		for (idx_t src_idx = 0; src_idx < source.pos; src_idx++) {
			target.FillReservoir(target.len, source.v[src_idx]);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.v) {
			free(state.v);
			state.v = nullptr;
		}
		if (state.r_samp) {
			delete state.r_samp;
			state.r_samp = nullptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct ReservoirQuantileScalarOperation : public ReservoirQuantileOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.pos == 0) {
			finalize_data.ReturnNull();
			return;
		}
		D_ASSERT(state.v);
		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->template Cast<ReservoirQuantileBindData>();
		auto v_t = state.v;
		D_ASSERT(bind_data.quantiles.size() == 1);
		auto offset = (idx_t)((double)(state.pos - 1) * bind_data.quantiles[0]);
		std::nth_element(v_t, v_t + offset, v_t + state.pos);
		target = v_t[offset];
	}
};

AggregateFunction GetReservoirQuantileAggregateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int8_t>, int8_t, int8_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::TINYINT,
		                                                                                     LogicalType::TINYINT);

	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int16_t>, int16_t, int16_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::SMALLINT,
		                                                                                     LogicalType::SMALLINT);

	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int32_t>, int32_t, int32_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::INTEGER,
		                                                                                     LogicalType::INTEGER);

	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int64_t>, int64_t, int64_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::BIGINT,
		                                                                                     LogicalType::BIGINT);

	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<hugeint_t>, hugeint_t, hugeint_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::HUGEINT,
		                                                                                     LogicalType::HUGEINT);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<float>, float, float,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::FLOAT,
		                                                                                     LogicalType::FLOAT);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<double>, double, double,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::DOUBLE,
		                                                                                     LogicalType::DOUBLE);
	default:
		throw InternalException("Unimplemented reservoir quantile aggregate");
	}
}

template <class CHILD_TYPE>
struct ReservoirQuantileListOperation : public ReservoirQuantileOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.pos == 0) {
			finalize_data.ReturnNull();
			return;
		}

		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->template Cast<ReservoirQuantileBindData>();

		auto &result = ListVector::GetEntry(finalize_data.result);
		auto ridx = ListVector::GetListSize(finalize_data.result);
		ListVector::Reserve(finalize_data.result, ridx + bind_data.quantiles.size());
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		auto v_t = state.v;
		D_ASSERT(v_t);

		auto &entry = target;
		entry.offset = ridx;
		entry.length = bind_data.quantiles.size();
		for (size_t q = 0; q < entry.length; ++q) {
			const auto &quantile = bind_data.quantiles[q];
			auto offset = (idx_t)((double)(state.pos - 1) * quantile);
			std::nth_element(v_t, v_t + offset, v_t + state.pos);
			rdata[ridx + q] = v_t[offset];
		}

		ListVector::SetListSize(finalize_data.result, entry.offset + entry.length);
	}
};

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction ReservoirQuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) {
	LogicalType result_type = LogicalType::LIST(child_type);
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>,
	    nullptr, AggregateFunction::StateDestroy<STATE, OP>);
}

template <typename INPUT_TYPE, typename SAVE_TYPE>
AggregateFunction GetTypedReservoirQuantileListAggregateFunction(const LogicalType &type) {
	using STATE = ReservoirQuantileState<SAVE_TYPE>;
	using OP = ReservoirQuantileListOperation<INPUT_TYPE>;
	auto fun = ReservoirQuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
	return fun;
}

AggregateFunction GetReservoirQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedReservoirQuantileListAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedReservoirQuantileListAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedReservoirQuantileListAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedReservoirQuantileListAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedReservoirQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
	case LogicalTypeId::FLOAT:
		return GetTypedReservoirQuantileListAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedReservoirQuantileListAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedReservoirQuantileListAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedReservoirQuantileListAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedReservoirQuantileListAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedReservoirQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented reservoir quantile list aggregate");
		}
	default:
		// TODO: Add quantitative temporal types
		throw NotImplementedException("Unimplemented reservoir quantile list aggregate");
	}
}

static double CheckReservoirQuantile(const Value &quantile_val) {
	if (quantile_val.IsNull()) {
		throw BinderException("RESERVOIR_QUANTILE QUANTILE parameter cannot be NULL");
	}
	auto quantile = quantile_val.GetValue<double>();
	if (quantile < 0 || quantile > 1) {
		throw BinderException("RESERVOIR_QUANTILE can only take parameters in the range [0, 1]");
	}
	return quantile;
}

unique_ptr<FunctionData> BindReservoirQuantile(ClientContext &context, AggregateFunction &function,
                                               vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(arguments.size() >= 2);
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("RESERVOIR_QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	vector<double> quantiles;
	if (quantile_val.type().id() != LogicalTypeId::LIST) {
		quantiles.push_back(CheckReservoirQuantile(quantile_val));
	} else {
		for (const auto &element_val : ListValue::GetChildren(quantile_val)) {
			quantiles.push_back(CheckReservoirQuantile(element_val));
		}
	}

	if (arguments.size() == 2) {
		// remove the quantile argument so we can use the unary aggregate
		if (function.arguments.size() == 2) {
			Function::EraseArgument(function, arguments, arguments.size() - 1);
		} else {
			arguments.pop_back();
		}
		return make_uniq<ReservoirQuantileBindData>(quantiles, 8192ULL);
	}
	if (!arguments[2]->IsFoldable()) {
		throw BinderException("RESERVOIR_QUANTILE can only take constant sample size parameters");
	}
	Value sample_size_val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
	if (sample_size_val.IsNull()) {
		throw BinderException("Size of the RESERVOIR_QUANTILE sample cannot be NULL");
	}
	auto sample_size = sample_size_val.GetValue<int32_t>();

	if (sample_size_val.IsNull() || sample_size <= 0) {
		throw BinderException("Size of the RESERVOIR_QUANTILE sample must be bigger than 0");
	}

	// remove the quantile arguments so we can use the unary aggregate
	if (function.arguments.size() == arguments.size()) {
		Function::EraseArgument(function, arguments, arguments.size() - 1);
		Function::EraseArgument(function, arguments, arguments.size() - 1);
	} else {
		arguments.pop_back();
		arguments.pop_back();
	}
	return make_uniq<ReservoirQuantileBindData>(quantiles, NumericCast<idx_t>(sample_size));
}

unique_ptr<FunctionData> BindReservoirQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                      vector<unique_ptr<Expression>> &arguments) {
	function = GetReservoirQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	auto bind_data = BindReservoirQuantile(context, function, arguments);
	function.name = "reservoir_quantile";
	function.serialize = ReservoirQuantileBindData::Serialize;
	function.deserialize = ReservoirQuantileBindData::Deserialize;
	return bind_data;
}

AggregateFunction GetReservoirQuantileAggregate(PhysicalType type) {
	auto fun = GetReservoirQuantileAggregateFunction(type);
	fun.bind = BindReservoirQuantile;
	fun.serialize = ReservoirQuantileBindData::Serialize;
	fun.deserialize = ReservoirQuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.emplace_back(LogicalType::DOUBLE);
	return fun;
}

unique_ptr<FunctionData> BindReservoirQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                          vector<unique_ptr<Expression>> &arguments) {
	function = GetReservoirQuantileListAggregateFunction(arguments[0]->return_type);
	auto bind_data = BindReservoirQuantile(context, function, arguments);
	function.serialize = ReservoirQuantileBindData::Serialize;
	function.deserialize = ReservoirQuantileBindData::Deserialize;
	function.name = "reservoir_quantile";
	return bind_data;
}

AggregateFunction GetReservoirQuantileListAggregate(const LogicalType &type) {
	auto fun = GetReservoirQuantileListAggregateFunction(type);
	fun.bind = BindReservoirQuantile;
	fun.serialize = ReservoirQuantileBindData::Serialize;
	fun.deserialize = ReservoirQuantileBindData::Deserialize;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
	fun.arguments.push_back(list_of_double);
	return fun;
}

static void DefineReservoirQuantile(AggregateFunctionSet &set, const LogicalType &type) {
	//	Four versions: type, scalar/list[, count]
	auto fun = GetReservoirQuantileAggregate(type.InternalType());
	set.AddFunction(fun);

	fun.arguments.emplace_back(LogicalType::INTEGER);
	set.AddFunction(fun);

	// List variants
	fun = GetReservoirQuantileListAggregate(type);
	set.AddFunction(fun);

	fun.arguments.emplace_back(LogicalType::INTEGER);
	set.AddFunction(fun);
}

static void GetReservoirQuantileDecimalFunction(AggregateFunctionSet &set, const vector<LogicalType> &arguments,
                                                const LogicalType &return_value) {
	AggregateFunction fun(arguments, return_value, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                      BindReservoirQuantileDecimal);
	fun.serialize = ReservoirQuantileBindData::Serialize;
	fun.deserialize = ReservoirQuantileBindData::Deserialize;
	set.AddFunction(fun);

	fun.arguments.emplace_back(LogicalType::INTEGER);
	set.AddFunction(fun);
}

AggregateFunctionSet ReservoirQuantileFun::GetFunctions() {
	AggregateFunctionSet reservoir_quantile;

	// DECIMAL
	GetReservoirQuantileDecimalFunction(reservoir_quantile, {LogicalTypeId::DECIMAL, LogicalType::DOUBLE},
	                                    LogicalTypeId::DECIMAL);
	GetReservoirQuantileDecimalFunction(reservoir_quantile,
	                                    {LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)},
	                                    LogicalType::LIST(LogicalTypeId::DECIMAL));

	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::TINYINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::SMALLINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::INTEGER);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::BIGINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::HUGEINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::FLOAT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::DOUBLE);
	return reservoir_quantile;
}

} // namespace duckdb
