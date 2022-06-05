#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/holistic_functions.hpp"
#include "t_digest.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

#include <algorithm>
#include <cmath>
#include <stdlib.h>

namespace duckdb {

struct ApproxQuantileState {
	duckdb_tdigest::TDigest *h;
	idx_t pos;
};

struct ApproximateQuantileBindData : public FunctionData {
	explicit ApproximateQuantileBindData(float quantile_p) : quantiles(1, quantile_p) {
	}

	explicit ApproximateQuantileBindData(const vector<float> &quantiles_p) : quantiles(quantiles_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_unique<ApproximateQuantileBindData>(quantiles);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = (ApproximateQuantileBindData &)other_p;
		return quantiles == other.quantiles;
	}

	vector<float> quantiles;
};

struct ApproxQuantileOperation {
	using SAVE_TYPE = duckdb_tdigest::Value;

	template <class STATE>
	static void Initialize(STATE *state) {
		state->pos = 0;
		state->h = nullptr;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, ValidityMask &mask,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, bind_data, input, mask, 0);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, INPUT_TYPE *data, ValidityMask &mask, idx_t idx) {
		if (!state->h) {
			state->h = new duckdb_tdigest::TDigest(100);
		}

		state->h->add(Cast::template Operation<INPUT_TYPE, SAVE_TYPE>(data[idx]));
		state->pos++;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, FunctionData *bind_data) {
		if (source.pos == 0) {
			return;
		}
		D_ASSERT(source.h);
		if (!target->h) {
			target->h = new duckdb_tdigest::TDigest(100);
		}
		target->h->merge(source.h);
		target->pos += source.pos;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->h) {
			delete state->h;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct ApproxQuantileScalarOperation : public ApproxQuantileOperation {

	template <class TARGET_TYPE, class STATE>
	static void Finalize(Vector &result, FunctionData *bind_data_p, STATE *state, TARGET_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {

		if (state->pos == 0) {
			mask.SetInvalid(idx);
			return;
		}
		D_ASSERT(state->h);
		D_ASSERT(bind_data_p);
		state->h->compress();
		auto bind_data = (ApproximateQuantileBindData *)bind_data_p;
		D_ASSERT(bind_data->quantiles.size() == 1);
		target[idx] = Cast::template Operation<SAVE_TYPE, TARGET_TYPE>(state->h->quantile(bind_data->quantiles[0]));
	}
};

AggregateFunction GetApproximateQuantileAggregateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, int16_t, int16_t,
		                                                   ApproxQuantileScalarOperation>(LogicalType::SMALLINT,
		                                                                                  LogicalType::SMALLINT);

	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, int32_t, int32_t,
		                                                   ApproxQuantileScalarOperation>(LogicalType::INTEGER,
		                                                                                  LogicalType::INTEGER);

	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, int64_t, int64_t,
		                                                   ApproxQuantileScalarOperation>(LogicalType::BIGINT,
		                                                                                  LogicalType::BIGINT);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<ApproxQuantileState, double, double,
		                                                   ApproxQuantileScalarOperation>(LogicalType::DOUBLE,
		                                                                                  LogicalType::DOUBLE);

	default:
		throw InternalException("Unimplemented quantile aggregate");
	}
}

static float CheckApproxQuantile(const Value &quantile_val) {
	auto quantile = quantile_val.GetValue<float>();

	if (quantile_val.IsNull() || quantile < 0 || quantile > 1) {
		throw BinderException("APPROXIMATE QUANTILE can only take parameters in range [0, 1]");
	}

	return quantile;
}

unique_ptr<FunctionData> BindApproxQuantile(ClientContext &context, AggregateFunction &function,
                                            vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[1]->IsScalar()) {
		throw BinderException("APPROXIMATE QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);

	vector<float> quantiles;
	if (quantile_val.type().id() != LogicalTypeId::LIST) {
		quantiles.push_back(CheckApproxQuantile(quantile_val));
	} else {
		for (const auto &element_val : ListValue::GetChildren(quantile_val)) {
			quantiles.push_back(CheckApproxQuantile(element_val));
		}
	}

	// remove the quantile argument so we can use the unary aggregate
	arguments.pop_back();
	return make_unique<ApproximateQuantileBindData>(quantiles);
}

unique_ptr<FunctionData> BindApproxQuantileDecimal(ClientContext &context, AggregateFunction &function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindApproxQuantile(context, function, arguments);
	function = GetApproximateQuantileAggregateFunction(arguments[0]->return_type.InternalType());
	function.name = "approx_quantile";
	return bind_data;
}

AggregateFunction GetApproximateQuantileAggregate(PhysicalType type) {
	auto fun = GetApproximateQuantileAggregateFunction(type);
	fun.bind = BindApproxQuantile;
	// temporarily push an argument so we can bind the actual quantile
	fun.arguments.emplace_back(LogicalType::FLOAT);
	return fun;
}

template <class CHILD_TYPE>
struct ApproxQuantileListOperation : public ApproxQuantileOperation {

	template <class RESULT_TYPE, class STATE>
	static void Finalize(Vector &result_list, FunctionData *bind_data_p, STATE *state, RESULT_TYPE *target,
	                     ValidityMask &mask, idx_t idx) {
		if (state->pos == 0) {
			mask.SetInvalid(idx);
			return;
		}

		D_ASSERT(bind_data_p);
		auto bind_data = (ApproximateQuantileBindData *)bind_data_p;

		auto &result = ListVector::GetEntry(result_list);
		auto ridx = ListVector::GetListSize(result_list);
		ListVector::Reserve(result_list, ridx + bind_data->quantiles.size());
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		D_ASSERT(state->h);
		state->h->compress();

		auto &entry = target[idx];
		entry.offset = ridx;
		entry.length = bind_data->quantiles.size();
		for (size_t q = 0; q < entry.length; ++q) {
			const auto &quantile = bind_data->quantiles[q];
			rdata[ridx + q] = Cast::template Operation<SAVE_TYPE, CHILD_TYPE>(state->h->quantile(quantile));
		}

		ListVector::SetListSize(result_list, entry.offset + entry.length);
	}

	template <class STATE_TYPE, class RESULT_TYPE>
	static void FinalizeList(Vector &states, FunctionData *bind_data_p, Vector &result, idx_t count, // NOLINT
	                         idx_t offset) {
		D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

		D_ASSERT(bind_data_p);
		auto bind_data = (ApproximateQuantileBindData *)bind_data_p;

		if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ListVector::Reserve(result, bind_data->quantiles.size());

			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
			auto &mask = ConstantVector::Validity(result);
			Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, sdata[0], rdata, mask, 0);
		} else {
			D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
			result.SetVectorType(VectorType::FLAT_VECTOR);
			ListVector::Reserve(result, (offset + count) * bind_data->quantiles.size());

			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			auto rdata = FlatVector::GetData<RESULT_TYPE>(result);
			auto &mask = FlatVector::Validity(result);
			for (idx_t i = 0; i < count; i++) {
				Finalize<RESULT_TYPE, STATE_TYPE>(result, bind_data, sdata[i], rdata, mask, i + offset);
			}
		}

		result.Verify(count);
	}
};

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
static AggregateFunction ApproxQuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) {
	LogicalType result_type = LogicalType::LIST(child_type);
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    OP::template FinalizeList<STATE, RESULT_TYPE>, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>, nullptr,
	    AggregateFunction::StateDestroy<STATE, OP>);
}

template <typename INPUT_TYPE, typename SAVE_TYPE>
AggregateFunction GetTypedApproxQuantileListAggregateFunction(const LogicalType &type) {
	using STATE = ApproxQuantileState;
	using OP = ApproxQuantileListOperation<INPUT_TYPE>;
	auto fun = ApproxQuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
	return fun;
}

AggregateFunction GetApproxQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedApproxQuantileListAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedApproxQuantileListAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedApproxQuantileListAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedApproxQuantileListAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedApproxQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);

	case LogicalTypeId::FLOAT:
		return GetTypedApproxQuantileListAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedApproxQuantileListAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedApproxQuantileListAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedApproxQuantileListAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedApproxQuantileListAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedApproxQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented approximate quantile list aggregate");
		}
		break;

	default:
		// TODO: Add quantitative temporal types
		throw NotImplementedException("Unimplemented approximate quantile list aggregate");
	}
}

unique_ptr<FunctionData> BindApproxQuantileDecimalList(ClientContext &context, AggregateFunction &function,
                                                       vector<unique_ptr<Expression>> &arguments) {
	auto bind_data = BindApproxQuantile(context, function, arguments);
	function = GetApproxQuantileListAggregateFunction(arguments[0]->return_type);
	function.name = "approx_quantile";
	return bind_data;
}

AggregateFunction GetApproxQuantileListAggregate(const LogicalType &type) {
	auto fun = GetApproxQuantileListAggregateFunction(type);
	fun.bind = BindApproxQuantile;
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_float = LogicalType::LIST(LogicalType::FLOAT);
	fun.arguments.push_back(list_of_float);
	return fun;
}

void ApproximateQuantileFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet approx_quantile("approx_quantile");
	approx_quantile.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::FLOAT}, LogicalTypeId::DECIMAL,
	                                              nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                              BindApproxQuantileDecimal));

	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT16));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT32));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::INT64));
	approx_quantile.AddFunction(GetApproximateQuantileAggregate(PhysicalType::DOUBLE));

	// List variants
	approx_quantile.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::FLOAT)},
	                                              LogicalType::LIST(LogicalTypeId::DECIMAL), nullptr, nullptr, nullptr,
	                                              nullptr, nullptr, nullptr, BindApproxQuantileDecimalList));

	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::TINYINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::SMALLINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::INTEGER));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::BIGINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::HUGEINT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::FLOAT));
	approx_quantile.AddFunction(GetApproxQuantileListAggregate(LogicalTypeId::DOUBLE));

	set.AddFunction(approx_quantile);
}

} // namespace duckdb
