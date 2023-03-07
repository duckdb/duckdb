#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

template <class T>
struct FirstState {
	T value;
	bool is_set;
	bool is_null;
};

struct FirstFunctionBase {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->is_set = false;
		state->is_null = false;
	}

	static bool IgnoreNull() {
		return false;
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstFunction : public FirstFunctionBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		if (LAST || !state->is_set) {
			if (!mask.RowIsValid(idx)) {
				if (!SKIP_NULLS) {
					state->is_set = true;
				}
				state->is_null = true;
			} else {
				state->is_set = true;
				state->is_null = false;
				state->value = input[idx];
			}
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (!target->is_set) {
			*target = source;
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->is_set || state->is_null) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = state->value;
		}
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstFunctionString : public FirstFunctionBase {
	template <class STATE>
	static void SetValue(STATE *state, string_t value, bool is_null) {
		if (LAST && state->is_set) {
			Destroy(state);
		}
		if (is_null) {
			if (!SKIP_NULLS) {
				state->is_set = true;
				state->is_null = true;
			}
		} else {
			state->is_set = true;
			if (value.IsInlined()) {
				state->value = value;
			} else {
				// non-inlined string, need to allocate space for it
				auto len = value.GetSize();
				auto ptr = new char[len];
				memcpy(ptr, value.GetDataUnsafe(), len);

				state->value = string_t(ptr, len);
			}
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, AggregateInputData &, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		if (LAST || !state->is_set) {
			SetValue(state, input[idx], !mask.RowIsValid(idx));
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, AggregateInputData &aggr_input_data, INPUT_TYPE *input,
	                              ValidityMask &mask, idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, aggr_input_data, input, mask, 0);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (source.is_set && (LAST || !target->is_set)) {
			SetValue(target, source.value, source.is_null);
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->is_set || state->is_null) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = StringVector::AddStringOrBlob(result, state->value);
		}
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->is_set && !state->is_null && !state->value.IsInlined()) {
			delete[] state->value.GetDataUnsafe();
		}
	}
};

struct FirstStateVector {
	Vector *value;
};

template <bool LAST, bool SKIP_NULLS>
struct FirstVectorFunction {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->value = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->value) {
			delete state->value;
		}
	}
	static bool IgnoreNull() {
		return SKIP_NULLS;
	}

	template <class STATE>
	static void SetValue(STATE *state, Vector &input, const idx_t idx) {
		if (!state->value) {
			state->value = new Vector(input.GetType());
			state->value->SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		sel_t selv = idx;
		SelectionVector sel(&selv);
		VectorOperations::Copy(input, *state->value, sel, 1, 0, 0);
	}

	static void Update(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector, idx_t count) {
		auto &input = inputs[0];
		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);

		auto states = (FirstStateVector **)sdata.data;
		for (idx_t i = 0; i < count; i++) {
			const auto idx = idata.sel->get_index(i);
			if (SKIP_NULLS && !idata.validity.RowIsValid(idx)) {
				continue;
			}
			auto state = states[sdata.sel->get_index(i)];
			if (LAST || !state->value) {
				SetValue(state, input, i);
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target, AggregateInputData &) {
		if (source.value && (LAST || !target->value)) {
			SetValue(target, *source.value, 0);
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, AggregateInputData &, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->value) {
			// we need to use FlatVector::SetNull here
			// since for STRUCT columns only setting the validity mask of the struct is incorrect
			// as for a struct column, we need to also set ALL child columns to NULL
			if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
				ConstantVector::SetNull(result, true);
			} else {
				FlatVector::SetNull(result, idx, true);
			}
		} else {
			VectorOperations::Copy(*state->value, result, 1, 0, idx);
		}
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		function.arguments[0] = arguments[0]->return_type;
		function.return_type = arguments[0]->return_type;
		return nullptr;
	}
};

template <class T, bool LAST, bool SKIP_NULLS>
static AggregateFunction GetFirstAggregateTemplated(LogicalType type) {
	return AggregateFunction::UnaryAggregate<FirstState<T>, T, T, FirstFunction<LAST, SKIP_NULLS>>(type, type);
}

template <bool LAST, bool SKIP_NULLS>
static AggregateFunction GetFirstFunction(const LogicalType &type);

template <bool LAST, bool SKIP_NULLS>
AggregateFunction GetDecimalFirstFunction(const LogicalType &type) {
	D_ASSERT(type.id() == LogicalTypeId::DECIMAL);
	switch (type.InternalType()) {
	case PhysicalType::INT16:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::SMALLINT);
	case PhysicalType::INT32:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::INTEGER);
	case PhysicalType::INT64:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::BIGINT);
	default:
		return GetFirstFunction<LAST, SKIP_NULLS>(LogicalType::HUGEINT);
	}
}

template <bool LAST, bool SKIP_NULLS>
static AggregateFunction GetFirstFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return GetFirstAggregateTemplated<int8_t, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::TINYINT:
		return GetFirstAggregateTemplated<int8_t, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::SMALLINT:
		return GetFirstAggregateTemplated<int16_t, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return GetFirstAggregateTemplated<int32_t, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
		return GetFirstAggregateTemplated<int64_t, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::UTINYINT:
		return GetFirstAggregateTemplated<uint8_t, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::USMALLINT:
		return GetFirstAggregateTemplated<uint16_t, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::UINTEGER:
		return GetFirstAggregateTemplated<uint32_t, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::UBIGINT:
		return GetFirstAggregateTemplated<uint64_t, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::HUGEINT:
		return GetFirstAggregateTemplated<hugeint_t, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::FLOAT:
		return GetFirstAggregateTemplated<float, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::DOUBLE:
		return GetFirstAggregateTemplated<double, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::INTERVAL:
		return GetFirstAggregateTemplated<interval_t, LAST, SKIP_NULLS>(type);
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return AggregateFunction::UnaryAggregateDestructor<FirstState<string_t>, string_t, string_t,
		                                                   FirstFunctionString<LAST, SKIP_NULLS>>(type, type);
	case LogicalTypeId::DECIMAL: {
		type.Verify();
		AggregateFunction function = GetDecimalFirstFunction<LAST, SKIP_NULLS>(type);
		function.arguments[0] = type;
		function.return_type = type;
		// TODO set_key here?
		return function;
	}
	default: {
		using OP = FirstVectorFunction<LAST, SKIP_NULLS>;
		return AggregateFunction({type}, type, AggregateFunction::StateSize<FirstStateVector>,
		                         AggregateFunction::StateInitialize<FirstStateVector, OP>, OP::Update,
		                         AggregateFunction::StateCombine<FirstStateVector, OP>,
		                         AggregateFunction::StateFinalize<FirstStateVector, void, OP>, nullptr, OP::Bind,
		                         AggregateFunction::StateDestroy<FirstStateVector, OP>, nullptr, nullptr);
	}
	}
}

AggregateFunction FirstFun::GetFunction(const LogicalType &type) {
	auto fun = GetFirstFunction<false, false>(type);
	fun.name = "first";
	return fun;
}

template <bool LAST, bool SKIP_NULLS>
unique_ptr<FunctionData> BindDecimalFirst(ClientContext &context, AggregateFunction &function,
                                          vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = GetFirstFunction<LAST, SKIP_NULLS>(decimal_type);
	function.name = "first";
	function.return_type = decimal_type;
	return nullptr;
}

template <bool LAST, bool SKIP_NULLS>
static AggregateFunction GetFirstOperator(const LogicalType &type) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		throw InternalException("FIXME: this shouldn't happen...");
	}
	return GetFirstFunction<LAST, SKIP_NULLS>(type);
}

template <bool LAST, bool SKIP_NULLS>
unique_ptr<FunctionData> BindFirst(ClientContext &context, AggregateFunction &function,
                                   vector<unique_ptr<Expression>> &arguments) {
	auto input_type = arguments[0]->return_type;
	auto name = std::move(function.name);
	function = GetFirstOperator<LAST, SKIP_NULLS>(input_type);
	function.name = std::move(name);
	if (function.bind) {
		return function.bind(context, function, arguments);
	} else {
		return nullptr;
	}
}

template <bool LAST, bool SKIP_NULLS>
static void AddFirstOperator(AggregateFunctionSet &set) {
	set.AddFunction(AggregateFunction({LogicalTypeId::DECIMAL}, LogicalTypeId::DECIMAL, nullptr, nullptr, nullptr,
	                                  nullptr, nullptr, nullptr, BindDecimalFirst<LAST, SKIP_NULLS>));
	set.AddFunction(AggregateFunction({LogicalType::ANY}, LogicalType::ANY, nullptr, nullptr, nullptr, nullptr, nullptr,
	                                  nullptr, BindFirst<LAST, SKIP_NULLS>));
}

void FirstFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet first("first");
	AggregateFunctionSet last("last");
	AggregateFunctionSet any_value("any_value");

	AddFirstOperator<false, false>(first);
	AddFirstOperator<true, false>(last);
	AddFirstOperator<false, true>(any_value);

	set.AddFunction(first);
	first.name = "arbitrary";
	set.AddFunction(first);

	set.AddFunction(last);

	set.AddFunction(any_value);
}

} // namespace duckdb
