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
	static void Initialize(STATE &state) {
		state.is_set = false;
		state.is_null = false;
	}

	static bool IgnoreNull() {
		return false;
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstFunction : public FirstFunctionBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (LAST || !state.is_set) {
			if (!unary_input.RowIsValid()) {
				if (!SKIP_NULLS) {
					state.is_set = true;
				}
				state.is_null = true;
			} else {
				state.is_set = true;
				state.is_null = false;
				state.value = input;
			}
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!target.is_set) {
			target = source;
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || state.is_null) {
			finalize_data.ReturnNull();
		} else {
			target = state.value;
		}
	}
};

template <bool LAST, bool SKIP_NULLS>
struct FirstFunctionString : public FirstFunctionBase {
	template <class STATE>
	static void SetValue(STATE &state, AggregateInputData &input_data, string_t value, bool is_null) {
		if (LAST && state.is_set) {
			Destroy(state, input_data);
		}
		if (is_null) {
			if (!SKIP_NULLS) {
				state.is_set = true;
				state.is_null = true;
			}
		} else {
			state.is_set = true;
			state.is_null = false;
			if (value.IsInlined()) {
				state.value = value;
			} else {
				// non-inlined string, need to allocate space for it
				auto len = value.GetSize();
				auto ptr = new char[len];
				memcpy(ptr, value.GetData(), len);

				state.value = string_t(ptr, len);
			}
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		if (LAST || !state.is_set) {
			SetValue(state, unary_input.input, input, !unary_input.RowIsValid());
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (source.is_set && (LAST || !target.is_set)) {
			SetValue(target, input_data, source.value, source.is_null);
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set || state.is_null) {
			finalize_data.ReturnNull();
		} else {
			target = StringVector::AddStringOrBlob(finalize_data.result, state.value);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.is_set && !state.is_null && !state.value.IsInlined()) {
			delete[] state.value.GetData();
		}
	}
};

struct FirstStateVector {
	Vector *value;
};

template <bool LAST, bool SKIP_NULLS>
struct FirstVectorFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.value = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.value) {
			delete state.value;
		}
	}
	static bool IgnoreNull() {
		return SKIP_NULLS;
	}

	template <class STATE>
	static void SetValue(STATE &state, Vector &input, const idx_t idx) {
		if (!state.value) {
			state.value = new Vector(input.GetType());
			state.value->SetVectorType(VectorType::CONSTANT_VECTOR);
		}
		sel_t selv = idx;
		SelectionVector sel(&selv);
		VectorOperations::Copy(input, *state.value, sel, 1, 0, 0);
	}

	static void Update(Vector inputs[], AggregateInputData &, idx_t input_count, Vector &state_vector, idx_t count) {
		auto &input = inputs[0];
		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);

		UnifiedVectorFormat sdata;
		state_vector.ToUnifiedFormat(count, sdata);

		auto states = UnifiedVectorFormat::GetData<FirstStateVector *>(sdata);
		for (idx_t i = 0; i < count; i++) {
			const auto idx = idata.sel->get_index(i);
			if (SKIP_NULLS && !idata.validity.RowIsValid(idx)) {
				continue;
			}
			auto &state = *states[sdata.sel->get_index(i)];
			if (LAST || !state.value) {
				SetValue(state, input, i);
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.value && (LAST || !target.value)) {
			SetValue(target, *source.value, 0);
		}
	}

	template <class STATE>
	static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
		if (!state.value) {
			finalize_data.ReturnNull();
		} else {
			VectorOperations::Copy(*state.value, finalize_data.result, 1, 0, finalize_data.result_idx);
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
		                         AggregateFunction::StateVoidFinalize<FirstStateVector, OP>, nullptr, OP::Bind,
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
	auto name = std::move(function.name);
	function = GetFirstFunction<LAST, SKIP_NULLS>(decimal_type);
	function.name = std::move(name);
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
