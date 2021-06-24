#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

template <class T>
struct MinMaxState {
	T value;
	bool isset;
};

template <class OP>
static AggregateFunction GetUnaryAggregate(LogicalType type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return AggregateFunction::UnaryAggregate<MinMaxState<int8_t>, int8_t, int8_t, OP>(type, type);
	case LogicalTypeId::TINYINT:
		return AggregateFunction::UnaryAggregate<MinMaxState<int8_t>, int8_t, int8_t, OP>(type, type);
	case LogicalTypeId::SMALLINT:
		return AggregateFunction::UnaryAggregate<MinMaxState<int16_t>, int16_t, int16_t, OP>(type, type);
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTEGER:
		return AggregateFunction::UnaryAggregate<MinMaxState<int32_t>, int32_t, int32_t, OP>(type, type);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIME:
	case LogicalTypeId::BIGINT:
		return AggregateFunction::UnaryAggregate<MinMaxState<int64_t>, int64_t, int64_t, OP>(type, type);
	case LogicalTypeId::UTINYINT:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint8_t>, uint8_t, uint8_t, OP>(type, type);
	case LogicalTypeId::USMALLINT:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint16_t>, uint16_t, uint16_t, OP>(type, type);
	case LogicalTypeId::UINTEGER:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint32_t>, uint32_t, uint32_t, OP>(type, type);
	case LogicalTypeId::UBIGINT:
		return AggregateFunction::UnaryAggregate<MinMaxState<uint64_t>, uint64_t, uint64_t, OP>(type, type);
	case LogicalTypeId::HUGEINT:
		return AggregateFunction::UnaryAggregate<MinMaxState<hugeint_t>, hugeint_t, hugeint_t, OP>(type, type);
	case LogicalTypeId::FLOAT:
		return AggregateFunction::UnaryAggregate<MinMaxState<float>, float, float, OP>(type, type);
	case LogicalTypeId::DOUBLE:
		return AggregateFunction::UnaryAggregate<MinMaxState<double>, double, double, OP>(type, type);
	case LogicalTypeId::INTERVAL:
		return AggregateFunction::UnaryAggregate<MinMaxState<interval_t>, interval_t, interval_t, OP>(type, type);
	default:
		throw NotImplementedException("Unimplemented type for min/max aggregate");
	}
}

struct MinMaxBase {
	template <class STATE>
	static void Initialize(STATE *state) {
		state->isset = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, ValidityMask &mask,
	                              idx_t count) {
		D_ASSERT(mask.RowIsValid(0));
		if (!state->isset) {
			OP::template Assign<INPUT_TYPE, STATE>(state, input[0]);
			state->isset = true;
		} else {
			OP::template Execute<INPUT_TYPE, STATE>(state, input[0]);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, FunctionData *bind_data, INPUT_TYPE *input, ValidityMask &mask, idx_t idx) {
		if (!state->isset) {
			OP::template Assign<INPUT_TYPE, STATE>(state, input[idx]);
			state->isset = true;
		} else {
			OP::template Execute<INPUT_TYPE, STATE>(state, input[idx]);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct NumericMinMaxBase : public MinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE *state, INPUT_TYPE input) {
		state->value = input;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		mask.Set(idx, state->isset);
		target[idx] = state->value;
	}
};

struct MinOperation : public NumericMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		if (LessThan::Operation<INPUT_TYPE>(input, state->value)) {
			state->value = input;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target->isset) {
			// target is NULL, use source value directly
			*target = source;
		} else if (GreaterThan::Operation(target->value, source.value)) {
			target->value = source.value;
		}
	}
};

struct MaxOperation : public NumericMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		if (GreaterThan::Operation<INPUT_TYPE>(input, state->value)) {
			state->value = input;
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target->isset) {
			// target is NULL, use source value directly
			*target = source;
		} else if (LessThan::Operation(target->value, source.value)) {
			target->value = source.value;
		}
	}
};

struct StringMinMaxBase : public MinMaxBase {
	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->isset && !state->value.IsInlined()) {
			delete[] state->value.GetDataUnsafe();
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE *state, INPUT_TYPE input) {
		Destroy(state);
		if (input.IsInlined()) {
			state->value = input;
		} else {
			// non-inlined string, need to allocate space for it
			auto len = input.GetSize();
			auto ptr = new char[len + 1];
			memcpy(ptr, input.GetDataUnsafe(), len + 1);

			state->value = string_t(ptr, len);
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->isset) {
			mask.SetInvalid(idx);
		} else {
			target[idx] = StringVector::AddStringOrBlob(result, state->value);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target->isset) {
			// target is NULL, use source value directly
			Assign(target, source.value);
			target->isset = true;
		} else {
			OP::template Execute<string_t, STATE>(target, source.value);
		}
	}
};

struct MinOperationString : public StringMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		if (LessThan::Operation<INPUT_TYPE>(input, state->value)) {
			Assign(state, input);
		}
	}
};

struct MaxOperationString : public StringMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		if (GreaterThan::Operation<INPUT_TYPE>(input, state->value)) {
			Assign(state, input);
		}
	}
};

struct ValueMinMaxState {
	Value *value;
};

struct ValueMinMaxBase {
	static bool IgnoreNull() {
		return true;
	}

	template <class STATE>
	static void Initialize(STATE *state) {
		state->value = nullptr;
	}

	template <class STATE>
	static void Destroy(STATE *state) {
		if (state->value) {
			delete state->value;
		}
		state->value = nullptr;
	}

	template <class STATE>
	static void Assign(STATE *state, const Value &input) {
		Destroy(state);
		state->value = new Value(input);
	}

	template <class STATE, typename OP>
	static void Execute(STATE *state, const Value &input) {
		if (OP::template Operation<Value>(input, *state->value)) {
			Assign(state, input);
		}
	}

	template <class STATE, class OP>
	static void Update(Vector inputs[], FunctionData *, idx_t input_count, Vector &state_vector, idx_t count) {
		auto &input = inputs[0];
		VectorData sdata;
		state_vector.Orrify(count, sdata);

		auto states = (STATE **)sdata.data;
		for (idx_t i = 0; i < count; i++) {
			auto val = input.GetValue(i);
			if (val.is_null) {
				continue;
			}
			const auto sidx = sdata.sel->get_index(i);
			auto state = states[sidx];
			if (!state->value) {
				Assign(state, val);
			} else {
				OP::template Execute(state, val);
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE *target) {
		if (!source.value) {
			return;
		} else if (!target->value) {
			Assign(target, *source.value);
		} else {
			OP::template Execute(target, *source.value);
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, FunctionData *, STATE *state, T *target, ValidityMask &mask, idx_t idx) {
		if (!state->value) {
			mask.SetInvalid(idx);
		} else {
			result.SetValue(idx, *state->value);
		}
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, AggregateFunction &function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		function.arguments[0] = arguments[0]->return_type;
		function.return_type = arguments[0]->return_type;
		return nullptr;
	}
};

struct MinOperationValue : public ValueMinMaxBase {
	template <class STATE>
	static void Execute(STATE *state, const Value &input) {
		if (ValueOperations::LessThan(input, *state->value)) {
			Assign(state, input);
		}
	}
};

struct MaxOperationValue : public ValueMinMaxBase {
	template <class STATE>
	static void Execute(STATE *state, const Value &input) {
		if (ValueOperations::GreaterThan(input, *state->value)) {
			Assign(state, input);
		}
	}
};

template <class OP>
unique_ptr<FunctionData> BindDecimalMinMax(ClientContext &context, AggregateFunction &function,
                                           vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	switch (decimal_type.InternalType()) {
	case PhysicalType::INT16:
		function = GetUnaryAggregate<OP>(LogicalType::SMALLINT);
		break;
	case PhysicalType::INT32:
		function = GetUnaryAggregate<OP>(LogicalType::INTEGER);
		break;
	case PhysicalType::INT64:
		function = GetUnaryAggregate<OP>(LogicalType::BIGINT);
		break;
	default:
		function = GetUnaryAggregate<OP>(LogicalType::HUGEINT);
		break;
	}
	function.arguments[0] = decimal_type;
	function.return_type = decimal_type;
	return nullptr;
}

template <class OP, class OP_STRING>
static void AddMinMaxOperator(AggregateFunctionSet &set) {
	for (auto &type : LogicalType::ALL_TYPES) {
		if (type.id() == LogicalTypeId::VARCHAR || type.id() == LogicalTypeId::BLOB) {
			set.AddFunction(
			    AggregateFunction::UnaryAggregateDestructor<MinMaxState<string_t>, string_t, string_t, OP_STRING>(
			        type.id(), type.id()));
		} else if (type.id() == LogicalTypeId::DECIMAL) {
			set.AddFunction(AggregateFunction({type}, type, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
			                                  BindDecimalMinMax<OP>));
		} else {
			set.AddFunction(GetUnaryAggregate<OP>(type));
		}
	}
}

template <typename OP, typename STATE>
static AggregateFunction GetMinMaxFunction(const LogicalType &type) {
	return AggregateFunction({type}, type, AggregateFunction::StateSize<STATE>,
	                         AggregateFunction::StateInitialize<STATE, OP>, OP::template Update<STATE, OP>,
	                         AggregateFunction::StateCombine<STATE, OP>,
	                         AggregateFunction::StateFinalize<STATE, void, OP>, nullptr, OP::Bind,
	                         AggregateFunction::StateDestroy<STATE, OP>);
}

void MinFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet min("min");
	AddMinMaxOperator<MinOperation, MinOperationString>(min);
	min.AddFunction(GetMinMaxFunction<MinOperationValue, ValueMinMaxState>(LogicalTypeId::LIST));
	min.AddFunction(GetMinMaxFunction<MinOperationValue, ValueMinMaxState>(LogicalTypeId::MAP));
	min.AddFunction(GetMinMaxFunction<MinOperationValue, ValueMinMaxState>(LogicalTypeId::STRUCT));
	set.AddFunction(min);
}

void MaxFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet max("max");
	AddMinMaxOperator<MaxOperation, MaxOperationString>(max);
	max.AddFunction(GetMinMaxFunction<MaxOperationValue, ValueMinMaxState>(LogicalTypeId::LIST));
	max.AddFunction(GetMinMaxFunction<MaxOperationValue, ValueMinMaxState>(LogicalTypeId::MAP));
	max.AddFunction(GetMinMaxFunction<MaxOperationValue, ValueMinMaxState>(LogicalTypeId::STRUCT));
	set.AddFunction(max);
}

} // namespace duckdb
