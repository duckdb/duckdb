#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/types/null_value.hpp"

using namespace std;

namespace duckdb {

template <class T> struct min_max_state_t {
	T value;
	bool isset;
};

template <class OP> static AggregateFunction GetUnaryAggregate(LogicalType type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return AggregateFunction::UnaryAggregate<min_max_state_t<int8_t>, int8_t, int8_t, OP>(type, type);
	case LogicalTypeId::TINYINT:
		return AggregateFunction::UnaryAggregate<min_max_state_t<int8_t>, int8_t, int8_t, OP>(type, type);
	case LogicalTypeId::SMALLINT:
		return AggregateFunction::UnaryAggregate<min_max_state_t<int16_t>, int16_t, int16_t, OP>(type, type);
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTEGER:
		return AggregateFunction::UnaryAggregate<min_max_state_t<int32_t>, int32_t, int32_t, OP>(type, type);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::BIGINT:
		return AggregateFunction::UnaryAggregate<min_max_state_t<int64_t>, int64_t, int64_t, OP>(type, type);
	case LogicalTypeId::HUGEINT:
		return AggregateFunction::UnaryAggregate<min_max_state_t<hugeint_t>, hugeint_t, hugeint_t, OP>(type, type);
	case LogicalTypeId::FLOAT:
		return AggregateFunction::UnaryAggregate<min_max_state_t<float>, float, float, OP>(type, type);
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DOUBLE:
		return AggregateFunction::UnaryAggregate<min_max_state_t<double>, double, double, OP>(type, type);
	case LogicalTypeId::INTERVAL:
		return AggregateFunction::UnaryAggregate<min_max_state_t<interval_t>, interval_t, interval_t, OP>(type, type);
	default:
		throw NotImplementedException("Unimplemented type for min/max aggregate");
	}
}

struct MinMaxBase {
	template <class STATE> static void Initialize(STATE *state) {
		state->isset = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		assert(!nullmask[0]);
		if (!state->isset) {
			OP::template Assign<INPUT_TYPE, STATE>(state, input[0]);
			state->isset = true;
		} else {
			OP::template Execute<INPUT_TYPE, STATE>(state, input[0]);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
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
	template <class INPUT_TYPE, class STATE> static void Assign(STATE *state, INPUT_TYPE input) {
		state->value = input;
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		nullmask[idx] = !state->isset;
		target[idx] = state->value;
	}
};

struct MinOperation : public NumericMinMaxBase {
	template <class INPUT_TYPE, class STATE> static void Execute(STATE *state, INPUT_TYPE input) {
		if (LessThan::Operation<INPUT_TYPE>(input, state->value)) {
			state->value = input;
		}
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
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
	template <class INPUT_TYPE, class STATE> static void Execute(STATE *state, INPUT_TYPE input) {
		if (GreaterThan::Operation<INPUT_TYPE>(input, state->value)) {
			state->value = input;
		}
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
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
	template <class STATE> static void Destroy(STATE *state) {
		if (state->isset && !state->value.IsInlined()) {
			delete[] state->value.GetData();
		}
	}

	template <class INPUT_TYPE, class STATE> static void Assign(STATE *state, INPUT_TYPE input) {
		Destroy(state);
		if (input.IsInlined()) {
			state->value = input;
		} else {
			// non-inlined string, need to allocate space for it
			auto len = input.GetSize();
			auto ptr = new char[len + 1];
			memcpy(ptr, input.GetData(), len + 1);

			state->value = string_t(ptr, len);
		}
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->isset) {
			nullmask[idx] = true;
		} else {
			target[idx] = StringVector::AddStringOrBlob(result, state->value);
		}
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		if (!source.isset) {
			// source is NULL, nothing to do
			return;
		}
		if (!target->isset) {
			// target is NULL, use source value directly
			*target = source;
		} else {
			OP::template Execute<string_t, STATE>(target, source.value);
		}
	}
};

struct MinOperationString : public StringMinMaxBase {
	template <class INPUT_TYPE, class STATE> static void Execute(STATE *state, INPUT_TYPE input) {
		if (LessThan::Operation<INPUT_TYPE>(input, state->value)) {
			Assign(state, input);
		}
	}
};

struct MaxOperationString : public StringMinMaxBase {
	template <class INPUT_TYPE, class STATE> static void Execute(STATE *state, INPUT_TYPE input) {
		if (GreaterThan::Operation<INPUT_TYPE>(input, state->value)) {
			Assign(state, input);
		}
	}
};

template <class OP, class OP_STRING> static void AddMinMaxOperator(AggregateFunctionSet &set) {
	for (auto type : LogicalType::ALL_TYPES) {
		if (type.id() == LogicalTypeId::VARCHAR || type.id() == LogicalTypeId::BLOB) {
			set.AddFunction(
			    AggregateFunction::UnaryAggregateDestructor<min_max_state_t<string_t>, string_t, string_t, OP_STRING>(
			        type.id(), type.id()));
		} else {
			set.AddFunction(GetUnaryAggregate<OP>(type));
		}
	}
}

void MinFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet min("min");
	AddMinMaxOperator<MinOperation, MinOperationString>(min);
	set.AddFunction(min);
}

void MaxFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet max("max");
	AddMinMaxOperator<MaxOperation, MaxOperationString>(max);
	set.AddFunction(max);
}

} // namespace duckdb
