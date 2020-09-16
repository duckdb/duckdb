#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression.hpp"

using namespace std;

namespace duckdb {

template <class T> struct FirstState {
	T value;
	bool is_set;
};

struct FirstFunctionBase {
	template <class STATE> static void Initialize(STATE *state) {
		state->is_set = false;
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		if (!target->is_set) {
			*target = source;
		}
	}

	static bool IgnoreNull() {
		return false;
	}
};

struct FirstFunction : public FirstFunctionBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		if (!state->is_set) {
			state->is_set = true;
			if (nullmask[idx]) {
				state->value = NullValue<INPUT_TYPE>();
			} else {
				state->value = input[idx];
			}
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, nullmask, 0);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->is_set || IsNullValue<T>(state->value)) {
			nullmask[idx] = true;
		} else {
			target[idx] = state->value;
		}
	}
};

struct FirstFunctionString : public FirstFunctionBase {
	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		if (!state->is_set) {
			state->is_set = true;
			if (nullmask[idx]) {
				state->value = NullValue<INPUT_TYPE>();
			} else {
				if (input[idx].IsInlined()) {
					state->value = input[idx];
				} else {
					// non-inlined string, need to allocate space for it
					auto len = input[idx].GetSize();
					auto ptr = new char[len + 1];
					memcpy(ptr, input[idx].GetData(), len + 1);

					state->value = string_t(ptr, len);
				}
			}
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		Operation<INPUT_TYPE, STATE, OP>(state, input, nullmask, 0);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		if (!state->is_set || IsNullValue<T>(state->value)) {
			nullmask[idx] = true;
		} else {
			target[idx] = StringVector::AddString(result, state->value);
		}
	}

	template <class STATE> static void Destroy(STATE *state) {
		if (state->is_set && !state->value.IsInlined()) {
			delete[] state->value.GetData();
		}
	}
};

template <class T> static AggregateFunction GetFirstAggregateTemplated(LogicalType type) {
	return AggregateFunction::UnaryAggregate<FirstState<T>, T, T, FirstFunction>(type, type);
}

AggregateFunction GetDecimalFirstFunction(LogicalType type) {
	assert(type.id() == LogicalTypeId::DECIMAL);
	switch (type.InternalType()) {
	case PhysicalType::INT16:
		return FirstFun::GetFunction(LogicalType::SMALLINT);
	case PhysicalType::INT32:
		return FirstFun::GetFunction(LogicalType::INTEGER);
	case PhysicalType::INT64:
		return FirstFun::GetFunction(LogicalType::BIGINT);
	default:
		return FirstFun::GetFunction(LogicalType::HUGEINT);
	}
}

AggregateFunction FirstFun::GetFunction(LogicalType type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return GetFirstAggregateTemplated<int8_t>(type);
	case LogicalTypeId::TINYINT:
		return GetFirstAggregateTemplated<int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetFirstAggregateTemplated<int16_t>(type);
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
		return GetFirstAggregateTemplated<int32_t>(type);
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIMESTAMP:
		return GetFirstAggregateTemplated<int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetFirstAggregateTemplated<hugeint_t>(type);
	case LogicalTypeId::FLOAT:
		return GetFirstAggregateTemplated<float>(type);
	case LogicalTypeId::DOUBLE:
		return GetFirstAggregateTemplated<double>(type);
	case LogicalTypeId::INTERVAL:
		return GetFirstAggregateTemplated<interval_t>(type);
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return AggregateFunction::UnaryAggregateDestructor<FirstState<string_t>, string_t, string_t,
		                                                   FirstFunctionString>(type, type);
	case LogicalTypeId::DECIMAL: {
		type.Verify();
		AggregateFunction function = GetDecimalFirstFunction(type);
		function.arguments[0] = type;
		function.return_type = type;
		return function;
	}
	default:
		throw NotImplementedException("Unimplemented type for FIRST aggregate");
	}
}

unique_ptr<FunctionData> bind_decimal_first(ClientContext &context, AggregateFunction &function,
                                            vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	function = FirstFun::GetFunction(decimal_type);
	return nullptr;
}

void FirstFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet first("first");
	for (auto type : LogicalType::ALL_TYPES) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			first.AddFunction(AggregateFunction({type}, type, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
			                                    bind_decimal_first));
		} else {
			first.AddFunction(FirstFun::GetFunction(type));
		}
	}
	set.AddFunction(first);
}

} // namespace duckdb
