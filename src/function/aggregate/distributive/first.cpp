#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

template <class T> struct FirstState {
	bool is_set;
	T value;
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

template <class T> static AggregateFunction GetFirstAggregateTemplated(SQLType type) {
	return AggregateFunction::UnaryAggregate<FirstState<T>, T, T, FirstFunction>(type, type);
}

AggregateFunction FirstFun::GetFunction(SQLType type) {
	switch (type.id) {
	case SQLTypeId::BOOLEAN:
		return GetFirstAggregateTemplated<int8_t>(type);
	case SQLTypeId::TINYINT:
		return GetFirstAggregateTemplated<int8_t>(type);
	case SQLTypeId::SMALLINT:
		return GetFirstAggregateTemplated<int16_t>(type);
	case SQLTypeId::INTEGER:
		return GetFirstAggregateTemplated<int32_t>(type);
	case SQLTypeId::BIGINT:
		return GetFirstAggregateTemplated<int64_t>(type);
	case SQLTypeId::FLOAT:
		return GetFirstAggregateTemplated<float>(type);
	case SQLTypeId::DOUBLE:
		return GetFirstAggregateTemplated<double>(type);
	case SQLTypeId::DECIMAL:
		return GetFirstAggregateTemplated<double>(type);
	case SQLTypeId::DATE:
		return GetFirstAggregateTemplated<date_t>(type);
	case SQLTypeId::TIMESTAMP:
		return GetFirstAggregateTemplated<timestamp_t>(type);
	case SQLTypeId::VARCHAR:
		return AggregateFunction::UnaryAggregateDestructor<FirstState<string_t>, string_t, string_t,
		                                                   FirstFunctionString>(type, type);
	default:
		throw NotImplementedException("Unimplemented type for FIRST aggregate");
	}
}

void FirstFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet first("first");
	for (auto type : SQLType::ALL_TYPES) {
		first.AddFunction(FirstFun::GetFunction(type));
	}
	set.AddFunction(first);
}

} // namespace duckdb
