#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/types/null_value.hpp"

using namespace std;

namespace duckdb {

struct MinMaxBase : public StandardDistributiveFunction {
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		assert(!nullmask[0]);
		if (IsNullValue<INPUT_TYPE>(*state)) {
			*state = input[0];
		} else {
			OP::template Execute<INPUT_TYPE, STATE>(state, input[0]);
		}
	}
};

struct NumericMinMaxBase : public MinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE *state, INPUT_TYPE input) {
		*state = input;
	}
};

struct MinOperation : public NumericMinMaxBase {
	template <class INPUT_TYPE, class STATE> static void Execute(STATE *state, INPUT_TYPE input) {
		if (LessThan::Operation<INPUT_TYPE>(input, *state)) {
			*state = input;
		}
	}
};

struct MaxOperation : public NumericMinMaxBase {
	template <class INPUT_TYPE, class STATE> static void Execute(STATE *state, INPUT_TYPE input) {
		if (GreaterThan::Operation<INPUT_TYPE>(input, *state)) {
			*state = input;
		}
	}
};

struct StringMinMaxBase : public MinMaxBase {
	template <class STATE> static void Destroy(STATE *state) {
		if (!state->IsInlined()) {
			delete[] state->GetData();
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE *state, INPUT_TYPE input) {
		if (input.IsInlined()) {
			*state = input;
		} else {
			// non-inlined string, need to allocate space for it
			auto len = input.GetSize();
			auto ptr = new char[len + 1];
			memcpy(ptr, input.GetData(), len + 1);

			*state = string_t(ptr, len);
		}
	}
};

struct MinOperationString : public StringMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		if (LessThan::Operation<INPUT_TYPE>(input, *state)) {
			Assign(state, input);
		}
	}
};

struct MaxOperationString : public StringMinMaxBase {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE *state, INPUT_TYPE input) {
		if (GreaterThan::Operation<INPUT_TYPE>(input, *state)) {
			Assign(state, input);
		}
	}
};

template<class OP, class OP_STRING>
static void AddMinMaxOperator(AggregateFunctionSet &set) {
	for (auto type : SQLType::ALL_TYPES) {
		if (type.id == SQLTypeId::VARCHAR) {
			set.AddFunction(AggregateFunction::UnaryAggregateDestructor<string_t, string_t, string_t, OP_STRING>(SQLType::VARCHAR, SQLType::VARCHAR));
		} else {
			set.AddFunction(AggregateFunction::GetUnaryAggregate<OP>(type));
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
