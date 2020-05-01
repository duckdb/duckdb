#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"

using namespace std;

namespace duckdb {

template <class OP> static AggregateFunction GetBitfieldUnaryAggregate(SQLType type) {
	switch (type.id) {
	case SQLTypeId::TINYINT:
		return AggregateFunction::UnaryAggregate<uint8_t, int8_t, int8_t, OP>(type, type);
	case SQLTypeId::SMALLINT:
		return AggregateFunction::UnaryAggregate<uint16_t, int16_t, int16_t, OP>(type, type);
	case SQLTypeId::INTEGER:
		return AggregateFunction::UnaryAggregate<uint32_t, int32_t, int32_t, OP>(type, type);
	case SQLTypeId::BIGINT:
		return AggregateFunction::UnaryAggregate<uint64_t, int64_t, int64_t, OP>(type, type);
	default:
		throw NotImplementedException("Unimplemented bitfield type for unary aggregate");
	}
}

struct BitAndOperation {
	template <class STATE> static void Initialize(STATE *state) {
		//  If there are no matching rows, BIT_AND() returns a neutral value (all bits set to 1)
		//  having the same length as the argument values.
		*state = 0;
		*state = ~*state;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		*state &= STATE(input[idx]);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		//  count is irrelevant
		Operation<INPUT_TYPE, STATE, OP>(state, input, nullmask, 0);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		target[idx] = T(*state);
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		*target &= source;
	}

	static bool IgnoreNull() {
		return true;
	}
};

void BitAndFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet bit_and("bit_and");
	for (auto type : SQLType::INTEGRAL) {
		bit_and.AddFunction(GetBitfieldUnaryAggregate<BitAndOperation>(type));
	}
	set.AddFunction(bit_and);
}

struct BitOrOperation {
	template <class STATE> static void Initialize(STATE *state) {
		//  If there are no matching rows, BIT_OR() returns a neutral value (all bits set to 0)
		//  having the same length as the argument values.
		*state = 0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		*state |= STATE(input[idx]);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		//  count is irrelevant
		Operation<INPUT_TYPE, STATE, OP>(state, input, nullmask, 0);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		target[idx] = T(*state);
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		*target |= source;
	}

	static bool IgnoreNull() {
		return true;
	}
};

void BitOrFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet bit_or("bit_or");
	for (auto type : SQLType::INTEGRAL) {
		bit_or.AddFunction(GetBitfieldUnaryAggregate<BitOrOperation>(type));
	}
	set.AddFunction(bit_or);
}

struct BitXorOperation {
	template <class STATE> static void Initialize(STATE *state) {
		//  If there are no matching rows, BIT_XOR() returns a neutral value (all bits set to 0)
		//  having the same length as the argument values.
		*state = 0;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t idx) {
		*state ^= STATE(input[idx]);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE *state, INPUT_TYPE *input, nullmask_t &nullmask, idx_t count) {
		//  count is irrelevant
		Operation<INPUT_TYPE, STATE, OP>(state, input, nullmask, 0);
	}

	template <class T, class STATE>
	static void Finalize(Vector &result, STATE *state, T *target, nullmask_t &nullmask, idx_t idx) {
		target[idx] = T(*state);
	}

	template <class STATE, class OP> static void Combine(STATE source, STATE *target) {
		*target ^= source;
	}

	static bool IgnoreNull() {
		return true;
	}
};

void BitXorFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet bit_xor("bit_xor");
	for (auto type : SQLType::INTEGRAL) {
		bit_xor.AddFunction(GetBitfieldUnaryAggregate<BitXorOperation>(type));
	}
	set.AddFunction(bit_xor);
}

} // namespace duckdb
