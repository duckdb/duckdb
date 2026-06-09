#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/aggregate_operators.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/common/optional.hpp"

namespace duckdb {

namespace {

template <class OP>
AggregateFunction GetBitfieldUnaryAggregate(LogicalType type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return AggregateFunction::UnaryAggregate<optional<uint8_t>, int8_t, int8_t, OP>(type, type);
	case LogicalTypeId::SMALLINT:
		return AggregateFunction::UnaryAggregate<optional<uint16_t>, int16_t, int16_t, OP>(type, type);
	case LogicalTypeId::INTEGER:
		return AggregateFunction::UnaryAggregate<optional<uint32_t>, int32_t, int32_t, OP>(type, type);
	case LogicalTypeId::BIGINT:
		return AggregateFunction::UnaryAggregate<optional<uint64_t>, int64_t, int64_t, OP>(type, type);
	case LogicalTypeId::HUGEINT:
		return AggregateFunction::UnaryAggregate<optional<hugeint_t>, hugeint_t, hugeint_t, OP>(type, type);
	case LogicalTypeId::UTINYINT:
		return AggregateFunction::UnaryAggregate<optional<uint8_t>, uint8_t, uint8_t, OP>(type, type);
	case LogicalTypeId::USMALLINT:
		return AggregateFunction::UnaryAggregate<optional<uint16_t>, uint16_t, uint16_t, OP>(type, type);
	case LogicalTypeId::UINTEGER:
		return AggregateFunction::UnaryAggregate<optional<uint32_t>, uint32_t, uint32_t, OP>(type, type);
	case LogicalTypeId::UBIGINT:
		return AggregateFunction::UnaryAggregate<optional<uint64_t>, uint64_t, uint64_t, OP>(type, type);
	case LogicalTypeId::UHUGEINT:
		return AggregateFunction::UnaryAggregate<optional<uhugeint_t>, uhugeint_t, uhugeint_t, OP>(type, type);
	default:
		throw InternalException("Unimplemented bitfield type for unary aggregate");
	}
}

struct BitwiseOperation {
	// Default Assign: trivial optional assignment. Overridden for string states that need deep copy.
	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input) {
		state = typename STATE::value_type(input);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		if (!state.has_value()) {
			OP::template Assign<INPUT_TYPE>(state, input);
		} else {
			OP::template Execute<INPUT_TYPE>(state, input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		OP::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.has_value()) {
			return;
		}
		if (!target.has_value()) {
			OP::template Assign<typename STATE::value_type>(target, *source);
		} else {
			OP::template Execute<typename STATE::value_type>(target, *source);
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.has_value()) {
			finalize_data.ReturnNull();
		} else {
			target = T(*state);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

template <class OP>
struct NumericBitwiseOperation : public BitwiseOperation, public ClusteredStateCopy {
	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input) {
		if (!local.has_value()) {
			local = typename STATE::value_type(input);
		} else {
			OP::template Execute<INPUT_TYPE>(local, input);
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input, idx_t count) {
		if (count != 0) {
			UpdateClusteredLocal(local, input);
		}
	}
};

template <class OP>
struct SimpleBitwiseOperation : public NumericBitwiseOperation<SimpleBitwiseOperation<OP>> {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		*state = OP::template Operation<typename STATE::value_type>(*state, typename STATE::value_type(input));
	}
};

using BitAndOperation = SimpleBitwiseOperation<BitAnd>;
using BitOrOperation = SimpleBitwiseOperation<BitOr>;

struct BitXorOperation : public NumericBitwiseOperation<BitXorOperation> {
	using NumericBitwiseOperation<BitXorOperation>::UpdateClusteredLocal;

	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		*state = BitXor::template Operation<typename STATE::value_type>(*state, typename STATE::value_type(input));
	}

	template <class INPUT_TYPE, class STATE>
	static void UpdateClusteredLocal(STATE &local, const INPUT_TYPE &input, idx_t count) {
		if ((count & 1) != 0) {
			NumericBitwiseOperation<BitXorOperation>::template UpdateClusteredLocal<INPUT_TYPE>(local, input);
		} else if (count != 0 && !local.has_value()) {
			local = typename STATE::value_type(0);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}
};

using BitStringState = optional<string_t>;

struct BitStringBitwiseOperation : public BitwiseOperation {
	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.has_value() && !state->IsInlined()) {
			delete[] state->GetData();
		}
	}

	// Deep-copy Assign: allocates new backing storage for non-inlined strings.
	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input) {
		if (input.IsInlined()) {
			state = input;
		} else {
			auto len = input.GetSize();
			auto ptr = new char[len];
			memcpy(ptr, input.GetData(), len);
			state = string_t(ptr, UnsafeNumericCast<uint32_t>(len));
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.has_value()) {
			finalize_data.ReturnNull();
		} else {
			target = finalize_data.ReturnString(*state);
		}
	}
};

struct BitStringAndOperation : public BitStringBitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		Bit::BitwiseAnd(input, *state, *state);
	}
};

struct BitStringOrOperation : public BitStringBitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		Bit::BitwiseOr(input, *state, *state);
	}
};

struct BitStringXorOperation : public BitStringBitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		Bit::BitwiseXor(input, *state, *state);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}
};

} // namespace

AggregateFunctionSet BitAndFun::GetFunctions() {
	AggregateFunctionSet bit_and;
	for (auto &type : LogicalType::Integral()) {
		bit_and.AddFunction(GetBitfieldUnaryAggregate<BitAndOperation>(type));
	}
	bit_and.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<BitStringState, string_t, string_t, BitStringAndOperation>(
	        LogicalType::BIT, LogicalType::BIT));
	return bit_and;
}

AggregateFunctionSet BitOrFun::GetFunctions() {
	AggregateFunctionSet bit_or;
	for (auto &type : LogicalType::Integral()) {
		bit_or.AddFunction(GetBitfieldUnaryAggregate<BitOrOperation>(type));
	}
	bit_or.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<BitStringState, string_t, string_t, BitStringOrOperation>(
	        LogicalType::BIT, LogicalType::BIT));
	return bit_or;
}

AggregateFunctionSet BitXorFun::GetFunctions() {
	AggregateFunctionSet bit_xor;
	for (auto &type : LogicalType::Integral()) {
		bit_xor.AddFunction(GetBitfieldUnaryAggregate<BitXorOperation>(type));
	}
	bit_xor.AddFunction(
	    AggregateFunction::UnaryAggregateDestructor<BitStringState, string_t, string_t, BitStringXorOperation>(
	        LogicalType::BIT, LogicalType::BIT));
	return bit_xor;
}

} // namespace duckdb
