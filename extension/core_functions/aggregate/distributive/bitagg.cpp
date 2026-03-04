#include "core_functions/aggregate/distributive_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/cast_helpers.hpp"

namespace duckdb {

namespace {

template <class T>
struct BitState {
	using TYPE = T;
	bool is_set;
	T value;
};

template <class T>
LogicalType GetBitStateType(const AggregateFunction &function) {
	child_list_t<LogicalType> child_types;
	child_types.emplace_back("is_set", LogicalType::BOOLEAN);

	LogicalType value_type = function.return_type;
	child_types.emplace_back("value", value_type);

	return LogicalType::STRUCT(std::move(child_types));
}

LogicalType GetBitStringStateType(const AggregateFunction &function) {
	child_list_t<LogicalType> child_types;
	child_types.emplace_back("is_set", LogicalType::BOOLEAN);
	child_types.emplace_back("value", function.return_type);
	return LogicalType::STRUCT(std::move(child_types));
}

template <class OP>
AggregateFunction GetBitfieldUnaryAggregate(LogicalType type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return AggregateFunction::UnaryAggregate<BitState<uint8_t>, int8_t, int8_t, OP>(type, type)
		    .SetStructStateExport(GetBitStateType<uint8_t>);
	case LogicalTypeId::SMALLINT:
		return AggregateFunction::UnaryAggregate<BitState<uint16_t>, int16_t, int16_t, OP>(type, type)
		    .SetStructStateExport(GetBitStateType<uint16_t>);
	case LogicalTypeId::INTEGER:
		return AggregateFunction::UnaryAggregate<BitState<uint32_t>, int32_t, int32_t, OP>(type, type)
		    .SetStructStateExport(GetBitStateType<uint32_t>);
	case LogicalTypeId::BIGINT:
		return AggregateFunction::UnaryAggregate<BitState<uint64_t>, int64_t, int64_t, OP>(type, type)
		    .SetStructStateExport(GetBitStateType<uint64_t>);
	case LogicalTypeId::HUGEINT:
		return AggregateFunction::UnaryAggregate<BitState<hugeint_t>, hugeint_t, hugeint_t, OP>(type, type)
		    .SetStructStateExport(GetBitStateType<hugeint_t>);
	case LogicalTypeId::UTINYINT:
		return AggregateFunction::UnaryAggregate<BitState<uint8_t>, uint8_t, uint8_t, OP>(type, type)
		    .SetStructStateExport(GetBitStateType<uint8_t>);
	case LogicalTypeId::USMALLINT:
		return AggregateFunction::UnaryAggregate<BitState<uint16_t>, uint16_t, uint16_t, OP>(type, type)
		    .SetStructStateExport(GetBitStateType<uint16_t>);
	case LogicalTypeId::UINTEGER:
		return AggregateFunction::UnaryAggregate<BitState<uint32_t>, uint32_t, uint32_t, OP>(type, type)
		    .SetStructStateExport(GetBitStateType<uint32_t>);
	case LogicalTypeId::UBIGINT:
		return AggregateFunction::UnaryAggregate<BitState<uint64_t>, uint64_t, uint64_t, OP>(type, type)
		    .SetStructStateExport(GetBitStateType<uint64_t>);
	case LogicalTypeId::UHUGEINT:
		return AggregateFunction::UnaryAggregate<BitState<uhugeint_t>, uhugeint_t, uhugeint_t, OP>(type, type)
		    .SetStructStateExport(GetBitStateType<uhugeint_t>);
	default:
		throw InternalException("Unimplemented bitfield type for unary aggregate");
	}
}

struct BitwiseOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		//  If there are no matching rows, returns a null value.
		state.is_set = false;
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &) {
		if (!state.is_set) {
			OP::template Assign<INPUT_TYPE>(state, input);
			state.is_set = true;
		} else {
			OP::template Execute<INPUT_TYPE>(state, input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		OP::template Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input) {
		state.value = typename STATE::TYPE(input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (!source.is_set) {
			// source is NULL, nothing to do.
			return;
		}
		if (!target.is_set) {
			// target is NULL, use source value directly.
			OP::template Assign<typename STATE::TYPE>(target, source.value);
			target.is_set = true;
		} else {
			OP::template Execute<typename STATE::TYPE>(target, source.value);
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = T(state.value);
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct BitAndOperation : public BitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		state.value &= typename STATE::TYPE(input);
		;
	}
};

struct BitOrOperation : public BitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		state.value |= typename STATE::TYPE(input);
		;
	}
};

struct BitXorOperation : public BitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		state.value ^= typename STATE::TYPE(input);
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}
};

struct BitStringBitwiseOperation : public BitwiseOperation {
	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.is_set && !state.value.IsInlined()) {
			delete[] state.value.GetData();
		}
	}

	template <class INPUT_TYPE, class STATE>
	static void Assign(STATE &state, INPUT_TYPE input) {
		D_ASSERT(state.is_set == false);
		if (input.IsInlined()) {
			state.value = input;
		} else { // non-inlined string, need to allocate space for it
			auto len = input.GetSize();
			auto ptr = new char[len];
			memcpy(ptr, input.GetData(), len);

			state.value = string_t(ptr, UnsafeNumericCast<uint32_t>(len));
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (!state.is_set) {
			finalize_data.ReturnNull();
		} else {
			target = finalize_data.ReturnString(state.value);
		}
	}
};

struct BitStringAndOperation : public BitStringBitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		Bit::BitwiseAnd(input, state.value, state.value);
	}
};

struct BitStringOrOperation : public BitStringBitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		Bit::BitwiseOr(input, state.value, state.value);
	}
};

struct BitStringXorOperation : public BitStringBitwiseOperation {
	template <class INPUT_TYPE, class STATE>
	static void Execute(STATE &state, INPUT_TYPE input) {
		Bit::BitwiseXor(input, state.value, state.value);
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

	auto bit_string_fun =
	    AggregateFunction::UnaryAggregateDestructor<BitState<string_t>, string_t, string_t, BitStringAndOperation>(
	        LogicalType::BIT, LogicalType::BIT);
	bit_string_fun.SetStructStateExport(GetBitStringStateType);
	bit_and.AddFunction(bit_string_fun);
	return bit_and;
}

AggregateFunctionSet BitOrFun::GetFunctions() {
	AggregateFunctionSet bit_or;
	for (auto &type : LogicalType::Integral()) {
		bit_or.AddFunction(GetBitfieldUnaryAggregate<BitOrOperation>(type));
	}
	auto bit_string_fun =
	    AggregateFunction::UnaryAggregateDestructor<BitState<string_t>, string_t, string_t, BitStringOrOperation>(
	        LogicalType::BIT, LogicalType::BIT);
	bit_string_fun.SetStructStateExport(GetBitStringStateType);
	bit_or.AddFunction(bit_string_fun);
	return bit_or;
}

AggregateFunctionSet BitXorFun::GetFunctions() {
	AggregateFunctionSet bit_xor;
	for (auto &type : LogicalType::Integral()) {
		bit_xor.AddFunction(GetBitfieldUnaryAggregate<BitXorOperation>(type));
	}
	auto bit_string_fun =
	    AggregateFunction::UnaryAggregateDestructor<BitState<string_t>, string_t, string_t, BitStringXorOperation>(
	        LogicalType::BIT, LogicalType::BIT);
	bit_string_fun.SetStructStateExport(GetBitStringStateType);
	bit_xor.AddFunction(bit_string_fun);
	return bit_xor;
}

} // namespace duckdb
