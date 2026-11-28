#include "duckdb/common/bignum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/bignum_decimal.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/uhugeint.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/function/aggregate/bignum_aggregates.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// State: running scaled-integer sum and a non-NULL row count.
// count == 0 is the empty-group sentinel (no separate boolean needed).
//===--------------------------------------------------------------------===//

struct DecimalAvgState {
	static constexpr const char *STATE_NAMES[] = {"sum", "count"};
	using STATE_TYPE = StructStateType<hugeint_t, uint64_t>;

	hugeint_t sum;  // accumulated integer value at the input's scale
	uint64_t count; // number of non-NULL rows accumulated so far
};

//===--------------------------------------------------------------------===//
// Operation
//===--------------------------------------------------------------------===//

template <class INPUT_TYPE>
struct DecimalAvgOperation {
	static bool IgnoreNull() {
		return true;
	}

	static void Initialize(DecimalAvgState &state) {
		state.sum = hugeint_t(0);
		state.count = 0;
	}

	template <class INPUT, class STATE, class OP>
	static void Operation(STATE &state, const INPUT &input, AggregateUnaryInput &) {
		if (!Hugeint::TryAddInPlace(state.sum, hugeint_t(input))) {
			OutOfRangeException::Throw("decimal_average: intermediate sum overflowed HUGEINT range");
		}
		state.count++;
	}

	template <class INPUT, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT &input, AggregateUnaryInput &unary_input, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT, STATE, OP>(state, input, unary_input);
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		// the counts of imported states can be anything, the count of rows of a table cannot overflow
		uint64_t count;
		if (!TryAddOperator::Operation(target.count, source.count, count)) {
			throw OutOfRangeException("Overflow in aggregate state combine");
		}
		if (!Hugeint::TryAddInPlace(target.sum, source.sum)) {
			OutOfRangeException::Throw("decimal_average: intermediate sum overflowed HUGEINT range during combine");
		}
		target.count = count;
	}

	// Overload for hugeint_t result: direct assignment.
	static void AssignResult(hugeint_t &target, hugeint_t signed_q) {
		target = signed_q;
	}

	// Overload for smaller integer result types: take lower 64 bits.
	// Safe because the average of values bounded by the type's range is also
	// within that range.
	template <class RESULT>
	static void AssignResult(RESULT &target, hugeint_t signed_q) {
		target = static_cast<RESULT>(signed_q.lower);
	}

	template <class RESULT, class STATE>
	static void Finalize(STATE &state, RESULT &target, AggregateFinalizeData &fd) {
		if (state.count == 0) {
			fd.ReturnNull();
			return;
		}

		bool negative = state.sum.upper < 0;
		uhugeint_t abs_sum(static_cast<uint64_t>(state.sum.upper), state.sum.lower);
		if (negative) {
			abs_sum = -abs_sum;
		}

		uhugeint_t remainder;
		uhugeint_t q = Uhugeint::DivMod(abs_sum, uhugeint_t(state.count), remainder);
		auto r64 = remainder.lower;

		// Banker's rounding (round-half-to-even).
		uint64_t dist = state.count - r64;
		bool round_up = (dist < r64) | ((dist == r64) & static_cast<bool>(q.lower & 1));
		if (round_up) {
			q = q + uhugeint_t(1);
		}

		auto signed_q = hugeint_t(q);
		if (negative) {
			signed_q = hugeint_t(-q);
		}
		AssignResult(target, signed_q);
	}
};

//===--------------------------------------------------------------------===//
// The bignum_t tier: decimals wider than 38 digits
//
// The state is the shared BIGNUM average accumulator (an exact sum and a non-NULL row count); the sum is divided once
// at the end with the rounding of the fixed-size tiers (half to even). The sum has no bound, so this tier cannot
// overflow.
//===--------------------------------------------------------------------===//

struct WideDecimalAvgOperation : BignumAvgOperation {
	template <class RESULT, class STATE>
	static void Finalize(STATE &state, RESULT &target, AggregateFinalizeData &finalize_data) {
		// the state is only read: it is exported while other threads may still read it
		if (state.count == 0) {
			finalize_data.ReturnNull();
			return;
		}
		StringHeap heap;
		BignumArithmeticState arithmetic;
		const string count_blob = DecimalHelper<bignum_t>::FromInteger(uhugeint_t(state.count));
		const bignum_t count_value {string_t(count_blob)};
		const BignumIntermediate divisor(count_value);
		// the sum is divided once and rounded half to even, like in the fixed-size tiers
		const auto quotient = DecimalHelper<bignum_t>::DivideAndRound(state.value, divisor,
		                                                              DecimalRounding::HALF_TO_EVEN, heap, arithmetic);
		// the average of values of the width fits the width, but an imported state can carry any sum
		const auto &result_type = finalize_data.result.GetType();
		if (!DecimalHelper<bignum_t>::FitsWidth(bignum_t(quotient), DecimalType::GetWidth(result_type))) {
			throw OutOfRangeException("decimal_average: the result does not fit in %s", result_type.ToString());
		}
		// the result vector owns a copy of the quotient, the scratch heap is released with the finalize
		target = bignum_t(finalize_data.ReturnString(quotient));
	}
};

//! The states are exported as STRUCT(sum BIGNUM, count UBIGINT), the sum is NULL for an empty state
static AggregateStateLayout WideDecimalAvgStateLayout(AggregateLayoutInput &input) {
	return AggregateStateLayout(LogicalType::STRUCT({{"sum", LogicalType::BIGNUM}, {"count", LogicalType::UBIGINT}}),
	                            AlignValue<idx_t>(sizeof(BignumAvgState)));
}

static void WideDecimalAvgExportState(Vector &state_vector, AggregateFinalizeInputData &, Vector &result, idx_t count,
                                      idx_t offset) {
	auto states = state_vector.Values<BignumAvgState *>();
	auto writer = FlatVector::Writer<VectorStructType<string_t, uint64_t>>(result, count, offset);
	for (idx_t i = 0; i < count; i++) {
		writer.WriteValue([&](auto &sum_writer, auto &count_writer) {
			BignumAggregates::WriteAvgState(*states[i].GetValue(), count_writer, sum_writer);
		});
	}
}

static void WideDecimalAvgImportState(AggregateImportInputData &input) {
	auto entries = input.input_vec.Values<VectorStructType<string_t, uint64_t>>();
	for (idx_t i = 0; i < input.input_vec.size(); i++) {
		auto &state = *reinterpret_cast<BignumAvgState *>(input.dest_buffer + i * input.layout.total_state_size);
		BignumAvgOperation::Initialize(state);
		const auto entry = entries[i];
		if (!entry.IsValid()) {
			// an empty state
			continue;
		}
		BignumAggregates::ImportAvgState(state, entry.GetChildValue<1>(), entry.GetChildValue<0>(), input.allocator,
		                                 "decimal_average", "sum");
	}
}

static AggregateFunction MakeWideDecimalAvgFunction(const LogicalType &type) {
	auto function =
	    AggregateFunction::UnaryAggregate<BignumAvgState, bignum_t, bignum_t, WideDecimalAvgOperation>(type, type);
	function.SetFallible(); // the finalize throws when the average of an imported state does not fit the width
	function.SetStateExportCallbacks(WideDecimalAvgStateLayout, WideDecimalAvgExportState, WideDecimalAvgImportState);
	return function;
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

template <class INPUT_TYPE>
static AggregateFunction MakeDecimalAvgFunction(const LogicalType &input_type, const LogicalType &return_type) {
	auto function =
	    AggregateFunction::UnaryAggregate<DecimalAvgState, INPUT_TYPE, INPUT_TYPE, DecimalAvgOperation<INPUT_TYPE>>(
	        input_type, return_type);
	if (std::is_same<INPUT_TYPE, hugeint_t>::value) {
		// adding DECIMAL(38) values to the HUGEINT accumulator throws on overflow
		function.SetFallible();
	}
	return function;
}

static unique_ptr<FunctionData> BindDecimalAverage(BindAggregateFunctionInput &input) {
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	auto &input_type = arguments[0]->GetReturnType();

	if (input_type.id() == LogicalTypeId::UNKNOWN) {
		// unresolved prepared parameter - defer binding until the type is known
		throw ParameterNotResolvedException();
	}
	if (input_type.id() != LogicalTypeId::DECIMAL) {
		throw InvalidInputException("decimal_average requires a DECIMAL argument, got %s — "
		                            "cast the input explicitly, e.g. col::DECIMAL(18,2)",
		                            input_type.ToString());
	}

	uint32_t width = DecimalType::GetWidth(input_type);
	uint32_t scale = DecimalType::GetScale(input_type);
	auto return_type = LogicalType::DECIMAL(width, scale);

	auto name = function.GetName();
	switch (input_type.InternalType()) {
	case PhysicalType::INT16:
		function.ReplaceImplementation(
		    MakeDecimalAvgFunction<int16_t>(LogicalType::DECIMAL(Decimal::MAX_WIDTH_INT16, scale), return_type));
		break;
	case PhysicalType::INT32:
		function.ReplaceImplementation(
		    MakeDecimalAvgFunction<int32_t>(LogicalType::DECIMAL(Decimal::MAX_WIDTH_INT32, scale), return_type));
		break;
	case PhysicalType::INT64:
		function.ReplaceImplementation(
		    MakeDecimalAvgFunction<int64_t>(LogicalType::DECIMAL(Decimal::MAX_WIDTH_INT64, scale), return_type));
		break;
	case PhysicalType::INT128:
		function.ReplaceImplementation(
		    MakeDecimalAvgFunction<hugeint_t>(LogicalType::DECIMAL(Decimal::MAX_WIDTH_INT128, scale), return_type));
		break;
	case PhysicalType::VARCHAR:
		// decimals wider than 38 digits sum their BIGNUM blobs
		function.ReplaceImplementation(MakeWideDecimalAvgFunction(return_type));
		break;
	default:
		throw InternalException("decimal_average: unexpected physical type");
	}
	function.SetName(std::move(name));
	function.GetArguments()[0] = input_type;
	return nullptr;
}

AggregateFunctionSet DecimalAverageFun::GetFunctions() {
	AggregateFunctionSet set("decimal_average");
	AggregateFunction fun({}, LogicalType(LogicalTypeId::DECIMAL), nullptr, nullptr, nullptr, nullptr, nullptr,
	                      FunctionNullHandling::DEFAULT_NULL_HANDLING, nullptr, BindDecimalAverage);
	fun.GetSignature().AddParameter("arg", LogicalTypeId::DECIMAL);
	set.AddFunction(fun);
	return set;
}

} // namespace duckdb
