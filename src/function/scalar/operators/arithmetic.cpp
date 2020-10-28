#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace std;

namespace duckdb {

//===--------------------------------------------------------------------===//
// + [add]
//===--------------------------------------------------------------------===//
template <class OP, class OPOVERFLOWCHECK>
unique_ptr<FunctionData> bind_decimal_add_subtract(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	// get the max width and scale of the input arguments
	uint8_t max_width = 0, max_scale = 0, max_width_over_scale = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		uint8_t width, scale;
		auto can_convert = arguments[i]->return_type.GetDecimalProperties(width, scale);
		if (!can_convert) {
			throw InternalException("Could not convert type %s to a decimal?", arguments[i]->return_type.ToString());
		}
		max_width = MaxValue<uint8_t>(width, max_width);
		max_scale = MaxValue<uint8_t>(scale, max_scale);
		max_width_over_scale = MaxValue<uint8_t>(width - scale, max_width_over_scale);
	}
	// for addition/subtraction, we add 1 to the width to ensure we don't overflow
	bool check_overflow = false;
	auto required_width = max_width = MaxValue<uint8_t>(max_scale + max_width_over_scale, max_width) + 1;
	if (required_width > Decimal::MAX_WIDTH_INT64 && max_width <= Decimal::MAX_WIDTH_INT64) {
		// we don't automatically promote past the hugeint boundary to avoid the large hugeint performance penalty
		check_overflow = true;
		required_width = Decimal::MAX_WIDTH_INT64;
	}
	if (required_width > Decimal::MAX_WIDTH_DECIMAL) {
		// target width does not fit in decimal at all: truncate the scale and perform overflow detection
		required_width = Decimal::MAX_WIDTH_DECIMAL;
		check_overflow = true;
	}
	// arithmetic between two decimal arguments: check the types of the input arguments
	LogicalType result_type = LogicalType(LogicalTypeId::DECIMAL, required_width, max_scale);
	// we cast all input types to the specified type
	for (idx_t i = 0; i < arguments.size(); i++) {
		// first check if the cast is necessary
		// if the argument has a matching scale and internal type as the output type, no casting is necessary
		auto &argument_type = arguments[i]->return_type;
		if (argument_type.scale() == result_type.scale() &&
		    argument_type.InternalType() == result_type.InternalType()) {
			bound_function.arguments[i] = argument_type;
		} else {
			bound_function.arguments[i] = result_type;
		}
	}
	bound_function.return_type = result_type;
	// now select the physical function to execute
	if (check_overflow) {
		bound_function.function = ScalarFunction::GetScalarBinaryFunction<OPOVERFLOWCHECK>(result_type.InternalType());
	} else {
		bound_function.function = ScalarFunction::GetScalarBinaryFunction<OP>(result_type.InternalType());
	}
	return nullptr;
}

unique_ptr<FunctionData> nop_decimal_bind(ClientContext &context, ScalarFunction &bound_function,
                                          vector<unique_ptr<Expression>> &arguments) {
	bound_function.return_type = arguments[0]->return_type;
	bound_function.arguments[0] = arguments[0]->return_type;
	return nullptr;
}

void AddFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("+");
	// binary add function adds two numbers together
	for (auto &type : LogicalType::NUMERIC) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, nullptr, false, bind_decimal_add_subtract<AddOperator, AddOperatorOverflowCheck>));
		} else if (TypeIsIntegral(type.InternalType()) && type.id() != LogicalTypeId::HUGEINT) {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, ScalarFunction::GetScalarBinaryFunction<AddOperatorOverflowCheck>(type.InternalType())));
		} else {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, ScalarFunction::GetScalarBinaryFunction<AddOperator>(type.InternalType())));
		}
	}
	// we can add integers to dates
	functions.AddFunction(ScalarFunction({LogicalType::DATE, LogicalType::INTEGER}, LogicalType::DATE,
	                                     ScalarFunction::GetScalarBinaryFunction<AddOperator>(PhysicalType::INT32)));
	functions.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::DATE}, LogicalType::DATE,
	                                     ScalarFunction::GetScalarBinaryFunction<AddOperator>(PhysicalType::INT32)));
	// we can add intervals together
	functions.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::INTERVAL}, LogicalType::INTERVAL,
	                   ScalarFunction::BinaryFunction<interval_t, interval_t, interval_t, AddOperator>));
	// we can add intervals to dates/times/timestamps
	functions.AddFunction(ScalarFunction({LogicalType::DATE, LogicalType::INTERVAL}, LogicalType::DATE,
	                                     ScalarFunction::BinaryFunction<date_t, interval_t, date_t, AddOperator>));
	functions.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::DATE}, LogicalType::DATE,
	                                     ScalarFunction::BinaryFunction<interval_t, date_t, date_t, AddOperator>));

	functions.AddFunction(
	    ScalarFunction({LogicalType::TIME, LogicalType::INTERVAL}, LogicalType::TIME,
	                   ScalarFunction::BinaryFunction<dtime_t, interval_t, dtime_t, AddTimeOperator>));
	functions.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::TIME}, LogicalType::TIME,
	                   ScalarFunction::BinaryFunction<interval_t, dtime_t, dtime_t, AddTimeOperator>));

	functions.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP, LogicalType::INTERVAL}, LogicalType::TIMESTAMP,
	                   ScalarFunction::BinaryFunction<timestamp_t, interval_t, timestamp_t, AddOperator>));
	functions.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP}, LogicalType::TIMESTAMP,
	                   ScalarFunction::BinaryFunction<interval_t, timestamp_t, timestamp_t, AddOperator>));
	// unary add function is a nop, but only exists for numeric types
	for (auto &type : LogicalType::NUMERIC) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			functions.AddFunction(ScalarFunction({type}, type, ScalarFunction::NopFunction, false, nop_decimal_bind));
		} else {
			functions.AddFunction(ScalarFunction({type}, type, ScalarFunction::NopFunction));
		}
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// - [subtract]
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> decimal_negate_bind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
	auto decimal_type = arguments[0]->return_type;
	if (decimal_type.width() <= Decimal::MAX_WIDTH_INT16) {
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<NegateOperator>(LogicalTypeId::SMALLINT);
	} else if (decimal_type.width() <= Decimal::MAX_WIDTH_INT32) {
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<NegateOperator>(LogicalTypeId::INTEGER);
	} else if (decimal_type.width() <= Decimal::MAX_WIDTH_INT64) {
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<NegateOperator>(LogicalTypeId::BIGINT);
	} else {
		assert(decimal_type.width() <= Decimal::MAX_WIDTH_INT128);
		bound_function.function = ScalarFunction::GetScalarUnaryFunction<NegateOperator>(LogicalTypeId::HUGEINT);
	}
	bound_function.arguments[0] = decimal_type;
	bound_function.return_type = decimal_type;
	return nullptr;
}

void SubtractFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("-");
	// binary subtract function "a - b", subtracts b from a
	for (auto &type : LogicalType::NUMERIC) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, nullptr, false, bind_decimal_add_subtract<SubtractOperator, SubtractOperatorOverflowCheck>));
		} else if (TypeIsIntegral(type.InternalType()) && type.id() != LogicalTypeId::HUGEINT) {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, ScalarFunction::GetScalarBinaryFunction<SubtractOperatorOverflowCheck>(type.InternalType())));
		} else {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, ScalarFunction::GetScalarBinaryFunction<SubtractOperator>(type.InternalType())));
		}
	}
	// we can subtract dates from each other
	functions.AddFunction(ScalarFunction({LogicalType::DATE, LogicalType::DATE}, LogicalType::INTEGER,
	                                     ScalarFunction::GetScalarBinaryFunction<SubtractOperator>(PhysicalType::INT32)));
	functions.AddFunction(ScalarFunction({LogicalType::DATE, LogicalType::INTEGER}, LogicalType::DATE,
	                                     ScalarFunction::GetScalarBinaryFunction<SubtractOperator>(PhysicalType::INT32)));
	// we can subtract timestamps from each other
	functions.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP}, LogicalType::INTERVAL,
	                   ScalarFunction::BinaryFunction<timestamp_t, timestamp_t, interval_t, SubtractOperator>));
	// we can subtract intervals from each other
	functions.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::INTERVAL}, LogicalType::INTERVAL,
	                   ScalarFunction::BinaryFunction<interval_t, interval_t, interval_t, SubtractOperator>));
	// we can subtract intervals from dates/times/timestamps, but not the other way around
	functions.AddFunction(ScalarFunction({LogicalType::DATE, LogicalType::INTERVAL}, LogicalType::DATE,
	                                     ScalarFunction::BinaryFunction<date_t, interval_t, date_t, SubtractOperator>));
	functions.AddFunction(
	    ScalarFunction({LogicalType::TIME, LogicalType::INTERVAL}, LogicalType::TIME,
	                   ScalarFunction::BinaryFunction<dtime_t, interval_t, dtime_t, SubtractTimeOperator>));
	functions.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP, LogicalType::INTERVAL}, LogicalType::TIMESTAMP,
	                   ScalarFunction::BinaryFunction<timestamp_t, interval_t, timestamp_t, SubtractOperator>));

	// unary subtract function, negates the input (i.e. multiplies by -1)
	for (auto &type : LogicalType::NUMERIC) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			functions.AddFunction(ScalarFunction({type}, type, nullptr, false, decimal_negate_bind));
		} else {
			functions.AddFunction(
			    ScalarFunction({type}, type, ScalarFunction::GetScalarUnaryFunction<NegateOperator>(type)));
		}
	}
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// * [multiply]
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> bind_decimal_multiply(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	uint8_t result_width = 0, result_scale = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		uint8_t width, scale;
		auto can_convert = arguments[i]->return_type.GetDecimalProperties(width, scale);
		if (!can_convert) {
			throw InternalException("Could not convert type %s to a decimal?", arguments[i]->return_type.ToString());
		}
		result_width += width;
		result_scale += scale;
	}
	if (result_scale > Decimal::MAX_WIDTH_DECIMAL) {
		throw OutOfRangeException(
		    "Needed scale %d to accurately represent the multiplication result, but this is out of range of the "
		    "DECIMAL type. Max scale is %d; could not perform an accurate multiplication. Either add a cast to DOUBLE, "
		    "or add an explicit cast to a decimal with a lower scale.",
		    result_scale, Decimal::MAX_WIDTH_DECIMAL);
	}
	if (result_width > Decimal::MAX_WIDTH_DECIMAL) {
		result_width = Decimal::MAX_WIDTH_DECIMAL;
	}
	LogicalType result_type = LogicalType(LogicalTypeId::DECIMAL, result_width, result_scale);
	// since our scale is the summation of our input scales, we do not need to cast to the result scale
	// however, we might need to cast to the correct internal type
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto &argument_type = arguments[i]->return_type;
		if (argument_type.InternalType() == result_type.InternalType()) {
			bound_function.arguments[i] = argument_type;
		} else {
			bound_function.arguments[i] = LogicalType(LogicalTypeId::DECIMAL, result_width, argument_type.scale());
		}
	}
	bound_function.return_type = result_type;
	// now select the physical function to execute
	bound_function.function = ScalarFunction::GetScalarBinaryFunction<MultiplyOperator>(result_type.InternalType());
	return nullptr;
}

void MultiplyFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("*");
	for (auto &type : LogicalType::NUMERIC) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			functions.AddFunction(ScalarFunction({type, type}, type, nullptr, false, bind_decimal_multiply));
		} else if (TypeIsIntegral(type.InternalType()) && type.id() != LogicalTypeId::HUGEINT) {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, ScalarFunction::GetScalarBinaryFunction<MultiplyOperatorOverflowCheck>(type.InternalType())));
		} else {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, ScalarFunction::GetScalarBinaryFunction<MultiplyOperator>(type.InternalType())));
		}
	}
	functions.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::BIGINT}, LogicalType::INTERVAL,
	                   ScalarFunction::BinaryFunction<interval_t, int64_t, interval_t, MultiplyOperator>));
	functions.AddFunction(
	    ScalarFunction({LogicalType::BIGINT, LogicalType::INTERVAL}, LogicalType::INTERVAL,
	                   ScalarFunction::BinaryFunction<int64_t, interval_t, interval_t, MultiplyOperator>));
	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// / [divide]
//===--------------------------------------------------------------------===//
template <> float DivideOperator::Operation(float left, float right) {
	auto result = left / right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in division of float!");
	}
	return result;
}

template <> double DivideOperator::Operation(double left, double right) {
	auto result = left / right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in division of double!");
	}
	return result;
}

template <> interval_t DivideOperator::Operation(interval_t left, int64_t right) {
	left.days /= right;
	left.months /= right;
	left.msecs /= right;
	return left;
}

struct BinaryZeroIsNullWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, nullmask_t &nullmask, idx_t idx) {
		if (right == 0) {
			nullmask[idx] = true;
			return left;
		} else {
			return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left, right);
		}
	}
};
struct BinaryZeroIsNullHugeintWrapper {
	template <class FUNC, class OP, class LEFT_TYPE, class RIGHT_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUNC fun, LEFT_TYPE left, RIGHT_TYPE right, nullmask_t &nullmask, idx_t idx) {
		if (right.upper == 0 && right.lower == 0) {
			nullmask[idx] = true;
			return left;
		} else {
			return OP::template Operation<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE>(left, right);
		}
	}
};

template <class TA, class TB, class TC, class OP, class ZWRAPPER=BinaryZeroIsNullWrapper>
static void BinaryScalarFunctionIgnoreZero(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<TA, TB, TC, OP, true, ZWRAPPER>(input.data[0], input.data[1], result,
	                                                                       input.size());
}

template <class OP> static scalar_function_t GetBinaryFunctionIgnoreZero(LogicalType type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return BinaryScalarFunctionIgnoreZero<int8_t, int8_t, int8_t, OP>;
	case LogicalTypeId::SMALLINT:
		return BinaryScalarFunctionIgnoreZero<int16_t, int16_t, int16_t, OP>;
	case LogicalTypeId::INTEGER:
		return BinaryScalarFunctionIgnoreZero<int32_t, int32_t, int32_t, OP>;
	case LogicalTypeId::BIGINT:
		return BinaryScalarFunctionIgnoreZero<int64_t, int64_t, int64_t, OP>;
	case LogicalTypeId::HUGEINT:
		return BinaryScalarFunctionIgnoreZero<hugeint_t, hugeint_t, hugeint_t, OP, BinaryZeroIsNullHugeintWrapper>;
	case LogicalTypeId::FLOAT:
		return BinaryScalarFunctionIgnoreZero<float, float, float, OP>;
	case LogicalTypeId::DOUBLE:
		return BinaryScalarFunctionIgnoreZero<double, double, double, OP>;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarUnaryFunction");
	}
}

void DivideFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("/");
	for (auto &type : LogicalType::NUMERIC) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			continue;
		} else {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, GetBinaryFunctionIgnoreZero<DivideOperator>(type)));
		}
	}
	functions.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::BIGINT}, LogicalType::INTERVAL,
	                   BinaryScalarFunctionIgnoreZero<interval_t, int64_t, interval_t, DivideOperator>));

	set.AddFunction(functions);
}

//===--------------------------------------------------------------------===//
// % [modulo]
//===--------------------------------------------------------------------===//
template <> float ModuloOperator::Operation(float left, float right) {
	assert(right != 0);
	return fmod(left, right);
}

template <> double ModuloOperator::Operation(double left, double right) {
	assert(right != 0);
	return fmod(left, right);
}

void ModFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("%");
	for (auto &type : LogicalType::NUMERIC) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			continue;
		} else {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, GetBinaryFunctionIgnoreZero<ModuloOperator>(type)));
		}
	}
	set.AddFunction(functions);
	functions.name = "mod";
	set.AddFunction(functions);
}

} // namespace duckdb
