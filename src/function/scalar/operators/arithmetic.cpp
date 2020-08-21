#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace std;

namespace duckdb {

template <class OP> static scalar_function_t GetScalarBinaryFunction(PhysicalType type) {
	scalar_function_t function;
	switch (type) {
	case PhysicalType::INT8:
		function = &ScalarFunction::BinaryFunction<int8_t, int8_t, int8_t, OP>;
		break;
	case PhysicalType::INT16:
		function = &ScalarFunction::BinaryFunction<int16_t, int16_t, int16_t, OP>;
		break;
	case PhysicalType::INT32:
		function = &ScalarFunction::BinaryFunction<int32_t, int32_t, int32_t, OP>;
		break;
	case PhysicalType::INT64:
		function = &ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, OP>;
		break;
	case PhysicalType::INT128:
		function = &ScalarFunction::BinaryFunction<hugeint_t, hugeint_t, hugeint_t, OP, true>;
		break;
	case PhysicalType::FLOAT:
		function = &ScalarFunction::BinaryFunction<float, float, float, OP, true>;
		break;
	case PhysicalType::DOUBLE:
		function = &ScalarFunction::BinaryFunction<double, double, double, OP, true>;
		break;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarBinaryFunction");
	}
	return function;
}

//===--------------------------------------------------------------------===//
// + [add]
//===--------------------------------------------------------------------===//
template <> float AddOperator::Operation(float left, float right) {
	auto result = left + right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in addition of float!");
	}
	return result;
}

template <> double AddOperator::Operation(double left, double right) {
	auto result = left + right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in addition of double!");
	}
	return result;
}

template <> interval_t AddOperator::Operation(interval_t left, interval_t right) {
	interval_t result;
	result.months = left.months + right.months;
	result.days = left.days + right.days;
	result.msecs = left.msecs + right.msecs;
	return result;
}

template <> date_t AddOperator::Operation(date_t left, interval_t right) {
	date_t result;
	if (right.months != 0) {
		int32_t year, month, day;
		Date::Convert(left, year, month, day);
		int32_t year_diff = right.months / Interval::MONTHS_PER_YEAR;
		year += year_diff;
		month += right.months - year_diff * Interval::MONTHS_PER_YEAR;
		if (month > Interval::MONTHS_PER_YEAR) {
			year++;
			month -= Interval::MONTHS_PER_YEAR;
		} else if (month <= 0) {
			year--;
			month += Interval::MONTHS_PER_YEAR;
		}
		result = Date::FromDate(year, month, day);
	} else {
		result = left;
	}
	if (right.days != 0) {
		result += right.days;
	}
	if (right.msecs != 0) {
		result += right.msecs / Interval::MSECS_PER_DAY;
	}
	return result;
}

template <> date_t AddOperator::Operation(interval_t left, date_t right) {
	return AddOperator::Operation<date_t, interval_t, date_t>(right, left);
}

struct AddTimeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		int64_t diff = right.msecs - ((right.msecs / Interval::MSECS_PER_DAY) * Interval::MSECS_PER_DAY);
		left += diff;
		if (left >= Interval::MSECS_PER_DAY) {
			left -= Interval::MSECS_PER_DAY;
		} else if (left < 0) {
			left += Interval::MSECS_PER_DAY;
		}
		return left;
	}
};

template <> dtime_t AddTimeOperator::Operation(interval_t left, dtime_t right) {
	return AddTimeOperator::Operation<dtime_t, interval_t, dtime_t>(right, left);
}

template <> timestamp_t AddOperator::Operation(timestamp_t left, interval_t right) {
	auto date = Timestamp::GetDate(left);
	auto time = Timestamp::GetTime(left);
	auto new_date = AddOperator::Operation<date_t, interval_t, date_t>(date, right);
	auto new_time = AddTimeOperator::Operation<dtime_t, interval_t, dtime_t>(time, right);
	return Timestamp::FromDatetime(new_date, new_time);
}

template <> timestamp_t AddOperator::Operation(interval_t left, timestamp_t right) {
	return AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(right, left);
}

void get_decimal_properties(const LogicalType &type, int &width, int &scale) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		// tinyint: [-127, 127] = DECIMAL(3,0)
		width = 3;
		scale = 0;
		break;
	case LogicalTypeId::SMALLINT:
		// smallint: [-32767, 32767] = DECIMAL(5,0)
		width = 5;
		scale = 0;
		break;
	case LogicalTypeId::INTEGER:
		// integer: [-2147483647, 2147483647] = DECIMAL(10,0)
		width = 10;
		scale = 0;
		break;
	case LogicalTypeId::BIGINT:
		// bigint: [-9223372036854775807, 9223372036854775807] = DECIMAL(19,0)
		width = 19;
		scale = 0;
		break;
	case LogicalTypeId::HUGEINT:
		// hugeint: max size decimal (38, 0)
		width = 38;
		scale = 0;
		break;
	case LogicalTypeId::DECIMAL:
		width = type.width();
		scale = type.scale();
		break;
	default:
		throw NotImplementedException("Unimplemented type for DECIMAL arithmetic");
	}
}

template <class OP>
unique_ptr<FunctionData> bind_decimal_add_subtract(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	// get the max width and scale of the input arguments
	int max_width = 0, max_scale = 0, max_width_over_scale = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		int width, scale;
		get_decimal_properties(arguments[i]->return_type, width, scale);
		max_width = std::max<int>(width, max_width);
		max_scale = std::max<int>(scale, max_scale);
		max_width_over_scale = std::max<int>(width - scale, max_width_over_scale);
	}
	// for addition/subtraction, we add 1 to the width to ensure we don't overflow
	// FIXME: use statistics to determine this
	max_width = std::max(max_scale + max_width_over_scale, max_width) + 1;
	if (max_width > Decimal::MAX_WIDTH_DECIMAL) {
		// target width does not fit in decimal: truncate the scale (if possible) to try and make it fit
		max_width = Decimal::MAX_WIDTH_DECIMAL;
	}
	// arithmetic between two decimal arguments: check the types of the input arguments
	LogicalType result_type = LogicalType(LogicalTypeId::DECIMAL, max_width, max_scale);
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
	bound_function.function = GetScalarBinaryFunction<OP>(result_type.InternalType());
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
			    ScalarFunction({type, type}, type, nullptr, false, bind_decimal_add_subtract<AddOperator>));
		} else {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, GetScalarBinaryFunction<AddOperator>(type.InternalType())));
		}
	}
	// we can add integers to dates
	functions.AddFunction(ScalarFunction({LogicalType::DATE, LogicalType::INTEGER}, LogicalType::DATE,
	                                     GetScalarBinaryFunction<AddOperator>(PhysicalType::INT32)));
	functions.AddFunction(ScalarFunction({LogicalType::INTEGER, LogicalType::DATE}, LogicalType::DATE,
	                                     GetScalarBinaryFunction<AddOperator>(PhysicalType::INT32)));
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
template <> float SubtractOperator::Operation(float left, float right) {
	auto result = left - right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in subtraction of float!");
	}
	return result;
}

template <> double SubtractOperator::Operation(double left, double right) {
	auto result = left - right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in subtraction of double!");
	}
	return result;
}

template <> interval_t SubtractOperator::Operation(interval_t left, interval_t right) {
	interval_t result;
	result.months = left.months - right.months;
	result.days = left.days - right.days;
	result.msecs = left.msecs - right.msecs;
	return result;
}

template <> date_t SubtractOperator::Operation(date_t left, interval_t right) {
	right.months = -right.months;
	right.days = -right.days;
	right.msecs = -right.msecs;
	return AddOperator::Operation<date_t, interval_t, date_t>(left, right);
}

struct SubtractTimeOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA left, TB right) {
		right.msecs = -right.msecs;
		return AddTimeOperator::Operation<dtime_t, interval_t, dtime_t>(left, right);
	}
};

template <> timestamp_t SubtractOperator::Operation(timestamp_t left, interval_t right) {
	right.months = -right.months;
	right.days = -right.days;
	right.msecs = -right.msecs;
	return AddOperator::Operation<timestamp_t, interval_t, timestamp_t>(left, right);
}

template <> interval_t SubtractOperator::Operation(timestamp_t left, timestamp_t right) {
	return Interval::GetDifference(left, right);
}

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
			    ScalarFunction({type, type}, type, nullptr, false, bind_decimal_add_subtract<SubtractOperator>));
		} else {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, GetScalarBinaryFunction<SubtractOperator>(type.InternalType())));
		}
	}
	// we can subtract dates from each other
	functions.AddFunction(ScalarFunction({LogicalType::DATE, LogicalType::DATE}, LogicalType::INTEGER,
	                                     GetScalarBinaryFunction<SubtractOperator>(PhysicalType::INT32)));
	functions.AddFunction(ScalarFunction({LogicalType::DATE, LogicalType::INTEGER}, LogicalType::DATE,
	                                     GetScalarBinaryFunction<SubtractOperator>(PhysicalType::INT32)));
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
	                   ScalarFunction::BinaryFunction<time_t, interval_t, time_t, SubtractTimeOperator>));
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
template <> float MultiplyOperator::Operation(float left, float right) {
	auto result = left * right;
	if (!Value::FloatIsValid(result)) {
		throw OutOfRangeException("Overflow in multiplication of float!");
	}
	return result;
}

template <> double MultiplyOperator::Operation(double left, double right) {
	auto result = left * right;
	if (!Value::DoubleIsValid(result)) {
		throw OutOfRangeException("Overflow in multiplication of double!");
	}
	return result;
}

template <> interval_t MultiplyOperator::Operation(interval_t left, int64_t right) {
	left.months *= right;
	left.days *= right;
	left.msecs *= right;
	return left;
}

template <> interval_t MultiplyOperator::Operation(int64_t left, interval_t right) {
	return MultiplyOperator::Operation<interval_t, int64_t, interval_t>(right, left);
}

unique_ptr<FunctionData> bind_decimal_multiply(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	int result_width = 0, result_scale = 0;
	for (idx_t i = 0; i < arguments.size(); i++) {
		int width, scale;
		get_decimal_properties(arguments[i]->return_type, width, scale);

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
	bound_function.function = GetScalarBinaryFunction<MultiplyOperator>(result_type.InternalType());
	return nullptr;
}

void MultiplyFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("*");
	for (auto &type : LogicalType::NUMERIC) {
		if (type.id() == LogicalTypeId::DECIMAL) {
			functions.AddFunction(ScalarFunction({type, type}, type, nullptr, false, bind_decimal_multiply));
		} else {
			functions.AddFunction(
			    ScalarFunction({type, type}, type, GetScalarBinaryFunction<MultiplyOperator>(type.InternalType())));
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

template <class TA, class TB, class TC, class OP>
static void BinaryScalarFunctionIgnoreZero(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<TA, TB, TC, OP, true, BinaryZeroIsNullWrapper>(input.data[0], input.data[1], result,
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
		return BinaryScalarFunctionIgnoreZero<hugeint_t, hugeint_t, hugeint_t, OP>;
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
