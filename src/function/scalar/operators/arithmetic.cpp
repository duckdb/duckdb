#include "duckdb/function/scalar/operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/operator/numeric_binary_operators.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

using namespace std;

namespace duckdb {

template <class OP> static scalar_function_t GetScalarBinaryFunction(SQLType type) {
	scalar_function_t function;
	switch (type.id) {
	case SQLTypeId::TINYINT:
		function = &ScalarFunction::BinaryFunction<int8_t, int8_t, int8_t, OP>;
		break;
	case SQLTypeId::SMALLINT:
		function = &ScalarFunction::BinaryFunction<int16_t, int16_t, int16_t, OP>;
		break;
	case SQLTypeId::INTEGER:
		function = &ScalarFunction::BinaryFunction<int32_t, int32_t, int32_t, OP>;
		break;
	case SQLTypeId::BIGINT:
		function = &ScalarFunction::BinaryFunction<int64_t, int64_t, int64_t, OP>;
		break;
	case SQLTypeId::HUGEINT:
		function = &ScalarFunction::BinaryFunction<hugeint_t, hugeint_t, hugeint_t, OP>;
		break;
	case SQLTypeId::FLOAT:
		function = &ScalarFunction::BinaryFunction<float, float, float, OP, true>;
		break;
	case SQLTypeId::DECIMAL:
	case SQLTypeId::DOUBLE:
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
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
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

template <> hugeint_t AddOperator::Operation(hugeint_t left, hugeint_t right) {
	return Hugeint::Add(left, right);
}

void AddFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("+");
	// binary add function adds two numbers together
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, GetScalarBinaryFunction<AddOperator>(type)));
	}
	// hugeint
	functions.AddFunction(ScalarFunction({SQLType::HUGEINT, SQLType::HUGEINT}, SQLType::HUGEINT,
	                                     GetScalarBinaryFunction<AddOperator>(SQLType::HUGEINT)));
	// we can add integers to dates
	functions.AddFunction(ScalarFunction({SQLType::DATE, SQLType::INTEGER}, SQLType::DATE,
	                                     GetScalarBinaryFunction<AddOperator>(SQLType::INTEGER)));
	functions.AddFunction(ScalarFunction({SQLType::INTEGER, SQLType::DATE}, SQLType::DATE,
	                                     GetScalarBinaryFunction<AddOperator>(SQLType::INTEGER)));
	// we can add intervals together
	functions.AddFunction(ScalarFunction({SQLType::INTERVAL, SQLType::INTERVAL}, SQLType::INTERVAL,
	                                     ScalarFunction::BinaryFunction<interval_t, interval_t, interval_t, AddOperator>));
	// we can add intervals to dates/times/timestamps
	functions.AddFunction(ScalarFunction({SQLType::DATE, SQLType::INTERVAL}, SQLType::DATE,
	                                     ScalarFunction::BinaryFunction<date_t, interval_t, date_t, AddOperator>));
	functions.AddFunction(ScalarFunction({SQLType::INTERVAL, SQLType::DATE}, SQLType::DATE,
	                                     ScalarFunction::BinaryFunction<interval_t, date_t, date_t, AddOperator>));

	functions.AddFunction(ScalarFunction({SQLType::TIME, SQLType::INTERVAL}, SQLType::TIME,
	                                     ScalarFunction::BinaryFunction<dtime_t, interval_t, dtime_t, AddTimeOperator>));
	functions.AddFunction(ScalarFunction({SQLType::INTERVAL, SQLType::TIME}, SQLType::TIME,
	                                     ScalarFunction::BinaryFunction<interval_t, dtime_t, dtime_t, AddTimeOperator>));

	functions.AddFunction(ScalarFunction({SQLType::TIMESTAMP, SQLType::INTERVAL}, SQLType::TIMESTAMP,
	                                     ScalarFunction::BinaryFunction<timestamp_t, interval_t, timestamp_t, AddOperator>));
	functions.AddFunction(ScalarFunction({SQLType::INTERVAL, SQLType::TIMESTAMP}, SQLType::TIMESTAMP,
	                                     ScalarFunction::BinaryFunction<interval_t, timestamp_t, timestamp_t, AddOperator>));
	// unary add function is a nop, but only exists for numeric types
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type}, type, ScalarFunction::NopFunction));
	}
	functions.AddFunction(ScalarFunction({SQLType::HUGEINT}, SQLType::HUGEINT, ScalarFunction::NopFunction));
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
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right) {
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

template <> hugeint_t SubtractOperator::Operation(hugeint_t left, hugeint_t right) {
	return Hugeint::Subtract(left, right);
}

template <> hugeint_t NegateOperator::Operation(hugeint_t input) {
	return Hugeint::Negate(input);
}

void SubtractFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("-");
	// binary subtract function "a - b", subtracts b from a
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, GetScalarBinaryFunction<SubtractOperator>(type)));
	}
	// hugeint
	functions.AddFunction(ScalarFunction({SQLType::HUGEINT, SQLType::HUGEINT}, SQLType::HUGEINT,
	                                     GetScalarBinaryFunction<SubtractOperator>(SQLType::HUGEINT)));
	// we can subtract dates from each other
	functions.AddFunction(ScalarFunction({SQLType::DATE, SQLType::DATE}, SQLType::INTEGER,
	                                     GetScalarBinaryFunction<SubtractOperator>(SQLType::INTEGER)));
	functions.AddFunction(ScalarFunction({SQLType::DATE, SQLType::INTEGER}, SQLType::DATE,
	                                     GetScalarBinaryFunction<SubtractOperator>(SQLType::INTEGER)));
	// we can subtract timestamps from each other
	functions.AddFunction(ScalarFunction({SQLType::TIMESTAMP, SQLType::TIMESTAMP}, SQLType::INTERVAL,
	                                     ScalarFunction::BinaryFunction<timestamp_t, timestamp_t, interval_t, SubtractOperator>));
	// we can subtract intervals from each other
	functions.AddFunction(ScalarFunction({SQLType::INTERVAL, SQLType::INTERVAL}, SQLType::INTERVAL,
										ScalarFunction::BinaryFunction<interval_t, interval_t, interval_t, SubtractOperator>));
	// we can subtract intervals from dates/times/timestamps, but not the other way around
	functions.AddFunction(ScalarFunction({SQLType::DATE, SQLType::INTERVAL}, SQLType::DATE,
										ScalarFunction::BinaryFunction<date_t, interval_t, date_t, SubtractOperator>));
	functions.AddFunction(ScalarFunction({SQLType::TIME, SQLType::INTERVAL}, SQLType::TIME,
										ScalarFunction::BinaryFunction<time_t, interval_t, time_t, SubtractTimeOperator>));
	functions.AddFunction(ScalarFunction({SQLType::TIMESTAMP, SQLType::INTERVAL}, SQLType::TIMESTAMP,
										ScalarFunction::BinaryFunction<timestamp_t, interval_t, timestamp_t, SubtractOperator>));

	// unary subtract function, negates the input (i.e. multiplies by -1)
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(
		    ScalarFunction({type}, type, ScalarFunction::GetScalarUnaryFunction<NegateOperator>(type)));
	}
	functions.AddFunction(
		ScalarFunction({SQLType::HUGEINT}, SQLType::HUGEINT, ScalarFunction::UnaryFunction<hugeint_t, hugeint_t, NegateOperator>));
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

template <> hugeint_t MultiplyOperator::Operation(hugeint_t left, hugeint_t right) {
	return Hugeint::Multiply(left, right);
}

void MultiplyFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("*");
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, GetScalarBinaryFunction<MultiplyOperator>(type)));
	}
	functions.AddFunction(ScalarFunction({SQLType::HUGEINT, SQLType::HUGEINT}, SQLType::HUGEINT,
	                                     GetScalarBinaryFunction<MultiplyOperator>(SQLType::HUGEINT)));
	functions.AddFunction(ScalarFunction({SQLType::INTERVAL, SQLType::BIGINT}, SQLType::INTERVAL,
										ScalarFunction::BinaryFunction<interval_t, int64_t, interval_t, MultiplyOperator>));
	functions.AddFunction(ScalarFunction({SQLType::BIGINT, SQLType::INTERVAL}, SQLType::INTERVAL,
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

template <class TA, class TB, class TC, class OP>
static void BinaryScalarFunctionIgnoreZero(DataChunk &input, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<TA, TB, TC, OP, true, BinaryZeroIsNullWrapper>(input.data[0], input.data[1], result,
	                                                                    input.size());
}

template <class OP> static scalar_function_t GetBinaryFunctionIgnoreZero(SQLType type) {
	switch (type.id) {
	case SQLTypeId::TINYINT:
		return BinaryScalarFunctionIgnoreZero<int8_t, int8_t, int8_t, OP>;
	case SQLTypeId::SMALLINT:
		return BinaryScalarFunctionIgnoreZero<int16_t, int16_t, int16_t, OP>;
	case SQLTypeId::INTEGER:
		return BinaryScalarFunctionIgnoreZero<int32_t, int32_t, int32_t, OP>;
	case SQLTypeId::BIGINT:
		return BinaryScalarFunctionIgnoreZero<int64_t, int64_t, int64_t, OP>;
	case SQLTypeId::FLOAT:
		return BinaryScalarFunctionIgnoreZero<float, float, float, OP>;
	case SQLTypeId::DOUBLE:
	case SQLTypeId::DECIMAL:
		return BinaryScalarFunctionIgnoreZero<double, double, double, OP>;
	default:
		throw NotImplementedException("Unimplemented type for GetScalarUnaryFunction");
	}
}

void DivideFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet functions("/");
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, GetBinaryFunctionIgnoreZero<DivideOperator>(type)));
	}
	functions.AddFunction(ScalarFunction({SQLType::INTERVAL, SQLType::BIGINT}, SQLType::INTERVAL, BinaryScalarFunctionIgnoreZero<interval_t, int64_t, interval_t, DivideOperator>));

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
	for (auto &type : SQLType::NUMERIC) {
		functions.AddFunction(ScalarFunction({type, type}, type, GetBinaryFunctionIgnoreZero<ModuloOperator>(type)));
	}
	set.AddFunction(functions);
	functions.name = "mod";
	set.AddFunction(functions);
}

} // namespace duckdb
