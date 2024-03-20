#include "duckdb/core_functions/scalar/date_functions.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/senary_executor.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

#include <cmath>

namespace duckdb {

struct MakeDateOperator {
	template <typename YYYY, typename MM, typename DD, typename RESULT_TYPE>
	static RESULT_TYPE Operation(YYYY yyyy, MM mm, DD dd) {
		return Date::FromDate(Cast::Operation<YYYY, int32_t>(yyyy), Cast::Operation<MM, int32_t>(mm),
		                      Cast::Operation<DD, int32_t>(dd));
	}
};

template <typename T>
static void ExecuteMakeDate(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 3);
	auto &yyyy = input.data[0];
	auto &mm = input.data[1];
	auto &dd = input.data[2];

	TernaryExecutor::Execute<T, T, T, date_t>(yyyy, mm, dd, result, input.size(),
	                                          MakeDateOperator::Operation<T, T, T, date_t>);
}

template <typename T>
static date_t FromDateCast(T year, T month, T day) {
	date_t result;
	if (!Date::TryFromDate(Cast::Operation<T, int32_t>(year), Cast::Operation<T, int32_t>(month),
	                       Cast::Operation<T, int32_t>(day), result)) {
		throw ConversionException("Date out of range: %d-%d-%d", year, month, day);
	}
	return result;
}

template <typename T>
static void ExecuteStructMakeDate(DataChunk &input, ExpressionState &state, Vector &result) {
	// this should be guaranteed by the binder
	D_ASSERT(input.ColumnCount() == 1);
	auto &vec = input.data[0];

	auto &children = StructVector::GetEntries(vec);
	D_ASSERT(children.size() == 3);
	auto &yyyy = *children[0];
	auto &mm = *children[1];
	auto &dd = *children[2];

	TernaryExecutor::Execute<T, T, T, date_t>(yyyy, mm, dd, result, input.size(), FromDateCast<T>);
}

struct MakeTimeOperator {
	template <typename HH, typename MM, typename SS, typename RESULT_TYPE>
	static RESULT_TYPE Operation(HH hh, MM mm, SS ss) {

		auto hh_32 = Cast::Operation<HH, int32_t>(hh);
		auto mm_32 = Cast::Operation<MM, int32_t>(mm);
		// Have to check this separately because safe casting of DOUBLE => INT32 can round.
		int32_t ss_32 = 0;
		if (ss < 0 || ss > Interval::SECS_PER_MINUTE) {
			ss_32 = Cast::Operation<SS, int32_t>(ss);
		} else {
			ss_32 = UnsafeNumericCast<int32_t>(ss);
		}
		auto micros = UnsafeNumericCast<int32_t>(std::round((ss - ss_32) * Interval::MICROS_PER_SEC));

		if (!Time::IsValidTime(hh_32, mm_32, ss_32, micros)) {
			throw ConversionException("Time out of range: %d:%d:%d.%d", hh_32, mm_32, ss_32, micros);
		}
		return Time::FromTime(hh_32, mm_32, ss_32, micros);
	}
};

template <typename T>
static void ExecuteMakeTime(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 3);
	auto &yyyy = input.data[0];
	auto &mm = input.data[1];
	auto &dd = input.data[2];

	TernaryExecutor::Execute<T, T, double, dtime_t>(yyyy, mm, dd, result, input.size(),
	                                                MakeTimeOperator::Operation<T, T, double, dtime_t>);
}

struct MakeTimestampOperator {
	template <typename YYYY, typename MM, typename DD, typename HR, typename MN, typename SS, typename RESULT_TYPE>
	static RESULT_TYPE Operation(YYYY yyyy, MM mm, DD dd, HR hr, MN mn, SS ss) {
		const auto d = MakeDateOperator::Operation<YYYY, MM, DD, date_t>(yyyy, mm, dd);
		const auto t = MakeTimeOperator::Operation<HR, MN, SS, dtime_t>(hr, mn, ss);
		return Timestamp::FromDatetime(d, t);
	}

	template <typename T, typename RESULT_TYPE>
	static RESULT_TYPE Operation(T micros) {
		return timestamp_t(micros);
	}
};

template <typename T>
static void ExecuteMakeTimestamp(DataChunk &input, ExpressionState &state, Vector &result) {
	if (input.ColumnCount() == 1) {
		auto func = MakeTimestampOperator::Operation<T, timestamp_t>;
		UnaryExecutor::Execute<T, timestamp_t>(input.data[0], result, input.size(), func);
		return;
	}

	D_ASSERT(input.ColumnCount() == 6);

	auto func = MakeTimestampOperator::Operation<T, T, T, T, T, double, timestamp_t>;
	SenaryExecutor::Execute<T, T, T, T, T, double, timestamp_t>(input, result, func);
}

ScalarFunctionSet MakeDateFun::GetFunctions() {
	ScalarFunctionSet make_date("make_date");
	make_date.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                     LogicalType::DATE, ExecuteMakeDate<int64_t>));

	child_list_t<LogicalType> make_date_children {
	    {"year", LogicalType::BIGINT}, {"month", LogicalType::BIGINT}, {"day", LogicalType::BIGINT}};
	make_date.AddFunction(
	    ScalarFunction({LogicalType::STRUCT(make_date_children)}, LogicalType::DATE, ExecuteStructMakeDate<int64_t>));
	return make_date;
}

ScalarFunction MakeTimeFun::GetFunction() {
	return ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::DOUBLE}, LogicalType::TIME,
	                      ExecuteMakeTime<int64_t>);
}

ScalarFunctionSet MakeTimestampFun::GetFunctions() {
	ScalarFunctionSet operator_set("make_timestamp");
	operator_set.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                         LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::DOUBLE},
	                                        LogicalType::TIMESTAMP, ExecuteMakeTimestamp<int64_t>));
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::BIGINT}, LogicalType::TIMESTAMP, ExecuteMakeTimestamp<int64_t>));
	return operator_set;
}

} // namespace duckdb
