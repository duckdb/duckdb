#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/senary_executor.hpp"

#include <cmath>

namespace duckdb {

struct MakeDateOperator {
	template <typename YYYY, typename MM, typename DD, typename RESULT_TYPE>
	static RESULT_TYPE Operation(YYYY yyyy, MM mm, DD dd) {
		return Date::FromDate(yyyy, mm, dd);
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
static void ExecuteStructMakeDate(DataChunk &input, ExpressionState &state, Vector &result) {
	// this should be guaranteed by the binder
	D_ASSERT(input.ColumnCount() == 1);
	auto &vec = input.data[0];

	auto &children = StructVector::GetEntries(vec);
	D_ASSERT(children.size() == 3);
	auto &yyyy = *children[0];
	auto &mm = *children[1];
	auto &dd = *children[2];

	TernaryExecutor::Execute<T, T, T, date_t>(yyyy, mm, dd, result, input.size(), Date::FromDate);
}

struct MakeTimeOperator {
	template <typename HH, typename MM, typename SS, typename RESULT_TYPE>
	static RESULT_TYPE Operation(HH hh, MM mm, SS ss) {
		int64_t secs = ss;
		int64_t micros = std::round((ss - secs) * Interval::MICROS_PER_SEC);
		return Time::FromTime(hh, mm, secs, micros);
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
};

template <typename T>
static void ExecuteMakeTimestamp(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 6);

	auto func = MakeTimestampOperator::Operation<T, T, T, T, T, double, timestamp_t>;
	SenaryExecutor::Execute<T, T, T, T, T, double, timestamp_t>(input, result, func);
}

void MakeDateFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet make_date("make_date");
	make_date.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT},
	                                     LogicalType::DATE, ExecuteMakeDate<int64_t>));

	child_list_t<LogicalType> make_date_children {
	    {"year", LogicalType::BIGINT}, {"month", LogicalType::BIGINT}, {"day", LogicalType::BIGINT}};
	make_date.AddFunction(
	    ScalarFunction({LogicalType::STRUCT(make_date_children)}, LogicalType::DATE, ExecuteStructMakeDate<int64_t>));
	set.AddFunction(make_date);

	ScalarFunctionSet make_time("make_time");
	make_time.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::DOUBLE},
	                                     LogicalType::TIME, ExecuteMakeTime<int64_t>));
	set.AddFunction(make_time);

	ScalarFunctionSet make_timestamp("make_timestamp");
	make_timestamp.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
	                                           LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::DOUBLE},
	                                          LogicalType::TIMESTAMP, ExecuteMakeTimestamp<int64_t>));
	set.AddFunction(make_timestamp);
}

} // namespace duckdb
