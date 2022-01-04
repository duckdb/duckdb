//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-datefunc.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "icu-collate.hpp"

#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ICUDateFunc {
	using CalendarPtr = unique_ptr<icu::Calendar>;

	struct BindData : public FunctionData {
		explicit BindData(ClientContext &context);
		BindData(const BindData &other);

		CalendarPtr calendar;

		bool Equals(FunctionData &other_p) override;
		unique_ptr<FunctionData> Copy() override;
	};

	//! Binds a default calendar object for use by the function
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments);

	//! Gets the timestamp from the calendar, throwing if it is not in range.
	static timestamp_t GetTime(icu::Calendar *calendar, uint64_t micros = 0);
	//! Gets the timestamp from the calendar, assuming it is in range.
	static timestamp_t GetTimeUnsafe(icu::Calendar *calendar, uint64_t micros = 0);
	//! Sets the calendar to the timestamp, returning the unused µs part
	static uint64_t SetTime(icu::Calendar *calendar, timestamp_t date);
	//! Extracts the field from the calendar
	static int32_t ExtractField(icu::Calendar *calendar, UCalendarDateFields field);
	//! Subtracts the field of the given date from the calendar
	static int64_t SubtractField(icu::Calendar *calendar, UCalendarDateFields field, timestamp_t end_date);

	//! Truncates the calendar time to the given part precision
	typedef void (*part_trunc_t)(icu::Calendar *calendar, uint64_t &micros);
	static part_trunc_t TruncationFactory(DatePartSpecifier part);

	//! Subtracts the two times at the given part precision
	typedef int64_t (*part_sub_t)(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date);
	static part_sub_t SubtractFactory(DatePartSpecifier part);
};

} // namespace duckdb
