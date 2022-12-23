//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-dateadd.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "icu-datefunc.hpp"

namespace duckdb {

void RegisterICUDateAddFunctions(ClientContext &context);

struct ICUCalendarAdd {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right, icu::Calendar *calendar) {
		throw InternalException("Unimplemented type for ICUCalendarAdd");
	}
};

struct ICUCalendarSub : public ICUDateFunc {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right, icu::Calendar *calendar) {
		throw InternalException("Unimplemented type for ICUCalendarSub");
	}
};

template <>
timestamp_t ICUCalendarAdd::Operation(timestamp_t timestamp, interval_t interval, icu::Calendar *calendar);

template <>
timestamp_t ICUCalendarAdd::Operation(interval_t interval, timestamp_t timestamp, icu::Calendar *calendar);

template <>
timestamp_t ICUCalendarSub::Operation(timestamp_t timestamp, interval_t interval, icu::Calendar *calendar);

template <>
interval_t ICUCalendarSub::Operation(timestamp_t end_date, timestamp_t start_date, icu::Calendar *calendar);

} // namespace duckdb
