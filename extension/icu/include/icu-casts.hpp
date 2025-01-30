//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-datefunc.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "icu-datefunc.hpp"

namespace duckdb {

struct ICUMakeDate : public ICUDateFunc {
	static date_t Operation(icu::Calendar *calendar, timestamp_t instant);

	static bool CastToDate(Vector &source, Vector &result, idx_t count, CastParameters &parameters);

	static BoundCastInfo BindCastToDate(BindCastInput &input, const LogicalType &source, const LogicalType &target);

	static void AddCasts(DatabaseInstance &db);

	static date_t ToDate(ClientContext &context, timestamp_t instant);
};

struct ICUToTimeTZ : public ICUDateFunc {
	static dtime_tz_t Operation(icu::Calendar *calendar, dtime_tz_t timetz);

	static bool ToTimeTZ(icu::Calendar *calendar, timestamp_t instant, dtime_tz_t &result);

	static bool CastToTimeTZ(Vector &source, Vector &result, idx_t count, CastParameters &parameters);

	static BoundCastInfo BindCastToTimeTZ(BindCastInput &input, const LogicalType &source, const LogicalType &target);

	static void AddCasts(DatabaseInstance &db);
};

} // namespace duckdb
