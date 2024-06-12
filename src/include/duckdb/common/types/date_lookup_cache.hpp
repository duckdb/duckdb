//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/date_lookup_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class DateLookupCache {
public:
	constexpr static int32_t CACHE_MIN_DATE = 0;     // 1970-01-01
	constexpr static int32_t CACHE_MAX_DATE = 29584; // 2050-12-31

public:
	DateLookupCache();

	void Convert(date_t date, int32_t &out_year, int32_t &out_month, int32_t &out_day);
	int32_t ExtractMonth(date_t date);
	int32_t ExtractDay(date_t date);

	//! Extracts the year, or sets the validity mask to NULL if the date is infinite
	int64_t ExtractYear(date_t date, ValidityMask &mask, idx_t idx);
	int64_t ExtractYear(timestamp_t ts, ValidityMask &mask, idx_t idx);

private:
	void BuildCache();

private:
	struct CachedDateInfo {
		uint16_t year;
		uint8_t month;
		uint8_t day;
	};

	unsafe_unique_array<CachedDateInfo> cache;
};

} // namespace duckdb
