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
struct ValidityMask;

class DateLookupCache {
public:
	constexpr static int32_t CACHE_MIN_DATE = 0;     // 1970-01-01
	constexpr static int32_t CACHE_MAX_DATE = 29584; // 2050-12-31

public:
	DateLookupCache();

	//! Extracts the component, or sets the validity mask to NULL if the date is infinite
	int64_t ExtractYear(date_t date, ValidityMask &mask, idx_t idx) const;
	int64_t ExtractYear(timestamp_t ts, ValidityMask &mask, idx_t idx) const;
	int64_t ExtractMonth(date_t date, ValidityMask &mask, idx_t idx) const;
	int64_t ExtractMonth(timestamp_t ts, ValidityMask &mask, idx_t idx) const;
	int64_t ExtractDay(date_t date, ValidityMask &mask, idx_t idx) const;
	int64_t ExtractDay(timestamp_t ts, ValidityMask &mask, idx_t idx) const;

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
