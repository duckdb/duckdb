#include "duckdb/common/types/date_lookup_cache.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

DateLookupCache::DateLookupCache() {
	BuildCache();
}

inline idx_t GetDateCacheEntry(date_t day) {
	return UnsafeNumericCast<idx_t>(day.days - DateLookupCache::CACHE_MIN_DATE);
}

int64_t DateLookupCache::ExtractYear(date_t date, ValidityMask &mask, idx_t idx) const {
	if (DUCKDB_UNLIKELY(date.days < CACHE_MIN_DATE || date.days > CACHE_MAX_DATE)) {
		if (DUCKDB_UNLIKELY(!Value::IsFinite(date))) {
			mask.SetInvalid(idx);
			return 0;
		}
		return Date::ExtractYear(date);
	}
	return cache[GetDateCacheEntry(date)].year;
}

int64_t DateLookupCache::ExtractYear(timestamp_t ts, ValidityMask &mask, idx_t idx) const {
	return ExtractYear(Timestamp::GetDate(ts), mask, idx);
}

int64_t DateLookupCache::ExtractMonth(date_t date, ValidityMask &mask, idx_t idx) const {
	if (DUCKDB_UNLIKELY(date.days < CACHE_MIN_DATE || date.days > CACHE_MAX_DATE)) {
		if (DUCKDB_UNLIKELY(!Value::IsFinite(date))) {
			mask.SetInvalid(idx);
			return 0;
		}
		return Date::ExtractMonth(date);
	}
	return cache[GetDateCacheEntry(date)].month;
}

int64_t DateLookupCache::ExtractMonth(timestamp_t ts, ValidityMask &mask, idx_t idx) const {
	return ExtractMonth(Timestamp::GetDate(ts), mask, idx);
}

int64_t DateLookupCache::ExtractDay(date_t date, ValidityMask &mask, idx_t idx) const {
	if (DUCKDB_UNLIKELY(date.days < CACHE_MIN_DATE || date.days > CACHE_MAX_DATE)) {
		if (DUCKDB_UNLIKELY(!Value::IsFinite(date))) {
			mask.SetInvalid(idx);
			return 0;
		}
		return Date::ExtractDay(date);
	}
	return cache[GetDateCacheEntry(date)].day;
}

int64_t DateLookupCache::ExtractDay(timestamp_t ts, ValidityMask &mask, idx_t idx) const {
	return ExtractDay(Timestamp::GetDate(ts), mask, idx);
}

void DateLookupCache::BuildCache() {
	D_ASSERT(CACHE_MAX_DATE > CACHE_MIN_DATE);
	cache = make_unsafe_uniq_array<CachedDateInfo>(CACHE_MAX_DATE - CACHE_MIN_DATE);
	// this can certainly be optimized
	// but maybe we don't care
	for (int32_t d = CACHE_MIN_DATE; d < CACHE_MAX_DATE; d++) {
		int32_t year, month, day;
		date_t date(d);
		Date::Convert(date, year, month, day);

		auto &cache_entry = cache[GetDateCacheEntry(date)];
		cache_entry.year = UnsafeNumericCast<uint16_t>(year);
		cache_entry.month = UnsafeNumericCast<uint8_t>(month);
		cache_entry.day = UnsafeNumericCast<uint8_t>(day);
	}
}

} // namespace duckdb
