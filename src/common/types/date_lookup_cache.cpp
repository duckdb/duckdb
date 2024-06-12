#include "duckdb/common/types/date_lookup_cache.hpp"

namespace duckdb {


DateLookupCache::DateLookupCache() {
  BuildCache();
}

inline idx_t GetDateCacheEntry(date_t day) {
  return UnsafeNumericCast<idx_t>(day.days - DateLookupCache::CACHE_MIN_DATE);
}

void DateLookupCache::Convert(date_t date, int32_t &out_year, int32_t &out_month, int32_t &out_day) {
  if (DUCKDB_UNLIKELY(date.days < CACHE_MIN_DATE || date.days > CACHE_MAX_DATE)) {
    Date::Convert(date, out_year, out_month, out_day);
    return;
  }
  auto &cache_entry = cache[GetDateCacheEntry(date)];
  out_year = cache_entry.year;
  out_month = cache_entry.month;
  out_day = cache_entry.day;
}

int64_t DateLookupCache::ExtractYear(date_t date, ValidityMask &mask, idx_t idx) {
  if (DUCKDB_UNLIKELY(date.days < CACHE_MIN_DATE || date.days > CACHE_MAX_DATE)) {
    if (DUCKDB_UNLIKELY(!Value::IsFinite(date))) {
      mask.SetInvalid(idx);
      return 0;
    }
    return Date::ExtractYear(date);
  }
  return cache[GetDateCacheEntry(date)].year;
}

int64_t DateLookupCache::ExtractYear(timestamp_t ts, ValidityMask &mask, idx_t idx) {
  return ExtractYear(Timestamp::GetDate(ts), mask, idx);
}

int32_t DateLookupCache::ExtractMonth(date_t date) {
  if (DUCKDB_UNLIKELY(date.days < CACHE_MIN_DATE || date.days > CACHE_MAX_DATE)) {
    return Date::ExtractMonth(date);
  }
  return cache[GetDateCacheEntry(date)].month;
}

int32_t DateLookupCache::ExtractDay(date_t date) {
  if (DUCKDB_UNLIKELY(date.days < CACHE_MIN_DATE || date.days > CACHE_MAX_DATE)) {
    return Date::ExtractDay(date);
  }
  return cache[GetDateCacheEntry(date)].day;
}

void DateLookupCache::BuildCache() {
  D_ASSERT(CACHE_MAX_DATE > CACHE_MIN_DATE);
  cache = make_unsafe_uniq_array<CachedDateInfo>(CACHE_MAX_DATE - CACHE_MIN_DATE);
  // this can certainly be optimized
  // but maybe we don't care
  for(int32_t d = CACHE_MIN_DATE; d < CACHE_MAX_DATE; d++) {
    int32_t year, month, day;
    Date::Convert(date_t(d), year, month, day);

    auto &cache_entry = cache[GetDateCacheEntry(date_t(d))];
    cache_entry.year = UnsafeNumericCast<uint16_t>(year);
    cache_entry.month = UnsafeNumericCast<uint8_t>(month);
    cache_entry.day = UnsafeNumericCast<uint8_t>(day);
  }
}

}
