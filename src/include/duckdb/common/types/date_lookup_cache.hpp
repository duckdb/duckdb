//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/date_lookup_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

template <class OP>
class DateLookupCache {
public:
	using CACHE_TYPE = uint16_t;
	constexpr static int32_t CACHE_MIN_DATE = 0;     // 1970-01-01
	constexpr static int32_t CACHE_MAX_DATE = 29584; // 2050-12-31

public:
	DateLookupCache() {
		BuildCache();
	}

	//! Extracts the component, or returns nullopt if the date is infinite
	optional<int64_t> ExtractElement(date_t date) const {
		if (DUCKDB_UNLIKELY(date.days < CACHE_MIN_DATE || date.days >= CACHE_MAX_DATE)) {
			if (DUCKDB_UNLIKELY(!Value::IsFinite(date))) {
				return {};
			}
			return OP::template Operation<date_t, int64_t>(date);
		}
		return cache[GetDateCacheEntry(date)];
	}
	optional<int64_t> ExtractElement(timestamp_t ts) const {
		return ExtractElement(Timestamp::GetDate(ts));
	}

private:
	static idx_t GetDateCacheEntry(date_t day) {
		return UnsafeNumericCast<idx_t>(day.days - DateLookupCache::CACHE_MIN_DATE);
	}

	void BuildCache() {
		D_ASSERT(CACHE_MAX_DATE > CACHE_MIN_DATE);
		cache = make_unsafe_uniq_array_uninitialized<CACHE_TYPE>(CACHE_MAX_DATE - CACHE_MIN_DATE);
		for (int32_t d = CACHE_MIN_DATE; d < CACHE_MAX_DATE; d++) {
			date_t date(d);
			auto cache_entry = OP::template Operation<date_t, int64_t>(date);
			cache[GetDateCacheEntry(date)] = UnsafeNumericCast<CACHE_TYPE>(cache_entry);
		}
	}

private:
	unsafe_unique_array<CACHE_TYPE> cache;
};

} // namespace duckdb
