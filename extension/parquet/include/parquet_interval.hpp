//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_interval.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/interval.hpp"
#include "zstd/common/xxhash.hpp"

namespace duckdb {

struct ParquetIntervalUtils {
	static constexpr const idx_t PARQUET_INTERVAL_SIZE = 12;

	static void Encode(const interval_t &interval, data_ptr_t target) {
		if (interval.micros / Interval::MICROS_PER_MSEC > NumericLimits<uint32_t>::Maximum()) {
			EncodeWithCarriedDays(interval, target);
			return;
		}
		EncodeFields(interval.months, interval.days, interval.micros, target);
	}

	static uint64_t HashNormalized(const interval_t &interval) {
		data_t bytes[PARQUET_INTERVAL_SIZE];
		EncodeWithCarriedDays(interval.Normalize(), bytes);
		return duckdb_zstd::XXH64(bytes, sizeof(bytes), 0);
	}

private:
	static void EncodeWithCarriedDays(const interval_t &interval, data_ptr_t target) {
		EncodeFields(interval.months, int64_t(interval.days) + interval.micros / Interval::MICROS_PER_DAY,
		             interval.micros % Interval::MICROS_PER_DAY, target);
	}

	static void EncodeFields(int32_t months, int64_t days, int64_t micros, data_ptr_t target) {
		Store<uint32_t>(static_cast<uint32_t>(months), target);
		Store<uint32_t>(NumericCast<uint32_t>(days), target + sizeof(uint32_t));
		Store<uint32_t>(NumericCast<uint32_t>(micros / Interval::MICROS_PER_MSEC), target + sizeof(uint32_t) * 2);
	}
};

} // namespace duckdb
