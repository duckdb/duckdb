//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_interval.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/interval.hpp"
#include "zstd/common/xxhash.hpp"

namespace duckdb {

struct ParquetIntervalUtils {
	static constexpr const idx_t PARQUET_INTERVAL_SIZE = 12;

	static void Encode(const interval_t &interval, data_ptr_t target) {
		auto days = int64_t(interval.days);
		auto micros = interval.micros;
		if (micros / Interval::MICROS_PER_MSEC > NumericLimits<uint32_t>::Maximum()) {
			// the milliseconds do not fit in 32 bits: move the whole days into the days field
			days += micros / Interval::MICROS_PER_DAY;
			micros %= Interval::MICROS_PER_DAY;
		}
		Store<uint32_t>(static_cast<uint32_t>(interval.months), target);
		Store<uint32_t>(NumericCast<uint32_t>(days), target + sizeof(uint32_t));
		Store<uint32_t>(NumericCast<uint32_t>(micros / Interval::MICROS_PER_MSEC), target + sizeof(uint32_t) * 2);
	}

	static uint64_t HashNormalized(const interval_t &interval) {
		data_t bytes[PARQUET_INTERVAL_SIZE];
		Encode(interval.Normalize(), bytes);
		return duckdb_zstd::XXH64(bytes, sizeof(bytes), 0);
	}
};

} // namespace duckdb
