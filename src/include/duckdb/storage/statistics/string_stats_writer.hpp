//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/string_stats_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/main/error_manager.hpp"

namespace duckdb {

struct StringStatsWriter {
	friend struct StringStats;

	explicit StringStatsWriter(const LogicalType &type)
	    : is_varchar(type.id() == LogicalTypeId::VARCHAR), is_geometry(type.id() == LogicalTypeId::GEOMETRY) {
		Clear();
	}

	inline void Clear() {
		if (!is_geometry) {
			for (idx_t i = 0; i < StringStatsData::MAX_STRING_MINMAX_SIZE; i++) {
				min[i] = 0xFF;
				max[i] = 0;
			}
		} else {
			geometry_stats.SetEmpty();
		}
	}

	inline void Update(const string_t &value) {
		if (is_geometry) {
			geometry_stats.Update(value);
			return;
		}
		auto data = const_data_ptr_cast(value.GetData());
		auto size = value.GetSize();

		//! we can only fit 8 bytes, so we might need to trim our string
		// construct the value
		data_t target[StringStatsData::MAX_STRING_MINMAX_SIZE];
		ConstructValue(data, size, target);

		// update the min and max
		if (StringValueComparison(target, StringStatsData::MAX_STRING_MINMAX_SIZE, min) < 0) {
			memcpy(min, target, StringStatsData::MAX_STRING_MINMAX_SIZE);
		}
		if (StringValueComparison(target, StringStatsData::MAX_STRING_MINMAX_SIZE, max) > 0) {
			memcpy(max, target, StringStatsData::MAX_STRING_MINMAX_SIZE);
		}
		if (size > max_string_length) {
			max_string_length = UnsafeNumericCast<uint32_t>(size);
		}
		if (is_varchar && !has_unicode) {
			auto unicode = Utf8Proc::Analyze(const_char_ptr_cast(data), size);
			if (unicode == UnicodeType::UTF8) {
				has_unicode = true;
			} else if (unicode == UnicodeType::INVALID) {
				throw ErrorManager::InvalidUnicodeError(string(const_char_ptr_cast(data), size),
				                                        "segment statistics update");
			}
		}
	}

	static bool AllCharsEqualTo(const data_t data[], data_t comp) {
		for (idx_t i = 0; i < StringStatsData::MAX_STRING_MINMAX_SIZE; i++) {
			if (data[i] != comp) {
				return false;
			}
		}
		return true;
	}

	static int StringValueComparison(const_data_ptr_t data, idx_t len, const_data_ptr_t comparison) {
		for (idx_t i = 0; i < len; i++) {
			if (data[i] < comparison[i]) {
				return -1;
			} else if (data[i] > comparison[i]) {
				return 1;
			}
		}
		return 0;
	}

	static void ConstructValue(const_data_ptr_t data, idx_t size, data_t target[],
	                           idx_t max_length = StringStatsData::MAX_STRING_MINMAX_SIZE) {
		idx_t value_size = size > max_length ? max_length : size;
		memcpy(target, data, value_size);
		for (idx_t i = value_size; i < max_length; i++) {
			target[i] = '\0';
		}
	}

	bool HasStats() const {
		return min[0] <= max[0];
	}

	void Merge(BaseStatistics &other) const {
		if (is_geometry) {
			GeometryStats::GetDataUnsafe(other).Merge(geometry_stats);
		} else {
			StringStats::Merge(other, *this);
		}
	}

private:
	data_t min[StringStatsData::MAX_STRING_MINMAX_SIZE];
	data_t max[StringStatsData::MAX_STRING_MINMAX_SIZE];
	bool has_unicode = false;
	uint32_t max_string_length = 0;
	bool is_varchar = true;
	bool is_geometry = false;
	GeometryStatsData geometry_stats;
};

} // namespace duckdb
