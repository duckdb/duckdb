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
			is_set = false;
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

		auto copy_count = MinValue<idx_t>(size, StringStatsData::CURRENT_MAX_STRING_MINMAX_SIZE);
		if (is_set) {
			// compare to current min/max
			auto min_cmp_count = MinValue<idx_t>(copy_count, min_size);
			auto min_cmp = memcmp(data, min, min_cmp_count);
			if (min_cmp < 0 || (min_cmp == 0 && size < min_size)) {
				memcpy(min, data, copy_count);
				min_size = size;
			}
			auto max_cmp_count = MinValue<idx_t>(copy_count, max_size);
			int max_cmp = memcmp(data, max, max_cmp_count);
			if (max_cmp > 0 || (max_cmp == 0 && size > max_size)) {
				memcpy(max, data, copy_count);
				max_size = size;
			}
		} else {
			memcpy(min, data, copy_count);
			memcpy(max, data, copy_count);
			min_size = size;
			max_size = size;
			is_set = true;
		}
		if (size > max_string_length) {
			max_string_length = UnsafeNumericCast<uint32_t>(size);
		}
		if (size < min_string_length) {
			min_string_length = UnsafeNumericCast<uint32_t>(size);
		}
		total_string_length += size;
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

	bool HasStats() const {
		return is_set;
	}

	void Merge(BaseStatistics &other) const {
		if (is_geometry) {
			GeometryStats::GetDataUnsafe(other).Merge(geometry_stats);
		} else {
			StringStats::Merge(other, *this);
		}
	}

private:
	data_t min[StringStatsData::CURRENT_MAX_STRING_MINMAX_SIZE];
	data_t max[StringStatsData::CURRENT_MAX_STRING_MINMAX_SIZE];
	idx_t min_size = 0;
	idx_t max_size = 0;
	bool is_set = false;
	bool has_unicode = false;
	uint32_t max_string_length = 0;
	uint32_t min_string_length = StringStatsData::MAXIMUM_MIN_STRING_LENGTH;
	idx_t total_string_length = 0;
	bool is_varchar = true;
	bool is_geometry = false;
	GeometryStatsData geometry_stats;
};

} // namespace duckdb
