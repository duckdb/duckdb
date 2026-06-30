//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/stats_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/string_type.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/main/error_manager.hpp"

namespace duckdb {

struct BaseStatsWriter {
	void SetHasNull() {
		has_null = true;
	}
	void SetHasValid() {
		has_valid = true;
	}

	bool HasStats() const {
		return has_null || has_valid;
	}

	bool AnyValid() const {
		return has_valid;
	}

	void ClearBase() {
		has_null = false;
		has_valid = false;
	}

	void MergeBase(BaseStatistics &other) const {
		if (has_null) {
			other.SetHasNullFast();
		}
		if (has_valid) {
			other.SetHasNoNullFast();
		}
	}

private:
	bool has_null = false;
	bool has_valid = false;
};

template <class T>
struct StatsWriter : public BaseStatsWriter {
	explicit StatsWriter() {
		Clear();
	}

	inline void Clear() {
		ClearBase();
		min = NumericLimits<T>::Maximum();
		max = NumericLimits<T>::Minimum();
	}

	void Update(T new_value) {
		SetHasValid();
		UpdateMinMax(new_value);
	}

	void UpdateMinMax(T new_value) {
		min = LessThan::Operation(new_value, min) ? new_value : min;
		max = GreaterThan::Operation(new_value, max) ? new_value : max;
	}

	void Merge(BaseStatistics &target) const {
		MergeBase(target);
		if (AnyValid()) {
			target.UpdateNumericStats<T>(min);
			target.UpdateNumericStats<T>(max);
		}
	}

private:
	T min;
	T max;
};

template <>
struct StatsWriter<void> : public BaseStatsWriter {
	explicit StatsWriter() {
		Clear();
	}

	inline void Clear() {
		ClearBase();
	}

	void Merge(BaseStatistics &target) const {
		MergeBase(target);
	}
};

template <>
struct StatsWriter<string_t> : public BaseStatsWriter {
	friend struct StringStats;

	explicit StatsWriter(const LogicalType &type)
	    : is_varchar(type.id() == LogicalTypeId::VARCHAR), is_geometry(type.id() == LogicalTypeId::GEOMETRY) {
		Clear();
	}

	inline void Clear() {
		ClearBase();
		if (is_geometry) {
			geometry_stats.SetEmpty();
		} else {
			is_set = false;
			min_size = 0;
			max_size = 0;
			has_unicode = false;
			max_string_length = 0;
			min_string_length = StringStatsData::MAXIMUM_MIN_STRING_LENGTH;
			total_string_length = 0;
		}
	}

	inline void Update(const string_t &value) {
		SetHasValid();
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

	void Merge(BaseStatistics &other) const {
		MergeBase(other);
		if (is_geometry) {
			GeometryStats::GetDataUnsafe(other).Merge(geometry_stats);
		} else {
			StringStats::Merge(other, *this);
		}
	}

private:
	data_t min[StringStatsData::CURRENT_MAX_STRING_MINMAX_SIZE];
	data_t max[StringStatsData::CURRENT_MAX_STRING_MINMAX_SIZE];
	idx_t min_size;
	idx_t max_size;
	bool is_set;
	bool has_unicode;
	uint32_t max_string_length;
	uint32_t min_string_length;
	idx_t total_string_length;
	bool is_varchar;
	bool is_geometry;
	GeometryStatsData geometry_stats;
};

} // namespace duckdb
