//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/parquet_write_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_writer.hpp"
#include "parquet_geometry.hpp"

namespace duckdb {

class ColumnWriterStatistics {
public:
	virtual ~ColumnWriterStatistics();

	virtual bool HasStats();
	virtual string GetMin();
	virtual string GetMax();
	virtual string GetMinValue();
	virtual string GetMaxValue();
	virtual bool CanHaveNaN();
	virtual bool HasNaN();
	virtual bool MinIsExact();
	virtual bool MaxIsExact();

	virtual bool HasGeoStats();
	virtual optional_ptr<GeometryStatsData> GetGeoStats();
	virtual void WriteGeoStats(duckdb_parquet::GeospatialStatistics &stats);

public:
	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

template <class SRC, class T, class OP>
class NumericStatisticsState : public ColumnWriterStatistics {
public:
	NumericStatisticsState() : min(NumericLimits<T>::Maximum()), max(NumericLimits<T>::Minimum()) {
	}

	T min;
	T max;

public:
	bool HasStats() override {
		return min <= max;
	}

	string GetMin() override {
		return NumericLimits<SRC>::IsSigned() ? GetMinValue() : string();
	}
	string GetMax() override {
		return NumericLimits<SRC>::IsSigned() ? GetMaxValue() : string();
	}
	string GetMinValue() override {
		return HasStats() ? string(char_ptr_cast(&min), sizeof(T)) : string();
	}
	string GetMaxValue() override {
		return HasStats() ? string(char_ptr_cast(&max), sizeof(T)) : string();
	}
};

template <class SRC, class T, class OP>
class FloatingPointStatisticsState : public NumericStatisticsState<SRC, T, OP> {
public:
	bool has_nan = false;

public:
	bool CanHaveNaN() override {
		return true;
	}
	bool HasNaN() override {
		return has_nan;
	}
};

class StringStatisticsState : public ColumnWriterStatistics {
	static constexpr const idx_t MAX_STRING_STATISTICS_SIZE = 256;

public:
	explicit StringStatisticsState(LogicalTypeId type = LogicalTypeId::VARCHAR)
	    : type(type), has_stats(false), min_truncated(false), max_truncated(false), min(), max() {
	}

	LogicalTypeId type;
	bool has_stats;
	bool min_truncated;
	bool max_truncated;
	bool failed_truncate = false;
	string min;
	string max;

public:
	bool HasStats() override {
		return has_stats;
	}

	void Update(const string_t &val) {
		if (failed_truncate) {
			return;
		}
		if (!has_stats || LessThan::Operation(val, string_t(min))) {
			if (val.GetSize() > MAX_STRING_STATISTICS_SIZE) {
				// string value exceeds our max string stats size - truncate
				min = TruncateMin(val, MAX_STRING_STATISTICS_SIZE);
				min_truncated = true;
			} else {
				min = val.GetString();
				min_truncated = false;
			}
		}
		if (!has_stats || GreaterThan::Operation(val, string_t(max))) {
			if (val.GetSize() > MAX_STRING_STATISTICS_SIZE) {
				// string value exceeds our max string stats size - truncate
				if (!TryTruncateMax(val, MAX_STRING_STATISTICS_SIZE, max)) {
					// we failed to truncate - this can happen in some edge cases
					// skip stats for this column
					failed_truncate = true;
					has_stats = false;
					min = string();
					max = string();
					return;
				}
				max_truncated = true;
			} else {
				max = val.GetString();
				max_truncated = false;
			}
		}
		has_stats = true;
	}

	static inline bool IsCharacter(char c) {
		return (c & 0xc0) != 0x80;
	}

	string TruncateMin(string_t str, idx_t max_size) {
		// truncate a string for the min value
		// since 'AAA' < 'AAAA', we can just truncate the string
		D_ASSERT(str.GetSize() > max_size);
		if (type == LogicalTypeId::BLOB) {
			// for blobs - just truncate directly
			return string(str.GetData(), max_size);
		}
		D_ASSERT(type == LogicalTypeId::VARCHAR);
		// for varchar we need to truncate to a valid UTF8 string - so we need to truncate to the last valid UTF8 byte
		auto str_data = str.GetData();
		for (; max_size > 0; max_size--) {
			if (IsCharacter(str_data[max_size])) {
				break;
			}
		}
		return string(str_data, max_size);
	}

	bool TryTruncateMax(string_t str, idx_t max_size, string &result, data_t max_byte) {
		auto data = const_data_ptr_cast(str.GetData());

		// find the last position in the string which we can increment for the truncation
		// if ALL characters are above the max byte we cannot truncate
		idx_t increment_pos;
		for (increment_pos = max_size; increment_pos > 0; increment_pos--) {
			idx_t str_idx = increment_pos - 1;
			if (data[str_idx] < max_byte) {
				// found the increment position
				break;
			}
		}
		if (increment_pos == 0) {
			// all characters are above the max byte - we cannot truncate - return false
			return false;
		}
		// set up the result string - we don't care about anything after the increment pos
		result = string(str.GetData(), increment_pos);
		// actually increment
		result[increment_pos - 1]++;
		return true;
	}

	bool TryTruncateMax(string_t str, idx_t max_size, string &result) {
		// truncate a string for the max value
		// since 'XXX' < 'XXXX', we need to "increment" a byte to get a correct max value
		// i.e. we need to generate 'XXY' as a string
		// note that this is not necessarily always possible
		D_ASSERT(str.GetSize() > max_size);
		if (type == LogicalTypeId::BLOB) {
			// for blobs we can always increment bytes - we just can't increment past the max of a single byte (2^8)
			return TryTruncateMax(str, max_size, result, static_cast<data_t>(0xFF));
		}
		D_ASSERT(type == LogicalTypeId::VARCHAR);
		// for varchar the situation is more complex - we need to truncate to a valid UTF8 string and increment
		// for now we only increment ASCII characters (characters below 0x7F)
		return TryTruncateMax(str, max_size, result, static_cast<data_t>(0x7F));
	}

	string GetMin() override {
		return GetMinValue();
	}
	string GetMax() override {
		return GetMaxValue();
	}
	string GetMinValue() override {
		return HasStats() ? min : string();
	}
	string GetMaxValue() override {
		return HasStats() ? max : string();
	}

	bool MinIsExact() override {
		return !min_truncated;
	}

	bool MaxIsExact() override {
		return !max_truncated;
	}
};

class UUIDStatisticsState : public ColumnWriterStatistics {
public:
	bool has_stats = false;
	data_t min[16] = {0};
	data_t max[16] = {0};

public:
	bool HasStats() override {
		return has_stats;
	}

	string GetMin() override {
		return GetMinValue();
	}
	string GetMax() override {
		return GetMaxValue();
	}
	string GetMinValue() override {
		return HasStats() ? string(char_ptr_cast(min), 16) : string();
	}
	string GetMaxValue() override {
		return HasStats() ? string(char_ptr_cast(max), 16) : string();
	}
};

class GeoStatisticsState final : public ColumnWriterStatistics {
public:
	explicit GeoStatisticsState() : has_stats(false) {
		geo_stats.SetEmpty();
	}

	bool has_stats;
	GeometryStatsData geo_stats;

public:
	void Update(const string_t &val) {
		geo_stats.Update(val);
		has_stats = true;
	}
	bool HasGeoStats() override {
		return has_stats;
	}
	optional_ptr<GeometryStatsData> GetGeoStats() override {
		return geo_stats;
	}
	void WriteGeoStats(duckdb_parquet::GeospatialStatistics &stats) override {
		const auto &types = geo_stats.types;
		const auto &bbox = geo_stats.extent;

		if (bbox.HasXY()) {
			stats.__isset.bbox = true;
			stats.bbox.xmin = bbox.x_min;
			stats.bbox.xmax = bbox.x_max;
			stats.bbox.ymin = bbox.y_min;
			stats.bbox.ymax = bbox.y_max;

			if (bbox.HasZ()) {
				stats.bbox.__isset.zmin = true;
				stats.bbox.__isset.zmax = true;
				stats.bbox.zmin = bbox.z_min;
				stats.bbox.zmax = bbox.z_max;
			}
			if (bbox.HasM()) {
				stats.bbox.__isset.mmin = true;
				stats.bbox.__isset.mmax = true;
				stats.bbox.mmin = bbox.m_min;
				stats.bbox.mmax = bbox.m_max;
			}
		}

		stats.__isset.geospatial_types = true;
		stats.geospatial_types = types.ToWKBList();
	}
};

} // namespace duckdb
