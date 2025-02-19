//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/parquet_write_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_writer.hpp"
#include "writer/parquet_write_stats.hpp"

namespace duckdb {

class ColumnWriterStatistics {
public:
	virtual ~ColumnWriterStatistics();

	virtual bool HasStats();
	virtual string GetMin();
	virtual string GetMax();
	virtual string GetMinValue();
	virtual string GetMaxValue();

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

class StringStatisticsState : public ColumnWriterStatistics {
	static constexpr const idx_t MAX_STRING_STATISTICS_SIZE = 10000;

public:
	StringStatisticsState() : has_stats(false), values_too_big(false), min(), max() {
	}

	bool has_stats;
	bool values_too_big;
	string min;
	string max;

public:
	bool HasStats() override {
		return has_stats;
	}

	void Update(const string_t &val) {
		if (values_too_big) {
			return;
		}
		auto str_len = val.GetSize();
		if (str_len > MAX_STRING_STATISTICS_SIZE) {
			// we avoid gathering stats when individual string values are too large
			// this is because the statistics are copied into the Parquet file meta data in uncompressed format
			// ideally we avoid placing several mega or giga-byte long strings there
			// we put a threshold of 10KB, if we see strings that exceed this threshold we avoid gathering stats
			values_too_big = true;
			has_stats = false;
			min = string();
			max = string();
			return;
		}
		if (!has_stats || LessThan::Operation(val, string_t(min))) {
			min = val.GetString();
		}
		if (!has_stats || GreaterThan::Operation(val, string_t(max))) {
			max = val.GetString();
		}
		has_stats = true;
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
};

} // namespace duckdb
