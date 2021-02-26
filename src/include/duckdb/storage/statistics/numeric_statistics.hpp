//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/numeric_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

class NumericStatistics : public BaseStatistics {
public:
	explicit NumericStatistics(LogicalType type);
	NumericStatistics(LogicalType type, Value min, Value max);

	//! The minimum value of the segment
	Value min;
	//! The maximum value of the segment
	Value max;

public:
	void Merge(const BaseStatistics &other) override;
	bool CheckZonemap(ExpressionType comparison_type, const Value &constant);

	unique_ptr<BaseStatistics> Copy() override;
	void Serialize(Serializer &serializer) override;
	static unique_ptr<BaseStatistics> Deserialize(Deserializer &source, LogicalType type);
	void Verify(Vector &vector, idx_t count) override;

	string ToString() override;

private:
	template <class T>
	void TemplatedVerify(Vector &vector, idx_t count);

public:
	template <class T>
	static inline void UpdateValue(T new_value, T &min, T &max) {
		if (LessThan::Operation(new_value, min)) {
			min = new_value;
		}
		if (GreaterThan::Operation(new_value, max)) {
			max = new_value;
		}
	}

	template <class T>
	static inline void Update(SegmentStatistics &stats, T new_value);

	template <>
	inline void Update<int8_t>(SegmentStatistics &stats, int8_t new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<int8_t>(new_value, nstats.min.value_.tinyint, nstats.max.value_.tinyint);
	}

	template <>
	inline void Update<int16_t>(SegmentStatistics &stats, int16_t new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<int16_t>(new_value, nstats.min.value_.smallint, nstats.max.value_.smallint);
	}

	template <>
	inline void Update<int32_t>(SegmentStatistics &stats, int32_t new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<int32_t>(new_value, nstats.min.value_.integer, nstats.max.value_.integer);
	}

	template <>
	inline void Update<int64_t>(SegmentStatistics &stats, int64_t new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<int64_t>(new_value, nstats.min.value_.bigint, nstats.max.value_.bigint);
	}

	template <>
	inline void Update<uint8_t>(SegmentStatistics &stats, uint8_t new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<uint8_t>(new_value, nstats.min.value_.utinyint, nstats.max.value_.utinyint);
	}

	template <>
	inline void Update<uint16_t>(SegmentStatistics &stats, uint16_t new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<uint16_t>(new_value, nstats.min.value_.usmallint, nstats.max.value_.usmallint);
	}

	template <>
	inline void Update<uint32_t>(SegmentStatistics &stats, uint32_t new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<uint32_t>(new_value, nstats.min.value_.uinteger, nstats.max.value_.uinteger);
	}

	template <>
	inline void Update<uint64_t>(SegmentStatistics &stats, uint64_t new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<uint64_t>(new_value, nstats.min.value_.ubigint, nstats.max.value_.ubigint);
	}

	template <>
	inline void Update<hugeint_t>(SegmentStatistics &stats, hugeint_t new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<hugeint_t>(new_value, nstats.min.value_.hugeint, nstats.max.value_.hugeint);
	}

	template <>
	inline void Update<float>(SegmentStatistics &stats, float new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<float>(new_value, nstats.min.value_.float_, nstats.max.value_.float_);
	}

	template <>
	inline void Update<double>(SegmentStatistics &stats, double new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<double>(new_value, nstats.min.value_.double_, nstats.max.value_.double_);
	}

	template <>
	void Update<interval_t>(SegmentStatistics &stats, interval_t new_value) {
	}
};

} // namespace duckdb
