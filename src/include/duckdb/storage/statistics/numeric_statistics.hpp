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
#include "duckdb/storage/statistics/validity_statistics.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/windows_undefs.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"

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
	FilterPropagateResult CheckZonemap(ExpressionType comparison_type, const Value &constant);

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
};

template <>
void NumericStatistics::Update<int8_t>(SegmentStatistics &stats, int8_t new_value);
template <>
void NumericStatistics::Update<int16_t>(SegmentStatistics &stats, int16_t new_value);
template <>
void NumericStatistics::Update<int32_t>(SegmentStatistics &stats, int32_t new_value);
template <>
void NumericStatistics::Update<int64_t>(SegmentStatistics &stats, int64_t new_value);
template <>
void NumericStatistics::Update<uint8_t>(SegmentStatistics &stats, uint8_t new_value);
template <>
void NumericStatistics::Update<uint16_t>(SegmentStatistics &stats, uint16_t new_value);
template <>
void NumericStatistics::Update<uint32_t>(SegmentStatistics &stats, uint32_t new_value);
template <>
void NumericStatistics::Update<uint64_t>(SegmentStatistics &stats, uint64_t new_value);
template <>
void NumericStatistics::Update<hugeint_t>(SegmentStatistics &stats, hugeint_t new_value);
template <>
void NumericStatistics::Update<float>(SegmentStatistics &stats, float new_value);
template <>
void NumericStatistics::Update<double>(SegmentStatistics &stats, double new_value);
template <>
void NumericStatistics::Update<interval_t>(SegmentStatistics &stats, interval_t new_value);
template <>
void NumericStatistics::Update<list_entry_t>(SegmentStatistics &stats, list_entry_t new_value);

} // namespace duckdb
