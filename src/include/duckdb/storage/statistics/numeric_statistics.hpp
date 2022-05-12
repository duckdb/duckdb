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
	explicit NumericStatistics(LogicalType type, StatisticsType stats_type);
	NumericStatistics(LogicalType type, Value min, Value max, StatisticsType stats_type);

	//! The minimum value of the segment
	Value min;
	//! The maximum value of the segment
	Value max;

public:
	void Merge(const BaseStatistics &other) override;

	bool IsConstant() const override;

	FilterPropagateResult CheckZonemap(ExpressionType comparison_type, const Value &constant) const;

	unique_ptr<BaseStatistics> Copy() const override;
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<BaseStatistics> Deserialize(FieldReader &reader, LogicalType type);
	void Verify(Vector &vector, const SelectionVector &sel, idx_t count) const override;

	string ToString() const override;

private:
	template <class T>
	void TemplatedVerify(Vector &vector, const SelectionVector &sel, idx_t count) const;

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
	static inline void Update(SegmentStatistics &stats, T new_value) {
		auto &nstats = (NumericStatistics &)*stats.statistics;
		UpdateValue<T>(new_value, nstats.min.GetReferenceUnsafe<T>(), nstats.max.GetReferenceUnsafe<T>());
	}
};

template <>
void NumericStatistics::Update<interval_t>(SegmentStatistics &stats, interval_t new_value);
template <>
void NumericStatistics::Update<list_entry_t>(SegmentStatistics &stats, list_entry_t new_value);

} // namespace duckdb
