//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/segment_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/table/base_statistics.hpp"
#include "duckdb/storage/table/numeric_statistics.hpp"

namespace duckdb {
struct TableFilter;

class SegmentStatistics {
public:
	SegmentStatistics(LogicalType type, idx_t type_size);
	SegmentStatistics(LogicalType type, idx_t type_size, unique_ptr<BaseStatistics> statistics);

	LogicalType type;
	idx_t type_size;

	//! Type-specific statistics of the segment
	unique_ptr<BaseStatistics> statistics;

public:
	template<class T>
	inline void UpdateStatistics(const T &value) {
		auto &stats = (NumericStatistics<T> &) *statistics;
		stats.Update(value);
	}
	bool CheckZonemap(TableFilter &filter);
	void Reset();
};

template<>
void SegmentStatistics::UpdateStatistics<string_t>(const string_t &value);
template<>
void SegmentStatistics::UpdateStatistics<interval_t>(const interval_t &value);

}
