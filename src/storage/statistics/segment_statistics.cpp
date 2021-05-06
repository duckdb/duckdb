#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

SegmentStatistics::SegmentStatistics(LogicalType type, idx_t type_size) : type(move(type)), type_size(type_size) {
	Reset();
}

SegmentStatistics::SegmentStatistics(LogicalType type, idx_t type_size, unique_ptr<BaseStatistics> stats)
    : type(move(type)), type_size(type_size), statistics(move(stats)) {
}

void SegmentStatistics::Reset() {
	statistics = BaseStatistics::CreateEmpty(type);
}

} // namespace duckdb
