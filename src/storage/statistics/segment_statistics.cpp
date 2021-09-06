#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

SegmentStatistics::SegmentStatistics(LogicalType type) : type(move(type)) {
	Reset();
}

SegmentStatistics::SegmentStatistics(LogicalType type, unique_ptr<BaseStatistics> stats)
    : type(move(type)), statistics(move(stats)) {
	if (!statistics) {
		Reset();
	}
}

void SegmentStatistics::Reset() {
	statistics = BaseStatistics::CreateEmpty(type);
	statistics->validity_stats = make_unique<ValidityStatistics>(false);
}

} // namespace duckdb
