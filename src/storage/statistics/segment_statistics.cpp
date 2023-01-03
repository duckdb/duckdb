#include "duckdb/storage/statistics/segment_statistics.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

SegmentStatistics::SegmentStatistics(LogicalType type) : type(Move(type)) {
	Reset();
}

SegmentStatistics::SegmentStatistics(LogicalType type, unique_ptr<BaseStatistics> stats)
    : type(Move(type)), statistics(Move(stats)) {
	if (!statistics) {
		Reset();
	}
}

void SegmentStatistics::Reset() {
	statistics = BaseStatistics::CreateEmpty(type, StatisticsType::LOCAL_STATS);
}

} // namespace duckdb
