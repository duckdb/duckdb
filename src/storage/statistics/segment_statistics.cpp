#include "duckdb/storage/statistics/segment_statistics.hpp"

#include "duckdb/common/exception.hpp"

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
	statistics = BaseStatistics::CreateEmpty(type, StatisticsType::LOCAL_STATS);
}

} // namespace duckdb
