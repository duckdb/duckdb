#include "duckdb/storage/statistics/segment_statistics.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

SegmentStatistics::SegmentStatistics(LogicalType type) : statistics(BaseStatistics::CreateEmpty(std::move(type))) {
}

SegmentStatistics::SegmentStatistics(BaseStatistics stats) : statistics(std::move(stats)) {
}

} // namespace duckdb
