#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

SegmentStatistics::SegmentStatistics(LogicalType type, idx_t type_size) : type(type), type_size(type_size) {
	Reset();
}

SegmentStatistics::SegmentStatistics(LogicalType type, idx_t type_size, unique_ptr<BaseStatistics> stats)
    : type(type), type_size(type_size), statistics(move(stats)) {
}

void SegmentStatistics::Reset() {
	statistics = BaseStatistics::CreateEmpty(type);
}

bool SegmentStatistics::CheckZonemap(TableFilter &filter) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return ((NumericStatistics &) *statistics).CheckZonemap(filter.comparison_type, filter.constant);
	case PhysicalType::VARCHAR:
		return ((StringStatistics &) *statistics).CheckZonemap(filter.comparison_type, filter.constant.ToString());
	default:
		return true;
	}
}

}