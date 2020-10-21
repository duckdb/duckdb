#include "duckdb/storage/table/segment_statistics.hpp"
#include "duckdb/storage/table/string_statistics.hpp"
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
	case PhysicalType::INT8: {
		auto constant = filter.constant.value_.tinyint;
		return ((NumericStatistics<int8_t> &) *statistics).CheckZonemap(filter.comparison_type, constant);
	}
	case PhysicalType::INT16: {
		auto constant = filter.constant.value_.smallint;
		return ((NumericStatistics<int16_t> &) *statistics).CheckZonemap(filter.comparison_type, constant);
	}
	case PhysicalType::INT32: {
		auto constant = filter.constant.value_.integer;
		return ((NumericStatistics<int32_t> &) *statistics).CheckZonemap(filter.comparison_type, constant);
	}
	case PhysicalType::INT64: {
		auto constant = filter.constant.value_.bigint;
		return ((NumericStatistics<int64_t> &) *statistics).CheckZonemap(filter.comparison_type, constant);
	}
	case PhysicalType::INT128: {
		auto constant = filter.constant.value_.hugeint;
		return ((NumericStatistics<hugeint_t> &) *statistics).CheckZonemap(filter.comparison_type, constant);
	}
	case PhysicalType::FLOAT: {
		auto constant = filter.constant.value_.float_;
		return ((NumericStatistics<float> &) *statistics).CheckZonemap(filter.comparison_type, constant);
	}
	case PhysicalType::DOUBLE: {
		auto constant = filter.constant.value_.double_;
		return ((NumericStatistics<double> &) *statistics).CheckZonemap(filter.comparison_type, constant);
	}
	case PhysicalType::VARCHAR:
		return ((StringStatistics &) *statistics).CheckZonemap(filter.comparison_type, filter.constant.ToString());
	default:
		return true;
	}
}

}