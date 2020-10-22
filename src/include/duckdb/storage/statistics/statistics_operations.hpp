//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/statistics/statistics_operations.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

class StatisticsOperations {
public:
	//! Cast the statistics from one type to the other
	static unique_ptr<BaseStatistics> Cast(const BaseStatistics *input, LogicalType target);

	//! Generate statistics from a specified value
	static unique_ptr<BaseStatistics> FromValue(const Value &value);
private:
	static unique_ptr<BaseStatistics> NumericCastSwitch(const BaseStatistics *input, LogicalType target);
	static unique_ptr<BaseStatistics> NumericNumericCast(const BaseStatistics *input, LogicalType target);
};

}
