//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/partition_stats.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! How a table is partitioned by a given set of columns
enum class TablePartitionInfo : uint8_t {
	NOT_PARTITIONED,         // the table is not partitioned by the given set of columns
	SINGLE_VALUE_PARTITIONS, // each partition has exactly one unique value (e.g. bounds = [1,1][2,2][3,3])
	OVERLAPPING_PARTITIONS,  // the partitions overlap **only** at the boundaries (e.g. bounds = [1,2][2,3][3,4]
	DISJOINT_PARTITIONS      // the partitions are disjoint (e.g. bounds = [1,2][3,4][5,6])
};

enum class CountType { COUNT_EXACT, COUNT_APPROXIMATE };

struct PartitionStatistics {
	PartitionStatistics();

	//! The row id start
	idx_t row_start;
	//! The amount of rows in the partition
	idx_t count;
	//! Whether or not the count is exact or approximate
	CountType count_type;
};

} // namespace duckdb
