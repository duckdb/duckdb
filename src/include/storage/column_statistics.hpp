//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/column_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/statistics.hpp"
#include "common/types/value.hpp"
#include "common/types/vector.hpp"

#include <mutex>

namespace duckdb {

//! The ColumnStatistics object holds statistics related to a physical column of
//! a table
class ColumnStatistics {
public:
	ColumnStatistics() {
		can_have_null = false;
		min.is_null = true;
		max.is_null = true;
		maximum_string_length = 0;
	}

	//! Update the column statistics with a new batch of data
	void Update(Vector &new_vector);
	//! Initialize ExpressionStatistics from the ColumnStatistics
	void Initialize(ExpressionStatistics &target);

	//! True if the vector can potentially contain NULL values
	bool can_have_null;
	//! The minimum value of the column [numeric only]
	Value min;
	//! The maximum value of the column [numeric only]
	Value max;
	//! The maximum string length of a character column [VARCHAR only]
	index_t maximum_string_length;

private:
	//! The lock used to update the statistics of the column
	std::mutex stats_lock;
};

} // namespace duckdb
