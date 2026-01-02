//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_cte.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

class LogicalCTE : public LogicalOperator {
public:
	explicit LogicalCTE(LogicalOperatorType logical_type = LogicalOperatorType::LOGICAL_INVALID)
	    : LogicalOperator(logical_type) {
	}

public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	explicit LogicalCTE(string ctename_p, idx_t table_index, idx_t column_count, unique_ptr<LogicalOperator> top,
	                    unique_ptr<LogicalOperator> bottom,
	                    LogicalOperatorType logical_type = LogicalOperatorType::LOGICAL_INVALID)
	    : LogicalOperator(logical_type), ctename(std::move(ctename_p)), table_index(table_index),
	      column_count(column_count) {
		children.push_back(std::move(top));
		children.push_back(std::move(bottom));
	}

	string ctename;
	idx_t table_index;
	idx_t column_count;
	CorrelatedColumns correlated_columns;
};
} // namespace duckdb
