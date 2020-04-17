//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalExecute : public LogicalOperator {
public:
	LogicalExecute(PreparedStatementData *prepared)
	    : LogicalOperator(LogicalOperatorType::EXECUTE), prepared(prepared) {
		assert(prepared);
		types = prepared->types;
	}

	PreparedStatementData *prepared;

protected:
	void ResolveTypes() override {
		// already resolved
	}
};
} // namespace duckdb
