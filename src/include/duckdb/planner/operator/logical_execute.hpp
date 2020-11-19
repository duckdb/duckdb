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
	    : LogicalOperator(LogicalOperatorType::LOGICAL_EXECUTE), prepared(prepared) {
		D_ASSERT(prepared);
		types = prepared->types;
	}

	PreparedStatementData *prepared;

protected:
	void ResolveTypes() override {
		// already resolved
	}
	vector<ColumnBinding> GetColumnBindings() override {
		vector<ColumnBinding> bindings;
		for(idx_t i = 0; i < types.size(); i++) {
			bindings.push_back(ColumnBinding(0, i));
		}
		return bindings;
	}
};
} // namespace duckdb
