//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalExecute : public LogicalOperator {
public:
	LogicalExecute(PreparedStatementCatalogEntry *prep) : LogicalOperator(LogicalOperatorType::EXECUTE), prep(prep) {
		assert(prep);
		types = prep->types;
	}

	PreparedStatementCatalogEntry *prep;

	vector<string> GetNames() override {
		return prep->names;
	}

protected:
	void ResolveTypes() override {
		// already resolved
	}
};
} // namespace duckdb
