//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_execute.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/catalog_entry/prepared_statement_catalog_entry.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalExecute : public LogicalOperator {
public:
	LogicalExecute(PreparedStatementCatalogEntry *prep) : LogicalOperator(LogicalOperatorType::EXECUTE), prep(prep) {
		assert(prep);
		types = prep->types;
	}

	PreparedStatementCatalogEntry *prep;

protected:
	void ResolveTypes() override {
		// already resolved
	}
};
} // namespace duckdb
