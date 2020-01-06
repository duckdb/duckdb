//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_subquery.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/table_binding_resolver.hpp"

namespace duckdb {

//! LogicalSubquery is a dummy node that represents a Subquery in a FROM clause.
//! It is created for use in column binding. The actual node will not be
//! transformed into a physical operator (only its children will be transformed
//! into the actual subquery).
class LogicalSubquery : public LogicalOperator {
public:
	LogicalSubquery(unique_ptr<LogicalOperator> child, index_t table_index);

	//! The table index of the subquery
	index_t table_index;
	//! The underlying column bindings of the subquery
	vector<ColumnBinding> columns;

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
