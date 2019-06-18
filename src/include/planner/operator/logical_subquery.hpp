//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_subquery.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"
#include "planner/table_binding_resolver.hpp"

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
	//! The total amount of columns of the subquery
	index_t column_count;
	//! The tables that are bound underneath the subquery
	vector<BoundTable> bound_tables;

protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
