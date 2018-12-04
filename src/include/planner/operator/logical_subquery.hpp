//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/operator/logical_subquery.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalSubquery is a dummy node that represents a Subquery in a FROM clause.
//! It is created for use in column binding. The actual node will not be
//! transformed into a physical operator (only its children will be transformed
//! into the actual subquery).
class LogicalSubquery : public LogicalOperator {
  public:
	LogicalSubquery(size_t table_index, size_t column_count)
	    : LogicalOperator(LogicalOperatorType::SUBQUERY),
	      table_index(table_index), column_count(column_count) {
		referenced_tables.insert(table_index);
	}

	size_t table_index;
	size_t column_count;

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}
	std::vector<string> GetNames() override {
		return children[0]->GetNames();
	}
  protected:
	void ResolveTypes() override {
		types = children[0]->types;
	}
};
} // namespace duckdb
