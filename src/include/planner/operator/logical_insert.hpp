//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/operator/logical_insert.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

//! LogicalInsert represents an insertion of data into a base table
class LogicalInsert : public LogicalOperator {
  public:
	LogicalInsert(TableCatalogEntry *table)
	    : LogicalOperator(LogicalOperatorType::INSERT), table(table) {
	}

	std::vector<std::vector<std::unique_ptr<Expression>>> insert_values;
	std::vector<int> column_index_map;

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}
	std::vector<string> GetNames() override {
		return {"Count"};
	}

	//! The base table to insert into
	TableCatalogEntry *table;
};
} // namespace duckdb
