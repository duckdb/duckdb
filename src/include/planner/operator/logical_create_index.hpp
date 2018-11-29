//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_create_index.hpp
//
// Author: Pedro Holanda
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCreateIndex : public LogicalOperator {
  public:
	LogicalCreateIndex(TableCatalogEntry &table,
	                   std::vector<column_t> column_ids,
	                   std::vector<std::unique_ptr<Expression>> expressions,
	                   std::unique_ptr<CreateIndexInformation> info)
	    : LogicalOperator(LogicalOperatorType::CREATE_INDEX), table(table),
	      column_ids(column_ids), expressions(std::move(expressions)),
	      info(std::move(info)) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}

	//! The table to create the index for
	TableCatalogEntry &table;
	//! Column IDs needed for index creation
	std::vector<column_t> column_ids;
	//! Set of expressions to index by
	std::vector<std::unique_ptr<Expression>> expressions;
	// Info for index creation
	std::unique_ptr<CreateIndexInformation> info;
};
} // namespace duckdb
