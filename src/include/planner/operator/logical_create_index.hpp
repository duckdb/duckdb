//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_create_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalCreateIndex : public LogicalOperator {
public:
	LogicalCreateIndex(TableCatalogEntry &table, vector<column_t> column_ids,
	                   vector<unique_ptr<Expression>> expressions, unique_ptr<CreateIndexInformation> info)
	    : LogicalOperator(LogicalOperatorType::CREATE_INDEX), table(table), column_ids(column_ids),
	      info(std::move(info)) {
		this->unbinded_expressions.push_back(expressions[0]->Copy());
		this->expressions = move(expressions);
	}

	//! The table to create the index for
	TableCatalogEntry &table;
	//! Column IDs needed for index creation
	vector<column_t> column_ids;
	//! Info for index creation
	unique_ptr<CreateIndexInformation> info;
	//! Unbinded expressions to be used in the optimizer
	vector<unique_ptr<Expression>> unbinded_expressions;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BIGINT);
	}
};
} // namespace duckdb
