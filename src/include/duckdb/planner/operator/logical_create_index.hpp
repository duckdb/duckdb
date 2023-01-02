//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_create_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class LogicalCreateIndex : public LogicalOperator {
public:
	LogicalCreateIndex(unique_ptr<FunctionData> bind_data_p, unique_ptr<CreateIndexInfo> info_p,
	                   vector<unique_ptr<Expression>> expressions_p, TableCatalogEntry &table_p,
	                   TableFunction function_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_INDEX), bind_data(std::move(bind_data_p)), info(std::move(info_p)),
	      table(table_p), function(std::move(function_p)) {

		for (auto &expr : expressions_p) {
			this->unbound_expressions.push_back(expr->Copy());
		}
		this->expressions = std::move(expressions_p);

		if (info->column_ids.empty()) {
			throw BinderException("CREATE INDEX does not refer to any columns in the base table!");
		}
	}

	//! The bind data of the function
	unique_ptr<FunctionData> bind_data;
	// Info for index creation
	unique_ptr<CreateIndexInfo> info;

	//! The table to create the index for
	TableCatalogEntry &table;
	//! The function that is called
	TableFunction function;

	//! Unbound expressions to be used in the optimizer
	vector<unique_ptr<Expression>> unbound_expressions;

public:
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
