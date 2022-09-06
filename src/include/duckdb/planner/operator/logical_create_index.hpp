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
	LogicalCreateIndex(TableCatalogEntry &table_p, vector<column_t> column_ids_p, TableFunction function_p,
	                   unique_ptr<FunctionData> bind_data_p, vector<unique_ptr<Expression>> expressions_p,
	                   unique_ptr<CreateIndexInfo> info_p, vector<string> names_p, vector<LogicalType> returned_types_p)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_INDEX), table(table_p), column_ids(column_ids_p),
	      function(move(function_p)), bind_data(move(bind_data_p)), info(std::move(info_p)), names(names_p),
	      returned_types(returned_types_p) {

		for (auto &expr : expressions_p) {
			this->unbound_expressions.push_back(expr->Copy());
		}
		this->expressions = move(expressions_p);

		if (column_ids.empty()) {
			throw BinderException("CREATE INDEX does not refer to any columns in the base table!");
		}
	}

	//! The table to create the index for
	TableCatalogEntry &table;
	//! Column IDs needed for index creation
	vector<column_t> column_ids;
	//! The function that is called
	TableFunction function;
	//! The bind data of the function
	unique_ptr<FunctionData> bind_data;
	// Info for index creation
	unique_ptr<CreateIndexInfo> info;
	//! The names of the columns
	vector<string> names;
	//! The types of ALL columns that can be returned by the table scan
	vector<LogicalType> returned_types;

	//! Unbound expressions to be used in the optimizer
	vector<unique_ptr<Expression>> unbound_expressions;

public:
	void Serialize(FieldWriter &writer) const override;
	static unique_ptr<LogicalOperator> Deserialize(LogicalDeserializationState &state, FieldReader &reader);

protected:
	void ResolveTypes() override {
		for (auto &index : column_ids) {
			types.push_back(returned_types[index]);
		}
	}
};
} // namespace duckdb
