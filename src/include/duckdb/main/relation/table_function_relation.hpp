//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/table_function_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class TableFunctionRelation : public Relation {
public:
	TableFunctionRelation(const std::shared_ptr<ClientContext> &context, string name, vector<Value> parameters,
	                      named_parameter_map_t named_parameters, shared_ptr<Relation> input_relation_p = nullptr);

	TableFunctionRelation(const std::shared_ptr<ClientContext> &context, string name, vector<Value> parameters,
	                      shared_ptr<Relation> input_relation_p = nullptr);

	string name;
	vector<Value> parameters;
	named_parameter_map_t named_parameters;
	vector<ColumnDefinition> columns;
	shared_ptr<Relation> input_relation;

public:
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
};

} // namespace duckdb
