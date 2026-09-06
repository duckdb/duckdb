//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/update_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class UpdateRelation : public Relation {
public:
	UpdateRelation(shared_ptr<ClientContextWrapper> &context, unique_ptr<ParsedExpression> condition,
	               Identifier catalog_name, Identifier schema_name, Identifier table_name,
	               vector<Identifier> update_columns, vector<unique_ptr<ParsedExpression>> expressions);

	vector<ColumnDefinition> columns;
	unique_ptr<ParsedExpression> condition;
	Identifier catalog_name;
	Identifier schema_name;
	Identifier table_name;
	vector<Identifier> update_columns;
	vector<unique_ptr<ParsedExpression>> expressions;

public:
	BoundStatement Bind(Binder &binder) override;
	unique_ptr<QueryNode> GetQueryNode() override;
	string GetQuery() override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	bool IsReadOnly() override {
		return false;
	}
};

} // namespace duckdb
