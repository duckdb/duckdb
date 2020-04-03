//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/projection_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

class ProjectionRelation : public Relation {
public:
	ProjectionRelation(shared_ptr<Relation> child, vector<unique_ptr<ParsedExpression>> expressions, vector<string> aliases);

	vector<unique_ptr<ParsedExpression>> expressions;
	vector<string> aliases;
	vector<ColumnDefinition> columns;
	shared_ptr<Relation> child;
public:
	BoundStatement Bind(Binder &binder) override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
};

} // namespace duckdb
