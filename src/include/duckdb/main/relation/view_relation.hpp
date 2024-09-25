//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/view_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class ViewRelation : public Relation {
public:
	ViewRelation(const shared_ptr<ClientContext> &context, string schema_name, string view_name);
	ViewRelation(const shared_ptr<RelationContextWrapper> &context, string schema_name, string view_name);
	ViewRelation(const shared_ptr<ClientContext> &context, unique_ptr<TableRef> ref, const string &view_name);

	string schema_name;
	string view_name;
	vector<ColumnDefinition> columns;
	unique_ptr<TableRef> premade_tableref;

public:
	unique_ptr<QueryNode> GetQueryNode() override;
	unique_ptr<TableRef> GetTableRef() override;

	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
};

} // namespace duckdb
