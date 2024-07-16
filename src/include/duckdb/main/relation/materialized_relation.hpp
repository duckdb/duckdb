//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/materialized_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/main/external_dependencies.hpp"

namespace duckdb {

class MaterializedDependency : public DependencyItem {
public:
	explicit MaterializedDependency(unique_ptr<ColumnDataCollection> &&collection_p)
	    : collection(std::move(collection_p)) {
	}
	~MaterializedDependency() override {};

public:
	unique_ptr<ColumnDataCollection> collection;
};

class MaterializedRelation : public Relation {
public:
	MaterializedRelation(const shared_ptr<ClientContext> &context, unique_ptr<ColumnDataCollection> &&collection,
	                     vector<string> names, string alias = "materialized");
	vector<ColumnDefinition> columns;
	string alias;

public:
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
	unique_ptr<TableRef> GetTableRef() override;
	unique_ptr<QueryNode> GetQueryNode() override;
	shared_ptr<DependencyItem> GetMaterializedDependency();
};

} // namespace duckdb
