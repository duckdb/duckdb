//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/explain_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/relation.hpp"

namespace duckdb {

class ExplainRelation : public Relation {
public:
	explicit ExplainRelation(shared_ptr<Relation> child, ExplainType type = ExplainType::EXPLAIN_STANDARD,
	                         const ProfilerPrintFormat &format = ProfilerPrintFormat::Default());

	shared_ptr<Relation> child;
	vector<ColumnDefinition> columns;
	ExplainType type;
	ProfilerPrintFormat format;

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
