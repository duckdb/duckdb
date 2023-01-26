//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/read_csv_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb/main/relation.hpp"

namespace duckdb {

struct BufferedCSVReaderOptions;

class ReadCSVRelation : public Relation {
public:
	ReadCSVRelation(const std::shared_ptr<ClientContext> &context, string csv_file, vector<ColumnDefinition> columns,
	                bool auto_detect = false, string alias = string());
	ReadCSVRelation(const std::shared_ptr<ClientContext> &context, string csv_file, BufferedCSVReaderOptions options,
	                string alias = string());

	string csv_file;
	bool auto_detect;
	string alias;
	vector<ColumnDefinition> columns;
	vector<unique_ptr<ParsedExpression>> arguments;

public:
	unique_ptr<QueryNode> GetQueryNode() override;
	const vector<ColumnDefinition> &Columns() override;
	string ToString(idx_t depth) override;
	string GetAlias() override;
	unique_ptr<TableRef> GetTableRef() override;
	void AddNamedParameter(const string &name, Value argument);
};

} // namespace duckdb
