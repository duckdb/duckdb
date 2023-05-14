//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/read_csv_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/csv_reader_options.hpp"
#include "duckdb/main/relation/table_function_relation.hpp"

namespace duckdb {

struct BufferedCSVReaderOptions;

class ReadCSVRelation : public TableFunctionRelation {
public:
	ReadCSVRelation(const std::shared_ptr<ClientContext> &context, const string &csv_file,
	                vector<ColumnDefinition> columns, string alias = string());
	ReadCSVRelation(const std::shared_ptr<ClientContext> &context, const string &csv_file,
	                BufferedCSVReaderOptions options, string alias = string());

	string alias;
	bool auto_detect;

public:
	string GetAlias() override;
};

} // namespace duckdb
