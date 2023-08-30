//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/read_csv_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/scan/csv/csv_reader_options.hpp"
#include "duckdb/main/relation/table_function_relation.hpp"

namespace duckdb {

struct CSVReaderOptions;

class ReadCSVRelation : public TableFunctionRelation {
public:
	ReadCSVRelation(const shared_ptr<ClientContext> &context, const string &csv_file, vector<ColumnDefinition> columns,
	                string alias = string());
	ReadCSVRelation(const shared_ptr<ClientContext> &context, const string &csv_file, CSVReaderOptions options,
	                string alias = string());

	string alias;
	bool auto_detect;

public:
	string GetAlias() override;
};

} // namespace duckdb
