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
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

class ReadCSVRelation : public TableFunctionRelation {
public:
	ReadCSVRelation(const shared_ptr<ClientContext> &context, const string &csv_file, vector<ColumnDefinition> columns,
	                string alias = string());
	ReadCSVRelation(const shared_ptr<ClientContext> &context, const string &csv_file, named_parameter_map_t &&options,
	                string alias = string());

	string alias;
	bool auto_detect;

public:
	string GetAlias() override;
};

} // namespace duckdb
