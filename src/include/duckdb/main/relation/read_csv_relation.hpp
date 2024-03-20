//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/relation/read_csv_relation.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/csv_scanner/csv_reader_options.hpp"
#include "duckdb/main/relation/table_function_relation.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

class ReadCSVRelation : public TableFunctionRelation {
public:
	ReadCSVRelation(const shared_ptr<ClientContext> &context, const vector<string> &csv_files,
	                named_parameter_map_t &&options, string alias = string());

	string alias;

protected:
	void InitializeAlias(const vector<string> &input);

public:
	string GetAlias() override;
};

} // namespace duckdb
