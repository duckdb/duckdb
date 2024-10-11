//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/csv_schema.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {
//! Basic CSV Column Info
struct CSVColumnInfo {
	CSVColumnInfo(string &name_p, LogicalType &type_p) : name(name_p), type(type_p) {
	}
	string name;
	LogicalType type;
};

//! Basic CSV Schema
struct CSVSchema {
	void Initialize(vector<string> &names, vector<LogicalType> &types, const string &file_path);
	bool Empty() const;
	bool SchemasMatch(string &error_message, vector<string> &names, vector<LogicalType> &types,
	                  const string &cur_file_path);

private:
	static bool CanWeCastIt(LogicalTypeId source, LogicalTypeId destination);
	vector<CSVColumnInfo> columns;
	unordered_map<string, idx_t> name_idx_map;
	string file_path;
};
} // namespace duckdb
