//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/export_table_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct ExportedTableData {
	//! Name of the exported table
	string table_name;

	//! Name of the schema
	string schema_name;

	//! Path to be exported
	string file_path;
};

struct BoundExportData : public ParseInfo {
	unordered_map<TableCatalogEntry *, ExportedTableData> data;
};

} // namespace duckdb
