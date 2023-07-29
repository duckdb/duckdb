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
class TableCatalogEntry;

struct ExportedTableData {
	//! Name of the exported table
	string table_name;

	//! Name of the schema
	string schema_name;

	//! Name of the database
	string database_name;

	//! Path to be exported
	string file_path;
};

struct ExportedTableInfo {
	ExportedTableInfo(TableCatalogEntry &entry, ExportedTableData table_data)
	    : entry(entry), table_data(std::move(table_data)) {
	}

	TableCatalogEntry &entry;
	ExportedTableData table_data;
};

struct BoundExportData : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::BOUND_EXPORT_DATA;

public:
	BoundExportData() : ParseInfo(TYPE) {
	}

	vector<ExportedTableInfo> data;
};

} // namespace duckdb
