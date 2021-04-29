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

struct ExportedTableData : public ParseInfo {
	ExportedTableData() {
	}

	//! Name of the exported table
	string table_name;

	//! Path to be exported
	string file_path;

public:
	unique_ptr<ExportedTableData> Copy() const {
		auto result = make_unique<ExportedTableData>();
		result->table_name = table_name;
		result->file_path = file_path;
		return result;
	}
};

} // namespace duckdb
