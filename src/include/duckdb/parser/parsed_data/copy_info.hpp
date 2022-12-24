//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/copy_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct CopyInfo : public ParseInfo {
	CopyInfo() : catalog(INVALID_CATALOG), schema(DEFAULT_SCHEMA) {
	}

	//! The catalog name to copy to/from
	string catalog;
	//! The schema name to copy to/from
	string schema;
	//! The table name to copy to/from
	string table;
	//! List of columns to copy to/from
	vector<string> select_list;
	//! The file path to copy to/from
	string file_path;
	//! Whether or not this is a copy to file (false) or copy from a file (true)
	bool is_from;
	//! The file format of the external file
	string format;
	//! Set of (key, value) options
	unordered_map<string, vector<Value>> options;

public:
	unique_ptr<CopyInfo> Copy() const {
		auto result = make_unique<CopyInfo>();
		result->catalog = catalog;
		result->schema = schema;
		result->table = table;
		result->select_list = select_list;
		result->file_path = file_path;
		result->is_from = is_from;
		result->format = format;
		result->options = options;
		return result;
	}
};

} // namespace duckdb
