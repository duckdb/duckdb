//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/copy_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// External File Format Types
//===--------------------------------------------------------------------===//
enum class ExternalFileFormat : uint8_t { INVALID, CSV };

struct CopyInfo {
	//! The schema name to copy to/from
	string schema;
	//! The table name to copy to/from
	string table;
	//! The file path to copy to or copy from
	string file_path;
	//! Whether or not this is a copy to file or copy from a file
	bool is_from;
	//! Delimiter to parse
	char delimiter;
	//! Quote to use
	char quote;
	//! Escape character to use
	char escape;
	//! Whether or not the file has a header line
	bool header;
	//! The file format of the external file
	ExternalFileFormat format;
	// List of Columns that will be copied from/to.
	vector<string> select_list;

	CopyInfo()
	    : schema(DEFAULT_SCHEMA), is_from(false), delimiter(','), quote('"'), escape('"'), header(false),
	      format(ExternalFileFormat::CSV) {
	}
};

} // namespace duckdb
