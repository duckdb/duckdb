//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/copy_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// External File Format Types
//===--------------------------------------------------------------------===//
enum class ExternalFileFormat : uint8_t { INVALID, CSV };

struct CopyInfo : public ParseInfo {
	CopyInfo()
	    : schema(DEFAULT_SCHEMA), is_from(false), auto_detect(false), delimiter(","), quote("\""), escape(""),
	      header(false), skip_rows(0), format(ExternalFileFormat::CSV), null_str(""), quote_all(false) {
	}

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
	//! Whether or not to automatically detect dialect and datatypes
	bool auto_detect;
	//! Delimiter to separate columns within each line
	string delimiter;
	//! Quote used for columns that contain reserved characters, e.g., delimiter
	string quote;
	//! Escape character to escape quote character
	string escape;
	//! Whether or not the file has a header line
	bool header;
	//! How many leading rows to skip
	idx_t skip_rows;
	//! Expected number of columns
	idx_t num_cols;
	//! The file format of the external file
	ExternalFileFormat format;
	//! Specifies the string that represents a null value
	string null_str;
	//! Determines whether all columns must be quoted
	bool quote_all;
	//! Forces quoting to be used for all non-NULL values in each specified column
	vector<string> force_quote_list;
	//! True, if column with that index must be quoted
	vector<bool> force_quote;
	//! Null values will be read as zero-length strings in each specified column
	vector<string> force_not_null_list;
	//! True, if column with that index must skip null check
	vector<bool> force_not_null;
};

} // namespace duckdb
