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
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

struct CopyInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::COPY_INFO;

public:
	CopyInfo() : ParseInfo(TYPE), catalog(INVALID_CATALOG), schema(DEFAULT_SCHEMA) {
	}

	//! The catalog name to copy to/from
	string catalog;
	//! The schema name to copy to/from
	string schema;
	//! The table name to copy to/from
	string table;
	//! List of columns to copy to/from
	vector<string> select_list;
	//! Whether or not this is a copy to file (false) or copy from a file (true)
	bool is_from;
	//! The file format of the external file
	string format;
	//! The file path to copy to/from
	string file_path;
	//! Set of (key, value) options
	case_insensitive_map_t<vector<Value>> options;

public:
	unique_ptr<CopyInfo> Copy() const {
		auto result = make_uniq<CopyInfo>();
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

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
