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
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

class QueryNode;

struct CopyInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::COPY_INFO;

public:
	CopyInfo();

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
	//! If the format is manually set (i.e., via the format parameter) or was discovered by inspecting the file path
	bool is_format_auto_detected;
	//! Expression to determine the file path (if any)
	unique_ptr<ParsedExpression> file_path_expression;
	//! The file path to copy to/from
	string file_path;
	//! Set of (key, value) options
	case_insensitive_map_t<unique_ptr<ParsedExpression>> parsed_options;
	//! Set of (key, value) options
	case_insensitive_map_t<vector<Value>> options;
	//! The SQL statement used instead of a table when copying data out to a file
	unique_ptr<QueryNode> select_statement;

public:
	string CopyOptionsToString() const;

public:
	unique_ptr<CopyInfo> Copy() const;
	string ToString() const;
	string TablePartToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
