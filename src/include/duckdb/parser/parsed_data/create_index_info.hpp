//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_index_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/index_constraint_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {

struct CreateIndexInfo : public CreateInfo {
	CreateIndexInfo();
	CreateIndexInfo(const CreateIndexInfo &info);

	//! The table name of the underlying table
	string table;
	//! The name of the index
	string index_name;

	//! Options values (WITH ...)
	case_insensitive_map_t<Value> options;

	//! The index type (ART, B+-tree, Skip-List, ...)
	string index_type;
	//! The index constraint type
	IndexConstraintType constraint_type;
	//! The column ids of the indexed table
	vector<column_t> column_ids;
	//! The set of expressions to index by
	vector<unique_ptr<ParsedExpression>> expressions;
	vector<unique_ptr<ParsedExpression>> parsed_expressions;

	//! The types of the logical columns (necessary for scanning the table during CREATE INDEX)
	vector<LogicalType> scan_types;
	//! The names of the logical columns (necessary for scanning the table during CREATE INDEX)
	vector<string> names;

public:
	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;

	string ToString() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
