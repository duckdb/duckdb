//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_index_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/enums/index_type.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct CreateIndexInfo : public CreateInfo {
	CreateIndexInfo() : CreateInfo(CatalogType::INDEX_ENTRY) {};

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

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
