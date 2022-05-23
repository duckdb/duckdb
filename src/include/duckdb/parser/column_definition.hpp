//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/column_definition.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/enums/compression_type.hpp"
#include "duckdb/catalog/catalog_entry/table_column_info.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

struct RenameColumnInfo;
struct RenameTableInfo;

enum ColumnExpressionType { DEFAULT, GENERATED };

struct ColumnExpression {
	unique_ptr<ParsedExpression> expression;
	ColumnExpressionType type;
	ColumnExpression(unique_ptr<ParsedExpression> expression, ColumnExpressionType type = ColumnExpressionType::DEFAULT)
	    : expression(move(expression)), type(type) {
	}
};

class ColumnDefinition;

//! Used in both TableCatalogEntry and TransformCreateTable
void AddToColumnDependencyMapping(ColumnDefinition &col, case_insensitive_map_t<unordered_set<string>> &dependents,
                                  case_insensitive_map_t<unordered_set<string>> &dependencies);

//! A column of a table.
class ColumnDefinition {
public:
	DUCKDB_API ColumnDefinition(string name, LogicalType type);
	DUCKDB_API ColumnDefinition(string name, LogicalType type, ColumnExpression expression);

	//! The name of the entry
	string name;
	//! The index of the column in the table
	idx_t oid;
	//! The index of the column in the storage of the table
	idx_t storage_oid;
	//! The type of the column
	LogicalType type;
	//! The default value of the column (if any)
	unique_ptr<ParsedExpression> default_value;
	//! Compression Type used for this column
	CompressionType compression_type = CompressionType::COMPRESSION_AUTO;
	//! The category of the column
	TableColumnType category = TableColumnType::STANDARD;
	//! Used by Generated Columns
	unique_ptr<ParsedExpression> generated_expression;

public:
	void SetGeneratedExpression(unique_ptr<ParsedExpression> expression);
	void ChangeGeneratedExpressionType(const LogicalType &type);
	ParsedExpression &GeneratedExpression() const;
	DUCKDB_API ColumnDefinition Copy() const;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static ColumnDefinition Deserialize(Deserializer &source);

	//! Whether this column is a Generated Column
	bool Generated() const;

	//===--------------------------------------------------------------------===//
	// Generated Columns (VIRTUAL)
	//===--------------------------------------------------------------------===//
	//! Has to be run on a newly added generated column to ensure that its valid
	void CheckValidity(const vector<ColumnDefinition> &columns, const string &table_name);
	void GetListOfDependencies(vector<string> &dependencies);

private:
private:
};

} // namespace duckdb
