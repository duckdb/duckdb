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
#include "duckdb/catalog/catalog_entry/table_column_type.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

struct RenameColumnInfo;
struct RenameTableInfo;

class ColumnDefinition;

//! A column of a table.
class ColumnDefinition {
public:
	DUCKDB_API ColumnDefinition(string name, LogicalType type);
	DUCKDB_API ColumnDefinition(string name, LogicalType type, unique_ptr<ParsedExpression> expression,
	                            TableColumnType category);

	//! The default value of the column (if any)
	unique_ptr<ParsedExpression> default_value;

public:
	//! default_value
	const unique_ptr<ParsedExpression> &DefaultValue() const;
	void SetDefaultValue(unique_ptr<ParsedExpression> default_value);

	//! type
	DUCKDB_API const LogicalType &Type() const;
	LogicalType &TypeMutable();
	void SetType(const LogicalType &type);

	//! name
	DUCKDB_API const string &Name() const;
	void SetName(const string &name);

	//! compression_type
	const duckdb::CompressionType &CompressionType() const;
	void SetCompressionType(duckdb::CompressionType compression_type);

	//! storage_oid
	const storage_t &StorageOid() const;
	void SetStorageOid(storage_t storage_oid);

	//! oid
	const column_t &Oid() const;
	void SetOid(column_t oid);

	//! category
	const TableColumnType &Category() const;
	//! Whether this column is a Generated Column
	bool Generated() const;
	DUCKDB_API ColumnDefinition Copy() const;

	DUCKDB_API void Serialize(Serializer &serializer) const;
	DUCKDB_API static ColumnDefinition Deserialize(Deserializer &source);

	//===--------------------------------------------------------------------===//
	// Generated Columns (VIRTUAL)
	//===--------------------------------------------------------------------===//

	ParsedExpression &GeneratedExpressionMutable();
	const ParsedExpression &GeneratedExpression() const;
	void SetGeneratedExpression(unique_ptr<ParsedExpression> expression);
	void ChangeGeneratedExpressionType(const LogicalType &type);
	void GetListOfDependencies(vector<string> &dependencies) const;

	string GetName();

	LogicalType GetType();

private:
	//! The name of the entry
	string name;
	//! The type of the column
	LogicalType type;
	//! Compression Type used for this column
	duckdb::CompressionType compression_type = duckdb::CompressionType::COMPRESSION_AUTO;
	//! The index of the column in the storage of the table
	storage_t storage_oid;
	//! The index of the column in the table
	idx_t oid;
	//! The category of the column
	TableColumnType category = TableColumnType::STANDARD;
	//! Used by Generated Columns
	unique_ptr<ParsedExpression> generated_expression;
};

} // namespace duckdb
