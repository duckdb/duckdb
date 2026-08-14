//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {
class SchemaCatalogEntry;

struct CreateTableInfo : public CreateInfo {
	DUCKDB_API CreateTableInfo();
	DUCKDB_API CreateTableInfo(string catalog, string schema, string name);
	DUCKDB_API CreateTableInfo(SchemaCatalogEntry &schema, string name);

	//! Table name to insert to
	string table;

	//! NOTE(backport): DuckDB 2.0 stores catalog/schema/name in a single `QualifiedName` on `CreateInfo`; here they are
	//! separate strings and the name lives on the subclass. These accessors only exist so that call sites can be
	//! spelled exactly as they are on the 2.0 branch.
	const string &GetTableName() const {
		return table;
	}
	void SetTableName(string name_p) {
		table = std::move(name_p);
	}
	const string &GetEntryName() const override {
		return table;
	}
	void SetEntryName(string name_p) override {
		table = std::move(name_p);
	}
	//! List of columns of the table
	ColumnList columns;
	//! List of constraints on the table
	vector<unique_ptr<Constraint>> constraints;
	//! CREATE TABLE as QUERY
	unique_ptr<SelectStatement> query;
	//! Table Partition definitions
	vector<unique_ptr<ParsedExpression>> partition_keys;
	//! Table Sort definitions
	vector<unique_ptr<ParsedExpression>> sort_keys;
	//! Extra Table options if any
	case_insensitive_map_t<unique_ptr<ParsedExpression>> options;

public:
	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;

	DUCKDB_API void Serialize(Serializer &serializer) const override;
	DUCKDB_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ExtraOptionsToString() const;
	string ToString() const override;
};

} // namespace duckdb
