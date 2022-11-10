//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/catalog/catalog_entry/column_dependency_manager.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {

struct CreateTableInfo : public CreateInfo {
	DUCKDB_API CreateTableInfo();
	DUCKDB_API CreateTableInfo(string schema, string name);

	//! Table name to insert to
	string table;
	//! List of columns of the table
	ColumnList columns;
	//! List of constraints on the table
	vector<unique_ptr<Constraint>> constraints;
	//! CREATE TABLE from QUERY
	unique_ptr<SelectStatement> query;

protected:
	void SerializeInternal(Serializer &serializer) const override;

public:
	DUCKDB_API static unique_ptr<CreateTableInfo> Deserialize(Deserializer &deserializer);

	DUCKDB_API unique_ptr<CreateInfo> Copy() const override;
};

} // namespace duckdb
