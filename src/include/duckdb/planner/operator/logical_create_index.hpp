//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_create_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

class LogicalCreateIndex : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_CREATE_INDEX;

public:
	LogicalCreateIndex(unique_ptr<CreateIndexInfo> info_p, vector<unique_ptr<Expression>> expressions_p,
	                   CatalogEntry &target_p, unique_ptr<AlterTableInfo> alter_table_info = nullptr);

	//! Index creation information.
	unique_ptr<CreateIndexInfo> info;
	//! Either a table or a view; catalog decides how to handle each.
	CatalogEntry &table;
	// Alter table information.
	unique_ptr<AlterTableInfo> alter_table_info;
	//! Unbound expressions of the indexed columns.
	vector<unique_ptr<Expression>> unbound_expressions;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;

private:
	LogicalCreateIndex(ClientContext &context, unique_ptr<CreateInfo> info, vector<unique_ptr<Expression>> expressions,
	                   unique_ptr<ParseInfo> alter_info);
	CatalogEntry &BindTable(ClientContext &context, CreateIndexInfo &info_p);
};
} // namespace duckdb
