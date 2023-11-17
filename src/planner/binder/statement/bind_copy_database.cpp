#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/operator/logical_copy_database.hpp"
#include "duckdb/execution/operator/persistent/physical_export.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::BindCopyDatabaseSchema(CopyDatabaseStatement &stmt) {
	auto &from_database = Catalog::GetCatalog(context, stmt.from_database);
	auto &to_database = Catalog::GetCatalog(context, stmt.to_database);

	auto from_schemas = from_database.GetSchemas(context);

	ExportEntries entries;
	PhysicalExport::ExtractEntries(context, from_schemas, entries);

	auto info = make_uniq<CopyDatabaseInfo>(from_database, to_database);

	// get a list of all schemas to copy over
	for(auto &schema_ref : from_schemas) {
		auto &schema = schema_ref.get().Cast<SchemaCatalogEntry>();
		auto create_info = schema.GetInfo();
		create_info->catalog = to_database.GetName();
		create_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		info->schemas.push_back(std::move(create_info));
	}
	// get a list of all tables to copy over
	for(auto &table_ref : entries.tables) {
		auto &table = table_ref.get().Cast<TableCatalogEntry>();
		auto create_info = table.GetInfo();
		create_info->catalog = to_database.GetName();
		create_info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
		info->tables.push_back(std::move(create_info));
	}
	return make_uniq<LogicalCopyDatabase>(std::move(info));
}

unique_ptr<LogicalOperator> Binder::BindCopyDatabaseData(CopyDatabaseStatement &stmt) {
	auto &from_database = Catalog::GetCatalog(context, stmt.from_database);

	auto from_schemas = from_database.GetSchemas(context);

	ExportEntries entries;
	PhysicalExport::ExtractEntries(context, from_schemas, entries);

	unique_ptr<LogicalOperator> result;
	for(auto &table_ref : entries.tables) {
		auto &table = table_ref.get();
		// generate the insert statement
		InsertStatement insert_stmt;
		insert_stmt.catalog = stmt.to_database;
		insert_stmt.schema = table.ParentSchema().name;
		insert_stmt.table = table.name;

		auto from_tbl = make_uniq<BaseTableRef>();
		from_tbl->catalog_name = stmt.from_database;
		from_tbl->schema_name = table.ParentSchema().name;
		from_tbl->table_name = table.name;

		auto select_node = make_uniq<SelectNode>();
		select_node->select_list.push_back(make_uniq<StarExpression>());
		select_node->from_table = std::move(from_tbl);

		auto select_stmt = make_uniq<SelectStatement>();
		select_stmt->node = std::move(select_node);

		insert_stmt.select_statement = std::move(select_stmt);
		auto bound_insert = Bind(insert_stmt);
		auto insert_plan = std::move(bound_insert.plan);
		if (result) {
			// use UNION ALL to combine the individual copy statements into a single node
			auto copy_union = make_uniq<LogicalSetOperation>(GenerateTableIndex(), 1, std::move(insert_plan),
			                                                 std::move(result), LogicalOperatorType::LOGICAL_UNION);
			result = std::move(copy_union);
		} else {
			result = std::move(insert_plan);
		}
	}
	return result;
}

BoundStatement Binder::Bind(CopyDatabaseStatement &stmt) {
	BoundStatement result;

	unique_ptr<LogicalOperator> plan;
	if (stmt.copy_type == CopyDatabaseType::COPY_SCHEMA) {
		result.types = {LogicalType::BOOLEAN};
		result.names = {"Success"};

		plan = BindCopyDatabaseSchema(stmt);
	} else {
		result.types = {LogicalType::BIGINT};
		result.names = {"Count"};

		plan = BindCopyDatabaseData(stmt);
	}

	result.plan = std::move(plan);
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
