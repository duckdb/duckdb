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
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::BindCopyDatabaseSchema(Catalog &from_database, const string &target_database_name) {

	catalog_entry_vector_t catalog_entries;
	catalog_entries = PhysicalExport::GetNaiveExportOrder(context, from_database);

	auto info = make_uniq<CopyDatabaseInfo>(target_database_name);
	for (auto &entry : catalog_entries) {
		auto create_info = entry.get().GetInfo();
		create_info->catalog = target_database_name;
		auto on_conflict = create_info->type == CatalogType::SCHEMA_ENTRY ? OnCreateConflict::IGNORE_ON_CONFLICT
		                                                                  : OnCreateConflict::ERROR_ON_CONFLICT;
		// Update all the dependencies of the entry to point to the newly created entries on the target database
		LogicalDependencyList altered_dependencies;
		for (auto &dep : create_info->dependencies.Set()) {
			auto altered_dep = dep;
			altered_dep.catalog = target_database_name;
			altered_dependencies.AddDependency(altered_dep);
		}
		create_info->dependencies = altered_dependencies;
		create_info->on_conflict = on_conflict;
		info->entries.push_back(std::move(create_info));
	}

	// FIXME: copy indexes
	return make_uniq<LogicalCopyDatabase>(std::move(info));
}

unique_ptr<LogicalOperator> Binder::BindCopyDatabaseData(Catalog &source_catalog, const string &target_database_name) {
	auto source_schemas = source_catalog.GetSchemas(context);

	// We can just use ExtractEntries here because the order doesn't matter
	ExportEntries entries;
	PhysicalExport::ExtractEntries(context, source_schemas, entries);

	unique_ptr<LogicalOperator> result;
	for (auto &table_ref : entries.tables) {
		auto &table = table_ref.get();
		// generate the insert statement
		InsertStatement insert_stmt;
		insert_stmt.catalog = target_database_name;
		insert_stmt.schema = table.ParentSchema().name;
		insert_stmt.table = table.name;

		auto from_tbl = make_uniq<BaseTableRef>();
		from_tbl->catalog_name = source_catalog.GetName();
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
			auto copy_union =
			    make_uniq<LogicalSetOperation>(GenerateTableIndex(), 1U, std::move(insert_plan), std::move(result),
			                                   LogicalOperatorType::LOGICAL_UNION, true, false);
			result = std::move(copy_union);
		} else {
			result = std::move(insert_plan);
		}
	}
	if (!result) {
		vector<LogicalType> result_types;
		result_types.push_back(LogicalType::BIGINT);
		vector<unique_ptr<Expression>> expression_list;
		expression_list.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
		vector<vector<unique_ptr<Expression>>> expressions;
		expressions.push_back(std::move(expression_list));
		result = make_uniq<LogicalExpressionGet>(GenerateTableIndex(), std::move(result_types), std::move(expressions));
		result->children.push_back(make_uniq<LogicalDummyScan>(GenerateTableIndex()));
	}
	return result;
}

BoundStatement Binder::Bind(CopyDatabaseStatement &stmt) {
	BoundStatement result;

	unique_ptr<LogicalOperator> plan;
	auto &source_catalog = Catalog::GetCatalog(context, stmt.from_database);
	auto &target_catalog = Catalog::GetCatalog(context, stmt.to_database);
	if (&source_catalog == &target_catalog) {
		throw BinderException("Cannot copy from \"%s\" to \"%s\" - FROM and TO databases are the same",
		                      stmt.from_database, stmt.to_database);
	}
	if (stmt.copy_type == CopyDatabaseType::COPY_SCHEMA) {
		result.types = {LogicalType::BOOLEAN};
		result.names = {"Success"};

		plan = BindCopyDatabaseSchema(source_catalog, target_catalog.GetName());
	} else {
		result.types = {LogicalType::BIGINT};
		result.names = {"Count"};

		plan = BindCopyDatabaseData(source_catalog, target_catalog.GetName());
	}

	result.plan = std::move(plan);

	auto &properties = GetStatementProperties();
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	properties.modified_databases.insert(target_catalog.GetName());
	return result;
}

} // namespace duckdb
