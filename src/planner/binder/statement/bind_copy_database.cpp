#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/planner/operator/logical_create.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/common/serializer/buffered_deserializer.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/operator/logical_copy_database.hpp"
#include "duckdb/execution/operator/persistent/physical_export.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"

namespace duckdb {

void AddNextOperator(Binder &binder, unique_ptr<LogicalOperator> next_operator, unique_ptr<LogicalOperator> &result) {
	if (result) {
		// use UNION ALL to combine the individual copy statements into a single node
		auto copy_union =
			make_uniq<LogicalSetOperation>(binder.GenerateTableIndex(), 1, std::move(result),
										   std::move(next_operator), LogicalOperatorType::LOGICAL_UNION, false);
		result = std::move(copy_union);
	} else {
		result = std::move(next_operator);
	}
}
template<class T, class SRC, typename... Args>
unique_ptr<T> GetCreateInfo(SRC &input, Args&&... args) {
	BufferedSerializer serializer;
	input.Serialize(serializer);

	BufferedDeserializer source(serializer);
	return SRC::Deserialize(source, args...);
}

BoundStatement Binder::Bind(CopyDatabaseStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	auto &from_database = Catalog::GetCatalog(context, stmt.from_database);
	auto &to_database = Catalog::GetCatalog(context, stmt.to_database);

	unique_ptr<LogicalOperator> plan;
	auto from_schemas = from_database.GetSchemas(context);

	ExportEntries entries;
	PhysicalExport::ExtractEntries(context, from_schemas, entries);

	// generate the set of CREATE SCHEMA statements
	for(auto &schema_ref : from_schemas) {
		auto &schema = schema_ref.get().Cast<SchemaCatalogEntry>();
		auto create_info = GetCreateInfo<CreateSchemaInfo>(schema);
		create_info->catalog = to_database.GetName();
		create_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		auto create_schema = make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_SCHEMA, std::move(create_info));
		AddNextOperator(*this, std::move(create_schema), plan);
	}
	// generate the tables
	for(auto &table_ref : entries.tables) {
		auto &table = table_ref.get().Cast<TableCatalogEntry>();
		auto create_info = GetCreateInfo<CreateTableInfo>(table, context);
		create_info->catalog = to_database.GetName();
		create_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		auto create_table = make_uniq<LogicalCreateTable>(LogicalOperatorType::LOGICAL_CREATE_TABLE, std::move(create_info));
		AddNextOperator(*this, std::move(create_table), plan);
	}

	auto copy_database = make_uniq<LogicalCopyDatabase>();
	if (!plan) {
		throw BinderException("Empty database to copy!?");
	}
	copy_database->AddChild(std::move(plan));

	result.plan = std::move(copy_database);
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
