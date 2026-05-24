#include "duckdb/execution/operator/schema/physical_create_feature.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PhysicalCreateFeature::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_uniq<CreateFeatureGlobalState>();

	auto &catalog = Catalog::GetCatalog(context, info->catalog);
	auto &schema = catalog.GetSchema(context, info->schema);
	auto transaction = catalog.GetCatalogTransaction(context);

	// Check IF NOT EXISTS early
	if (info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		auto &duck_schema = schema.Cast<DuckSchemaEntry>();
		auto &set = duck_schema.GetCatalogSet(CatalogType::FEATURE_ENTRY);
		auto old_entry = set.GetEntry(transaction, info->feature_name);
		if (old_entry) {
			// Feature already exists, skip materialization
			return std::move(result);
		}
	}

	// Create the backing table using Catalog::CreateTable (same transaction, no deadlock)
	auto table_info = make_uniq<CreateTableInfo>();
	table_info->catalog = info->catalog;
	table_info->schema = info->schema;
	table_info->table = info->feature_name;
	table_info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	table_info->temporary = false;

	for (idx_t i = 0; i < info->result_names.size(); i++) {
		table_info->columns.AddColumn(ColumnDefinition(info->result_names[i], info->result_types[i]));
	}

	auto bound_info = make_uniq<BoundCreateTableInfo>(schema, std::move(table_info));
	auto table_entry = catalog.CreateTable(transaction, schema, *bound_info);
	result->table = &table_entry->Cast<DuckTableEntry>();

	// Create the feature catalog entry
	auto &duck_schema = schema.Cast<DuckSchemaEntry>();
	auto &set = duck_schema.GetCatalogSet(CatalogType::FEATURE_ENTRY);
	auto &dependencies = info->dependencies;
	auto entry = make_uniq<FeatureCatalogEntry>(catalog, schema, *info);

	if (!set.CreateEntry(transaction, info->feature_name, std::move(entry), dependencies)) {
		throw CatalogException::EntryAlreadyExists(CatalogType::FEATURE_ENTRY, info->feature_name);
	}

	// Make the feature own the backing table so dropping feature cascades to table
	auto feature_entry = set.GetEntry(transaction, info->feature_name);
	auto &duck_catalog = catalog.Cast<DuckCatalog>();
	duck_catalog.GetDependencyManager()->AddOwnership(transaction, *feature_entry, *table_entry);

	return std::move(result);
}

SinkResultType PhysicalCreateFeature::Sink(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CreateFeatureGlobalState>();
	if (!gstate.table) {
		// IF NOT EXISTS and feature already existed - discard data
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto &storage = gstate.table->GetStorage();
	chunk.Flatten();
	vector<unique_ptr<BoundConstraint>> empty_constraints;
	storage.LocalAppend(*gstate.table, context.client, chunk, empty_constraints, true);
	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalCreateFeature::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCreateFeature::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                        OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<CreateFeatureGlobalState>();
	chunk.SetCardinality(1);
	chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(gstate.insert_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
