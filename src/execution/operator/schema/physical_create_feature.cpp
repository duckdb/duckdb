#include "duckdb/execution/operator/schema/physical_create_feature.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/planner/bound_constraint.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/sql_identifier.hpp"

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

	// Create the versioned backing table: feature_name__v1
	auto versioned_table_name = info->feature_name + "__v1";
	auto table_info = make_uniq<CreateTableInfo>();
	table_info->catalog = info->catalog;
	table_info->schema = info->schema;
	table_info->table = versioned_table_name;
	table_info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	table_info->temporary = false;

	for (idx_t i = 0; i < info->result_names.size(); i++) {
		table_info->columns.AddColumn(ColumnDefinition(info->result_names[i], info->result_types[i]));
	}

	auto bound_info = make_uniq<BoundCreateTableInfo>(schema, std::move(table_info));
	auto table_entry = catalog.CreateTable(transaction, schema, *bound_info);
	result->table = &table_entry->Cast<DuckTableEntry>();

	// Create a view named feature_name that resolves via current_feature()
	auto view_info = make_uniq<CreateViewInfo>(info->catalog, info->schema, info->feature_name);
	view_info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	auto select_sql = "SELECT * FROM current_feature('" + info->feature_name + "')";
	view_info->query = CreateViewInfo::ParseSelect(select_sql);
	auto view_entry = catalog.CreateView(context, *view_info);

	// Create the feature catalog entry
	auto &duck_schema = schema.Cast<DuckSchemaEntry>();
	auto &set = duck_schema.GetCatalogSet(CatalogType::FEATURE_ENTRY);
	auto &dependencies = info->dependencies;
	auto entry = make_uniq<FeatureCatalogEntry>(catalog, schema, *info);

	if (!set.CreateEntry(transaction, info->feature_name, std::move(entry), dependencies)) {
		throw CatalogException::EntryAlreadyExists(CatalogType::FEATURE_ENTRY, info->feature_name);
	}

	// Make the feature own the view so dropping feature cascades to the view
	// Version tables are managed explicitly (not via ownership) to allow GC during refresh
	auto feature_entry = set.GetEntry(transaction, info->feature_name);
	auto &duck_catalog = catalog.Cast<DuckCatalog>();
	duck_catalog.GetDependencyManager()->AddOwnership(transaction, *feature_entry, *view_entry);

	return std::move(result);
}

SinkResultType PhysicalCreateFeature::Sink(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CreateFeatureGlobalState>();
	if (!gstate.table) {
		// IF NOT EXISTS and feature already existed - discard data
		return SinkResultType::NEED_MORE_INPUT;
	}

	auto &lstate = input.local_state.Cast<CreateFeatureLocalState>();
	auto &storage = gstate.table->GetStorage();
	chunk.Flatten();

	// Lazily create a per-thread optimistic row group collection so that each
	// pipeline thread appends to its own collection without contention.
	if (!lstate.collection_index.IsValid()) {
		lock_guard<mutex> l(gstate.lock);
		lstate.optimistic_writer = make_uniq<OptimisticDataWriter>(context.client, storage);
		auto optimistic_collection = lstate.optimistic_writer->CreateCollection(storage, info->result_types);
		auto &collection = *optimistic_collection->collection;
		collection.InitializeEmpty();
		collection.InitializeAppend(lstate.local_append_state);
		lstate.collection_index = storage.CreateOptimisticCollection(context.client, std::move(optimistic_collection));
	}

	auto &optimistic_collection = storage.GetOptimisticCollection(context.client, lstate.collection_index);
	auto &collection = *optimistic_collection.collection;
	auto new_row_group = collection.Append(chunk, lstate.local_append_state);
	if (new_row_group) {
		lstate.optimistic_writer->WriteNewRowGroup(optimistic_collection);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateFeature::Combine(ExecutionContext &context,
                                                     OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<CreateFeatureGlobalState>();
	auto &lstate = input.local_state.Cast<CreateFeatureLocalState>();
	if (!gstate.table || !lstate.collection_index.IsValid()) {
		return SinkCombineResultType::FINISHED;
	}

	auto &storage = gstate.table->GetStorage();
	const idx_t row_group_size = storage.GetRowGroupSize();
	auto &optimistic_collection = storage.GetOptimisticCollection(context.client, lstate.collection_index);
	auto &collection = *optimistic_collection.collection;

	TransactionData tdata(0, 0);
	collection.FinalizeAppend(tdata, lstate.local_append_state);
	auto append_count = collection.GetTotalRows();

	lock_guard<mutex> l(gstate.lock);
	gstate.insert_count += append_count;
	vector<unique_ptr<BoundConstraint>> empty_constraints;
	if (append_count < row_group_size) {
		// Few rows - append directly to the transaction-local storage.
		LocalAppendState append_state;
		storage.InitializeLocalAppend(append_state, *gstate.table, context.client, empty_constraints);
		auto &transaction = DuckTransaction::Get(context.client, gstate.table->catalog);
		for (auto &append_chunk : collection.Chunks(transaction)) {
			storage.LocalAppend(append_state, *gstate.table, context.client, append_chunk, false);
		}
		storage.FinalizeLocalAppend(append_state);
	} else {
		// We optimistically wrote row groups to disk - merge them into the transaction-local storage.
		lstate.optimistic_writer->WriteUnflushedRowGroups(optimistic_collection);
		lstate.optimistic_writer->FinalFlush();
		storage.LocalMerge(context.client, *gstate.table, optimistic_collection);
		auto &optimistic_writer = storage.GetOptimisticWriter(context.client);
		optimistic_writer.Merge(*lstate.optimistic_writer);
	}
	return SinkCombineResultType::FINISHED;
}

unique_ptr<LocalSinkState> PhysicalCreateFeature::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<CreateFeatureLocalState>();
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
