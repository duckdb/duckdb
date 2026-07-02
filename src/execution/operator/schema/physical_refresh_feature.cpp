#include "duckdb/execution/operator/schema/physical_refresh_feature.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/alter_feature_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/bound_constraint.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

static optional_ptr<FeatureCatalogEntry> LookupFeature(ClientContext &context, const string &feature_name) {
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		auto entry = schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::FEATURE_ENTRY,
		                                   feature_name);
		if (entry) {
			return &entry->Cast<FeatureCatalogEntry>();
		}
	}
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PhysicalRefreshFeature::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_uniq<RefreshFeatureGlobalState>();

	auto feature_entry = LookupFeature(context, feature_name);
	if (!feature_entry) {
		throw CatalogException("Feature \"%s\" does not exist", feature_name);
	}
	auto &feat = *feature_entry;
	auto &catalog = feat.ParentCatalog();
	auto &schema = feat.ParentSchema();
	auto transaction = catalog.GetCatalogTransaction(context);

	result->catalog_name = catalog.GetName();
	result->schema_name = schema.name;
	result->new_version = feat.current_version + 1;
	result->retain_versions = feat.retain_versions;

	// Create the new version table: feature_name__v{new_version}
	auto versioned_table_name = feature_name + "__v" + duckdb::to_string(result->new_version);
	auto table_info = make_uniq<CreateTableInfo>();
	table_info->catalog = result->catalog_name;
	table_info->schema = result->schema_name;
	table_info->table = versioned_table_name;
	table_info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	table_info->temporary = false;
	for (idx_t i = 0; i < result_names.size(); i++) {
		table_info->columns.AddColumn(ColumnDefinition(result_names[i], result_types[i]));
	}

	auto bound_info = make_uniq<BoundCreateTableInfo>(schema, std::move(table_info));
	auto table_entry = catalog.CreateTable(transaction, schema, *bound_info);
	result->table = &table_entry->Cast<DuckTableEntry>();

	return std::move(result);
}

SinkResultType PhysicalRefreshFeature::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<RefreshFeatureGlobalState>();
	auto &lstate = input.local_state.Cast<RefreshFeatureLocalState>();
	auto &storage = gstate.table->GetStorage();
	chunk.Flatten();

	// The child projects the feature columns plus a trailing boolean marker (TRUE for recomputed rows,
	// FALSE for rows copied forward). Sum the marker to report only the recomputed rows as rows_affected,
	// then append just the feature columns to the version table.
	const idx_t feature_column_count = result_types.size();
	auto &marker = chunk.data[feature_column_count];
	auto marker_data = FlatVector::GetData<bool>(marker);
	auto &marker_validity = FlatVector::Validity(marker);
	for (idx_t i = 0; i < chunk.size(); i++) {
		if (marker_validity.RowIsValid(i) && marker_data[i]) {
			lstate.recomputed_count++;
		}
	}

	DataChunk append_chunk;
	append_chunk.InitializeEmpty(result_types);
	for (idx_t c = 0; c < feature_column_count; c++) {
		append_chunk.data[c].Reference(chunk.data[c]);
	}
	append_chunk.SetCardinality(chunk.size());

	// Lazily create a per-thread optimistic row group collection so that each pipeline thread appends
	// to its own collection without contention.
	if (!lstate.collection_index.IsValid()) {
		lock_guard<mutex> l(gstate.lock);
		lstate.optimistic_writer = make_uniq<OptimisticDataWriter>(context.client, storage);
		auto optimistic_collection = lstate.optimistic_writer->CreateCollection(storage, result_types);
		auto &collection = *optimistic_collection->collection;
		collection.InitializeEmpty();
		collection.InitializeAppend(lstate.local_append_state);
		lstate.collection_index = storage.CreateOptimisticCollection(context.client, std::move(optimistic_collection));
	}

	auto &optimistic_collection = storage.GetOptimisticCollection(context.client, lstate.collection_index);
	auto &collection = *optimistic_collection.collection;
	auto new_row_group = collection.Append(append_chunk, lstate.local_append_state);
	if (new_row_group) {
		lstate.optimistic_writer->WriteNewRowGroup(optimistic_collection);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalRefreshFeature::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<RefreshFeatureGlobalState>();
	auto &lstate = input.local_state.Cast<RefreshFeatureLocalState>();
	if (!lstate.collection_index.IsValid()) {
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
	// rows_affected reports the recomputed rows only; append_count (all rows, including those copied
	// forward) still drives the append-path decision below.
	gstate.insert_count += lstate.recomputed_count;
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

unique_ptr<LocalSinkState> PhysicalRefreshFeature::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<RefreshFeatureLocalState>();
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalRefreshFeature::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                         OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<RefreshFeatureGlobalState>();
	auto &catalog = Catalog::GetCatalog(context.client, gstate.catalog_name);

	// Garbage-collect the version table that just fell outside the retain_versions limit. Each refresh
	// adds one version, so at most one table becomes newly evictable here. This runs after all rows have
	// been appended (the source phase follows the sink), so dropping the previous version does not race
	// the child that read from it.
	int64_t evicted_version = gstate.new_version - gstate.retain_versions;
	if (evicted_version >= 1) {
		auto old_table_name = feature_name + "__v" + duckdb::to_string(evicted_version);
		DropInfo drop_info;
		drop_info.type = CatalogType::TABLE_ENTRY;
		drop_info.catalog = gstate.catalog_name;
		drop_info.schema = gstate.schema_name;
		drop_info.name = old_table_name;
		drop_info.if_not_found = OnEntryNotFound::RETURN_NULL;
		catalog.DropEntry(context.client, drop_info);
	}

	// Bump the feature's current version through the catalog so it is recorded transactionally (WAL /
	// checkpoint) and commits atomically with the new version table.
	AlterEntryData alter_data(gstate.catalog_name, gstate.schema_name, feature_name, OnEntryNotFound::THROW_EXCEPTION);
	AlterFeatureInfo alter_info(std::move(alter_data), gstate.new_version);
	catalog.Alter(context.client, alter_info);

	chunk.SetCardinality(1);
	chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(gstate.insert_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
