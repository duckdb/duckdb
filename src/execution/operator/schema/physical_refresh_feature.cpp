#include "duckdb/execution/operator/schema/physical_refresh_feature.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/feature_query.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/alter_feature_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/bound_constraint.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
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

	// Get-or-create the persistent store table. The first refresh creates it; later refreshes append to it.
	auto store_name = FeatureStoreTableName(feature_name);
	auto existing = schema.GetEntry(transaction, CatalogType::TABLE_ENTRY, store_name);
	if (existing) {
		result->table = &existing->Cast<DuckTableEntry>();
	} else {
		auto table_info = make_uniq<CreateTableInfo>();
		table_info->catalog = result->catalog_name;
		table_info->schema = result->schema_name;
		table_info->table = store_name;
		table_info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
		table_info->temporary = false;
		for (idx_t i = 0; i < result_names.size(); i++) {
			table_info->columns.AddColumn(ColumnDefinition(result_names[i], result_types[i]));
		}

		auto bound_info = make_uniq<BoundCreateTableInfo>(schema, std::move(table_info));
		auto table_entry = catalog.CreateTable(transaction, schema, *bound_info);
		result->table = &table_entry->Cast<DuckTableEntry>();
	}

	return std::move(result);
}

SinkResultType PhysicalRefreshFeature::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<RefreshFeatureGlobalState>();
	auto &lstate = input.local_state.Cast<RefreshFeatureLocalState>();
	auto &storage = gstate.table->GetStorage();
	chunk.Flatten();

	// The child projects exactly the feature columns: one snapshot row per entity. Every row is appended
	// to the new version table and counted as an affected row.
	lstate.recomputed_count += chunk.size();

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
	auto new_row_group = collection.Append(chunk, lstate.local_append_state);
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
	// rows_affected is the number of snapshot rows appended (one per entity).
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

//! Delete every row of the store whose __feature_version has fallen at or below the retain cutoff.
//! Collect the matching row ids in a first pass, then delete them — never delete while scanning.
static void EvictOldVersions(ClientContext &context, DuckTableEntry &table, idx_t version_column_index,
                             int64_t cutoff) {
	auto &storage = table.GetStorage();
	auto &transaction = DuckTransaction::Get(context, table.catalog);

	// Scan just the version column and the row id.
	vector<StorageIndex> column_ids;
	column_ids.emplace_back(version_column_index);
	column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);

	TableScanState scan_state;
	storage.InitializeScan(context, transaction, scan_state, column_ids);

	DataChunk scan_chunk;
	scan_chunk.Initialize(Allocator::Get(context), vector<LogicalType> {LogicalType::BIGINT, LogicalType::ROW_TYPE});

	vector<row_t> evicted;
	while (true) {
		scan_chunk.Reset();
		storage.Scan(transaction, scan_chunk, scan_state);
		if (scan_chunk.size() == 0) {
			break;
		}
		scan_chunk.Flatten();
		auto versions = FlatVector::GetData<int64_t>(scan_chunk.data[0]);
		auto row_ids = FlatVector::GetData<row_t>(scan_chunk.data[1]);
		for (idx_t i = 0; i < scan_chunk.size(); i++) {
			if (versions[i] <= cutoff) {
				evicted.push_back(row_ids[i]);
			}
		}
	}
	if (evicted.empty()) {
		return;
	}

	// Delete the collected row ids in vector-sized batches. The internal store has no delete constraints
	// (no primary key, unique index, or foreign key), so a default TableDeleteState is sufficient.
	TableDeleteState delete_state;
	Vector row_id_vector(LogicalType::ROW_TYPE);
	auto row_id_data = FlatVector::GetDataMutable<row_t>(row_id_vector);
	for (idx_t offset = 0; offset < evicted.size(); offset += STANDARD_VECTOR_SIZE) {
		idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, evicted.size() - offset);
		for (idx_t i = 0; i < count; i++) {
			row_id_data[i] = evicted[offset + i];
		}
		storage.Delete(delete_state, context, table, row_id_vector, count);
	}
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalRefreshFeature::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                         OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<RefreshFeatureGlobalState>();
	auto &catalog = Catalog::GetCatalog(context.client, gstate.catalog_name);

	// Evict versions that have fallen outside the retain window by deleting their rows from the store. This
	// runs after all new rows have been appended (the source phase follows the sink); the just-appended rows
	// carry the new version, which is above the cutoff, so they are never evicted. The __feature_version
	// column is the second-to-last column of the store schema.
	int64_t cutoff = gstate.new_version - gstate.retain_versions;
	if (cutoff >= 1) {
		EvictOldVersions(context.client, *gstate.table, result_types.size() - 2, cutoff);
	}

	// Bump the feature's current version through the catalog so it is recorded transactionally (WAL /
	// checkpoint) and commits atomically with the appended snapshot and the eviction.
	AlterEntryData alter_data(gstate.catalog_name, gstate.schema_name, feature_name, OnEntryNotFound::THROW_EXCEPTION);
	AlterFeatureInfo alter_info(std::move(alter_data), gstate.new_version);
	catalog.Alter(context.client, alter_info);

	chunk.SetCardinality(1);
	chunk.data[0].Append(Value::BIGINT(NumericCast<int64_t>(gstate.insert_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
