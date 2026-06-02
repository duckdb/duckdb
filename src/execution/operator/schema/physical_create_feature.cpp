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
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
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
	// Add the version tracking column
	table_info->columns.AddColumn(ColumnDefinition("__feature_version", LogicalType::BIGINT));

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

	// Append __feature_version column (version 1 for initial materialization)
	DataChunk versioned_chunk;
	vector<LogicalType> types;
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		types.push_back(chunk.data[i].GetType());
	}
	types.push_back(LogicalType::BIGINT);
	versioned_chunk.Initialize(Allocator::DefaultAllocator(), types);
	for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
		versioned_chunk.data[i].Reference(chunk.data[i]);
	}
	versioned_chunk.data[chunk.ColumnCount()].Reference(Value::BIGINT(1), count_t(chunk.size()));
	versioned_chunk.SetCardinality(chunk.size());

	vector<unique_ptr<BoundConstraint>> empty_constraints;
	storage.LocalAppend(*gstate.table, context.client, versioned_chunk, empty_constraints, true);
	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalCreateFeature::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<CreateFeatureGlobalState>();
	if (!gstate.table || info->refresh_mode != FeatureRefreshMode::INCREMENTAL) {
		return SinkFinalizeType::READY;
	}

	// Set initial last_bucket_row_count on the feature catalog entry.
	// Watermark = MAX(feature_timestamp) from the freshly materialized table,
	// last floor bucket = watermark - 1 gran, count = source rows in that bucket.
	auto &catalog = Catalog::GetCatalog(context, info->catalog);
	auto &schema = catalog.GetSchema(context, info->schema);
	auto transaction = catalog.GetCatalogTransaction(context);
	auto &duck_schema = schema.Cast<DuckSchemaEntry>();
	auto &set = duck_schema.GetCatalogSet(CatalogType::FEATURE_ENTRY);
	auto feat_entry = set.GetEntry(transaction, info->feature_name);
	if (feat_entry) {
		auto &feat = feat_entry->Cast<FeatureCatalogEntry>();
		auto &db = DatabaseInstance::GetDatabase(context);
		Connection con(db);

		auto table_id = SQLIdentifier::ToString(info->feature_name);
		string gran;
		switch (info->granularity) {
		case FeatureGranularity::DAY:
			gran = "day";
			break;
		case FeatureGranularity::HOUR:
			gran = "hour";
			break;
		case FeatureGranularity::MINUTE:
			gran = "minute";
			break;
		default:
			gran = "day";
			break;
		}
		auto ts_col = SQLIdentifier::ToString(info->timestamp_column);
		auto src_table = SQLIdentifier::ToString(info->source_table);

		auto wm_result = con.Query("SELECT MAX(feature_timestamp) FROM " + table_id);
		if (!wm_result->HasError() && wm_result->RowCount() > 0) {
			auto wm_val = wm_result->GetValue(0, 0);
			if (!wm_val.IsNull()) {
				auto watermark = wm_val.ToString();
				auto count_sql = "SELECT COUNT(*) FROM " + src_table + " WHERE DATE_TRUNC('" + gran + "', " + ts_col +
				                 ") = '" + watermark + "'::TIMESTAMP - INTERVAL '1 " + gran + "'";
				auto count_result = con.Query(count_sql);
				if (!count_result->HasError() && count_result->RowCount() > 0) {
					auto cnt_val = count_result->GetValue(0, 0);
					if (!cnt_val.IsNull()) {
						feat.last_bucket_row_count = cnt_val.GetValue<int64_t>();
					}
				}
			}
		}
	}

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
