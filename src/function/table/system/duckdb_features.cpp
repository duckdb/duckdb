#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/feature_refresh_scheduler.hpp"

namespace duckdb {

struct DuckDBFeaturesData : public GlobalTableFunctionState {
	DuckDBFeaturesData() : offset(0) {
	}

	vector<reference<FeatureCatalogEntry>> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBFeaturesBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("database_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("feature_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("entity_columns");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("timestamp_column");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("retain_versions");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("current_version");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("last_refresh_timestamp");
	return_types.emplace_back(LogicalType::TIMESTAMP);

	names.emplace_back("schedule_interval");
	return_types.emplace_back(LogicalType::INTERVAL);

	names.emplace_back("schedule_enabled");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("next_refresh_at");
	return_types.emplace_back(LogicalType::TIMESTAMP);

	names.emplace_back("window_interval");
	return_types.emplace_back(LogicalType::INTERVAL);

	// The staleness bound (TTL): a matched snapshot older than this before the request time serves as NULL.
	names.emplace_back("ttl_interval");
	return_types.emplace_back(LogicalType::INTERVAL);

	names.emplace_back("entity_table");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBFeaturesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBFeaturesData>();

	// Scan all schemas for feature catalog entries.
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::FEATURE_ENTRY,
		                  [&](CatalogEntry &entry) { result->entries.push_back(entry.Cast<FeatureCatalogEntry>()); });
	}
	return std::move(result);
}

static void DuckDBFeaturesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBFeaturesData>();
	if (data.offset >= data.entries.size()) {
		return;
	}

	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &feat = data.entries[data.offset++].get();
		auto scheduler = DatabaseInstance::GetDatabase(context).GetFeatureRefreshScheduler();
		timestamp_t next_refresh_at;

		// database_name
		output.data[0].Append(Value(feat.catalog.GetName()));
		// schema_name
		output.data[1].Append(Value(feat.schema.name));
		// feature_name
		output.data[2].Append(Value(feat.name));
		// entity_columns
		output.data[3].Append(Value(StringUtil::Join(feat.entity_columns, ",")));
		// timestamp_column
		output.data[4].Append(Value(feat.timestamp_column));
		// retain_versions
		output.data[5].Append(Value::BIGINT(feat.retain_versions));
		// current_version
		output.data[6].Append(Value::BIGINT(feat.current_version));
		// sql
		output.data[7].Append(Value(feat.ToSQL()));
		// last_refresh_timestamp
		output.data[8].Append(Value::TIMESTAMP(feat.last_refresh_timestamp));
		// schedule_interval (NULL when no schedule is attached)
		output.data[9].Append(feat.has_schedule ? Value::INTERVAL(feat.schedule_interval)
		                                        : Value(LogicalType::INTERVAL));
		// schedule_enabled (false when no schedule is attached)
		output.data[10].Append(Value::BOOLEAN(feat.has_schedule && feat.schedule_enabled));
		// next_refresh_at (NULL when the scheduler is not tracking this feature)
		if (scheduler &&
		    scheduler->GetNextRefreshAt(feat.catalog.GetName(), feat.schema.name, feat.name, next_refresh_at)) {
			output.data[11].Append(Value::TIMESTAMP(next_refresh_at));
		} else {
			output.data[11].Append(Value(LogicalType::TIMESTAMP));
		}
		// window_interval
		output.data[12].Append(Value::INTERVAL(feat.window_interval));
		// ttl_interval
		output.data[13].Append(Value::INTERVAL(feat.ttl_interval));
		// entity_table
		output.data[14].Append(Value(feat.entity_table));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBFeaturesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_features", {}, DuckDBFeaturesFunction, DuckDBFeaturesBind, DuckDBFeaturesInit));
}

} // namespace duckdb
