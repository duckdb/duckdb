#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

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

	names.emplace_back("source_table");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("entity_column");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("timestamp_column");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("granularity");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("window_size");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("refresh_mode");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("retain_versions");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("sql");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("last_refresh_timestamp");
	return_types.emplace_back(LogicalType::TIMESTAMP);

	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DuckDBFeaturesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBFeaturesData>();

	// scan all the schemas for features and collect them
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::FEATURE_ENTRY,
		                  [&](CatalogEntry &entry) { result->entries.push_back(entry.Cast<FeatureCatalogEntry>()); });
	}
	return std::move(result);
}

static string GranularityToString(FeatureGranularity granularity) {
	switch (granularity) {
	case FeatureGranularity::DAY:
		return "DAY";
	case FeatureGranularity::HOUR:
		return "HOUR";
	case FeatureGranularity::MINUTE:
		return "MINUTE";
	default:
		return "UNKNOWN";
	}
}

static string RefreshModeToString(FeatureRefreshMode mode) {
	switch (mode) {
	case FeatureRefreshMode::FULL:
		return "FULL";
	case FeatureRefreshMode::INCREMENTAL:
		return "INCREMENTAL";
	default:
		return "UNKNOWN";
	}
}

static void DuckDBFeaturesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBFeaturesData>();
	if (data.offset >= data.entries.size()) {
		return;
	}

	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &feat = data.entries[data.offset++].get();

		// database_name
		output.data[0].Append(Value(feat.catalog.GetName()));
		// schema_name
		output.data[1].Append(Value(feat.schema.name));
		// feature_name
		output.data[2].Append(Value(feat.name));
		// source_table
		output.data[3].Append(Value(feat.source_table));
		// entity_column
		output.data[4].Append(Value(feat.entity_column));
		// timestamp_column
		output.data[5].Append(Value(feat.timestamp_column));
		// granularity
		output.data[6].Append(Value(GranularityToString(feat.granularity)));
		// window_size
		output.data[7].Append(Value::BIGINT(feat.window_size));
		// refresh_mode
		output.data[8].Append(Value(RefreshModeToString(feat.refresh_mode)));
		// retain_versions
		output.data[9].Append(Value::BIGINT(feat.retain_versions));
		// sql
		output.data[10].Append(Value(feat.ToSQL()));
		// last_refresh_timestamp
		output.data[11].Append(Value::TIMESTAMP(feat.last_refresh_timestamp));

		count++;
	}
	output.SetCardinality(count);
}

void DuckDBFeaturesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(
	    TableFunction("duckdb_features", {}, DuckDBFeaturesFunction, DuckDBFeaturesBind, DuckDBFeaturesInit));
}

} // namespace duckdb
