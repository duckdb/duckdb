#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

struct RefreshFeatureBindData : public FunctionData {
	string feature_name;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<RefreshFeatureBindData>();
		result->feature_name = feature_name;
		return std::move(result);
	}

	bool Equals(const FunctionData &other) const override {
		return feature_name == other.Cast<RefreshFeatureBindData>().feature_name;
	}
};

struct RefreshFeatureState : public GlobalTableFunctionState {
	bool done = false;
	idx_t rows_affected = 0;
};

static string GranularityToSQL(FeatureGranularity gran) {
	switch (gran) {
	case FeatureGranularity::DAY:
		return "day";
	case FeatureGranularity::HOUR:
		return "hour";
	case FeatureGranularity::MINUTE:
		return "minute";
	default:
		return "day";
	}
}

static string BuildPITQuery(const FeatureCatalogEntry &feat, const string &spine_filter) {
	auto &select_node = feat.query->node->Cast<SelectNode>();
	string agg_exprs;
	for (auto &expr : select_node.select_list) {
		if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			auto &col_ref = expr->Cast<ColumnRefExpression>();
			if (col_ref.GetColumnName() == feat.entity_column) {
				continue;
			}
		}
		if (!agg_exprs.empty()) {
			agg_exprs += ", ";
		}
		agg_exprs += expr->ToString();
		if (expr->HasAlias()) {
			agg_exprs += " AS " + expr->GetAlias();
		}
	}

	auto gran = GranularityToSQL(feat.granularity);
	auto &entity = feat.entity_column;
	auto &ts = feat.timestamp_column;
	auto &table = feat.source_table;
	auto window_interval = StringUtil::Format("%d %s", feat.window_size, gran);

	string pit_sql = StringUtil::Format("SELECT spine.%s, spine.bucket AS feature_timestamp, %s "
	                                    "FROM (SELECT DISTINCT %s, DATE_TRUNC('%s', %s) AS bucket FROM %s%s) AS spine "
	                                    "JOIN %s ON %s.%s = spine.%s "
	                                    "AND DATE_TRUNC('%s', %s.%s) <= spine.bucket "
	                                    "AND DATE_TRUNC('%s', %s.%s) > spine.bucket - INTERVAL '%s' "
	                                    "GROUP BY spine.%s, spine.bucket "
	                                    "ORDER BY spine.%s, spine.bucket",
	                                    entity, agg_exprs,                     // outer SELECT
	                                    entity, gran, ts, table, spine_filter, // spine subquery
	                                    table, table, entity, entity,          // JOIN
	                                    gran, table, ts,                       // AND <=
	                                    gran, table, ts, window_interval,      // AND >
	                                    entity, entity);                       // GROUP BY, ORDER BY

	return pit_sql;
}

static unique_ptr<FunctionData> RefreshFeatureBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RefreshFeatureBindData>();
	result->feature_name = input.inputs[0].GetValue<string>();

	names.emplace_back("rows_affected");
	return_types.emplace_back(LogicalType::BIGINT);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RefreshFeatureInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RefreshFeatureState>();
}

static void RefreshFeatureFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<RefreshFeatureState>();
	if (state.done) {
		return;
	}
	state.done = true;

	auto &bind_data = data_p.bind_data->Cast<RefreshFeatureBindData>();
	auto &feature_name = bind_data.feature_name;

	// Look up the feature catalog entry
	auto schemas = Catalog::GetAllSchemas(context);
	optional_ptr<FeatureCatalogEntry> feature_entry;
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::FEATURE_ENTRY, [&](CatalogEntry &entry) {
			auto &feat = entry.Cast<FeatureCatalogEntry>();
			if (feat.name == feature_name) {
				feature_entry = &feat;
			}
		});
		if (feature_entry) {
			break;
		}
	}

	if (!feature_entry) {
		throw CatalogException("Feature \"%s\" does not exist", feature_name);
	}

	auto &feat = *feature_entry;
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	string table_id = "\"" + feature_name + "\"";

	if (feat.refresh_mode == FeatureRefreshMode::FULL) {
		// FULL refresh: delete all rows and re-insert from full PIT query
		auto del_result = con.Query("DELETE FROM " + table_id);
		if (del_result->HasError()) {
			throw InternalException("Failed to clear feature table '%s': %s", feature_name, del_result->GetError());
		}

		auto pit_sql = BuildPITQuery(feat, "");
		auto insert_sql = "INSERT INTO " + table_id + " " + pit_sql;
		auto ins_result = con.Query(insert_sql);
		if (ins_result->HasError()) {
			throw InternalException("Failed to refresh feature '%s': %s", feature_name, ins_result->GetError());
		}

		state.rows_affected = ins_result->GetValue(0, 0).GetValue<idx_t>();

	} else {
		// INCREMENTAL refresh using watermark + window expansion.
		// Industry-standard approach (Feast/dbt/Spark pattern):
		// 1. Watermark = MAX(feature_timestamp) from backing table
		// 2. New source data = WHERE DATE_TRUNC(gran, ts) > watermark (append-only assumption)
		// 3. Affected bucket range = [watermark, MAX(new buckets) + WINDOW)
		//    because a new row at time T can affect any bucket in [T, T + WINDOW)
		// 4. Delete & recompute only the affected range

		auto gran = GranularityToSQL(feat.granularity);
		auto &ts_col = feat.timestamp_column;
		auto &src_table = feat.source_table;

		// Step 1: Get watermark (last materialized bucket)
		auto max_result = con.Query("SELECT MAX(feature_timestamp) FROM " + table_id);
		string watermark;
		if (!max_result->HasError() && max_result->RowCount() > 0) {
			auto val = max_result->GetValue(0, 0);
			if (!val.IsNull()) {
				watermark = val.ToString();
			}
		}

		if (watermark.empty()) {
			// No existing data — do a full materialization
			auto pit_sql = BuildPITQuery(feat, "");
			auto insert_sql = "INSERT INTO " + table_id + " " + pit_sql;
			auto ins_result = con.Query(insert_sql);
			if (ins_result->HasError()) {
				throw InternalException("Failed to refresh feature '%s': %s", feature_name, ins_result->GetError());
			}
			state.rows_affected = ins_result->GetValue(0, 0).GetValue<idx_t>();
		} else {
			// Step 2: Find the range of new source data (buckets > watermark)
			auto range_sql = "SELECT MIN(DATE_TRUNC('" + gran + "', " + ts_col +
			                 ")), "
			                 "MAX(DATE_TRUNC('" +
			                 gran + "', " + ts_col +
			                 ")) "
			                 "FROM " +
			                 src_table + " WHERE DATE_TRUNC('" + gran + "', " + ts_col + ") > '" + watermark +
			                 "'::TIMESTAMP";
			auto range_result = con.Query(range_sql);

			string earliest_new, latest_new;
			if (!range_result->HasError() && range_result->RowCount() > 0) {
				auto min_val = range_result->GetValue(0, 0);
				auto max_val = range_result->GetValue(1, 0);
				if (!min_val.IsNull()) {
					earliest_new = min_val.ToString();
				}
				if (!max_val.IsNull()) {
					latest_new = max_val.ToString();
				}
			}

			if (earliest_new.empty()) {
				// No new source data — nothing to do
				state.rows_affected = 0;
				output.SetCardinality(1);
				output.data[0].Append(Value::BIGINT(0));
				return;
			}

			// Step 3: Compute affected bucket range
			// A new row at bucket T affects buckets [T, T + WINDOW).
			// So affected range = [earliest_new, latest_new + WINDOW).
			auto window_interval = StringUtil::Format("%d %s", feat.window_size, gran);
			string upper_bound = "'" + latest_new + "'::TIMESTAMP + INTERVAL '" + window_interval + "'";

			// Step 4: Delete affected buckets from backing table
			auto del_sql = "DELETE FROM " + table_id + " WHERE feature_timestamp >= '" + earliest_new +
			               "'::TIMESTAMP"
			               " AND feature_timestamp < " +
			               upper_bound;
			auto del_result = con.Query(del_sql);
			if (del_result->HasError()) {
				throw InternalException("Failed to delete stale rows from '%s': %s", feature_name,
				                        del_result->GetError());
			}

			// Step 5: Recompute — filter spine to affected range
			string filter = " WHERE DATE_TRUNC('" + gran + "', " + ts_col + ") >= '" + earliest_new +
			                "'::TIMESTAMP"
			                " AND DATE_TRUNC('" +
			                gran + "', " + ts_col + ") < " + upper_bound;
			auto pit_sql = BuildPITQuery(feat, filter);
			auto insert_sql = "INSERT INTO " + table_id + " " + pit_sql;
			auto ins_result = con.Query(insert_sql);
			if (ins_result->HasError()) {
				throw InternalException("Failed to incrementally refresh feature '%s': %s", feature_name,
				                        ins_result->GetError());
			}
			state.rows_affected = ins_result->GetValue(0, 0).GetValue<idx_t>();
		}
	}

	output.SetCardinality(1);
	output.data[0].Append(Value::BIGINT(NumericCast<int64_t>(state.rows_affected)));
}

void RefreshFeatureFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("refresh_feature", {LogicalType::VARCHAR}, RefreshFeatureFunction, RefreshFeatureBind,
	                              RefreshFeatureInit));
}

} // namespace duckdb
