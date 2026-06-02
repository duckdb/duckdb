#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/common/sql_identifier.hpp"

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

static string QuoteIdent(const string &name) {
	return SQLIdentifier::ToString(name);
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
			agg_exprs += " AS " + QuoteIdent(expr->GetAlias());
		}
	}

	auto gran = GranularityToSQL(feat.granularity);
	auto entity = QuoteIdent(feat.entity_column);
	auto ts = QuoteIdent(feat.timestamp_column);
	auto table = QuoteIdent(feat.source_table);
	auto window_interval = StringUtil::Format("%d %s", feat.window_size, gran);

	string pit_sql = StringUtil::Format(
	    "SELECT spine.%s, spine.bucket AS feature_timestamp, %s "
	    "FROM (SELECT DISTINCT %s, DATE_TRUNC('%s', %s) + INTERVAL '1 %s' AS bucket FROM %s%s) AS spine "
	    "JOIN %s ON %s.%s = spine.%s "
	    "AND %s.%s < spine.bucket "
	    "AND %s.%s >= spine.bucket - INTERVAL '%s' "
	    "GROUP BY spine.%s, spine.bucket "
	    "ORDER BY spine.%s, spine.bucket",
	    entity, agg_exprs,                           // outer SELECT
	    entity, gran, ts, gran, table, spine_filter, // spine subquery
	    table, table, entity, entity,                // JOIN
	    table, ts,                                   // AND <
	    table, ts, window_interval,                  // AND >=
	    entity, entity);                             // GROUP BY, ORDER BY

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
	const auto &feature_name = bind_data.feature_name;

	// Look up the feature catalog entry via catalog system
	optional_ptr<FeatureCatalogEntry> feature_entry;
	auto schemas = Catalog::GetAllSchemas(context);
	for (auto &schema : schemas) {
		auto entry = schema.get().GetEntry(schema.get().GetCatalogTransaction(context), CatalogType::FEATURE_ENTRY,
		                                   feature_name);
		if (entry) {
			feature_entry = &entry->Cast<FeatureCatalogEntry>();
			break;
		}
	}

	if (!feature_entry) {
		throw CatalogException("Feature \"%s\" does not exist", feature_name);
	}

	auto &feat = *feature_entry;
	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	// Set the connection's default catalog/schema to match the feature's location
	// so that unqualified table references resolve correctly
	auto &feat_catalog = feat.ParentCatalog();
	auto &feat_schema = feat.ParentSchema();
	if (!feat_catalog.GetName().empty()) {
		auto use_result = con.Query("USE " + QuoteIdent(feat_catalog.GetName()));
		if (use_result->HasError()) {
			throw InternalException("Failed to set catalog for feature refresh: %s", use_result->GetError());
		}
	}
	if (!feat_schema.name.empty() && feat_schema.name != DEFAULT_SCHEMA) {
		auto schema_result = con.Query("SET schema = '" + feat_schema.name + "'");
		if (schema_result->HasError()) {
			throw InternalException("Failed to set schema for feature refresh: %s", schema_result->GetError());
		}
	}

	auto table_id = QuoteIdent(feature_name);
	int64_t new_version = feat.current_version + 1;
	bool did_work = false;

	// Wrap all operations in a single transaction for atomicity
	con.BeginTransaction();

	try {
		if (feat.refresh_mode == FeatureRefreshMode::FULL) {
			// FULL refresh: compute all rows with new version tag
			auto pit_sql = BuildPITQuery(feat, "");
			auto insert_sql = "INSERT INTO " + table_id + " SELECT *, " + duckdb::to_string(new_version) +
			                  " AS __feature_version FROM (" + pit_sql + ")";
			auto ins_result = con.Query(insert_sql);
			if (ins_result->HasError()) {
				throw InternalException("Failed to refresh feature '%s': %s", feature_name, ins_result->GetError());
			}

			if (ins_result->RowCount() > 0) {
				state.rows_affected = ins_result->GetValue(0, 0).GetValue<idx_t>();
			}
			did_work = true;

		} else {
			// INCREMENTAL refresh using watermark + late-arrival detection via row count.
			auto gran = GranularityToSQL(feat.granularity);
			auto ts_col = QuoteIdent(feat.timestamp_column);
			auto src_table = QuoteIdent(feat.source_table);

			// Step 1: Get watermark (last materialized ceiling bucket in latest version)
			auto max_result = con.Query("SELECT MAX(feature_timestamp) FROM " + table_id +
			                            " WHERE __feature_version = " + duckdb::to_string(feat.current_version));
			string watermark;
			if (!max_result->HasError() && max_result->RowCount() > 0) {
				auto val = max_result->GetValue(0, 0);
				if (!val.IsNull()) {
					watermark = val.ToString();
				}
			}

			if (watermark.empty()) {
				// No existing data — do a full materialization with new version
				auto pit_sql = BuildPITQuery(feat, "");
				auto insert_sql = "INSERT INTO " + table_id + " SELECT *, " + duckdb::to_string(new_version) +
				                  " AS __feature_version FROM (" + pit_sql + ")";
				auto ins_result = con.Query(insert_sql);
				if (ins_result->HasError()) {
					throw InternalException("Failed to refresh feature '%s': %s", feature_name, ins_result->GetError());
				}
				if (ins_result->RowCount() > 0) {
					state.rows_affected = ins_result->GetValue(0, 0).GetValue<idx_t>();
				}
				did_work = true;
			} else {
				// Step 2a: Check for new data beyond watermark (floor bucket >= watermark)
				auto range_sql = "SELECT MIN(DATE_TRUNC('" + gran + "', " + ts_col +
				                 ")), "
				                 "MAX(DATE_TRUNC('" +
				                 gran + "', " + ts_col +
				                 ")) "
				                 "FROM " +
				                 src_table + " WHERE DATE_TRUNC('" + gran + "', " + ts_col + ") >= '" + watermark +
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
				bool has_forward_data = !earliest_new.empty();

				// Step 2b: Detect late arrivals to the last processed floor bucket.
				// The last floor bucket = watermark - 1 gran (since watermark is a ceiling).
				string last_floor_bucket = "'" + watermark + "'::TIMESTAMP - INTERVAL '1 " + gran + "'";
				auto count_sql = "SELECT COUNT(*) FROM " + src_table + " WHERE DATE_TRUNC('" + gran + "', " + ts_col +
				                 ") = " + last_floor_bucket;
				auto count_result = con.Query(count_sql);
				int64_t current_bucket_count = 0;
				if (!count_result->HasError() && count_result->RowCount() > 0) {
					auto val = count_result->GetValue(0, 0);
					if (!val.IsNull()) {
						current_bucket_count = val.GetValue<int64_t>();
					}
				}
				bool has_late_arrival = (current_bucket_count != feat.last_bucket_row_count);

				if (!has_forward_data && !has_late_arrival) {
					// True no-op: no new data and no late arrivals
					state.rows_affected = 0;
				} else {
					// Determine the effective scan range.
					string effective_earliest;
					string effective_latest;

					if (has_late_arrival) {
						// Extend back to the last floor bucket
						auto floor_result = con.Query("SELECT " + last_floor_bucket);
						if (!floor_result->HasError() && floor_result->RowCount() > 0) {
							effective_earliest = floor_result->GetValue(0, 0).ToString();
						}
						if (!has_forward_data) {
							effective_latest = effective_earliest;
						}
					}

					if (has_forward_data) {
						if (effective_earliest.empty()) {
							effective_earliest = earliest_new;
						}
						effective_latest = latest_new;
					}

					// Step 3: Compute affected bucket range
					auto window_interval = StringUtil::Format("%d %s", feat.window_size, gran);
					string upper_bound = "'" + effective_latest + "'::TIMESTAMP + INTERVAL '" + window_interval + "'";
					auto window_plus_one = StringUtil::Format("%d %s", feat.window_size + 1, gran);
					string upper_bound_ceiling =
					    "'" + effective_latest + "'::TIMESTAMP + INTERVAL '" + window_plus_one + "'";

					// Step 4: Copy unaffected rows from current version to new version
					auto copy_sql = "INSERT INTO " + table_id + " SELECT * REPLACE (" + duckdb::to_string(new_version) +
					                " AS __feature_version) FROM " + table_id +
					                " WHERE __feature_version = " + duckdb::to_string(feat.current_version) +
					                " AND NOT (feature_timestamp > '" + effective_earliest +
					                "'::TIMESTAMP AND feature_timestamp < " + upper_bound_ceiling + ")";
					auto copy_result = con.Query(copy_sql);
					if (copy_result->HasError()) {
						throw InternalException("Failed to copy unaffected rows for '%s': %s", feature_name,
						                        copy_result->GetError());
					}

					// Step 5: Recompute affected range with new version tag
					string filter = " WHERE DATE_TRUNC('" + gran + "', " + ts_col + ") >= '" + effective_earliest +
					                "'::TIMESTAMP"
					                " AND DATE_TRUNC('" +
					                gran + "', " + ts_col + ") < " + upper_bound;
					auto pit_sql = BuildPITQuery(feat, filter);
					auto insert_sql = "INSERT INTO " + table_id + " SELECT *, " + duckdb::to_string(new_version) +
					                  " AS __feature_version FROM (" + pit_sql + ")";
					auto ins_result = con.Query(insert_sql);
					if (ins_result->HasError()) {
						throw InternalException("Failed to incrementally refresh feature '%s': %s", feature_name,
						                        ins_result->GetError());
					}
					if (ins_result->RowCount() > 0) {
						state.rows_affected = ins_result->GetValue(0, 0).GetValue<idx_t>();
					}
					did_work = true;
				}
			}
		}

		// Garbage-collect old versions beyond retain limit (only if we created a new version)
		if (did_work) {
			int64_t min_retain_version = new_version - feat.retain_versions + 1;
			if (min_retain_version > 1) {
				auto gc_sql =
				    "DELETE FROM " + table_id + " WHERE __feature_version < " + duckdb::to_string(min_retain_version);
				auto gc_result = con.Query(gc_sql);
				if (gc_result->HasError()) {
					throw InternalException("Failed to garbage-collect old versions for '%s': %s", feature_name,
					                        gc_result->GetError());
				}
			}

			// Compute new last_bucket_row_count for late-arrival detection on next refresh.
			// New watermark = MAX(feature_timestamp) of the new version.
			// Last floor bucket = new_watermark - 1 gran.
			if (feat.refresh_mode == FeatureRefreshMode::INCREMENTAL) {
				auto gran = GranularityToSQL(feat.granularity);
				auto ts_col = QuoteIdent(feat.timestamp_column);
				auto src_table = QuoteIdent(feat.source_table);
				auto new_wm_result =
				    con.Query("SELECT MAX(feature_timestamp) FROM " + table_id +
				              " WHERE __feature_version = " + duckdb::to_string(new_version));
				if (!new_wm_result->HasError() && new_wm_result->RowCount() > 0) {
					auto new_wm_val = new_wm_result->GetValue(0, 0);
					if (!new_wm_val.IsNull()) {
						auto new_watermark = new_wm_val.ToString();
						auto new_count_sql = "SELECT COUNT(*) FROM " + src_table + " WHERE DATE_TRUNC('" + gran +
						                     "', " + ts_col + ") = '" + new_watermark +
						                     "'::TIMESTAMP - INTERVAL '1 " + gran + "'";
						auto new_count_result = con.Query(new_count_sql);
						if (!new_count_result->HasError() && new_count_result->RowCount() > 0) {
							auto cnt_val = new_count_result->GetValue(0, 0);
							if (!cnt_val.IsNull()) {
								feat.last_bucket_row_count = cnt_val.GetValue<int64_t>();
							}
						}
					}
				}
			}
		}

		con.Commit();
	} catch (...) {
		con.Rollback();
		throw;
	}

	// Update catalog entry version only if work was done
	if (did_work) {
		feat.current_version = new_version;
		feat.last_refresh_timestamp = Timestamp::GetCurrentTimestamp();
	}

	output.SetCardinality(1);
	output.data[0].Append(Value::BIGINT(NumericCast<int64_t>(state.rows_affected)));
}

void RefreshFeatureFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("refresh_feature", {LogicalType::VARCHAR}, RefreshFeatureFunction, RefreshFeatureBind,
	                              RefreshFeatureInit));
}

} // namespace duckdb
