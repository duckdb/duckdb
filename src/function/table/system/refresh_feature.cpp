#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_schema_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
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

	int64_t new_version = feat.current_version + 1;
	auto new_table_name = feature_name + "__v" + duckdb::to_string(new_version);
	auto new_table_id = QuoteIdent(new_table_name);
	auto cur_table_name = feature_name + "__v" + duckdb::to_string(feat.current_version);
	auto cur_table_id = QuoteIdent(cur_table_name);
	bool did_work = false;

	// Wrap all operations in a single transaction for atomicity
	con.BeginTransaction();

	try {
		if (feat.refresh_mode == FeatureRefreshMode::FULL) {
			// FULL refresh: create new version table with all rows
			auto pit_sql = BuildPITQuery(feat, "");
			auto create_sql = "CREATE TABLE " + new_table_id + " AS " + pit_sql;
			auto create_result = con.Query(create_sql);
			if (create_result->HasError()) {
				throw InternalException("Failed to refresh feature '%s': %s", feature_name, create_result->GetError());
			}

			// Count rows inserted
			auto count_result = con.Query("SELECT COUNT(*) FROM " + new_table_id);
			if (!count_result->HasError() && count_result->RowCount() > 0) {
				state.rows_affected = count_result->GetValue(0, 0).GetValue<idx_t>();
			}
			did_work = true;

		} else {
			// INCREMENTAL refresh using watermark + window expansion.
			auto gran = GranularityToSQL(feat.granularity);
			auto ts_col = QuoteIdent(feat.timestamp_column);
			auto src_table = QuoteIdent(feat.source_table);

			// Step 1: Get watermark (last materialized bucket in current version table)
			auto max_result = con.Query("SELECT MAX(feature_timestamp) FROM " + cur_table_id);
			string watermark;
			if (!max_result->HasError() && max_result->RowCount() > 0) {
				auto val = max_result->GetValue(0, 0);
				if (!val.IsNull()) {
					watermark = val.ToString();
				}
			}

			if (watermark.empty()) {
				// No existing data — do a full materialization into new version table
				auto pit_sql = BuildPITQuery(feat, "");
				auto create_sql = "CREATE TABLE " + new_table_id + " AS " + pit_sql;
				auto create_result = con.Query(create_sql);
				if (create_result->HasError()) {
					throw InternalException("Failed to refresh feature '%s': %s", feature_name,
					                        create_result->GetError());
				}
				auto count_result = con.Query("SELECT COUNT(*) FROM " + new_table_id);
				if (!count_result->HasError() && count_result->RowCount() > 0) {
					state.rows_affected = count_result->GetValue(0, 0).GetValue<idx_t>();
				}
				did_work = true;
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
					// No new source data — nothing to do (version stays the same)
					state.rows_affected = 0;
				} else {
					// Step 3: Compute affected bucket range
					auto window_interval = StringUtil::Format("%d %s", feat.window_size, gran);
					string upper_bound = "'" + latest_new + "'::TIMESTAMP + INTERVAL '" + window_interval + "'";

					// Step 4: Create new version table with unaffected rows from current version
					auto create_sql = "CREATE TABLE " + new_table_id + " AS SELECT * FROM " + cur_table_id +
					                  " WHERE NOT (feature_timestamp >= '" + earliest_new +
					                  "'::TIMESTAMP AND feature_timestamp < " + upper_bound + ")";
					auto create_result = con.Query(create_sql);
					if (create_result->HasError()) {
						throw InternalException("Failed to copy unaffected rows for '%s': %s", feature_name,
						                        create_result->GetError());
					}

					// Step 5: Recompute affected range and insert into new version table
					string filter = " WHERE DATE_TRUNC('" + gran + "', " + ts_col + ") >= '" + earliest_new +
					                "'::TIMESTAMP"
					                " AND DATE_TRUNC('" +
					                gran + "', " + ts_col + ") < " + upper_bound;
					auto pit_sql = BuildPITQuery(feat, filter);
					auto insert_sql = "INSERT INTO " + new_table_id + " SELECT * FROM (" + pit_sql + ")";
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

		// Garbage-collect old version tables beyond retain limit (only if we created a new version)
		if (did_work) {
			// Garbage-collect old version tables beyond retain limit
			int64_t min_retain_version = new_version - feat.retain_versions + 1;
			if (min_retain_version > 1) {
				for (int64_t v = min_retain_version - 1; v >= 1; v--) {
					auto old_table_name = feature_name + "__v" + duckdb::to_string(v);
					auto drop_sql = "DROP TABLE IF EXISTS " + QuoteIdent(old_table_name);
					auto drop_result = con.Query(drop_sql);
					if (drop_result->HasError()) {
						throw InternalException("Failed to garbage-collect version table '%s': %s", old_table_name,
						                        drop_result->GetError());
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
