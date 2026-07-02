#include "duckdb/common/feature_refresh.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/feature_query.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_data/alter_feature_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

static string QuoteIdent(const string &name) {
	return SQLIdentifier::ToString(name);
}

static unique_ptr<ColumnRefExpression> FeatureTimestampRef() {
	return make_uniq<ColumnRefExpression>("feature_timestamp");
}

static unique_ptr<SelectStatement> BuildPITQueryAST(const FeatureCatalogEntry &feat,
                                                    unique_ptr<ParsedExpression> anchor_filter = nullptr) {
	FeaturePITQueryParameters parameters;
	parameters.source_table = feat.source_table;
	parameters.timestamp_column = feat.timestamp_column;
	parameters.entity_columns = feat.entity_columns;
	parameters.window_interval = feat.window_interval;
	parameters.anchor_filter = std::move(anchor_filter);
	return BuildFeaturePITQuery(feat.query->node->Cast<SelectNode>(), parameters);
}

static unique_ptr<CreateStatement> BuildCreateTableAsStatement(const string &catalog, const string &schema,
                                                               const string &table, unique_ptr<SelectStatement> query) {
	auto result = make_uniq<CreateStatement>();
	auto info = make_uniq<CreateTableInfo>(catalog, schema, table);
	info->query = std::move(query);
	result->info = std::move(info);
	return result;
}

static unique_ptr<BaseTableRef> BuildVersionTableRef(const string &catalog, const string &schema, const string &table) {
	auto result = make_uniq<BaseTableRef>();
	result->catalog_name = catalog;
	result->schema_name = schema;
	result->table_name = table;
	return result;
}

static unique_ptr<SelectStatement> BuildCountTableStatement(const string &catalog, const string &schema,
                                                            const string &table) {
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<FunctionExpression>("count_star", vector<unique_ptr<ParsedExpression>>()));
	node->from_table = BuildVersionTableRef(catalog, schema, table);

	auto result = make_uniq<SelectStatement>();
	result->node = std::move(node);
	return result;
}

static unique_ptr<SelectStatement> BuildMaxFeatureTimestampStatement(const string &catalog, const string &schema,
                                                                     const string &table) {
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(FeatureTimestampRef());

	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<FunctionExpression>("max", std::move(args)));
	node->from_table = BuildVersionTableRef(catalog, schema, table);

	auto result = make_uniq<SelectStatement>();
	result->node = std::move(node);
	return result;
}

static unique_ptr<ParsedExpression> BuildRefreshBoundaryExpression(const Value &max_timestamp,
                                                                   const interval_t &watermark_interval) {
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(make_uniq<ConstantExpression>(max_timestamp));
	args.push_back(make_uniq<ConstantExpression>(Value::INTERVAL(watermark_interval)));
	return make_uniq<FunctionExpression>("-", std::move(args), nullptr, nullptr, false, true);
}

static unique_ptr<ParsedExpression> BuildCopyUnaffectedFilter(unique_ptr<ParsedExpression> boundary_expression) {
	return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, FeatureTimestampRef(),
	                                       std::move(boundary_expression));
}

static unique_ptr<ParsedExpression> BuildTailAnchorFilter(const string &timestamp_column,
                                                          unique_ptr<ParsedExpression> boundary_expression) {
	return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	                                       make_uniq<ColumnRefExpression>(timestamp_column),
	                                       std::move(boundary_expression));
}

static unique_ptr<CreateStatement> BuildCopyUnaffectedRowsStatement(const string &catalog, const string &schema,
                                                                    const string &new_table,
                                                                    const string &current_table,
                                                                    unique_ptr<ParsedExpression> boundary_expression) {
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());
	node->from_table = BuildVersionTableRef(catalog, schema, current_table);
	node->where_clause = BuildCopyUnaffectedFilter(std::move(boundary_expression));

	auto query = make_uniq<SelectStatement>();
	query->node = std::move(node);
	return BuildCreateTableAsStatement(catalog, schema, new_table, std::move(query));
}

static unique_ptr<InsertStatement> BuildInsertIntoTableStatement(const string &catalog, const string &schema,
                                                                 const string &table,
                                                                 unique_ptr<SelectStatement> query) {
	auto result = make_uniq<InsertStatement>();
	result->node->catalog = catalog;
	result->node->schema = schema;
	result->node->table = table;
	result->node->select_statement = std::move(query);
	return result;
}

static unique_ptr<DropStatement> BuildDropVersionTableStatement(const string &catalog, const string &schema,
                                                                const string &table) {
	auto result = make_uniq<DropStatement>();
	result->info->catalog = catalog;
	result->info->schema = schema;
	result->info->name = table;
	result->info->type = CatalogType::TABLE_ENTRY;
	result->info->if_not_found = OnEntryNotFound::RETURN_NULL;
	return result;
}

FeatureRefreshResult RefreshFeature(ClientContext &context, const string &feature_name) {
	FeatureRefreshResult result;

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
	auto cur_table_name = feature_name + "__v" + duckdb::to_string(feat.current_version);

	// Wrap all operations in a single transaction for atomicity
	con.BeginTransaction();

	try {
		if (feat.refresh_mode == FeatureRefreshMode::FULL) {
			// FULL refresh: create new version table with all rows
			auto create_statement = BuildCreateTableAsStatement(feat_catalog.GetName(), feat_schema.name,
			                                                    new_table_name, BuildPITQueryAST(feat));
			auto create_result = con.Query(std::move(create_statement));
			if (create_result->HasError()) {
				throw InternalException("Failed to refresh feature '%s': %s", feature_name, create_result->GetError());
			}

			// Count rows inserted
			auto count_statement = BuildCountTableStatement(feat_catalog.GetName(), feat_schema.name, new_table_name);
			auto count_result = con.Query(std::move(count_statement));
			if (!count_result->HasError() && count_result->RowCount() > 0) {
				result.rows_affected = count_result->GetValue(0, 0).GetValue<idx_t>();
			}

		} else {
			// INCREMENTAL refresh: copy rows before the recompute boundary and rebuild the tail.
			// The boundary is max(feature_timestamp) minus the configured watermark interval.
			auto max_statement =
			    BuildMaxFeatureTimestampStatement(feat_catalog.GetName(), feat_schema.name, cur_table_name);
			auto max_result = con.Query(std::move(max_statement));
			if (max_result->HasError()) {
				throw InternalException("Failed to read current max timestamp for feature '%s': %s", feature_name,
				                        max_result->GetError());
			}
			Value max_timestamp;
			if (max_result->RowCount() > 0) {
				auto val = max_result->GetValue(0, 0);
				if (!val.IsNull()) {
					max_timestamp = std::move(val);
				}
			}

			if (max_timestamp.IsNull()) {
				// No existing data — do a full materialization into the new version table.
				auto create_statement = BuildCreateTableAsStatement(feat_catalog.GetName(), feat_schema.name,
				                                                    new_table_name, BuildPITQueryAST(feat));
				auto create_result = con.Query(std::move(create_statement));
				if (create_result->HasError()) {
					throw InternalException("Failed to refresh feature '%s': %s", feature_name,
					                        create_result->GetError());
				}

				auto count_statement =
				    BuildCountTableStatement(feat_catalog.GetName(), feat_schema.name, new_table_name);
				auto count_result = con.Query(std::move(count_statement));
				if (!count_result->HasError() && count_result->RowCount() > 0) {
					result.rows_affected = count_result->GetValue(0, 0).GetValue<idx_t>();
				}
			} else {
				auto recompute_from = BuildRefreshBoundaryExpression(max_timestamp, feat.watermark_interval);

				// Create the new version table with unaffected rows copied forward from the current version table.
				auto create_statement = BuildCopyUnaffectedRowsStatement(
				    feat_catalog.GetName(), feat_schema.name, new_table_name, cur_table_name, recompute_from->Copy());
				auto create_result = con.Query(std::move(create_statement));
				if (create_result->HasError()) {
					throw InternalException("Failed to copy unaffected rows for '%s': %s", feature_name,
					                        create_result->GetError());
				}

				// Recompute the tail while the join still looks back the full window for correct aggregation.
				auto tail_filter = BuildTailAnchorFilter(feat.timestamp_column, std::move(recompute_from));
				auto insert_statement =
				    BuildInsertIntoTableStatement(feat_catalog.GetName(), feat_schema.name, new_table_name,
				                                  BuildPITQueryAST(feat, std::move(tail_filter)));
				auto ins_result = con.Query(std::move(insert_statement));
				if (ins_result->HasError()) {
					throw InternalException("Failed to incrementally refresh feature '%s': %s", feature_name,
					                        ins_result->GetError());
				}
				if (ins_result->RowCount() > 0) {
					result.rows_affected = ins_result->GetValue(0, 0).GetValue<idx_t>();
				}
			}
		}

		// Garbage-collect the version table that just fell outside the retain_versions limit.
		// Each refresh adds one version, so at most one table becomes newly evictable here.
		int64_t evicted_version = new_version - feat.retain_versions;
		if (evicted_version >= 1) {
			auto old_table_name = feature_name + "__v" + duckdb::to_string(evicted_version);
			auto drop_statement =
			    BuildDropVersionTableStatement(feat_catalog.GetName(), feat_schema.name, old_table_name);
			auto drop_result = con.Query(std::move(drop_statement));
			if (drop_result->HasError()) {
				throw InternalException("Failed to garbage-collect version table '%s': %s", old_table_name,
				                        drop_result->GetError());
			}
		}

		// A refresh always creates a new version. Persist the version bump transactionally through the
		// catalog (rather than mutating feat.current_version in place) so it is recorded in the WAL /
		// checkpoint and survives a restart. This is done on the internal connection's (read-write)
		// transaction — the outer table-function transaction is read-only — and is committed atomically
		// together with the version-table creation/GC above.
		AlterEntryData alter_data(feat_catalog.GetName(), feat_schema.name, feature_name,
		                          OnEntryNotFound::THROW_EXCEPTION);
		AlterFeatureInfo alter_info(std::move(alter_data), new_version);
		feat_catalog.Alter(*con.context, alter_info);

		con.Commit();
	} catch (...) {
		con.Rollback();
		throw;
	}

	return result;
}

} // namespace duckdb
