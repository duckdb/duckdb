#include "duckdb/common/feature_refresh.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/common/feature_query.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

//! Alias of the trailing marker column that flags recomputed rows. The refresh operator sums this
//! column to report rows_affected (the recomputed tail) and strips it before appending to the version
//! table, so it never becomes part of the feature schema.
static constexpr const char *RECOMPUTED_MARKER_ALIAS = "__feature_recomputed";

static unique_ptr<ColumnRefExpression> FeatureTimestampRef() {
	return make_uniq<ColumnRefExpression>("feature_timestamp");
}

//! A boolean literal (aliased) appended as the last projected column of each refresh branch. TRUE
//! marks a freshly recomputed row, FALSE a row copied forward unchanged from the current version.
static unique_ptr<ParsedExpression> RecomputedMarker(bool recomputed) {
	auto marker = make_uniq<ConstantExpression>(Value::BOOLEAN(recomputed));
	marker->SetAlias(RECOMPUTED_MARKER_ALIAS);
	return std::move(marker);
}

static unique_ptr<BaseTableRef> BuildVersionTableRef(const string &catalog, const string &schema, const string &table) {
	auto result = make_uniq<BaseTableRef>();
	result->catalog_name = catalog;
	result->schema_name = schema;
	result->table_name = table;
	return result;
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

//! (SELECT max(feature_timestamp) FROM <cur_table>)
static unique_ptr<ParsedExpression> BuildMaxTimestampSubquery(const string &catalog, const string &schema,
                                                              const string &cur_table) {
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(FeatureTimestampRef());

	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<FunctionExpression>("max", std::move(args)));
	node->from_table = BuildVersionTableRef(catalog, schema, cur_table);

	auto stmt = make_uniq<SelectStatement>();
	stmt->node = std::move(node);

	auto subquery = make_uniq<SubqueryExpression>();
	subquery->subquery = std::move(stmt);
	subquery->subquery_type = SubqueryType::SCALAR;
	return std::move(subquery);
}

//! max(feature_timestamp) - INTERVAL '<watermark>'. NULL when the current version is empty.
static unique_ptr<ParsedExpression> BuildBoundaryExpression(const string &catalog, const string &schema,
                                                            const string &cur_table,
                                                            const interval_t &watermark_interval) {
	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(BuildMaxTimestampSubquery(catalog, schema, cur_table));
	args.push_back(make_uniq<ConstantExpression>(Value::INTERVAL(watermark_interval)));
	return make_uniq<FunctionExpression>("-", std::move(args), nullptr, nullptr, false, true);
}

//! SELECT * FROM <cur_table> WHERE feature_timestamp < boundary
//! Rows strictly before the recompute boundary are unaffected and carried forward unchanged. When the
//! current version is empty the boundary is NULL, so no rows are copied (there are none anyway).
static unique_ptr<QueryNode> BuildCopyUnaffectedNode(const string &catalog, const string &schema,
                                                     const string &cur_table, const interval_t &watermark_interval) {
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());
	node->select_list.push_back(RecomputedMarker(false));
	node->from_table = BuildVersionTableRef(catalog, schema, cur_table);
	node->where_clause =
	    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, FeatureTimestampRef(),
	                                    BuildBoundaryExpression(catalog, schema, cur_table, watermark_interval));
	return std::move(node);
}

//! <timestamp_column> >= boundary OR boundary IS NULL
//! Anchors at or after the boundary are recomputed. When the current version is empty the boundary is
//! NULL, so every anchor is recomputed (a full materialization).
static unique_ptr<ParsedExpression> BuildTailAnchorFilter(const string &timestamp_column, const string &catalog,
                                                          const string &schema, const string &cur_table,
                                                          const interval_t &watermark_interval) {
	auto at_or_after = make_uniq<ComparisonExpression>(
	    ExpressionType::COMPARE_GREATERTHANOREQUALTO, make_uniq<ColumnRefExpression>(timestamp_column),
	    BuildBoundaryExpression(catalog, schema, cur_table, watermark_interval));
	auto boundary_is_null = make_uniq<OperatorExpression>(
	    ExpressionType::OPERATOR_IS_NULL, BuildBoundaryExpression(catalog, schema, cur_table, watermark_interval));
	return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(at_or_after),
	                                        std::move(boundary_is_null));
}

unique_ptr<SelectStatement> BuildFeatureRefreshQuery(const FeatureCatalogEntry &feat) {
	// current_version == 0 means the feature was created but never refreshed: there is no prior version
	// table to copy forward from, so the first refresh always fully materializes (regardless of refresh
	// mode) and produces feature_name__v1.
	if (feat.refresh_mode == FeatureRefreshMode::FULL || feat.current_version < 1) {
		// Every row is recomputed, so mark them all.
		auto full_stmt = BuildPITQueryAST(feat);
		full_stmt->node->Cast<SelectNode>().select_list.push_back(RecomputedMarker(true));
		return full_stmt;
	}

	// INCREMENTAL: copy the unaffected rows forward and recompute the tail.
	auto catalog = feat.ParentCatalog().GetName();
	auto schema = feat.ParentSchema().name;
	auto cur_table = feat.name + "__v" + duckdb::to_string(feat.current_version);

	auto copy_node = BuildCopyUnaffectedNode(catalog, schema, cur_table, feat.watermark_interval);
	auto tail_filter =
	    BuildTailAnchorFilter(feat.timestamp_column, catalog, schema, cur_table, feat.watermark_interval);
	auto tail_stmt = BuildPITQueryAST(feat, std::move(tail_filter));
	tail_stmt->node->Cast<SelectNode>().select_list.push_back(RecomputedMarker(true));

	auto setop = make_uniq<SetOperationNode>();
	setop->setop_type = SetOperationType::UNION;
	setop->setop_all = true;
	setop->children.push_back(std::move(copy_node));
	setop->children.push_back(std::move(tail_stmt->node));

	auto result = make_uniq<SelectStatement>();
	result->node = std::move(setop);
	return result;
}

} // namespace duckdb
