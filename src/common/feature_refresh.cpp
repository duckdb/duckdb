#include "duckdb/common/feature_refresh.hpp"

#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/common/feature_query.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

static FeatureSnapshotParameters SnapshotParameters(const FeatureCatalogEntry &feat, timestamp_t feature_ts) {
	FeatureSnapshotParameters parameters;
	parameters.entity_table = feat.entity_table;
	parameters.source_table = feat.source_table;
	parameters.entity_columns = feat.entity_columns;
	parameters.entity_key_columns = feat.entity_key_columns;
	parameters.timestamp_column = feat.timestamp_column;
	parameters.window_interval = feat.window_interval;
	parameters.feature_ts = feature_ts;
	return parameters;
}

//! SELECT snap.*, <new_version> AS __feature_version, TIMESTAMP '<feature_ts>' AS __feature_timestamp
//! FROM (<snapshot query>) snap
static unique_ptr<SelectNode> BuildTaggedSnapshotNode(const FeatureCatalogEntry &feat, timestamp_t feature_ts,
                                                      int64_t new_version) {
	auto snapshot = BuildFeatureSnapshotQuery(feat.query->node->Cast<SelectNode>(), SnapshotParameters(feat, feature_ts));

	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>("snap"));

	auto version_expr = make_uniq<ConstantExpression>(Value::BIGINT(new_version));
	version_expr->SetAlias(FEATURE_VERSION_COLUMN);
	node->select_list.push_back(std::move(version_expr));

	auto timestamp_expr = make_uniq<ConstantExpression>(Value::TIMESTAMP(feature_ts));
	timestamp_expr->SetAlias(FEATURE_TIMESTAMP_COLUMN);
	node->select_list.push_back(std::move(timestamp_expr));

	node->from_table = make_uniq<SubqueryRef>(std::move(snapshot), "snap");
	return node;
}

//! SELECT * FROM <feature>__v<current_version> WHERE __feature_version > <new_version - retain>
//! Carries forward the rows of versions that are still within the retain window.
static unique_ptr<SelectNode> BuildCarryForwardNode(const FeatureCatalogEntry &feat, int64_t new_version) {
	auto node = make_uniq<SelectNode>();
	node->select_list.push_back(make_uniq<StarExpression>());

	auto prev_table = make_uniq<BaseTableRef>();
	prev_table->catalog_name = feat.ParentCatalog().GetName();
	prev_table->schema_name = feat.ParentSchema().name;
	prev_table->table_name = feat.name + "__v" + to_string(feat.current_version);
	node->from_table = std::move(prev_table);

	int64_t cutoff = new_version - feat.retain_versions;
	node->where_clause = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN,
	                                                     make_uniq<ColumnRefExpression>(FEATURE_VERSION_COLUMN),
	                                                     make_uniq<ConstantExpression>(Value::BIGINT(cutoff)));
	return node;
}

unique_ptr<SelectStatement> BuildFeatureRefreshQuery(const FeatureCatalogEntry &feat, timestamp_t feature_ts,
                                                     int64_t new_version) {
	auto snapshot_node = BuildTaggedSnapshotNode(feat, feature_ts, new_version);

	auto result = make_uniq<SelectStatement>();
	// current_version < 1 means the feature has never been refreshed: there is no previous store table to
	// carry rows forward from, so the new store is just the fresh snapshot.
	if (feat.current_version < 1) {
		result->node = std::move(snapshot_node);
		return result;
	}

	auto setop = make_uniq<SetOperationNode>();
	setop->setop_type = SetOperationType::UNION;
	setop->setop_all = true;
	setop->children.push_back(std::move(snapshot_node));
	setop->children.push_back(BuildCarryForwardNode(feat, new_version));
	result->node = std::move(setop);
	return result;
}

} // namespace duckdb
