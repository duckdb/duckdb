#include "duckdb/common/feature_refresh.hpp"

#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/feature_query.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

static FeatureSnapshotParameters SnapshotParameters(const FeatureCatalogEntry &feat, timestamp_t feature_ts) {
	FeatureSnapshotParameters parameters;
	parameters.entity_table = feat.entity_table;
	parameters.entity_columns = feat.entity_columns;
	parameters.entity_key_columns = feat.entity_key_columns;
	parameters.timestamp_column = feat.timestamp_column;
	parameters.timestamp_table = feat.timestamp_table;
	parameters.window_interval = feat.window_interval;
	parameters.feature_ts = feature_ts;
	return parameters;
}

//! SELECT snap.*, <new_version> AS __feature_version, TIMESTAMP '<feature_ts>' AS __feature_timestamp
//! FROM (<snapshot query>) snap
static unique_ptr<SelectNode> BuildTaggedSnapshotNode(const FeatureCatalogEntry &feat, timestamp_t feature_ts,
                                                      int64_t new_version) {
	auto snapshot =
	    BuildFeatureSnapshotQuery(feat.query->node->Cast<SelectNode>(), SnapshotParameters(feat, feature_ts));

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

unique_ptr<SelectStatement> BuildFeatureRefreshQuery(const FeatureCatalogEntry &feat, timestamp_t feature_ts,
                                                     int64_t new_version) {
	// The child produces exactly the new snapshot (one row per entity), tagged with the version and
	// timestamp. It is appended to the persistent store table; eviction of old versions is handled by the
	// refresh operator, not by rewriting the store.
	auto result = make_uniq<SelectStatement>();
	result->node = BuildTaggedSnapshotNode(feat, feature_ts, new_version);
	return result;
}

} // namespace duckdb
