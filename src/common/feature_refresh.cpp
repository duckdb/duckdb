#include "duckdb/common/feature_refresh.hpp"

#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/common/feature_query.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

unique_ptr<SelectStatement> BuildFeatureRefreshQuery(const FeatureCatalogEntry &feat, timestamp_t feature_ts) {
	FeatureSnapshotParameters parameters;
	parameters.entity_table = feat.entity_table;
	parameters.source_table = feat.source_table;
	parameters.entity_columns = feat.entity_columns;
	parameters.entity_key_columns = feat.entity_key_columns;
	parameters.timestamp_column = feat.timestamp_column;
	parameters.window_interval = feat.window_interval;
	parameters.feature_ts = feature_ts;
	return BuildFeatureSnapshotQuery(feat.query->node->Cast<SelectNode>(), parameters);
}

} // namespace duckdb
