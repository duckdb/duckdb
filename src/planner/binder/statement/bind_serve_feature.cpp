#include "duckdb/common/feature_serve.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/serve_feature_statement.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ServeFeatureStatement &stmt) {
	auto serve_select = BuildServeFeatureSelect(context, stmt.features, stmt.spine_table, stmt.spine_entity_override,
	                                            stmt.spine_asof_column);
	return Bind(*serve_select);
}

} // namespace duckdb
