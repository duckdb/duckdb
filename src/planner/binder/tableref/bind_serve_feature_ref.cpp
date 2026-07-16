#include "duckdb/common/feature_serve.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/serve_feature_ref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ServeFeatureRef &ref) {
	// Resolve to a subquery that joins the spine against each feature's denormalized store, then bind that.
	// Wrapping in a subquery (rather than binding the select directly) lets the surrounding query own WHERE /
	// GROUP BY / ORDER BY / JOIN and carries any alias the FROM clause attached to the SERVE reference.
	auto serve_select = BuildServeFeatureSelect(context, ref.features, ref.spine_table, ref.spine_entity_override,
	                                            ref.spine_asof_column);
	auto subquery = make_uniq<SubqueryRef>(std::move(serve_select));
	subquery->alias = ref.alias;
	subquery->column_name_alias = ref.column_name_alias;
	return Bind(*subquery);
}

} // namespace duckdb
