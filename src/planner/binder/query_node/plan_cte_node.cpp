#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/query_node/bound_cte_node.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCTENode &node) {
	// Generate the logical plan for the cte_query and child.
	auto cte_query = CreatePlan(*node.query);
	auto cte_child = CreatePlan(*node.child);

	auto root = make_uniq<LogicalCTE>(node.ctename, node.setop_index, node.types.size(), std::move(cte_query), std::move(cte_child));

	// check if there are any unplanned subqueries left in either child
	has_unplanned_subqueries =
	    node.child_binder->has_unplanned_subqueries || node.query_binder->has_unplanned_subqueries;

	return VisitQueryNode(node, std::move(root));
}

} // namespace duckdb
