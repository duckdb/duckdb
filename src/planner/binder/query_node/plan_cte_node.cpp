#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/query_node/bound_cte_node.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCTENode &node) {
	// Generate the logical plan for the cte_query and child.
	auto cte_query = CreatePlan(*node.query);
	auto cte_child = CreatePlan(*node.child);

	auto root = make_uniq<LogicalMaterializedCTE>(node.ctename, node.setop_index, node.types.size(),
	                                              std::move(cte_query), std::move(cte_child));

	// check if there are any unplanned subqueries left in either child
	has_unplanned_dependent_joins = has_unplanned_dependent_joins || node.child_binder->has_unplanned_dependent_joins ||
	                                node.query_binder->has_unplanned_dependent_joins;

	return VisitQueryNode(node, std::move(root));
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCTENode &node, unique_ptr<LogicalOperator> base) {
	// Generate the logical plan for the cte_query and child.
	auto cte_query = CreatePlan(*node.query);
	unique_ptr<LogicalOperator> cte_child;
	if (node.child && node.child->type == QueryNodeType::CTE_NODE) {
		cte_child = CreatePlan(node.child->Cast<BoundCTENode>(), std::move(base));
	} else if (node.child) {
		cte_child = CreatePlan(*node.child);
	} else {
		cte_child = std::move(base);
	}

	// Only keep the materialized CTE, if it is used
	if (node.child_binder->bind_context.cte_references[node.ctename] &&
	    *node.child_binder->bind_context.cte_references[node.ctename] > 0) {
		auto root = make_uniq<LogicalMaterializedCTE>(node.ctename, node.setop_index, node.types.size(),
		                                              std::move(cte_query), std::move(cte_child));

		// check if there are any unplanned subqueries left in either child
		has_unplanned_dependent_joins = has_unplanned_dependent_joins ||
		                                node.child_binder->has_unplanned_dependent_joins ||
		                                node.query_binder->has_unplanned_dependent_joins;

		return VisitQueryNode(node, std::move(root));
	} else {
		return VisitQueryNode(node, std::move(cte_child));
	}
}

} // namespace duckdb
