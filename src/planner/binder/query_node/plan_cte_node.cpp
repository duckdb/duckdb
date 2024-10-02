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
	unique_ptr<LogicalOperator> root;
	if (node.child && node.child->type == QueryNodeType::CTE_NODE) {
		root = CreatePlan(node.child->Cast<BoundCTENode>(), std::move(base));
	} else if (node.child) {
		root = CreatePlan(*node.child);
	} else {
		root = std::move(base);
	}

	// Only keep the materialized CTE, if it is used
	if (node.child_binder->bind_context.cte_references[node.ctename] &&
	    *node.child_binder->bind_context.cte_references[node.ctename] > 0) {

		// Push the CTE through single-child operators so query modifiers appear ABOVE the CTE (internal issue #2652)
		// Otherwise, we may have a LIMIT on top of the CTE, and an ORDER BY in the query, and we can't make a TopN
		reference<unique_ptr<LogicalOperator>> cte_child = root;
		while (cte_child.get()->children.size() == 1 && cte_child.get()->type != LogicalOperatorType::LOGICAL_CTE_REF) {
			cte_child = cte_child.get()->children[0];
		}
		cte_child.get() = make_uniq<LogicalMaterializedCTE>(node.ctename, node.setop_index, node.types.size(),
		                                                    std::move(cte_query), std::move(cte_child.get()));

		// check if there are any unplanned subqueries left in either child
		has_unplanned_dependent_joins = has_unplanned_dependent_joins ||
		                                node.child_binder->has_unplanned_dependent_joins ||
		                                node.query_binder->has_unplanned_dependent_joins;
	}
	return VisitQueryNode(node, std::move(root));
}

} // namespace duckdb
