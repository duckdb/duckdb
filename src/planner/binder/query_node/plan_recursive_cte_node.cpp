#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/query_node/bound_recursive_cte_node.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundRecursiveCTENode &node) {
	// Generate the logical plan for the left and right sides of the set operation
	node.left_binder->is_outside_flattened = is_outside_flattened;
	node.right_binder->is_outside_flattened = is_outside_flattened;

	auto left_node = node.left_binder->CreatePlan(*node.left);
	auto right_node = node.right_binder->CreatePlan(*node.right);

	// check if there are any unplanned subqueries left in either child
	has_unplanned_dependent_joins =
	    node.left_binder->has_unplanned_dependent_joins || node.right_binder->has_unplanned_dependent_joins;

	// for both the left and right sides, cast them to the same types
	left_node = CastLogicalOperatorToTypes(node.left->types, node.types, std::move(left_node));
	right_node = CastLogicalOperatorToTypes(node.right->types, node.types, std::move(right_node));

	if (!node.right_binder->bind_context.cte_references[node.ctename] ||
	    *node.right_binder->bind_context.cte_references[node.ctename] == 0) {
		auto root = make_uniq<LogicalSetOperation>(node.setop_index, node.types.size(), std::move(left_node),
		                                           std::move(right_node), LogicalOperatorType::LOGICAL_UNION);
		return VisitQueryNode(node, std::move(root));
	}
	auto root = make_uniq<LogicalRecursiveCTE>(node.ctename, node.setop_index, node.types.size(), node.union_all,
	                                           std::move(left_node), std::move(right_node));

	return VisitQueryNode(node, std::move(root));
}

} // namespace duckdb
