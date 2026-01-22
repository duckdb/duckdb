#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_recursive_cte.hpp"

namespace duckdb {

BoundStatement Binder::BindNode(RecursiveCTENode &statement) {
	// first recursively visit the recursive CTE operations
	// the left side is visited first and is added to the BindContext of the right side
	D_ASSERT(statement.left);
	D_ASSERT(statement.right);
	if (statement.union_all && !statement.key_targets.empty()) {
		throw BinderException("UNION ALL cannot be used with USING KEY in recursive CTE.");
	}

	auto ctename = statement.ctename;
	auto union_all = statement.union_all;
	auto setop_index = GenerateTableIndex();

	auto left_binder = Binder::CreateBinder(context, this);
	auto left = left_binder->BindNode(*statement.left);

	BoundStatement result;
	// the result types of the CTE are the types of the LHS
	result.types = left.types;
	// names are picked from the LHS, unless aliases are explicitly specified
	result.names = left.names;
	for (idx_t i = 0; i < statement.aliases.size() && i < result.names.size(); i++) {
		result.names[i] = statement.aliases[i];
	}

	// This allows the right side to reference the CTE recursively
	bind_context.AddGenericBinding(setop_index, statement.ctename, result.names, result.types);

	auto right_binder = Binder::CreateBinder(context, this);

	// Add bindings of left side to temporary CTE bindings context
	BindingAlias cte_alias(statement.ctename);
	right_binder->bind_context.AddCTEBinding(setop_index, std::move(cte_alias), result.names, result.types);
	if (!statement.key_targets.empty()) {
		BindingAlias recurring_alias("recurring", statement.ctename);
		right_binder->bind_context.AddCTEBinding(setop_index, std::move(recurring_alias), result.names, result.types);
	}

	auto right = right_binder->BindNode(*statement.right);
	for (auto &c : left_binder->correlated_columns) {
		right_binder->AddCorrelatedColumn(c);
	}

	// move the correlated expressions from the child binders to this binder
	MoveCorrelatedExpressions(*left_binder);
	MoveCorrelatedExpressions(*right_binder);

	vector<unique_ptr<Expression>> key_targets;
	// bind specified keys to the referenced column
	auto expression_binder = ExpressionBinder(*this, context);
	for (auto &expr : statement.key_targets) {
		auto bound_expr = expression_binder.Bind(expr);
		D_ASSERT(bound_expr->type == ExpressionType::BOUND_COLUMN_REF);
		key_targets.push_back(std::move(bound_expr));
	}

	// now both sides have been bound we can resolve types
	if (left.types.size() != right.types.size()) {
		throw BinderException("Set operations can only apply to expressions with the "
		                      "same number of result columns");
	}

	if (!statement.modifiers.empty()) {
		throw NotImplementedException("FIXME: bind modifiers in recursive CTE");
	}

	// Generate the logical plan for the left and right sides of the set operation
	left_binder->is_outside_flattened = is_outside_flattened;
	right_binder->is_outside_flattened = is_outside_flattened;

	auto left_node = std::move(left.plan);
	auto right_node = std::move(right.plan);

	// check if there are any unplanned subqueries left in either child
	has_unplanned_dependent_joins = has_unplanned_dependent_joins || left_binder->has_unplanned_dependent_joins ||
	                                right_binder->has_unplanned_dependent_joins;

	// for both the left and right sides, cast them to the same types
	left_node = CastLogicalOperatorToTypes(left.types, result.types, std::move(left_node));
	right_node = CastLogicalOperatorToTypes(right.types, result.types, std::move(right_node));

	auto recurring_binding = right_binder->GetCTEBinding(BindingAlias("recurring", ctename));
	bool ref_recurring = recurring_binding && recurring_binding->IsReferenced();
	if (key_targets.empty() && ref_recurring) {
		throw InvalidInputException("RECURRING can only be used with USING KEY in recursive CTE.");
	}

	// Check if there is a reference to the recursive or recurring table, if not create a set operator.
	auto cte_binding = right_binder->GetCTEBinding(BindingAlias(ctename));
	bool ref_cte = cte_binding && cte_binding->IsReferenced();
	if (!ref_cte && !ref_recurring) {
		auto root =
		    make_uniq<LogicalSetOperation>(setop_index, result.types.size(), std::move(left_node),
		                                   std::move(right_node), LogicalOperatorType::LOGICAL_UNION, union_all);
		result.plan = std::move(root);
	} else {
		auto root = make_uniq<LogicalRecursiveCTE>(ctename, setop_index, result.types.size(), union_all,
		                                           std::move(key_targets), std::move(left_node), std::move(right_node));
		root->ref_recurring = ref_recurring;
		result.plan = std::move(root);
	}
	return result;
}

} // namespace duckdb
