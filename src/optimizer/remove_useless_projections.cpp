#include "duckdb/optimizer/remove_useless_projections.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> RemoveUselessProjections::RemoveProjectionsChildren(unique_ptr<LogicalOperator> op) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		op->children[i] = RemoveProjections(std::move(op->children[i]));
	}
	return op;
}

unique_ptr<LogicalOperator> RemoveUselessProjections::RemoveProjections(unique_ptr<LogicalOperator> op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
		for (idx_t i = 0; i < op->children.size(); i++) {
			first_projection = true;
			op->children[i] = RemoveProjections(std::move(op->children[i]));
		}
		return op;
	}
	default:
		break;
	}
	if (op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return RemoveProjectionsChildren(std::move(op));
	}
	// operator is a projection. Remove if possible
	if (first_projection) {
		first_projection = false;
		return RemoveProjectionsChildren(std::move(op));
	}
	auto &proj = op->Cast<LogicalProjection>();
	auto child_bindings = op->children[0]->GetColumnBindings();
	if (proj.GetColumnBindings().size() != child_bindings.size()) {
		return op;
	}
	idx_t binding_index = 0;
	for (auto &expr : proj.expressions) {
		if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
			return op;
		}
		auto &bound_ref = expr->Cast<BoundColumnRefExpression>();
		if (bound_ref.binding != child_bindings[binding_index]) {
			return op;
		}
		binding_index++;
	}
	D_ASSERT(binding_index == op->GetColumnBindings().size());
	// we have a projection where every expression is a bound column ref, and they are in the same order as the
	// bindings of the child. We can remove this projection
	binding_index = 0;
	for (auto &binding : op->GetColumnBindings()) {
		replacer.replacement_bindings.push_back(ReplacementBinding(binding, child_bindings[binding_index]));
		binding_index++;
	}
	return RemoveProjectionsChildren(std::move(op->children[0]));
}

void RemoveUselessProjections::ReplaceBindings(LogicalOperator &op) {
	replacer.VisitOperator(op);
}

} // namespace duckdb
