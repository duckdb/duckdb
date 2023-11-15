#include "duckdb/optimizer/operation_converter.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

OperationConverter::OperationConverter(LogicalOperator &root) : root(root) {
	root.ResolveOperatorTypes();
}

void OperationConverter::Optimize(unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		Optimize(child);
	}
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT: {
		auto &left = op->children[0];
		auto &right = op->children[1];
		auto left_bindings = left->GetColumnBindings();
		auto right_bindings = right->GetColumnBindings();
		D_ASSERT(left_bindings.size() == right_bindings.size());

		D_ASSERT(left->types.size() == left_bindings.size());
		D_ASSERT(right->types.size() == right_bindings.size());
		vector<JoinCondition> conditions;
		// create equality condition for all columns
		for (idx_t i = 0; i < left_bindings.size(); i++) {
			JoinCondition cond;
			cond.left = make_uniq<BoundColumnRefExpression>(left->types[i], left_bindings[i]);
			cond.right = make_uniq<BoundColumnRefExpression>(right->types[i], right_bindings[i]);
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			conditions.push_back(std::move(cond));
		}
		JoinType join_type = op->type == LogicalOperatorType::LOGICAL_EXCEPT ? JoinType::ANTI : JoinType::SEMI;

		auto old_bindings = op->GetColumnBindings();

		auto join_op = make_uniq<LogicalComparisonJoin>(join_type);
		join_op->children.push_back(std::move(left));
		join_op->children.push_back(std::move(right));
		join_op->conditions = std::move(conditions);

		// update the op so that it is the join op.
		op = std::move(join_op);

		// now perform column binding replacement
		auto new_bindings = op->GetColumnBindings();
		D_ASSERT(old_bindings.size() == new_bindings.size());
		vector<ReplacementBinding> replacement_bindings;
		for (idx_t i = 0; i < old_bindings.size(); i++) {
			replacement_bindings.push_back({old_bindings[i], new_bindings[i]});
		}

		auto binding_replacer = ColumnBindingReplacer();
		binding_replacer.replacement_bindings = replacement_bindings;
		binding_replacer.VisitOperator(root);
	}
	default:
		break;
	}
}

} // namespace duckdb
