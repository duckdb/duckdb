#include "duckdb/optimizer/operation_converter.hpp"

#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"

namespace duckdb {


unique_ptr<LogicalOperator> OperationConverter::Optimize(unique_ptr<LogicalOperator> op) {
	for (auto &child : op->children) {
		child = Optimize(std::move(child));
	}
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT: {
		auto left = std::move(op->children[0]);
		auto right = std::move(op->children[1]);
		D_ASSERT(left->expressions.size() == right->expressions.size());
		vector<JoinCondition> conditions;
		// create equality condition for all columns
		for (idx_t i = 0; i < left->expressions.size(); i++) {
			JoinCondition cond;
			cond.left = left->expressions[i]->Copy();
			cond.right = right->expressions[i]->Copy();
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			conditions.push_back(std::move(cond));
		}
		JoinType join_type = op->type == LogicalOperatorType::LOGICAL_EXCEPT ? JoinType::ANTI : JoinType::SEMI;

		auto join_op = make_uniq<LogicalComparisonJoin>(join_type);
		join_op->children.push_back(std::move(left));
		join_op->children.push_back(std::move(right));
		join_op->conditions = std::move(conditions);

		op = std::move(join_op);
	}
	default:
		break;
	}
	return std::move(op);
}

} // namespace duckdb
