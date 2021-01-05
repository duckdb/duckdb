#include "duckdb/optimizer/deliminator.hpp"

#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Deliminator::Rewrite(unique_ptr<LogicalOperator> op) {
	root = op.get();
    op = FindDelimGets(move(op));
	op = RemoveDelimJoins(move(op));
    return op;

}

unique_ptr<LogicalOperator> Deliminator::FindDelimGets(unique_ptr<LogicalOperator> op) {
	// evaluate whether current operator is redundant
	for (idx_t i = 0; i < op->children.size(); i++) {
		if (op->children[i]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
            if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
                // join with a DelimGet as a child: redundant
				// TODO: something wrong here
                auto join = (LogicalComparisonJoin &)*op;
				vector<ColumnBinding> from;
                vector<ColumnBinding> to;
				for (auto &cond : join.conditions) {
                    auto &left = (BoundColumnRefExpression &)*cond.left;
                    auto &right = (BoundColumnRefExpression &)*cond.left;
					from.push_back(i == 0 ? left.binding : right.binding);
                    to.push_back(i == 0 ? right.binding : left.binding);
				}
//                // replace all occurrences of the duplicate eliminated column with the original value
//				LogicalOperatorVisitor::EnumerateExpressions(*root, [&](unique_ptr<Expression> *child) {
//					Replace(**child, from, to);
//				});
				// now replace the operator
                op = i == 0 ? move(op->children[1]) : move(op->children[0]);
                return FindDelimGets(move(op));
            }
		}
	}
    // continue searching for delim gets in children
    for (idx_t i = 0; i < op->children.size(); i++) {
        op->children[i] = FindDelimGets(move(op->children[i]));
    }
    return op;
}

void Deliminator::Replace(Expression &expr, vector<ColumnBinding> &from, vector<ColumnBinding> &to) {
    if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expr;
		for (idx_t i = 0; i < from.size(); i++) {
            if (colref.binding == from[i]) {
				colref.binding = to[i];
			}
		}
	}
}

unique_ptr<LogicalOperator> Deliminator::RemoveDelimJoins(unique_ptr<LogicalOperator> op) {
    if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
//        auto delim_join = (LogicalDelimJoin &)*op;
//		if (delim_join.duplicate_eliminated_columns.empty()) {
//			op->type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
//		}
	}
    for (idx_t i = 0; i < op->children.size(); i++) {
        op->children[i] = RemoveDelimJoins(move(op->children[i]));
    }
    return op;
}

} // namespace duckdb
