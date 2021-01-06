#include "duckdb/optimizer/deliminator.hpp"

#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Deliminator::Optimize(unique_ptr<LogicalOperator> op) {
	root = op.get();
	RemoveRedundantDelims(&op);
	return op;
}

void Deliminator::RemoveRedundantDelims(unique_ptr<LogicalOperator> *op_ptr) {
	auto &op = *op_ptr->get();
	// comparison joins with a DelimGet as a child can be redundant
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		for (idx_t i = 0; i < op.children.size(); i++) {
			if (op.children[i]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
				auto &join = (LogicalComparisonJoin &)op;
				auto &delim_get = (LogicalDelimGet &)*op.children[i];
				if (join.conditions.size() != delim_get.chunk_types.size()) {
					// joining with DelimGet adds new information
					continue;
				}
				// collect information from the join conditions
				bool new_information = false;
				vector<unique_ptr<Expression>> from;
				vector<unique_ptr<Expression>> to;
				vector<unique_ptr<Expression>> not_null_to;
				for (auto &cond : join.conditions) {
					if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
						// one of the join conditions is not equality, therefore new information is introduced
						new_information = true;
						break;
					}
					from.push_back(i == 0 ? cond.left->Copy() : cond.right->Copy());
					to.push_back(i == 0 ? cond.right->Copy() : cond.left->Copy());
					if (!cond.null_values_are_equal) {
						not_null_to.push_back(i == 0 ? cond.right->Copy() : cond.left->Copy());
					}
					if (from.back()->type != ExpressionType::BOUND_COLUMN_REF) {
						// FIXME: remove properly deal with non-colref e.g. operator -(4, 1) in 4-i=j
						new_information = true;
						break;
					}
				}
				if (new_information) {
					// we cannot discard the operator because it introduces new information
					break;
				}
				// the operator is redundant, so we can discard it. However, we may need an IS NOT NULL filter
				// because comparing equality to a NULL in the DelimGet evaluates to NULL, adding new information
				if (not_null_to.empty()) {
                    // replace the operator with its child (discarding the join with DelimGet)
					*op_ptr = move(i == 0 ? move(op.children[1]) : move(op.children[0]));
				} else {
					auto filter_expr = make_unique<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL,
					                                                        LogicalType::BOOLEAN);
					for (auto &to_expr : not_null_to) {
						filter_expr->children.push_back(to_expr->Copy());
					}
					auto not_null_filter = make_unique<LogicalFilter>(move(filter_expr));
					not_null_filter->children.push_back(i == 0 ? move(op.children[1]) : move(op.children[0]));
					// replace the operator with its filtered child (discarding the join with DelimGet)
					*op_ptr = move(not_null_filter);
				}
                // replace columns from the DelimGet with corresponding ones from the other side of the join
				UpdatePlan(*root, from, to);
				// continue at the replaced operator
				RemoveRedundantDelims(op_ptr);
				return;
			}
		}
	}
	// continue searching for DelimGets in children
	for (auto &child : op.children) {
		RemoveRedundantDelims(&child);
	}
}

void Deliminator::UpdatePlan(LogicalOperator &op, vector<unique_ptr<Expression>> &from,
                             vector<unique_ptr<Expression>> &to) {
	// update delim joins
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &delim_join = (LogicalDelimJoin &)op;
		// if an expression in 'from' appears in a condition of the delim join
		// AND an expression in 'to' appears in the duplicate-eliminated columns
		// ONLY THEN can it be removed
		auto decs = &delim_join.duplicate_eliminated_columns;
		for (idx_t from_idx = 0; from_idx < from.size(); from_idx++) {
			for (auto &cond : delim_join.conditions) {
				cond.null_values_are_equal = true;
				if (cond.left->Equals(from[from_idx].get()) || cond.right->Equals(from[from_idx].get())) {
					// expression in 'from' appears in a condition of the delim join
					for (idx_t dec_idx = 0; dec_idx < decs->size(); dec_idx++) {
						if (decs->at(dec_idx)->Equals(from[from_idx].get())) {
							// corresponding expression in 'to' appears in the duplicate-eliminated columns, remove it
							decs->erase(decs->begin() + dec_idx);
							break;
						}
					}
					break;
				}
			}
		}
		// if there are no duplicate eliminated columns left, the delim join can be a regular join
		if (decs->empty()) {
			op.type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
		}
	}
	// replace occurrences of 'from' with 'to'
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *child) {
        for (idx_t i = 0; i < from.size(); i++) {
            if (child->get()->Equals(from[i].get())) {
              *child = to[i]->Copy();
            }
        }
	});
	// continue in children
	for (auto &child : op.children) {
        UpdatePlan(*child, from, to);
	}
}

} // namespace duckdb
