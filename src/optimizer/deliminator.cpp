#include "duckdb/optimizer/deliminator.hpp"

#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Deliminator::Rewrite(unique_ptr<LogicalOperator> op) {
	root = op.get();
	RemoveRedundantDelimGets(&op);
	return op;
}

void Deliminator::RemoveRedundantDelimGets(unique_ptr<LogicalOperator> *op_ptr) {
	auto &op = *op_ptr->get();
    // joins with a DelimGet as a child are redundant
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		for (idx_t i = 0; i < op.children.size(); i++) {
			if (op.children[i]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
				// find columns coming from the DelimGet
				auto &join = (LogicalComparisonJoin &)op;
				bool all_equal = true;
				vector<unique_ptr<Expression>> from;
				vector<unique_ptr<Expression>> to;
				for (auto &cond : join.conditions) {
					if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
						all_equal = false;
						break;
					}
					from.push_back(i == 0 ? cond.left->Copy() : cond.right->Copy());
					to.push_back(i == 0 ? cond.right->Copy() : cond.left->Copy());
				}
                if (!all_equal) {
                    break;
                }
                // replace the operator with its child (discarding the join with DelimGet)
                *op_ptr = i == 0 ? move(op.children[1]) : move(op.children[0]);
				// replace columns from the DelimGet with corresponding ones from the other side of the join
				ReplaceRemovedBindings(*root, from, to);
				// continue at the replaced operator
                RemoveRedundantDelimGets(op_ptr);
				return;
			}
		}
	}
	// continue searching for DelimGets in children
	for (auto &child : op.children) {
		RemoveRedundantDelimGets(&child);
	}
}

void Deliminator::ReplaceRemovedBindings(LogicalOperator &op, vector<unique_ptr<Expression>> &from,
                                         vector<unique_ptr<Expression>> &to) {
	// update delim joins
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        // TODO: might need to check whether the FROM column appears in the join conditions of delim_join
		auto &delim_join = (LogicalDelimJoin &)op;
		// if an expression in 'from' appears in a condition of the delim join
		// AND an expression in 'to' appears in the duplicate-eliminated columns
		// ONLY THEN can it be removed
        auto decs = &delim_join.duplicate_eliminated_columns;
		for (idx_t from_idx = 0; from_idx < from.size(); from_idx++) {
			for (auto &cond : delim_join.conditions) {
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
		if ((*child)->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			for (idx_t i = 0; i < from.size(); i++) {
				if (child->get()->Equals(from[i].get())) {
					*child = to[i]->Copy();
				}
			}
		}
	});
	// continue in children
	for (auto &child : op.children) {
		ReplaceRemovedBindings(*child, from, to);
	}
}

} // namespace duckdb
