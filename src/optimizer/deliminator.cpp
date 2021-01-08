#include "duckdb/optimizer/deliminator.hpp"

#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

//#include "duckdb/parser/expression_map.hpp"
//#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Deliminator::Optimize(unique_ptr<LogicalOperator> op) {
	vector<unique_ptr<LogicalOperator> *> candidates;
	FindCandidates(&op, candidates);

	for (auto candidate : candidates) {
		auto expr_map = expression_map_t<Expression *>();
        unique_ptr<LogicalOperator> temp_ptr = nullptr;
		if (RemoveCandidate(candidate, expr_map, &temp_ptr)) {
			UpdatePlan(*op, expr_map);
		}
	}
	return op;
}

void Deliminator::FindCandidates(unique_ptr<LogicalOperator> *op_ptr,
                                 vector<unique_ptr<LogicalOperator> *> &candidates) {
	auto op = op_ptr->get();
	// search children before adding, so the deepest candidates get added first
	for (auto &child : op->children) {
		FindCandidates(&child, candidates);
	}
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION &&
	    op->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    op->children[0]->children[0]->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		candidates.push_back(op_ptr);
	}
}

bool Deliminator::RemoveCandidate(unique_ptr<LogicalOperator> *op_ptr,
                                  expression_map_t<Expression *> &expr_map,
                                  unique_ptr<LogicalOperator> *temp_ptr) {
	auto &projection = (LogicalProjection &)**op_ptr;
	auto &join = (LogicalComparisonJoin &)*projection.children[0];
	auto &delim_get = (LogicalDelimGet &)*join.children[0];
	if (join.conditions.size() != delim_get.chunk_types.size()) {
		// joining with DelimGet adds new information
		return false;
	}

	// check if redundant, and collect relevant column information
	vector<Expression *> nulls_are_equal_exprs;
	for (auto &cond : join.conditions) {
		if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
			// non-equality join condition
			return false;
		}
		if (cond.left->type != ExpressionType::BOUND_COLUMN_REF) {
			// FIXME: properly deal with non-colref e.g. operator -(4, 1) in 4-i=j where i is from DelimGet
			return false;
		}
		expr_map[cond.left.get()] = cond.right.get();
		if (!cond.null_values_are_equal) {
			nulls_are_equal_exprs.push_back(cond.right.get());
		}
	}
	// make a filter if needed
	if (!nulls_are_equal_exprs.empty()) {
		auto filter_expr =
		    make_unique<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
		for (auto &expr : nulls_are_equal_exprs) {
			filter_expr->children.push_back(expr->Copy());
		}
		auto filter_op = make_unique<LogicalFilter>(move(filter_expr));
		filter_op->children.push_back(move(join.children[1]));
		join.children[1] = move(filter_op);
	}
	// temporarily save deleted operator so its expressions are still available
    *temp_ptr = move(projection.children[0]);
    // replace the redundant join
	projection.children[0] = move(join.children[1]);
	// TODO: collect aliases from the projection somewhere here
	return true;
}

void Deliminator::UpdatePlan(LogicalOperator &op, expression_map_t<Expression *> &expr_map) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &delim_join = (LogicalDelimJoin &)op;
		auto decs = &delim_join.duplicate_eliminated_columns;
		for (auto &cond : delim_join.conditions) {
			if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
            // we already applied an IS NOT NULL filter TODO: move this to the right spot
			cond.null_values_are_equal = true;
			if (expr_map.find(cond.right.get()) != expr_map.end()) {
				for (idx_t i = 0; i < decs->size(); i++) {
					if (decs->at(i)->Equals(expr_map[cond.right.get()])) {
                        decs->erase(decs->begin() + i);
						break;
					}
				}
			}
		}
		// change type if there are no more duplicate-eliminated columns
		if (decs->empty()) {
			delim_join.type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
		}
	}
	// replace occurrences of removed DelimGet bindings
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *child) {
		if (expr_map.find(child->get()) != expr_map.end()) {
			*child = expr_map[child->get()]->Copy();
		}
	});
	// continue in children
	for (auto &child : op.children) {
		UpdatePlan(*child, expr_map);
	}
}

} // namespace duckdb
