#include "duckdb/optimizer/deliminator.hpp"

#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Deliminator::Optimize(unique_ptr<LogicalOperator> op) {
    vector<unique_ptr<LogicalOperator> *> candidates;
	FindCandidates(&op, candidates);
	
	for (auto candidate : candidates) {
        auto cb_map = column_binding_map_t<unique_ptr<Expression>>();
		if (RemoveCandidate(candidate, cb_map)) {
			UpdatePlan(*op, cb_map);
		}
	}
	return op;
}

void Deliminator::FindCandidates(unique_ptr<LogicalOperator> *op_ptr, vector<unique_ptr<LogicalOperator> *> &candidates) {
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

bool Deliminator::RemoveCandidate(unique_ptr<LogicalOperator> *op_ptr, column_binding_map_t<unique_ptr<Expression>> &cb_map) {
    auto &projection = (LogicalProjection &)**op_ptr;
	auto &join = (LogicalComparisonJoin &)*projection.children[0];
	auto &delim_get = (LogicalDelimGet &)*join.children[0];
    if (join.conditions.size() != delim_get.chunk_types.size()) {
        // joining with DelimGet adds new information
        return false;
    }

	// check if redundant, and collect relevant column information
    vector<unique_ptr<Expression>> nulls_are_equal_exprs;
    for (auto &cond : join.conditions) {
		if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
			// non-equality join condition
			return false;
		}
		if (cond.left->type != ExpressionType::BOUND_COLUMN_REF) {
            // FIXME: properly deal with non-colref e.g. operator -(4, 1) in 4-i=j where i is from DelimGet
            return false;
		}
        auto &left = (BoundColumnRefExpression &)*cond.left;
		cb_map[left.binding] = cond.right->Copy();
		if (!cond.null_values_are_equal) {
            nulls_are_equal_exprs.push_back(cond.right->Copy());
		}
	}
	// replace the redundant join
	if (!nulls_are_equal_exprs.empty()) {
        auto filter_expr = make_unique<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL,
                                                                LogicalType::BOOLEAN);
        for (auto &expr : nulls_are_equal_exprs) {
            filter_expr->children.push_back(expr->Copy());
        }
        auto filter_op = make_unique<LogicalFilter>(move(filter_expr));
		filter_op->children.push_back(move(join.children[0]));
		join.children[0] = move(filter_op);
	}
	projection.children[0] = move(join.children[0]);
    // TODO: collect aliases from the projection somewhere here
	return true;
}

void Deliminator::UpdatePlan(LogicalOperator &op, column_binding_map_t<unique_ptr<Expression>> &cb_map) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
        auto &delim_join = (LogicalDelimJoin &)op;
        auto decs = &delim_join.duplicate_eliminated_columns;
        for (auto &cond : delim_join.conditions) {
			if (cond.comparison != ExpressionType::COMPARE_EQUAL || cond.right->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
            cond.null_values_are_equal = true;
			auto &colref = (BoundColumnRefExpression &)*cond.right;
			if (cb_map.find(colref.binding) != cb_map.end()) {
				auto dec_pos = std::find(decs->begin(), decs->end(), cb_map[colref.binding]);
				if (dec_pos != decs->end()) {
					decs->erase(dec_pos);
				}
			}
		}
		if (decs->empty()) {
			delim_join.type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
		}
	}
	// replace occurrences of removed DelimGet bindings
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *child) {
        if (child->get()->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &colref = (BoundColumnRefExpression &)**child;
            if (cb_map.find(colref.binding) != cb_map.end()) {
                *child = cb_map[colref.binding]->Copy();
			}
		}
	});
	// continue in children
	for (auto &child : op.children) {
        UpdatePlan(*child, cb_map);
	}
}

} // namespace duckdb
