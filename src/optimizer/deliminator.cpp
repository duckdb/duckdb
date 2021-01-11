#include "duckdb/optimizer/deliminator.hpp"

#include "duckdb/planner/operator/logical_delim_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Deliminator::Optimize(unique_ptr<LogicalOperator> op) {
	vector<unique_ptr<LogicalOperator> *> candidates;
	FindCandidates(&op, candidates);

	for (auto candidate : candidates) {
		auto expr_map = expression_map_t<Expression *>();
		column_binding_map_t<bool> projection_map;
		unique_ptr<LogicalOperator> temp_ptr = nullptr;
		if (RemoveCandidate(candidate, expr_map, projection_map, &temp_ptr)) {
			UpdatePlan(*op, expr_map, projection_map);
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
	if ( // Projection/Aggregate
	    (op->type == LogicalOperatorType::LOGICAL_PROJECTION ||
	     op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) &&
	    // followed by a Join
	    op->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    // DelimGet as direct child
	    (op->children[0]->children[0]->type == LogicalOperatorType::LOGICAL_DELIM_GET ||
	     // Filter + DelimGet as a child
	     (op->children[0]->children[0]->type == LogicalOperatorType::LOGICAL_FILTER &&
	      op->children[0]->children[0]->children[0]->type == LogicalOperatorType::LOGICAL_DELIM_GET))) {
		candidates.push_back(op_ptr);
	}
}

bool Deliminator::RemoveCandidate(unique_ptr<LogicalOperator> *op_ptr, expression_map_t<Expression *> &expr_map,
                                  column_binding_map_t<bool> &projection_map, unique_ptr<LogicalOperator> *temp_ptr) {
	auto &proj_or_agg = **op_ptr;
	auto &join = (LogicalComparisonJoin &)*proj_or_agg.children[0];
	auto filter =
	    (LogicalFilter *)(join.children[0]->type == LogicalOperatorType::LOGICAL_FILTER ? join.children[0].get()
	                                                                                    : nullptr);
	auto &delim_get = (LogicalDelimGet &)*(join.children[0]->type == LogicalOperatorType::LOGICAL_FILTER
	                                           ? join.children[0]->children[0]
	                                           : join.children[0]);
	if (join.conditions.size() != delim_get.chunk_types.size()) {
		// joining with DelimGet adds new information
		return false;
	}
	// check if joining with the DelimGet is redundant, and collect relevant column information
	vector<Expression *> nulls_are_not_equal_exprs;
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
			nulls_are_not_equal_exprs.push_back(cond.right.get());
		}
	}
	// removed DelimGet columns are assigned a new ColumnBinding by Projection/Aggregation, keep track here
	if (proj_or_agg.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		for (auto &cb : proj_or_agg.GetColumnBindings()) {
			projection_map[cb] = true;
			for (auto &expr : nulls_are_not_equal_exprs) {
				if (proj_or_agg.expressions[cb.column_index]->Equals(expr)) {
					projection_map[cb] = false;
					break;
				}
			}
		}
	} else {
		auto &agg = (LogicalAggregate &)proj_or_agg;
		for (auto &cb : agg.GetColumnBindings()) {
			projection_map[cb] = true;
			for (auto &expr : nulls_are_not_equal_exprs) {
				if ((cb.table_index == agg.group_index && agg.groups[cb.column_index]->Equals(expr)) ||
				    (cb.table_index == agg.aggregate_index && agg.expressions[cb.column_index]->Equals(expr))) {
					projection_map[cb] = false;
					break;
				}
			}
		}
	}
	// make a filter if needed
	if (!nulls_are_not_equal_exprs.empty() || filter != nullptr) {
		auto filter_op = make_unique<LogicalFilter>();
		if (!nulls_are_not_equal_exprs.empty()) {
			// IS NOT NULL filter that was in JoinCondition::null_values_are_equal before
			auto is_not_null_expr =
			    make_unique<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
			for (auto &expr : nulls_are_not_equal_exprs) {
				is_not_null_expr->children.push_back(expr->Copy());
			}
			filter_op->expressions.push_back(move(is_not_null_expr));
		}
		if (filter != nullptr) {
            LogicalOperatorVisitor::EnumerateExpressions(*filter, [&](unique_ptr<Expression> *child) {
              if (expr_map.find(child->get()) != expr_map.end()) {
                  *child = expr_map[child->get()]->Copy();
              }
            });

			// from the filter above the DelimGet
			for (auto &expr : filter->expressions) {
				filter_op->expressions.push_back(move(expr));
			}
		}
		filter_op->children.push_back(move(join.children[1]));
		join.children[1] = move(filter_op);
	}
	// temporarily save deleted operator so its expressions are still available
	*temp_ptr = move(proj_or_agg.children[0]);
	// replace the redundant join
	proj_or_agg.children[0] = move(join.children[1]);
	return true;
}

void Deliminator::UpdatePlan(LogicalOperator &op, expression_map_t<Expression *> &expr_map,
                             column_binding_map_t<bool> &projection_map) {
	if (op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &delim_join = (LogicalDelimJoin &)op;
		auto decs = &delim_join.duplicate_eliminated_columns;
		for (auto &cond : delim_join.conditions) {
			if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			auto &colref = (BoundColumnRefExpression &)*cond.right;
			if (projection_map.find(colref.binding) != projection_map.end()) {
				// value on the right is a projection of removed DelimGet
				for (idx_t i = 0; i < decs->size(); i++) {
					if (decs->at(i)->Equals(cond.left.get())) {
						// the value on the left no longer needs to be a duplicate-eliminated column
						decs->erase(decs->begin() + i);
						break;
					}
				}
				// whether we applied an IS NOT NULL filter
				cond.null_values_are_equal = projection_map[colref.binding];
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
		UpdatePlan(*child, expr_map, projection_map);
	}
}

} // namespace duckdb
