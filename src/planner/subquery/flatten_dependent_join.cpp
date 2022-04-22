#include "duckdb/planner/subquery/flatten_dependent_join.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/subquery/has_correlated_expressions.hpp"
#include "duckdb/planner/subquery/rewrite_correlated_expressions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"

namespace duckdb {

FlattenDependentJoins::FlattenDependentJoins(Binder &binder, const vector<CorrelatedColumnInfo> &correlated,
                                             bool any_join)
    : binder(binder), correlated_columns(correlated), any_join(any_join) {
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		correlated_map[col.binding] = i;
		delim_types.push_back(col.type);
	}
}

bool FlattenDependentJoins::DetectCorrelatedExpressions(LogicalOperator *op) {
	D_ASSERT(op);
	// check if this entry has correlated expressions
	HasCorrelatedExpressions visitor(correlated_columns);
	visitor.VisitOperator(*op);
	bool has_correlation = visitor.has_correlated_expressions;
	// now visit the children of this entry and check if they have correlated expressions
	for (auto &child : op->children) {
		// we OR the property with its children such that has_correlation is true if either
		// (1) this node has a correlated expression or
		// (2) one of its children has a correlated expression
		if (DetectCorrelatedExpressions(child.get())) {
			has_correlation = true;
		}
	}
	// set the entry in the map
	has_correlated_expressions[op] = has_correlation;
	return has_correlation;
}

unique_ptr<LogicalOperator> FlattenDependentJoins::PushDownDependentJoin(unique_ptr<LogicalOperator> plan) {
	bool propagate_null_values = true;
	auto result = PushDownDependentJoinInternal(move(plan), propagate_null_values);
	if (!replacement_map.empty()) {
		// check if we have to replace any COUNT aggregates into "CASE WHEN X IS NULL THEN 0 ELSE COUNT END"
		RewriteCountAggregates aggr(replacement_map);
		aggr.VisitOperator(*result);
	}
	return result;
}

bool SubqueryDependentFilter(Expression *expr) {
	if (expr->expression_class == ExpressionClass::BOUND_CONJUNCTION &&
	    expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto bound_conjuction = (BoundConjunctionExpression *)expr;
		for (auto &child : bound_conjuction->children) {
			if (SubqueryDependentFilter(child.get())) {
				return true;
			}
		}
	}
	if (expr->expression_class == ExpressionClass::BOUND_SUBQUERY) {
		return true;
	}
	return false;
}
unique_ptr<LogicalOperator> FlattenDependentJoins::PushDownDependentJoinInternal(unique_ptr<LogicalOperator> plan,
                                                                                 bool &parent_propagate_null_values) {
	// first check if the logical operator has correlated expressions
	auto entry = has_correlated_expressions.find(plan.get());
	D_ASSERT(entry != has_correlated_expressions.end());
	if (!entry->second) {
		// we reached a node without correlated expressions
		// we can eliminate the dependent join now and create a simple cross product
		auto cross_product = make_unique<LogicalCrossProduct>();
		// now create the duplicate eliminated scan for this node
		auto delim_index = binder.GenerateTableIndex();
		this->base_binding = ColumnBinding(delim_index, 0);
		auto delim_scan = make_unique<LogicalDelimGet>(delim_index, delim_types);
		cross_product->children.push_back(move(delim_scan));
		cross_product->children.push_back(move(plan));
		return move(cross_product);
	}
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_FILTER: {
		// filter
		// first we flatten the dependent join in the child of the filter
		for (auto &expr : plan->expressions) {
			any_join |= SubqueryDependentFilter(expr.get());
		}
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);

		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
		rewriter.VisitOperator(*plan);
		return plan;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// projection
		// first we flatten the dependent join in the child of the projection
		for (auto &expr : plan->expressions) {
			parent_propagate_null_values &= expr->PropagatesNullValues();
		}
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);

		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
		rewriter.VisitOperator(*plan);
		// now we add all the columns of the delim_scan to the projection list
		auto proj = (LogicalProjection *)plan.get();
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			auto colref = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			plan->expressions.push_back(move(colref));
		}

		base_binding.table_index = proj->table_index;
		this->delim_offset = base_binding.column_index = plan->expressions.size() - correlated_columns.size();
		this->data_offset = 0;
		return plan;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = (LogicalAggregate &)*plan;
		// aggregate and group by
		// first we flatten the dependent join in the child of the projection
		for (auto &expr : plan->expressions) {
			parent_propagate_null_values &= expr->PropagatesNullValues();
		}
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
		rewriter.VisitOperator(*plan);
		// now we add all the columns of the delim_scan to the grouping operators AND the projection list
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			auto colref = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			for (auto &set : aggr.grouping_sets) {
				set.insert(aggr.groups.size());
			}
			aggr.groups.push_back(move(colref));
		}
		if (aggr.groups.size() == correlated_columns.size()) {
			// we have to perform a LEFT OUTER JOIN between the result of this aggregate and the delim scan
			// FIXME: this does not always have to be a LEFT OUTER JOIN, depending on whether aggr.expressions return
			// NULL or a value
			unique_ptr<LogicalComparisonJoin> join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
			for (auto &aggr_exp : aggr.expressions) {
				auto b_aggr_exp = (BoundAggregateExpression *)aggr_exp.get();
				if (!b_aggr_exp->PropagatesNullValues() || any_join || !parent_propagate_null_values) {
					join = make_unique<LogicalComparisonJoin>(JoinType::LEFT);
					break;
				}
			}
			auto left_index = binder.GenerateTableIndex();
			auto delim_scan = make_unique<LogicalDelimGet>(left_index, delim_types);
			join->children.push_back(move(delim_scan));
			join->children.push_back(move(plan));
			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				JoinCondition cond;
				cond.left =
				    make_unique<BoundColumnRefExpression>(correlated_columns[i].type, ColumnBinding(left_index, i));
				cond.right = make_unique<BoundColumnRefExpression>(
				    correlated_columns[i].type,
				    ColumnBinding(aggr.group_index, (aggr.groups.size() - correlated_columns.size()) + i));
				cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
				join->conditions.push_back(move(cond));
			}
			// for any COUNT aggregate we replace references to the column with: CASE WHEN COUNT(*) IS NULL THEN 0
			// ELSE COUNT(*) END
			for (idx_t i = 0; i < aggr.expressions.size(); i++) {
				D_ASSERT(aggr.expressions[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
				auto bound = (BoundAggregateExpression *)&*aggr.expressions[i];
				vector<LogicalType> arguments;
				if (bound->function == CountFun::GetFunction() || bound->function == CountStarFun::GetFunction()) {
					// have to replace this ColumnBinding with the CASE expression
					replacement_map[ColumnBinding(aggr.aggregate_index, i)] = i;
				}
			}
			// now we update the delim_index

			base_binding.table_index = left_index;
			this->delim_offset = base_binding.column_index = 0;
			this->data_offset = 0;
			return move(join);
		} else {
			// update the delim_index
			base_binding.table_index = aggr.group_index;
			this->delim_offset = base_binding.column_index = aggr.groups.size() - correlated_columns.size();
			this->data_offset = aggr.groups.size();
			return plan;
		}
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// cross product
		// push into both sides of the plan
		bool left_has_correlation = has_correlated_expressions.find(plan->children[0].get())->second;
		bool right_has_correlation = has_correlated_expressions.find(plan->children[1].get())->second;
		if (!right_has_correlation) {
			// only left has correlation: push into left
			plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
			return plan;
		}
		if (!left_has_correlation) {
			// only right has correlation: push into right
			plan->children[1] = PushDownDependentJoinInternal(move(plan->children[1]), parent_propagate_null_values);
			return plan;
		}
		// both sides have correlation
		// turn into an inner join
		auto join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		auto left_binding = this->base_binding;
		plan->children[1] = PushDownDependentJoinInternal(move(plan->children[1]), parent_propagate_null_values);
		// add the correlated columns to the join conditions
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			JoinCondition cond;
			cond.left = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(left_binding.table_index, left_binding.column_index + i));
			cond.right = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			join->conditions.push_back(move(cond));
		}
		join->children.push_back(move(plan->children[0]));
		join->children.push_back(move(plan->children[1]));
		return move(join);
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = (LogicalJoin &)*plan;
		D_ASSERT(plan->children.size() == 2);
		// check the correlated expressions in the children of the join
		bool left_has_correlation = has_correlated_expressions.find(plan->children[0].get())->second;
		bool right_has_correlation = has_correlated_expressions.find(plan->children[1].get())->second;

		if (join.join_type == JoinType::INNER) {
			// inner join
			if (!right_has_correlation) {
				// only left has correlation: push into left
				plan->children[0] =
				    PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
				return plan;
			}
			if (!left_has_correlation) {
				// only right has correlation: push into right
				plan->children[1] =
				    PushDownDependentJoinInternal(move(plan->children[1]), parent_propagate_null_values);
				return plan;
			}
		} else if (join.join_type == JoinType::LEFT) {
			// left outer join
			if (!right_has_correlation) {
				// only left has correlation: push into left
				plan->children[0] =
				    PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
				return plan;
			}
		} else if (join.join_type == JoinType::RIGHT) {
			// left outer join
			if (!left_has_correlation) {
				// only right has correlation: push into right
				plan->children[1] =
				    PushDownDependentJoinInternal(move(plan->children[1]), parent_propagate_null_values);
				return plan;
			}
		} else if (join.join_type == JoinType::MARK) {
			if (right_has_correlation) {
				throw Exception("MARK join with correlation in RHS not supported");
			}
			// push the child into the LHS
			plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
			// rewrite expressions in the join conditions
			RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
			rewriter.VisitOperator(*plan);
			return plan;
		} else {
			throw Exception("Unsupported join type for flattening correlated subquery");
		}
		// both sides have correlation
		// push into both sides
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		auto left_binding = this->base_binding;
		plan->children[1] = PushDownDependentJoinInternal(move(plan->children[1]), parent_propagate_null_values);
		auto right_binding = this->base_binding;
		// NOTE: for OUTER JOINS it matters what the BASE BINDING is after the join
		// for the LEFT OUTER JOIN, we want the LEFT side to be the base binding after we push
		// because the RIGHT binding might contain NULL values
		if (join.join_type == JoinType::LEFT) {
			this->base_binding = left_binding;
		} else if (join.join_type == JoinType::RIGHT) {
			this->base_binding = right_binding;
		}
		// add the correlated columns to the join conditions
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			auto left = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(left_binding.table_index, left_binding.column_index + i));
			auto right = make_unique<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(right_binding.table_index, right_binding.column_index + i));

			if (join.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
				JoinCondition cond;
				cond.left = move(left);
				cond.right = move(right);
				cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;

				auto &comparison_join = (LogicalComparisonJoin &)join;
				comparison_join.conditions.push_back(move(cond));
			} else {
				auto &any_join = (LogicalAnyJoin &)join;
				auto comparison = make_unique<BoundComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
				                                                         move(left), move(right));
				auto conjunction = make_unique<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
				                                                           move(comparison), move(any_join.condition));
				any_join.condition = move(conjunction);
			}
		}
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(right_binding, correlated_map);
		rewriter.VisitOperator(*plan);
		return plan;
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = (LogicalLimit &)*plan;
		if (limit.offset_val > 0) {
			throw ParserException("OFFSET not supported in correlated subquery");
		}
		if (limit.limit) {
			throw ParserException("Non-constant limit not supported in correlated subquery");
		}
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		if (limit.limit_val == 0) {
			// limit = 0 means we return zero columns here
			return plan;
		} else {
			// limit > 0 does nothing
			return move(plan->children[0]);
		}
	}
	case LogicalOperatorType::LOGICAL_LIMIT_PERCENT: {
		throw ParserException("Limit percent operator not supported in correlated subquery");
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		auto &window = (LogicalWindow &)*plan;
		// push into children
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		// add the correlated columns to the PARTITION BY clauses in the Window
		for (auto &expr : window.expressions) {
			D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto &w = (BoundWindowExpression &)*expr;
			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				w.partitions.push_back(make_unique<BoundColumnRefExpression>(
				    correlated_columns[i].type,
				    ColumnBinding(base_binding.table_index, base_binding.column_index + i)));
			}
		}
		return plan;
	}
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_UNION: {
		auto &setop = (LogicalSetOperation &)*plan;
		// set operator, push into both children
		plan->children[0] = PushDownDependentJoin(move(plan->children[0]));
		plan->children[1] = PushDownDependentJoin(move(plan->children[1]));
		// we have to refer to the setop index now
		base_binding.table_index = setop.table_index;
		base_binding.column_index = setop.column_count;
		setop.column_count += correlated_columns.size();
		return plan;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT:
		plan->children[0] = PushDownDependentJoin(move(plan->children[0]));
		return plan;
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// expression get
		// first we flatten the dependent join in the child
		plan->children[0] = PushDownDependentJoinInternal(move(plan->children[0]), parent_propagate_null_values);
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map);
		rewriter.VisitOperator(*plan);
		// now we add all the correlated columns to each of the expressions of the expression scan
		auto expr_get = (LogicalExpressionGet *)plan.get();
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			for (auto &expr_list : expr_get->expressions) {
				auto colref = make_unique<BoundColumnRefExpression>(
				    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
				expr_list.push_back(move(colref));
			}
			expr_get->expr_types.push_back(correlated_columns[i].type);
		}

		base_binding.table_index = expr_get->table_index;
		this->delim_offset = base_binding.column_index = expr_get->expr_types.size() - correlated_columns.size();
		this->data_offset = 0;
		return plan;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		throw ParserException("ORDER BY not supported in correlated subquery");
	default:
		throw InternalException("Logical operator type \"%s\" for dependent join", LogicalOperatorToString(plan->type));
	}
}

} // namespace duckdb
