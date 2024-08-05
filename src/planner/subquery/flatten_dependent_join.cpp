#include "duckdb/planner/subquery/flatten_dependent_join.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/subquery/has_correlated_expressions.hpp"
#include "duckdb/planner/subquery/rewrite_correlated_expressions.hpp"
#include "duckdb/planner/subquery/rewrite_cte_scan.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"

namespace duckdb {

FlattenDependentJoins::FlattenDependentJoins(Binder &binder, const vector<CorrelatedColumnInfo> &correlated,
                                             bool perform_delim, bool any_join)
    : binder(binder), delim_offset(DConstants::INVALID_INDEX), correlated_columns(correlated),
      perform_delim(perform_delim), any_join(any_join) {
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		correlated_map[col.binding] = i;
		delim_types.push_back(col.type);
	}
}

bool FlattenDependentJoins::DetectCorrelatedExpressions(LogicalOperator &op, bool lateral, idx_t lateral_depth) {

	bool is_lateral_join = false;

	// check if this entry has correlated expressions
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		is_lateral_join = true;
	}
	HasCorrelatedExpressions visitor(correlated_columns, lateral, lateral_depth);
	visitor.VisitOperator(op);
	bool has_correlation = visitor.has_correlated_expressions;
	int child_idx = 0;
	// now visit the children of this entry and check if they have correlated expressions
	for (auto &child : op.children) {
		auto new_lateral_depth = lateral_depth;
		if (is_lateral_join && child_idx == 1) {
			new_lateral_depth = lateral_depth + 1;
		}
		// we OR the property with its children such that has_correlation is true if either
		// (1) this node has a correlated expression or
		// (2) one of its children has a correlated expression
		if (DetectCorrelatedExpressions(*child, lateral, new_lateral_depth)) {
			has_correlation = true;
		}
		child_idx++;
	}
	// set the entry in the map
	has_correlated_expressions[op] = has_correlation;

	// If we detect correlation in a materialized or recursive CTE, the entire right side of the operator
	// needs to be marked as correlated. Otherwise, function PushDownDependentJoinInternal does not do the
	// right thing.
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
	    op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		if (has_correlation) {
			MarkSubtreeCorrelated(*op.children[1].get());
		}
	}
	return has_correlation;
}

bool FlattenDependentJoins::MarkSubtreeCorrelated(LogicalOperator &op) {
	// Do not mark base table scans as correlated
	auto entry = has_correlated_expressions.find(op);
	D_ASSERT(entry != has_correlated_expressions.end());
	bool has_correlation = entry->second;
	for (auto &child : op.children) {
		has_correlation |= MarkSubtreeCorrelated(*child.get());
	}
	if (op.type != LogicalOperatorType::LOGICAL_GET || op.children.size() == 1) {
		if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
			has_correlated_expressions[op] = true;
			return true;
		} else {
			has_correlated_expressions[op] = has_correlation;
		}
	}
	return has_correlation;
}

unique_ptr<LogicalOperator> FlattenDependentJoins::PushDownDependentJoin(unique_ptr<LogicalOperator> plan,
                                                                         bool propagate_null_values) {
	auto result = PushDownDependentJoinInternal(std::move(plan), propagate_null_values, 0);
	if (!replacement_map.empty()) {
		// check if we have to replace any COUNT aggregates into "CASE WHEN X IS NULL THEN 0 ELSE COUNT END"
		RewriteCountAggregates aggr(replacement_map);
		aggr.VisitOperator(*result);
	}
	return result;
}

bool SubqueryDependentFilter(Expression &expr) {
	if (expr.expression_class == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &bound_conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : bound_conjunction.children) {
			if (SubqueryDependentFilter(*child)) {
				return true;
			}
		}
	}
	if (expr.expression_class == ExpressionClass::BOUND_SUBQUERY) {
		return true;
	}
	return false;
}

unique_ptr<LogicalOperator> FlattenDependentJoins::PushDownDependentJoinInternal(unique_ptr<LogicalOperator> plan,
                                                                                 bool &parent_propagate_null_values,
                                                                                 idx_t lateral_depth) {
	// first check if the logical operator has correlated expressions
	auto entry = has_correlated_expressions.find(*plan);
	bool exit_projection = false;
	unique_ptr<LogicalDelimGet> delim_scan;
	D_ASSERT(entry != has_correlated_expressions.end());
	if (!entry->second) {
		// we reached a node without correlated expressions
		// we can eliminate the dependent join now and create a simple cross product
		// now create the duplicate eliminated scan for this node
		if (plan->type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto &op = plan->Cast<LogicalCTERef>();

			auto rec_cte = binder.recursive_ctes.find(op.cte_index);
			if (rec_cte != binder.recursive_ctes.end()) {
				D_ASSERT(rec_cte->second->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE);
				auto &rec_cte_op = rec_cte->second->Cast<LogicalRecursiveCTE>();
				RewriteCTEScan cte_rewriter(op.cte_index, rec_cte_op.correlated_columns);
				cte_rewriter.VisitOperator(*plan);
			}
		}

		// create cross product with Delim Join
		auto delim_index = binder.GenerateTableIndex();
		base_binding = ColumnBinding(delim_index, 0);

		auto left_columns = plan->GetColumnBindings().size();
		delim_offset = left_columns;
		data_offset = 0;
		delim_scan = make_uniq<LogicalDelimGet>(delim_index, delim_types);
		if (plan->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			// we want to keep the logical projection for positionality.
			exit_projection = true;
		} else {
			auto cross_product = LogicalCrossProduct::Create(std::move(plan), std::move(delim_scan));
			return cross_product;
		}
	}
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_FILTER: {
		// filter
		// first we flatten the dependent join in the child of the filter
		for (auto &expr : plan->expressions) {
			any_join |= SubqueryDependentFilter(*expr);
		}
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);

		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		return plan;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// projection
		// first we flatten the dependent join in the child of the projection
		for (auto &expr : plan->expressions) {
			parent_propagate_null_values &= expr->PropagatesNullValues();
		}

		// if the node has no correlated expressions,
		// push the cross product with the delim get only below the projection.
		// This will preserve positionality of the columns and prevent errors when reordering of
		// delim gets is enabled.
		if (exit_projection) {
			auto cross_product = LogicalCrossProduct::Create(std::move(plan->children[0]), std::move(delim_scan));
			plan->children[0] = std::move(cross_product);
		} else {
			plan->children[0] = PushDownDependentJoinInternal(std::move(plan->children[0]),
			                                                  parent_propagate_null_values, lateral_depth);
		}

		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		// now we add all the columns of the delim_scan to the projection list
		auto &proj = plan->Cast<LogicalProjection>();
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			auto &col = correlated_columns[i];
			auto colref = make_uniq<BoundColumnRefExpression>(
			    col.name, col.type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			plan->expressions.push_back(std::move(colref));
		}

		base_binding.table_index = proj.table_index;
		this->delim_offset = base_binding.column_index = plan->expressions.size() - correlated_columns.size();
		this->data_offset = 0;
		return plan;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = plan->Cast<LogicalAggregate>();
		// aggregate and group by
		// first we flatten the dependent join in the child of the projection
		for (auto &expr : plan->expressions) {
			parent_propagate_null_values &= expr->PropagatesNullValues();
		}
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		// now we add all the columns of the delim_scan to the grouping operators AND the projection list
		idx_t delim_table_index;
		idx_t delim_column_offset;
		idx_t delim_data_offset;
		auto new_group_count = perform_delim ? correlated_columns.size() : 1;
		for (idx_t i = 0; i < new_group_count; i++) {
			auto &col = correlated_columns[i];
			auto colref = make_uniq<BoundColumnRefExpression>(
			    col.name, col.type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			for (auto &set : aggr.grouping_sets) {
				set.insert(aggr.groups.size());
			}
			aggr.groups.push_back(std::move(colref));
		}
		if (!perform_delim) {
			// if we are not performing the duplicate elimination, we have only added the row_id column to the grouping
			// operators in this case, we push a FIRST aggregate for each of the remaining expressions
			delim_table_index = aggr.aggregate_index;
			delim_column_offset = aggr.expressions.size();
			delim_data_offset = aggr.groups.size();
			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				auto &col = correlated_columns[i];
				auto first_aggregate = FirstFun::GetFunction(col.type);
				auto colref = make_uniq<BoundColumnRefExpression>(
				    col.name, col.type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
				vector<unique_ptr<Expression>> aggr_children;
				aggr_children.push_back(std::move(colref));
				auto first_fun =
				    make_uniq<BoundAggregateExpression>(std::move(first_aggregate), std::move(aggr_children), nullptr,
				                                        nullptr, AggregateType::NON_DISTINCT);
				aggr.expressions.push_back(std::move(first_fun));
			}
		} else {
			delim_table_index = aggr.group_index;
			delim_column_offset = aggr.groups.size() - correlated_columns.size();
			delim_data_offset = aggr.groups.size();
		}
		bool ungrouped_join = false;
		if (aggr.grouping_sets.empty()) {
			ungrouped_join = aggr.groups.size() == new_group_count;
		} else {
			for (auto &grouping_set : aggr.grouping_sets) {
				if (grouping_set.size() == new_group_count) {
					ungrouped_join = true;
				}
			}
		}
		if (ungrouped_join) {
			// we have to perform an INNER or LEFT OUTER JOIN between the result of this aggregate and the delim scan
			// this does not always have to be a LEFT OUTER JOIN, depending on whether aggr.expressions return
			// NULL or a value
			JoinType join_type = JoinType::INNER;
			if (any_join || !parent_propagate_null_values) {
				join_type = JoinType::LEFT;
			}
			for (auto &aggr_exp : aggr.expressions) {
				auto &b_aggr_exp = aggr_exp->Cast<BoundAggregateExpression>();
				if (!b_aggr_exp.PropagatesNullValues()) {
					join_type = JoinType::LEFT;
					break;
				}
			}
			unique_ptr<LogicalComparisonJoin> join = make_uniq<LogicalComparisonJoin>(join_type);
			auto left_index = binder.GenerateTableIndex();
			delim_scan = make_uniq<LogicalDelimGet>(left_index, delim_types);
			join->children.push_back(std::move(delim_scan));
			join->children.push_back(std::move(plan));
			for (idx_t i = 0; i < new_group_count; i++) {
				auto &col = correlated_columns[i];
				JoinCondition cond;
				cond.left = make_uniq<BoundColumnRefExpression>(col.name, col.type, ColumnBinding(left_index, i));
				cond.right = make_uniq<BoundColumnRefExpression>(
				    correlated_columns[i].type, ColumnBinding(delim_table_index, delim_column_offset + i));
				cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
				join->conditions.push_back(std::move(cond));
			}
			// for any COUNT aggregate we replace references to the column with: CASE WHEN COUNT(*) IS NULL THEN 0
			// ELSE COUNT(*) END
			for (idx_t i = 0; i < aggr.expressions.size(); i++) {
				D_ASSERT(aggr.expressions[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
				auto &bound = aggr.expressions[i]->Cast<BoundAggregateExpression>();
				vector<LogicalType> arguments;
				if (bound.function == CountFun::GetFunction() || bound.function == CountStarFun::GetFunction()) {
					// have to replace this ColumnBinding with the CASE expression
					replacement_map[ColumnBinding(aggr.aggregate_index, i)] = i;
				}
			}
			// now we update the delim_index
			base_binding.table_index = left_index;
			this->delim_offset = base_binding.column_index = 0;
			this->data_offset = 0;
			return std::move(join);
		} else {
			// update the delim_index
			base_binding.table_index = delim_table_index;
			this->delim_offset = base_binding.column_index = delim_column_offset;
			this->data_offset = delim_data_offset;
			return plan;
		}
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// cross product
		// push into both sides of the plan
		bool left_has_correlation = has_correlated_expressions.find(*plan->children[0])->second;
		bool right_has_correlation = has_correlated_expressions.find(*plan->children[1])->second;
		if (!right_has_correlation) {
			// only left has correlation: push into left
			plan->children[0] = PushDownDependentJoinInternal(std::move(plan->children[0]),
			                                                  parent_propagate_null_values, lateral_depth);
			return plan;
		}
		if (!left_has_correlation) {
			// only right has correlation: push into right
			plan->children[1] = PushDownDependentJoinInternal(std::move(plan->children[1]),
			                                                  parent_propagate_null_values, lateral_depth);
			return plan;
		}
		// both sides have correlation
		// turn into an inner join
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);
		auto left_binding = this->base_binding;
		plan->children[1] =
		    PushDownDependentJoinInternal(std::move(plan->children[1]), parent_propagate_null_values, lateral_depth);
		// add the correlated columns to the join conditions
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			JoinCondition cond;
			cond.left = make_uniq<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(left_binding.table_index, left_binding.column_index + i));
			cond.right = make_uniq<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			join->conditions.push_back(std::move(cond));
		}
		join->children.push_back(std::move(plan->children[0]));
		join->children.push_back(std::move(plan->children[1]));
		return std::move(join);
	}
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		auto &dependent_join = plan->Cast<LogicalJoin>();
		if (!((dependent_join.join_type == JoinType::INNER) || (dependent_join.join_type == JoinType::LEFT))) {
			throw NotImplementedException("Dependent join can only be INNER or LEFT type");
		}
		D_ASSERT(plan->children.size() == 2);
		// Push all the bindings down to the left side so the right side knows where to refer DELIM_GET from
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);

		// Normal rewriter like in other joins
		RewriteCorrelatedExpressions rewriter(this->base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);

		// Recursive rewriter to visit right side of lateral join and update bindings from left
		RewriteCorrelatedExpressions recursive_rewriter(this->base_binding, correlated_map, lateral_depth + 1, true);
		recursive_rewriter.VisitOperator(*plan->children[1]);

		return plan;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = plan->Cast<LogicalJoin>();
		D_ASSERT(plan->children.size() == 2);
		// check the correlated expressions in the children of the join
		bool left_has_correlation = has_correlated_expressions.find(*plan->children[0])->second;
		bool right_has_correlation = has_correlated_expressions.find(*plan->children[1])->second;

		if (join.join_type == JoinType::INNER) {
			// inner join
			if (!right_has_correlation) {
				// only left has correlation: push into left
				plan->children[0] = PushDownDependentJoinInternal(std::move(plan->children[0]),
				                                                  parent_propagate_null_values, lateral_depth);
				// Remove the correlated columns coming from outside for current join node
				return plan;
			}
			if (!left_has_correlation) {
				// only right has correlation: push into right
				plan->children[1] = PushDownDependentJoinInternal(std::move(plan->children[1]),
				                                                  parent_propagate_null_values, lateral_depth);
				// Remove the correlated columns coming from outside for current join node
				return plan;
			}
		} else if (join.join_type == JoinType::LEFT) {
			// left outer join
			if (!right_has_correlation) {
				// only left has correlation: push into left
				plan->children[0] = PushDownDependentJoinInternal(std::move(plan->children[0]),
				                                                  parent_propagate_null_values, lateral_depth);
				// Remove the correlated columns coming from outside for current join node
				return plan;
			}
		} else if (join.join_type == JoinType::RIGHT) {
			// left outer join
			if (!left_has_correlation) {
				// only right has correlation: push into right
				plan->children[1] = PushDownDependentJoinInternal(std::move(plan->children[1]),
				                                                  parent_propagate_null_values, lateral_depth);
				return plan;
			}
		} else if (join.join_type == JoinType::MARK) {
			if (right_has_correlation) {
				throw NotImplementedException("MARK join with correlation in RHS not supported");
			}
			// push the child into the LHS
			plan->children[0] = PushDownDependentJoinInternal(std::move(plan->children[0]),
			                                                  parent_propagate_null_values, lateral_depth);
			// rewrite expressions in the join conditions
			RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
			rewriter.VisitOperator(*plan);
			return plan;
		} else {
			throw NotImplementedException("Unsupported join type for flattening correlated subquery");
		}
		// both sides have correlation
		// push into both sides
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);
		auto left_binding = this->base_binding;
		plan->children[1] =
		    PushDownDependentJoinInternal(std::move(plan->children[1]), parent_propagate_null_values, lateral_depth);
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
			auto left = make_uniq<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(left_binding.table_index, left_binding.column_index + i));
			auto right = make_uniq<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(right_binding.table_index, right_binding.column_index + i));

			if (join.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
			    join.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
				JoinCondition cond;
				cond.left = std::move(left);
				cond.right = std::move(right);
				cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;

				auto &comparison_join = join.Cast<LogicalComparisonJoin>();
				comparison_join.conditions.push_back(std::move(cond));
			} else {
				auto &logical_any_join = join.Cast<LogicalAnyJoin>();
				auto comparison = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
				                                                       std::move(left), std::move(right));
				auto conjunction = make_uniq<BoundConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(comparison), std::move(logical_any_join.condition));
				logical_any_join.condition = std::move(conjunction);
			}
		}
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(right_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		return plan;
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = plan->Cast<LogicalLimit>();
		switch (limit.limit_val.Type()) {
		case LimitNodeType::CONSTANT_PERCENTAGE:
		case LimitNodeType::EXPRESSION_PERCENTAGE:
			// NOTE: limit percent could be supported in a manner similar to the LIMIT above
			// but instead of filtering by an exact number of rows, the limit should be expressed as
			// COUNT computed over the partition multiplied by the percentage
			throw ParserException("Limit percent operator not supported in correlated subquery");
		case LimitNodeType::EXPRESSION_VALUE:
			throw ParserException("Non-constant limit not supported in correlated subquery");
		default:
			break;
		}
		switch (limit.offset_val.Type()) {
		case LimitNodeType::EXPRESSION_VALUE:
			throw ParserException("Non-constant offset not supported in correlated subquery");
		case LimitNodeType::CONSTANT_PERCENTAGE:
		case LimitNodeType::EXPRESSION_PERCENTAGE:
			throw InternalException("Percentage offset in FlattenDependentJoin");
		default:
			break;
		}
		auto rownum_alias = "limit_rownum";
		unique_ptr<LogicalOperator> child;
		unique_ptr<LogicalOrder> order_by;

		// check if the direct child of this LIMIT node is an ORDER BY node, if so, keep it separate
		// this is done for an optimization to avoid having to compute the total order
		if (plan->children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
			order_by = unique_ptr_cast<LogicalOperator, LogicalOrder>(std::move(plan->children[0]));
			child = PushDownDependentJoinInternal(std::move(order_by->children[0]), parent_propagate_null_values,
			                                      lateral_depth);
		} else {
			child = PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values,
			                                      lateral_depth);
		}
		auto child_column_count = child->GetColumnBindings().size();
		// we push a row_number() OVER (PARTITION BY [correlated columns])
		auto window_index = binder.GenerateTableIndex();
		auto window = make_uniq<LogicalWindow>(window_index);
		auto row_number =
		    make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_ROW_NUMBER, LogicalType::BIGINT, nullptr, nullptr);
		auto partition_count = perform_delim ? correlated_columns.size() : 1;
		for (idx_t i = 0; i < partition_count; i++) {
			auto &col = correlated_columns[i];
			auto colref = make_uniq<BoundColumnRefExpression>(
			    col.name, col.type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			row_number->partitions.push_back(std::move(colref));
		}
		if (order_by) {
			// optimization: if there is an ORDER BY node followed by a LIMIT
			// rather than computing the entire order, we push the ORDER BY expressions into the row_num computation
			// this way, the order only needs to be computed per partition
			row_number->orders = std::move(order_by->orders);
		}
		row_number->start = WindowBoundary::UNBOUNDED_PRECEDING;
		row_number->end = WindowBoundary::CURRENT_ROW_ROWS;
		window->expressions.push_back(std::move(row_number));
		window->children.push_back(std::move(child));

		// add a filter based on the row_number
		// the filter we add is "row_number > offset AND row_number <= offset + limit"
		auto filter = make_uniq<LogicalFilter>();
		unique_ptr<Expression> condition;
		auto row_num_ref =
		    make_uniq<BoundColumnRefExpression>(rownum_alias, LogicalType::BIGINT, ColumnBinding(window_index, 0));

		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			auto upper_bound_limit = NumericLimits<int64_t>::Maximum();
			auto limit_val = int64_t(limit.limit_val.GetConstantValue());
			if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				// both offset and limit specified - upper bound is offset + limit
				auto offset_val = int64_t(limit.offset_val.GetConstantValue());
				TryAddOperator::Operation(limit_val, offset_val, upper_bound_limit);
			} else {
				// no offset - upper bound is only the limit
				upper_bound_limit = limit_val;
			}
			auto upper_bound = make_uniq<BoundConstantExpression>(Value::BIGINT(upper_bound_limit));
			condition = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO,
			                                                 row_num_ref->Copy(), std::move(upper_bound));
		}
		// we only need to add "row_number >= offset + 1" if offset is bigger than 0
		if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			auto offset_val = int64_t(limit.offset_val.GetConstantValue());
			auto lower_bound = make_uniq<BoundConstantExpression>(Value::BIGINT(offset_val));
			auto lower_comp = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN,
			                                                       row_num_ref->Copy(), std::move(lower_bound));
			if (condition) {
				auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
				                                                  std::move(lower_comp), std::move(condition));
				condition = std::move(conj);
			} else {
				condition = std::move(lower_comp);
			}
		}
		filter->expressions.push_back(std::move(condition));
		filter->children.push_back(std::move(window));
		// we prune away the row_number after the filter clause using the projection map
		for (idx_t i = 0; i < child_column_count; i++) {
			filter->projection_map.push_back(i);
		}
		return std::move(filter);
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		auto &window = plan->Cast<LogicalWindow>();
		// push into children
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);

		// we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);

		// add the correlated columns to the PARTITION BY clauses in the Window
		for (auto &expr : window.expressions) {
			D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto &w = expr->Cast<BoundWindowExpression>();
			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				w.partitions.push_back(make_uniq<BoundColumnRefExpression>(
				    correlated_columns[i].type,
				    ColumnBinding(base_binding.table_index, base_binding.column_index + i)));
			}
		}
		return plan;
	}
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_UNION: {
		auto &setop = plan->Cast<LogicalSetOperation>();
		// set operator, push into both children
#ifdef DEBUG
		plan->children[0]->ResolveOperatorTypes();
		plan->children[1]->ResolveOperatorTypes();
		D_ASSERT(plan->children[0]->types == plan->children[1]->types);
#endif
		plan->children[0] = PushDownDependentJoin(std::move(plan->children[0]));
		plan->children[1] = PushDownDependentJoin(std::move(plan->children[1]));
#ifdef DEBUG
		D_ASSERT(plan->children[0]->GetColumnBindings().size() == plan->children[1]->GetColumnBindings().size());
		plan->children[0]->ResolveOperatorTypes();
		plan->children[1]->ResolveOperatorTypes();
		D_ASSERT(plan->children[0]->types == plan->children[1]->types);
#endif
		// we have to refer to the setop index now
		base_binding.table_index = setop.table_index;
		base_binding.column_index = setop.column_count;
		setop.column_count += correlated_columns.size();
		return plan;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = plan->Cast<LogicalDistinct>();
		// push down into child
		distinct.children[0] = PushDownDependentJoin(std::move(distinct.children[0]));
		// add all correlated columns to the distinct targets
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			distinct.distinct_targets.push_back(make_uniq<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i)));
		}
		return plan;
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// expression get
		// first we flatten the dependent join in the child
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		// now we add all the correlated columns to each of the expressions of the expression scan
		auto &expr_get = plan->Cast<LogicalExpressionGet>();
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			for (auto &expr_list : expr_get.expressions) {
				auto colref = make_uniq<BoundColumnRefExpression>(
				    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
				expr_list.push_back(std::move(colref));
			}
			expr_get.expr_types.push_back(correlated_columns[i].type);
		}

		base_binding.table_index = expr_get.table_index;
		this->delim_offset = base_binding.column_index = expr_get.expr_types.size() - correlated_columns.size();
		this->data_offset = 0;
		return plan;
	}
	case LogicalOperatorType::LOGICAL_PIVOT:
		throw BinderException("PIVOT is not supported in correlated subqueries yet");
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		plan->children[0] = PushDownDependentJoin(std::move(plan->children[0]));
		return plan;
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = plan->Cast<LogicalGet>();
		if (get.children.size() != 1) {
			throw InternalException("Flatten dependent joins - logical get encountered without children");
		}
		plan->children[0] = PushDownDependentJoin(std::move(plan->children[0]));
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			get.projected_input.push_back(this->delim_offset + i);
		}
		this->delim_offset = get.returned_types.size();
		this->data_offset = 0;

		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		return plan;
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {

#ifdef DEBUG
		plan->children[0]->ResolveOperatorTypes();
		plan->children[1]->ResolveOperatorTypes();
#endif
		idx_t table_index = 0;
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);
		if (plan->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
			auto &setop = plan->Cast<LogicalRecursiveCTE>();
			base_binding.table_index = setop.table_index;
			base_binding.column_index = setop.column_count;
			table_index = setop.table_index;
			setop.correlated_columns = correlated_columns;
		} else if (plan->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			auto &setop = plan->Cast<LogicalMaterializedCTE>();
			base_binding.table_index = setop.table_index;
			base_binding.column_index = setop.column_count;
			table_index = setop.table_index;
		}

		RewriteCTEScan cte_rewriter(table_index, correlated_columns);
		cte_rewriter.VisitOperator(*plan->children[1]);

		parent_propagate_null_values = false;
		plan->children[1] =
		    PushDownDependentJoinInternal(std::move(plan->children[1]), parent_propagate_null_values, lateral_depth);
		RewriteCorrelatedExpressions rewriter(this->base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);

		RewriteCorrelatedExpressions recursive_rewriter(this->base_binding, correlated_map, lateral_depth, true);
		recursive_rewriter.VisitOperator(*plan->children[0]);
		recursive_rewriter.VisitOperator(*plan->children[1]);

#ifdef DEBUG
		plan->children[0]->ResolveOperatorTypes();
		plan->children[1]->ResolveOperatorTypes();
#endif
		if (plan->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
			// we have to refer to the recursive CTE index now
			auto &setop = plan->Cast<LogicalRecursiveCTE>();
			base_binding.table_index = setop.table_index;
			base_binding.column_index = setop.column_count;
			setop.column_count += correlated_columns.size();
		}

		return plan;
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		auto &cteref = plan->Cast<LogicalCTERef>();
		// Read correlated columns from CTE_SCAN instead of from DELIM_SCAN
		base_binding.table_index = cteref.table_index;
		base_binding.column_index = cteref.chunk_types.size() - cteref.correlated_columns;
		return plan;
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		throw BinderException("Nested lateral joins or lateral joins in correlated subqueries are not (yet) supported");
	}
	case LogicalOperatorType::LOGICAL_SAMPLE:
		throw BinderException("Sampling in correlated subqueries is not (yet) supported");
	default:
		throw InternalException("Logical operator type \"%s\" for dependent join", LogicalOperatorToString(plan->type));
	}
}

} // namespace duckdb
