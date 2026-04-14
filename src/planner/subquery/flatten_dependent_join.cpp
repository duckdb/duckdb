#include "duckdb/planner/subquery/flatten_dependent_join.hpp"

#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/exception/parser_exception.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/function/window/rows_functions.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/subquery/has_correlated_expressions.hpp"
#include "duckdb/planner/subquery/rewrite_correlated_expressions.hpp"
#include "duckdb/planner/subquery/rewrite_cte_scan.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"

namespace duckdb {

FlattenDependentJoins::FlattenDependentJoins(Binder &binder, const CorrelatedColumns &correlated, bool perform_delim,
                                             bool any_join, optional_ptr<FlattenDependentJoins> parent)
    : binder(binder), correlated_columns(correlated), perform_delim(perform_delim), any_join(any_join), parent(parent) {
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		correlated_map[col.binding] = i;
		delim_types.push_back(col.type);
	}
}

static void CreateDelimJoinConditions(LogicalComparisonJoin &delim_join, const CorrelatedColumns &correlated_columns,
                                      vector<ColumnBinding> bindings, const FlattenDependentJoins::PushDownState &state,
                                      bool perform_delim) {
	// Determine the range of columns to process
	idx_t start = 0;
	idx_t end = perform_delim ? correlated_columns.size() : 1;

	// Special case: if not doing a full delim join, use the specific delim index if it's valid
	if (!perform_delim && correlated_columns.GetDelimIndex() < correlated_columns.size()) {
		start = correlated_columns.GetDelimIndex();
		end = start + 1;
	}

	for (idx_t i = start; i < end; i++) {
		auto &col = correlated_columns[i];
		auto binding_idx = state.GetOffset(i);
		if (binding_idx >= bindings.size()) {
			throw InternalException("Delim join - binding index out of range");
		}
		JoinCondition cond(make_uniq<BoundColumnRefExpression>(col.name, col.type, col.binding),
		                   make_uniq<BoundColumnRefExpression>(col.name, col.type, bindings[binding_idx]),
		                   ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		delim_join.conditions.push_back(std::move(cond));
	}
}

unique_ptr<LogicalOperator> FlattenDependentJoins::DecorrelateIndependent(Binder &binder,
                                                                          unique_ptr<LogicalOperator> plan) {
	CorrelatedColumns correlated;
	FlattenDependentJoins flatten(binder, correlated);
	return flatten.Decorrelate(std::move(plan)).plan;
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::Decorrelate(unique_ptr<LogicalOperator> plan,
                                                                         bool parent_propagate_null_values,
                                                                         idx_t lateral_depth, PushDownState state) {
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		auto &delim_join = plan;
		auto &op = plan->Cast<LogicalDependentJoin>();

		// If we have a parent, we unnest the left side of the DEPENDENT JOIN in the parent's context.
		if (parent) {
			// only push the dependent join to the left side, if there is correlation.
			auto entry = has_correlated_expressions.find(*plan);
			D_ASSERT(entry != has_correlated_expressions.end());

			if (entry->second) {
				auto left_result = PushDownDependentJoin(std::move(op.children[0]), parent_propagate_null_values,
				                                         lateral_depth, state);
				op.children[0] = std::move(left_result.plan);
				state = left_result.state;
			} else {
				// There might be unrelated correlation, so we have to traverse the tree
				op.children[0] = DecorrelateIndependent(binder, std::move(op.children[0]));
			}

			// we are now done with the left side, mark it as uncorrelated
			entry->second = false;

			// rewrite
			idx_t next_lateral_depth = 0;

			RewriteCorrelatedExpressions rewriter(state.correlated_bindings, correlated_map, next_lateral_depth);
			rewriter.VisitOperator(*plan);

			RewriteCorrelatedExpressions recursive_rewriter(state.correlated_bindings, correlated_map,
			                                                next_lateral_depth, true);
			recursive_rewriter.VisitOperator(*plan);
		} else {
			auto left_result = Decorrelate(std::move(op.children[0]), true, 0, state);
			op.children[0] = std::move(left_result.plan);
			state = left_result.state;
		}

		if (!op.perform_delim) {
			// if we are not performing a delim join, we push a row_number() OVER() window operator on the LHS
			// and perform all duplicate elimination on that row number instead
			const auto &op_col = op.correlated_columns[op.correlated_columns.GetDelimIndex()];
			auto window = make_uniq<LogicalWindow>(op_col.binding.table_index);
			auto row_number_func = make_uniq<WindowFunction>(RowNumberFun::GetFunction());
			auto row_number =
			    make_uniq<BoundWindowExpression>(LogicalType::BIGINT, nullptr, std::move(row_number_func), nullptr);
			row_number->start = WindowBoundary::UNBOUNDED_PRECEDING;
			row_number->end = WindowBoundary::CURRENT_ROW_ROWS;
			row_number->SetAlias("delim_index");
			window->expressions.push_back(std::move(row_number));
			window->AddChild(std::move(op.children[0]));
			op.children[0] = std::move(window);
		}

		lateral_depth = 0;
		bool propagate_null_values = op.propagate_null_values;
		FlattenDependentJoins flatten(binder, op.correlated_columns, op.perform_delim, op.any_join, this);

		// first we check which logical operators have correlated expressions in the first place
		flatten.DetectCorrelatedExpressions(*delim_join->children[1], op.is_lateral_join, lateral_depth);

		if (delim_join->children[1]->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			auto &cte_ref = delim_join->children[1]->Cast<LogicalMaterializedCTE>();
			// check if the left side of the CTE has correlated expressions
			auto entry = flatten.has_correlated_expressions.find(*cte_ref.children[0]);
			if (entry != flatten.has_correlated_expressions.end()) {
				if (!entry->second) {
					// the left side of the CTE has no correlated expressions, we can push the DEPENDENT_JOIN down
					auto cte = std::move(delim_join->children[1]);
					delim_join->children[1] = std::move(cte->children[1]);
					auto decorrelated =
					    Decorrelate(std::move(delim_join), parent_propagate_null_values, lateral_depth, state);
					cte->children[1] = std::move(decorrelated.plan);
					return PushDownResult(std::move(cte), decorrelated.state, parent_propagate_null_values);
				}
			}
		}

		// now we push the dependent join down
		auto flatten_result =
		    flatten.PushDownDependentJoin(std::move(delim_join->children[1]), propagate_null_values, lateral_depth);
		delim_join->children[1] = std::move(flatten_result.plan);
		const auto left_offset = delim_join->children[0]->GetColumnBindings().size();
		if (!parent) {
			state = flatten_result.state;
			state.ShiftOffsets(left_offset);
		}

		RewriteCorrelatedExpressions rewriter(state.correlated_bindings, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);

		op.duplicate_eliminated_columns.clear();
		op.mark_types.clear();
		for (idx_t i = 0; i < op.correlated_columns.size(); i++) {
			auto &col = op.correlated_columns[i];
			op.duplicate_eliminated_columns.push_back(make_uniq<BoundColumnRefExpression>(col.type, col.binding));
			op.mark_types.push_back(col.type);
		}

		// We are done using the operator as a DEPENDENT JOIN, it is now fully decorrelated,
		// and we change the type to a DELIM JOIN.
		delim_join->type = LogicalOperatorType::LOGICAL_DELIM_JOIN;

		auto plan_columns = delim_join->children[1]->GetColumnBindings();

		// Handle lateral joins
		if (op.is_lateral_join && op.subquery_type == SubqueryType::INVALID) {
			// in case of a materialized CTE, the output is defined by the second children operator
			if (delim_join->children[1]->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
				plan_columns = delim_join->children[1]->children[1]->GetColumnBindings();
			}

			// then add the delim join conditions
			CreateDelimJoinConditions(op, op.correlated_columns, plan_columns, flatten_result.state, op.perform_delim);

			// check if there are any arbitrary expressions left
			if (!op.arbitrary_expressions.empty()) {
				// we can only evaluate scalar arbitrary expressions for inner joins
				if (op.join_type != JoinType::INNER) {
					throw BinderException("Join condition for non-inner LATERAL JOIN must be a comparison between the "
					                      "left and right side");
				}
				auto filter = make_uniq<LogicalFilter>();
				filter->expressions = std::move(op.arbitrary_expressions);
				filter->AddChild(std::move(plan));
				return PushDownResult(std::move(filter), state, parent_propagate_null_values);
			}
			return PushDownResult(std::move(plan), state, parent_propagate_null_values);
		}

		CreateDelimJoinConditions(op, op.correlated_columns, plan_columns, flatten_result.state, op.perform_delim);

		if (op.subquery_type == SubqueryType::ANY) {
			// add the actual condition based on the ANY/ALL predicate
			for (idx_t child_idx = 0; child_idx < op.expression_children.size(); child_idx++) {
				auto left_expr = std::move(op.expression_children[child_idx]);
				auto &child_type = op.child_types[child_idx];
				auto &compare_type = op.child_targets[child_idx];
				auto right_expr = BoundCastExpression::AddDefaultCastToType(
				    make_uniq<BoundColumnRefExpression>(child_type, plan_columns[child_idx]),
				    op.child_targets[child_idx]);
				JoinCondition compare_cond(std::move(left_expr), std::move(right_expr), op.comparison_type);

				// push collations
				ExpressionBinder::PushCollation(binder.context, compare_cond.LeftReference(), compare_type);
				ExpressionBinder::PushCollation(binder.context, compare_cond.RightReference(), compare_type);
				op.conditions.push_back(std::move(compare_cond));
			}
		}

		return PushDownResult(std::move(plan), state, parent_propagate_null_values);
	}
	default: {
		for (auto &child : plan->children) {
			auto child_result = Decorrelate(std::move(child), true, 0, state);
			child = std::move(child_result.plan);
			state = child_result.state;
		}
	}
	}

	return PushDownResult(std::move(plan), state, parent_propagate_null_values);
}

bool FlattenDependentJoins::DetectCorrelatedExpressions(LogicalOperator &op, bool lateral, idx_t lateral_depth,
                                                        bool parent_is_dependent_join) {
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
		bool condition = (parent_is_dependent_join || is_lateral_join) && child_idx == 0;
		if (DetectCorrelatedExpressions(*child, lateral, new_lateral_depth, condition)) {
			has_correlation = true;
		}

		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && child_idx == 0) {
			auto &setop = op.Cast<LogicalCTE>();
			binder.recursive_ctes[setop.table_index] = &setop;
			has_correlated_expressions[op] = has_correlation;
			if (has_correlation) {
				setop.correlated_columns = correlated_columns;
			}
		}

		child_idx++;
	}

	// We found a CTE reference
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		// Check, if the operator already has an entry in has_correlated_expressions.
		// This would only be the case, if we hit MarkSubtreeCorrelated previously.
		auto entry = has_correlated_expressions.find(op);

		if (entry == has_correlated_expressions.end()) {
			// Try to find a recursive CTE for this operator
			auto &cteref = op.Cast<LogicalCTERef>();
			auto cte = binder.recursive_ctes.find(cteref.cte_index);

			has_correlated_expressions[op] = false; // Default: not correlated

			// recursive_ctes may be a misnomer at this point, as it may also contain materialized CTEs
			if (cte != binder.recursive_ctes.end()) {
				auto cte_node = cte->second;

				if (cte_node->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
					// Found a recursive CTE, subtree is correlated
					return true;
				}
				// Found a materialized CTE, subtree correlation depends on the CTE node
				bool handled_by_dependent_join = false;
				idx_t join_depth = lateral_depth + 1;
				for (auto &ctx_col : correlated_columns) {
					if (ctx_col.depth == join_depth) {
						// Check if the current dependent join will handle the correlation
						handled_by_dependent_join = true;
						break;
					}
				}

				auto &setop = cte_node->Cast<LogicalCTE>();
				has_correlated_expressions[op] = (!handled_by_dependent_join && !setop.correlated_columns.empty()) ||
				                                 has_correlated_expressions[*cte_node];
				return has_correlated_expressions[op];
			}
			// No CTE found: subtree is correlated
			return true;
		}
	}

	// set the entry in the map
	has_correlated_expressions[op] = has_correlation;

	// If we detect correlation in a materialized or recursive CTE, the entire right side of the operator
	// needs to be marked as correlated. Otherwise, function PushDownDependentJoinInternal does not do the
	// right thing.
	if (op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		auto &setop = op.Cast<LogicalCTE>();
		binder.recursive_ctes[setop.table_index] = &setop;
		if (has_correlation) {
			setop.correlated_columns = correlated_columns;
			MarkSubtreeCorrelated(*op.children[1].get(), setop.table_index);
		}
	}

	return has_correlation;
}

bool FlattenDependentJoins::MarkSubtreeCorrelated(LogicalOperator &op, TableIndex cte_index) {
	// Do not mark base table scans as correlated
	auto entry = has_correlated_expressions.find(op);
	D_ASSERT(entry != has_correlated_expressions.end());
	bool has_correlation = entry->second;
	for (auto &child : op.children) {
		has_correlation |= MarkSubtreeCorrelated(*child.get(), cte_index);
	}
	if (op.type != LogicalOperatorType::LOGICAL_GET || op.children.size() == 1) {
		if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
			// There may be multiple recursive CTEs. Only mark CTE_REFs as correlated,
			// IFF the CTE that we are reading from is correlated.
			auto &cteref = op.Cast<LogicalCTERef>();
			if (cteref.cte_index != cte_index) {
				has_correlated_expressions[op] = has_correlation;
				return has_correlation;
			}
			has_correlated_expressions[op] = true;
			return true;
		} else {
			has_correlated_expressions[op] = has_correlation;
		}
	}
	return has_correlation;
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownDependentJoin(unique_ptr<LogicalOperator> plan,
                                                                                   bool propagate_null_values,
                                                                                   idx_t lateral_depth,
                                                                                   PushDownState state) {
	auto result = PushDownDependentJoinInternal(std::move(plan), propagate_null_values, lateral_depth, state);
	if (!replacement_map.empty()) {
		// check if we have to replace any COUNT aggregates into "CASE WHEN X IS NULL THEN 0 ELSE COUNT END"
		RewriteCountAggregates aggr(replacement_map);
		aggr.VisitOperator(*result.plan);
	}
	return PushDownResult(std::move(result.plan), result.state, propagate_null_values);
}

bool SubqueryDependentFilter(Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &bound_conjunction = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : bound_conjunction.children) {
			if (SubqueryDependentFilter(*child)) {
				return true;
			}
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		return true;
	}
	return false;
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownFilter(unique_ptr<LogicalOperator> plan,
                                                                            bool parent_propagate_null_values,
                                                                            idx_t lateral_depth, PushDownState state) {
	for (auto &expr : plan->expressions) {
		any_join |= SubqueryDependentFilter(*expr);
	}
	auto child_result =
	    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth, state);
	plan->children[0] = std::move(child_result.plan);
	state = child_result.state;
	parent_propagate_null_values = child_result.propagate_null_values;

	RewriteCorrelatedExpressions rewriter(state.correlated_bindings, correlated_map, lateral_depth);
	rewriter.VisitOperator(*plan);
	return PushDownResult(std::move(plan), state, parent_propagate_null_values);
}

FlattenDependentJoins::PushDownResult FlattenDependentJoins::PushDownUnnest(unique_ptr<LogicalOperator> plan,
                                                                            bool parent_propagate_null_values,
                                                                            idx_t lateral_depth, PushDownState state) {
	for (auto &expr : plan->expressions) {
		any_join |= SubqueryDependentFilter(*expr);
	}
	auto child_result =
	    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth, state);
	plan->children[0] = std::move(child_result.plan);
	state = child_result.state;
	parent_propagate_null_values = child_result.propagate_null_values;

	RewriteCorrelatedExpressions rewriter(state.correlated_bindings, correlated_map, lateral_depth);
	rewriter.VisitOperator(*plan);
	return PushDownResult(std::move(plan), state, parent_propagate_null_values);
}

		// If our immediate children is a DEPENDENT JOIN, the projection expressions did contain
		// a subquery expression previously—Which does not propagate null values.
		// We have to account for that.
		bool child_is_dependent_join = plan->children[0]->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
		parent_propagate_null_values &= !child_is_dependent_join;

		// if the node has no correlated expressions,
		// push the cross product with the delim get only below the projection.
		// This will preserve positionality of the columns and prevent errors when reordering of
		// delim gets is enabled.
		if (exit_projection) {
			auto decorrelated = Decorrelate(std::move(plan->children[0]), true, 0, state);
			auto cross_product = LogicalCrossProduct::Create(std::move(decorrelated.plan), std::move(delim_scan));
			if (cross_product->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
				auto bindings = cross_product->GetColumnBindings();
				vector<idx_t> correlated_offsets;
				correlated_offsets.reserve(correlated_columns.size());
				vector<ColumnBinding> correlated_bindings;
				correlated_bindings.reserve(correlated_columns.size());
				for (idx_t i = 0; i < correlated_columns.size(); i++) {
					correlated_offsets.push_back(i);
					correlated_bindings.push_back(bindings[i]);
				}
				state = PushDownState(std::move(correlated_bindings), std::move(correlated_offsets));
			}
			plan->children[0] = std::move(cross_product);
		} else {
			auto child_result = PushDownDependentJoinInternal(std::move(plan->children[0]),
			                                                  parent_propagate_null_values, lateral_depth, state);
			plan->children[0] = std::move(child_result.plan);
			state = child_result.state;
			parent_propagate_null_values = child_result.propagate_null_values;
		}

		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(state.correlated_bindings, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		// now we add all the columns of the delim_scan to the projection list
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			auto &col = correlated_columns[i];
			auto colref = make_uniq<BoundColumnRefExpression>(col.name, col.type, state.GetBinding(i));
			plan->expressions.push_back(std::move(colref));
		}
		auto &proj = plan->Cast<LogicalProjection>();
		auto correlated_offset = plan->expressions.size() - correlated_columns.size();
		state = PushDownState::CreateContiguous(ColumnBinding(proj.table_index, ProjectionIndex(correlated_offset)),
		                                        correlated_offset, correlated_columns.size());
		return PushDownResult(std::move(plan), state, parent_propagate_null_values);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = plan->Cast<LogicalAggregate>();
		// aggregate and group by
		// first we flatten the dependent join in the child of the projection
		for (auto &expr : plan->expressions) {
			parent_propagate_null_values &= expr->PropagatesNullValues();
		}
		auto child_result = PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values,
		                                                  lateral_depth, state);
		plan->children[0] = std::move(child_result.plan);
		state = child_result.state;
		parent_propagate_null_values = child_result.propagate_null_values;
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(state.correlated_bindings, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		// now we add all the columns of the delim_scan to the grouping operators AND the projection list
		TableIndex delim_table_index;
		idx_t delim_column_offset;
		auto new_group_count = perform_delim ? correlated_columns.size() : 1;
		for (idx_t i = 0; i < new_group_count; i++) {
			auto &col = correlated_columns[i];
			auto colref = make_uniq<BoundColumnRefExpression>(col.name, col.type, state.GetBinding(i));
			auto new_group_index = ColumnBinding::PushExpression(aggr.groups, std::move(colref));
			for (auto &set : aggr.grouping_sets) {
				set.insert(new_group_index);
			}
		}
		if (!perform_delim) {
			// if we are not performing the duplicate elimination, we have only added the row_id column to the grouping
			// operators in this case, we push a FIRST aggregate for each of the remaining expressions
			delim_table_index = aggr.aggregate_index;
			delim_column_offset = aggr.expressions.size();
			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				auto &col = correlated_columns[i];
				auto first_aggregate = FirstFunctionGetter::GetFunction(col.type);
				auto colref = make_uniq<BoundColumnRefExpression>(col.name, col.type, state.GetBinding(i));
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
				JoinCondition cond(make_uniq<BoundColumnRefExpression>(col.name, col.type,
				                                                       ColumnBinding(left_index, ProjectionIndex(i))),
				                   make_uniq<BoundColumnRefExpression>(
				                       correlated_columns[i].type,
				                       ColumnBinding(delim_table_index, ProjectionIndex(delim_column_offset + i))),
				                   ExpressionType::COMPARE_NOT_DISTINCT_FROM);
				join->conditions.push_back(std::move(cond));
			}
			// for any COUNT aggregate we replace references to the column with: CASE WHEN COUNT(*) IS NULL THEN 0
			// ELSE COUNT(*) END
			for (idx_t i = 0; i < aggr.expressions.size(); i++) {
				D_ASSERT(aggr.expressions[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
				auto &bound = aggr.expressions[i]->Cast<BoundAggregateExpression>();
				vector<LogicalType> arguments;
				if (bound.function == CountFunctionBase::GetFunction() ||
				    bound.function == CountStarFun::GetFunction()) {
					// have to replace this ColumnBinding with the CASE expression
					replacement_map[ColumnBinding(aggr.aggregate_index, ProjectionIndex(i))] = i;
				}
			}
			// now we update the delim_index
			state = PushDownState::CreateContiguous(ColumnBinding(left_index, ProjectionIndex(0)), 0,
			                                        correlated_columns.size());
			return PushDownResult(std::move(join), state, parent_propagate_null_values);
		}
		// update the delim_index
		state = PushDownState::CreateContiguous(ColumnBinding(delim_table_index, ProjectionIndex(delim_column_offset)),
		                                        delim_column_offset, correlated_columns.size());
		return PushDownResult(std::move(plan), state, parent_propagate_null_values);
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// cross product
		// push into both sides of the plan
		D_ASSERT(has_correlated_expressions.find(*plan->children[0]) != has_correlated_expressions.end());
		D_ASSERT(has_correlated_expressions.find(*plan->children[1]) != has_correlated_expressions.end());
		bool left_has_correlation = has_correlated_expressions.find(*plan->children[0])->second;
		bool right_has_correlation = has_correlated_expressions.find(*plan->children[1])->second;
		if (!right_has_correlation) {
			// only left has correlation: push into left
			auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values,
			                                                 lateral_depth, state);
			plan->children[0] = std::move(left_result.plan);
			parent_propagate_null_values = left_result.propagate_null_values;

			// recurse into right children, there may be more local correlations
			plan->children[1] = DecorrelateIndependent(binder, std::move(plan->children[1]));
			return PushDownResult(std::move(plan), left_result.state, parent_propagate_null_values);
		}
		if (!left_has_correlation) {
			// only right has correlation: push into right
			auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]),
			                                                  parent_propagate_null_values, lateral_depth, state);
			plan->children[1] = std::move(right_result.plan);
			parent_propagate_null_values = right_result.propagate_null_values;

			// recurse into left children
			plan->children[0] = DecorrelateIndependent(binder, std::move(plan->children[0]));
			// Similar to the LOGICAL_COMPARISON_JOIN
			right_result.state.ShiftOffsets(plan->children[0]->GetColumnBindings().size());
			return PushDownResult(std::move(plan), right_result.state, parent_propagate_null_values);
		}
		// both sides have correlation
		// turn into an inner join
		// correctly use left child's delim_offset so execute left child as the last one
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]), parent_propagate_null_values,
		                                                  lateral_depth, state);
		plan->children[1] = std::move(right_result.plan);
		parent_propagate_null_values = right_result.propagate_null_values;
		auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values,
		                                                 lateral_depth, right_result.state);
		plan->children[0] = std::move(left_result.plan);
		parent_propagate_null_values = left_result.propagate_null_values;
		// add the correlated columns to the join conditions
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			JoinCondition cond(
			    make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, left_result.state.GetBinding(i)),
			    make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, right_result.state.GetBinding(i)),
			    ExpressionType::COMPARE_NOT_DISTINCT_FROM);
			join->conditions.push_back(std::move(cond));
		}
		join->children.push_back(std::move(plan->children[0]));
		join->children.push_back(std::move(plan->children[1]));
		return PushDownResult(std::move(join), left_result.state, parent_propagate_null_values);
	}
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		D_ASSERT(plan->children.size() == 2);
		return Decorrelate(std::move(plan), parent_propagate_null_values, lateral_depth, state);
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
				auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]),
				                                                 parent_propagate_null_values, lateral_depth, state);
				plan->children[0] = std::move(left_result.plan);
				parent_propagate_null_values = left_result.propagate_null_values;
				plan->children[1] = DecorrelateIndependent(binder, std::move(plan->children[1]));
				// Remove the correlated columns coming from outside for current join node
				return PushDownResult(std::move(plan), left_result.state, parent_propagate_null_values);
			}
			if (!left_has_correlation) {
				// only right has correlation: push into right
				auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]),
				                                                  parent_propagate_null_values, lateral_depth, state);
				plan->children[1] = std::move(right_result.plan);
				parent_propagate_null_values = right_result.propagate_null_values;
				plan->children[0] = DecorrelateIndependent(binder, std::move(plan->children[0]));
				right_result.state.ShiftOffsets(plan->children[0]->GetColumnBindings().size());
				// Remove the correlated columns coming from outside for current join node
				return PushDownResult(std::move(plan), right_result.state, parent_propagate_null_values);
			}
		} else if (join.join_type == JoinType::LEFT) {
			// left outer join
			if (!right_has_correlation) {
				// only left has correlation: push into left
				auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]),
				                                                 parent_propagate_null_values, lateral_depth, state);
				plan->children[0] = std::move(left_result.plan);
				parent_propagate_null_values = left_result.propagate_null_values;
				plan->children[1] = DecorrelateIndependent(binder, std::move(plan->children[1]));
				// Remove the correlated columns coming from outside for current join node
				return PushDownResult(std::move(plan), left_result.state, parent_propagate_null_values);
			}
		} else if (join.join_type == JoinType::RIGHT) {
			// right outer join
			if (!left_has_correlation) {
				// only right has correlation: push into right
				auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]),
				                                                  parent_propagate_null_values, lateral_depth, state);
				plan->children[1] = std::move(right_result.plan);
				parent_propagate_null_values = right_result.propagate_null_values;
				plan->children[0] = DecorrelateIndependent(binder, std::move(plan->children[0]));
				right_result.state.ShiftOffsets(plan->children[0]->GetColumnBindings().size());
				return PushDownResult(std::move(plan), right_result.state, parent_propagate_null_values);
			}
		} else if (join.join_type == JoinType::MARK) {
			if (!left_has_correlation && right_has_correlation) {
				// found a MARK join where the left side has no correlation
				auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]),
				                                                  parent_propagate_null_values, lateral_depth, state);
				plan->children[1] = std::move(right_result.plan);
				parent_propagate_null_values = right_result.propagate_null_values;

				// now push into the left side of the MARK join even though it has no correlation
				// this is necessary to add the correlated columns to the column bindings and allow
				// the join condition to be rewritten correctly
				auto left_result = PushDownDependentJoinInternal(
				    std::move(plan->children[0]), parent_propagate_null_values, lateral_depth, right_result.state);
				plan->children[0] = std::move(left_result.plan);
				parent_propagate_null_values = left_result.propagate_null_values;

				// add the correlated columns to the join conditions
				for (idx_t i = 0; i < correlated_columns.size(); i++) {
					JoinCondition cond(make_uniq<BoundColumnRefExpression>(correlated_columns[i].type,
					                                                       left_result.state.GetBinding(i)),
					                   make_uniq<BoundColumnRefExpression>(correlated_columns[i].type,
					                                                       right_result.state.GetBinding(i)),
					                   ExpressionType::COMPARE_NOT_DISTINCT_FROM);

					auto &comparison_join = join.Cast<LogicalComparisonJoin>();
					comparison_join.conditions.push_back(std::move(cond));
				}
				return PushDownResult(std::move(plan), left_result.state, parent_propagate_null_values);
			}

			// push the child into the LHS
			auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values,
			                                                 lateral_depth, state);
			plan->children[0] = std::move(left_result.plan);
			parent_propagate_null_values = left_result.propagate_null_values;
			plan->children[1] = DecorrelateIndependent(binder, std::move(plan->children[1]));
			// rewrite expressions in the join conditions
			RewriteCorrelatedExpressions rewriter(left_result.state.correlated_bindings, correlated_map, lateral_depth);
			rewriter.VisitOperator(*plan);
			return PushDownResult(std::move(plan), left_result.state, parent_propagate_null_values);
		} else {
			throw NotImplementedException("Unsupported join type for flattening correlated subquery");
		}
		// both sides have correlation
		// push into both sides
		auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values,
		                                                 lateral_depth, state);
		plan->children[0] = std::move(left_result.plan);
		parent_propagate_null_values = left_result.propagate_null_values;
		auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]), parent_propagate_null_values,
		                                                  lateral_depth, left_result.state);
		plan->children[1] = std::move(right_result.plan);
		parent_propagate_null_values = right_result.propagate_null_values;
		PushDownState result_state = right_result.state;
		// NOTE: for OUTER JOINS it matters what the BASE BINDING is after the join
		// for the LEFT OUTER JOIN, we want the LEFT side to be the base binding after we push
		// because the RIGHT binding might contain NULL values
		if (join.join_type == JoinType::LEFT) {
			result_state = left_result.state;
		} else if (join.join_type == JoinType::RIGHT) {
			result_state.ShiftOffsets(plan->children[0]->GetColumnBindings().size());
		}
		// add the correlated columns to the join conditions
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			auto left =
			    make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, left_result.state.GetBinding(i));
			auto right =
			    make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, right_result.state.GetBinding(i));

			if (join.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
			    join.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
				JoinCondition cond(std::move(left), std::move(right), ExpressionType::COMPARE_NOT_DISTINCT_FROM);

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
		RewriteCorrelatedExpressions rewriter(right_result.state.correlated_bindings, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		return PushDownResult(std::move(plan), result_state, parent_propagate_null_values);
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
			auto child_result = PushDownDependentJoinInternal(std::move(order_by->children[0]),
			                                                  parent_propagate_null_values, lateral_depth, state);
			child = std::move(child_result.plan);
			state = child_result.state;
			parent_propagate_null_values = child_result.propagate_null_values;
		} else {
			auto child_result = PushDownDependentJoinInternal(std::move(plan->children[0]),
			                                                  parent_propagate_null_values, lateral_depth, state);
			child = std::move(child_result.plan);
			state = child_result.state;
			parent_propagate_null_values = child_result.propagate_null_values;
		}
		auto child_column_count = child->GetColumnBindings().size();
		// we push a row_number() OVER (PARTITION BY [correlated columns])
		auto window_index = binder.GenerateTableIndex();
		auto window = make_uniq<LogicalWindow>(window_index);
		auto rn = make_uniq<WindowFunction>(RowNumberFun::GetFunction());
		auto row_number = make_uniq<BoundWindowExpression>(LogicalType::BIGINT, nullptr, std::move(rn), nullptr);
		auto partition_count = perform_delim ? correlated_columns.size() : 1;
		for (idx_t i = 0; i < partition_count; i++) {
			auto &col = correlated_columns[i];
			auto colref = make_uniq<BoundColumnRefExpression>(col.name, col.type, state.GetBinding(i));
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
		auto row_num_ref = make_uniq<BoundColumnRefExpression>(rownum_alias, LogicalType::BIGINT,
		                                                       ColumnBinding(window_index, ProjectionIndex(0)));

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
			filter->projection_map.emplace_back(i);
		}
		return PushDownResult(std::move(filter), state, parent_propagate_null_values);
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		auto &window = plan->Cast<LogicalWindow>();
		// push into children
		auto child_result = PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values,
		                                                  lateral_depth, state);
		plan->children[0] = std::move(child_result.plan);
		state = child_result.state;
		parent_propagate_null_values = child_result.propagate_null_values;

		// we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(state.correlated_bindings, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);

		// add the correlated columns to the PARTITION BY clauses in the Window
		for (auto &expr : window.expressions) {
			D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto &w = expr->Cast<BoundWindowExpression>();
			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				w.partitions.push_back(
				    make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, state.GetBinding(i)));
			}
		}
		return PushDownResult(std::move(plan), state, parent_propagate_null_values);
	}
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_UNION: {
		auto &setop = plan->Cast<LogicalSetOperation>();
		// set operator, push into both children
#ifdef DEBUG
		for (auto &child : plan->children) {
			child->ResolveOperatorTypes();
		}
		for (idx_t i = 1; i < plan->children.size(); i++) {
			D_ASSERT(plan->children[0]->types.size() == plan->children[i]->types.size());
		}
#endif
		for (auto &child : plan->children) {
			auto child_result = PushDownDependentJoin(std::move(child), true, 0, state);
			child = std::move(child_result.plan);
			state = child_result.state;
		}
		for (idx_t i = 0; i < plan->children.size(); i++) {
			if (plan->children[i]->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
				auto proj_index = binder.GenerateTableIndex();
				auto bindings = plan->children[i]->GetColumnBindings();
				plan->children[i]->ResolveOperatorTypes();
				auto types = plan->children[i]->types;
				vector<unique_ptr<Expression>> expressions;
				expressions.reserve(bindings.size());
				D_ASSERT(bindings.size() == types.size());

				// No column binding replacement is needed because the parent operator is
				// a setop which will immediately assign new bindings.
				for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
					expressions.push_back(make_uniq<BoundColumnRefExpression>(types[col_idx], bindings[col_idx]));
				}
				auto proj = make_uniq<LogicalProjection>(proj_index, std::move(expressions));
				proj->children.push_back(std::move(plan->children[i]));
				plan->children[i] = std::move(proj);
			}
		}

		// here we need to check the children. If they have reorderable bindings, you need to plan a projection
		// on top that will guarantee the order of the bindings.
#ifdef DEBUG
		for (idx_t i = 1; i < plan->children.size(); i++) {
			D_ASSERT(plan->children[0]->GetColumnBindings().size() == plan->children[i]->GetColumnBindings().size());
		}
		for (auto &child : plan->children) {
			child->ResolveOperatorTypes();
		}
		for (idx_t i = 1; i < plan->children.size(); i++) {
			D_ASSERT(plan->children[0]->types.size() == plan->children[i]->types.size());
		}
#endif
		// we have to refer to the setop index now
		state = PushDownState::CreateContiguous(ColumnBinding(setop.table_index, ProjectionIndex(setop.column_count)),
		                                        setop.column_count, correlated_columns.size());
		setop.column_count += correlated_columns.size();
		return PushDownResult(std::move(plan), state, parent_propagate_null_values);
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = plan->Cast<LogicalDistinct>();
		// push down into child
		auto child_result = PushDownDependentJoin(std::move(distinct.children[0]), true, 0, state);
		distinct.children[0] = std::move(child_result.plan);
		state = child_result.state;
		// add all correlated columns to the distinct targets
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			distinct.distinct_targets.push_back(
			    make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, state.GetBinding(i)));
		}
		return PushDownResult(std::move(plan), state, parent_propagate_null_values);
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// expression get
		// first we flatten the dependent join in the child
		auto child_result = PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values,
		                                                  lateral_depth, state);
		plan->children[0] = std::move(child_result.plan);
		state = child_result.state;
		parent_propagate_null_values = child_result.propagate_null_values;
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(state.correlated_bindings, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		// now we add all the correlated columns to each of the expressions of the expression scan
		auto &expr_get = plan->Cast<LogicalExpressionGet>();
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			for (auto &expr_list : expr_get.expressions) {
				auto colref = make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, state.GetBinding(i));
				expr_list.push_back(std::move(colref));
			}
			expr_get.expr_types.push_back(correlated_columns[i].type);
		}
		auto correlated_offset = expr_get.expr_types.size() - correlated_columns.size();
		state = PushDownState::CreateContiguous(ColumnBinding(expr_get.table_index, ProjectionIndex(correlated_offset)),
		                                        correlated_offset, correlated_columns.size());
		return PushDownResult(std::move(plan), state, parent_propagate_null_values);
	}
	case LogicalOperatorType::LOGICAL_PIVOT:
		throw BinderException("PIVOT is not supported in correlated subqueries yet");
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto child_result = PushDownDependentJoin(std::move(plan->children[0]), true, 0, state);
		plan->children[0] = std::move(child_result.plan);
		return PushDownResult(std::move(plan), child_result.state, parent_propagate_null_values);
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = plan->Cast<LogicalGet>();
		if (get.children.size() != 1) {
			throw InternalException("Flatten dependent joins - logical get encountered without children");
		}
		auto child_result = PushDownDependentJoin(std::move(plan->children[0]), true, 0, state);
		plan->children[0] = std::move(child_result.plan);
		state = child_result.state;
		auto correlated_offset = get.GetColumnBindings().size();
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			get.projected_input.push_back(state.GetOffset(i));
		}

		RewriteCorrelatedExpressions rewriter(state.correlated_bindings, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		state.correlated_offsets.clear();
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			state.correlated_offsets.push_back(correlated_offset + i);
		}
		return PushDownResult(std::move(plan), state, parent_propagate_null_values);
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
#ifdef DEBUG
		plan->children[0]->ResolveOperatorTypes();
		plan->children[1]->ResolveOperatorTypes();
#endif
		if (plan->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			// check the correlated expressions in the children of the join
			bool left_has_correlation = has_correlated_expressions.find(*plan->children[0])->second;
			bool right_has_correlation = has_correlated_expressions.find(*plan->children[1])->second;

			if (!left_has_correlation && right_has_correlation) {
				// only right has correlation: push into right
				auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]),
				                                                  parent_propagate_null_values, lateral_depth, state);
				plan->children[1] = std::move(right_result.plan);
				parent_propagate_null_values = right_result.propagate_null_values;
				plan->children[0] = DecorrelateIndependent(binder, std::move(plan->children[0]));
				return PushDownResult(std::move(plan), right_result.state, parent_propagate_null_values);
			}
		}

		auto left_result = PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values,
		                                                 lateral_depth, state);
		plan->children[0] = std::move(left_result.plan);
		state = left_result.state;
		parent_propagate_null_values = left_result.propagate_null_values;

		auto &setop = plan->Cast<LogicalCTE>();
		state = PushDownState::CreateContiguous(ColumnBinding(setop.table_index, ProjectionIndex(setop.column_count)),
		                                        setop.column_count, correlated_columns.size());
		auto table_index = setop.table_index;
		setop.correlated_columns = correlated_columns;
		binder.recursive_ctes[setop.table_index] = &setop;

		if (plan->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
			auto &rec_cte = plan->Cast<LogicalRecursiveCTE>();

			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				if (!rec_cte.key_targets.empty()) {
					auto colref = make_uniq<BoundColumnRefExpression>(correlated_columns[i].type, state.GetBinding(i));
					rec_cte.key_targets.push_back(std::move(colref));
				}
				rec_cte.internal_types.push_back(correlated_columns[i].type);
			}
		}

		CTEScanRewriteMode rewrite_mode;
		if (plan->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
			rewrite_mode = CTEScanRewriteMode::WITH_RECURSIVE_DEPENDENT_JOINS;
		} else if (plan->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			rewrite_mode = CTEScanRewriteMode::WITH_NON_RECURSIVE_DEPENDENT_JOINS;
		} else {
			throw InternalException("Unsupported CTE operator type for CTEScanRewriteMode selection");
		}
		RewriteCTEScan cte_rewriter(table_index, correlated_columns, rewrite_mode);
		cte_rewriter.VisitOperator(*plan->children[1]);

		parent_propagate_null_values = false;
		auto right_result = PushDownDependentJoinInternal(std::move(plan->children[1]), parent_propagate_null_values,
		                                                  lateral_depth, state);
		plan->children[1] = std::move(right_result.plan);
		state = right_result.state;
		parent_propagate_null_values = right_result.propagate_null_values;

		RewriteCorrelatedExpressions rewriter(state.correlated_bindings, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);

		RewriteCorrelatedExpressions recursive_rewriter(state.correlated_bindings, correlated_map, lateral_depth + 1,
		                                                true);
		recursive_rewriter.VisitOperator(*plan->children[0]);
		recursive_rewriter.VisitOperator(*plan->children[1]);

#ifdef DEBUG
		plan->children[0]->ResolveOperatorTypes();
		plan->children[1]->ResolveOperatorTypes();
#endif

		if (plan->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
			// we have to refer to the recursive CTE index now
			state =
			    PushDownState::CreateContiguous(ColumnBinding(setop.table_index, ProjectionIndex(setop.column_count)),
			                                    setop.column_count, correlated_columns.size());
		}

	case LogicalOperatorType::LOGICAL_FILTER: {
		return PushDownFilter(std::move(plan), parent_propagate_null_values, lateral_depth, std::move(state));
	}
	case LogicalOperatorType::LOGICAL_UNNEST: {
		return PushDownUnnest(std::move(plan), parent_propagate_null_values, lateral_depth, std::move(state));
	}
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		throw BinderException("Nested lateral joins or lateral joins in correlated subqueries are not (yet) supported");
	}
	case LogicalOperatorType::LOGICAL_SAMPLE:
		throw BinderException("Sampling in correlated subqueries is not (yet) supported");
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		throw BinderException("Positional join in correlated subqueries is not (yet) supported");
	default:
		throw InternalException("Logical operator type \"%s\" for dependent join", LogicalOperatorToString(plan->type));
	}
}

} // namespace duckdb
