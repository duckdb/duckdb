#include "optimizer/filter_pushdown.hpp"

#include "planner/operator/list.hpp"
#include "execution/expression_executor.hpp"

using namespace duckdb;
using namespace std;

using Filter = FilterPushdown::Filter;

static void GetExpressionBindings(Expression &expr, unordered_set<size_t> &bindings) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expr;
		assert(colref.depth == 0);
		bindings.insert(colref.binding.table_index);
	}
	expr.EnumerateChildren([&](Expression *child) { GetExpressionBindings(*child, bindings); });
}

unique_ptr<LogicalOperator> FilterPushdown::Rewrite(unique_ptr<LogicalOperator> op) {
	switch (op->type) {
	case LogicalOperatorType::FILTER:
		return PushdownFilter(move(op));
	case LogicalOperatorType::CROSS_PRODUCT:
		return PushdownCrossProduct(move(op));
	case LogicalOperatorType::COMPARISON_JOIN:
	case LogicalOperatorType::ANY_JOIN:
	case LogicalOperatorType::DELIM_JOIN:
		return PushdownJoin(move(op));
	case LogicalOperatorType::SUBQUERY:
		return PushdownSubquery(move(op));
	case LogicalOperatorType::PROJECTION:
		return PushdownProjection(move(op));
	default:
		return FinishPushdown(move(op));
	}
}

static JoinSide GetExpressionSide(unordered_set<size_t> bindings, unordered_set<size_t> &left_bindings,
                                  unordered_set<size_t> &right_bindings) {
	JoinSide side = JoinSide::NONE;
	for (auto binding : bindings) {
		if (left_bindings.find(binding) != left_bindings.end()) {
			assert(right_bindings.find(binding) == right_bindings.end());
			if (side == JoinSide::RIGHT) {
				return JoinSide::BOTH;
			} else {
				side = JoinSide::LEFT;
			}
		} else {
			assert(right_bindings.find(binding) != right_bindings.end());
			if (side == JoinSide::LEFT) {
				return JoinSide::BOTH;
			} else {
				side = JoinSide::RIGHT;
			}
		}
	}
	return side;
}

bool FilterPushdown::AddFilter(unique_ptr<Expression> expr) {
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(move(expr));
	LogicalFilter::SplitPredicates(expressions);
	for(auto &expr : expressions) {
		auto f = make_unique<Filter>();
		f->filter = move(expr);
		GetExpressionBindings(*f->filter, f->bindings);
		if (f->bindings.size() == 0) {
			// scalar condition, evaluate it
			auto result = ExpressionExecutor::EvaluateScalar(*f->filter).CastAs(TypeId::BOOLEAN);
			// check if the filter passes
			if (result.is_null || !result.value_.boolean) {
				// the filter does not pass the scalar test, create an empty result
				return true;
			} else {
				// the filter passes the scalar test, just remove the condition
				continue;
			}
		}
		filters.push_back(move(f));
	}
	return false;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownInnerJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<size_t> &left_bindings,
                                                             unordered_set<size_t> &right_bindings) {
 	auto &join = (LogicalJoin &)*op;
	assert(join.type == JoinType::INNER);
	assert(op->type != LogicalOperatorType::DELIM_JOIN);
	// inner join: gather all the conditions of the inner join and add to the filter list
	if (op->type == LogicalOperatorType::ANY_JOIN) {
	 	auto &any_join = (LogicalAnyJoin &) join;
		// any join: only one filter to add
		if (AddFilter(move(any_join.condition))) {
			// filter statically evaluates to false, strip tree
			return make_unique<LogicalEmptyResult>(move(op));
		}
	} else {
		// comparison join
		assert(op->type == LogicalOperatorType::COMPARISON_JOIN);
		auto &comp_join = (LogicalComparisonJoin&) join;
		// turn the conditions into filters
		for(size_t i = 0; i < comp_join.conditions.size(); i++) {
			auto condition = LogicalComparisonJoin::CreateExpressionFromCondition(move(comp_join.conditions[i]));
			if (AddFilter(move(condition))) {
				// filter statically evaluates to false, strip tree
				return make_unique<LogicalEmptyResult>(move(op));
			}
		}
	}
	// turn the inner join into a cross product
	auto cross_product = make_unique<LogicalCrossProduct>();
	cross_product->children.push_back(move(op->children[0]));
	cross_product->children.push_back(move(op->children[1]));
	// then push down cross product
	return PushdownCrossProduct(move(cross_product));
}

static unique_ptr<Expression> ReplaceColRefWithNull(unique_ptr<Expression> expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		return make_unique<ConstantExpression>(Value(expr->return_type));
	}
	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		return ReplaceColRefWithNull(move(child));
	});
	return expr;
}

static bool FilterRemovesNull(Expression *expr) {
	// make a copy of the expression
	auto copy = expr->Copy();
	// replace all BoundColumnRef expressions with NULL constants in the copied expression
	copy = ReplaceColRefWithNull(move(copy));
	if (!copy->IsScalar()) {
		return false;
	}
	// flatten the scalar
	auto val = ExpressionExecutor::EvaluateScalar(*copy).CastAs(TypeId::BOOLEAN);
	// if the result of the expression with all expressions replaced with NULL is "NULL" or "false"
	// then any extra entries generated by the LEFT OUTER JOIN will be filtered out!
	// hence the LEFT OUTER JOIN is equivalent to an inner join
	return val.is_null || !val.value_.boolean;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownLeftJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<size_t> &left_bindings,
                                                             unordered_set<size_t> &right_bindings) {
 	auto &join = (LogicalJoin &)*op;
	assert(join.type == JoinType::LEFT);
	assert(op->type != LogicalOperatorType::DELIM_JOIN);
	FilterPushdown left_pushdown, right_pushdown;
	// now check the set of filters
	for (size_t i = 0; i < filters.size(); i++) {
		auto side = GetExpressionSide(filters[i]->bindings, left_bindings, right_bindings);
		if (side == JoinSide::LEFT) {
			// bindings match left side: push into left
			left_pushdown.filters.push_back(move(filters[i]));
			// erase the filter from the list of filters
			filters.erase(filters.begin() + i);
			i--;
		} else if (side == JoinSide::RIGHT) {
			// bindings match right side: we cannot directly push it into the right
			// however, if the filter removes rows with null values we can turn the left outer join
			// in an inner join, and then push down as we would push down an inner join
			if (FilterRemovesNull(filters[i]->filter.get())) {
				// the filter removes NULL values, turn it into an inner join
				join.type = JoinType::INNER;
				// now we can do more pushdown
				// move all filters we added to the left_pushdown back into the filter list
				for(auto &left_filter : left_pushdown.filters) {
					filters.push_back(move(left_filter));
				}
				// now push down the inner join
				return PushdownInnerJoin(move(op), left_bindings, right_bindings);
			}
		}
	}
	op->children[0] = left_pushdown.Rewrite(move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(move(op->children[1]));
	return FinishPushdown(move(op));
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownMarkJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<size_t> &left_bindings,
                                                             unordered_set<size_t> &right_bindings) {
 	auto &join = (LogicalJoin &)*op;
	auto &comp_join = (LogicalComparisonJoin &)*op;
	assert(join.type == JoinType::MARK);
	assert(op->type == LogicalOperatorType::COMPARISON_JOIN || op->type == LogicalOperatorType::DELIM_JOIN);

	FilterPushdown left_pushdown, right_pushdown;
	bool found_mark_reference = false;
	// now check the set of filters
	for (size_t i = 0; i < filters.size(); i++) {
		auto side = GetExpressionSide(filters[i]->bindings, left_bindings, right_bindings);
		if (side == JoinSide::LEFT) {
			// bindings match left side: push into left
			left_pushdown.filters.push_back(move(filters[i]));
			// erase the filter from the list of filters
			filters.erase(filters.begin() + i);
			i--;
		} else if (side == JoinSide::RIGHT) {
			// there can only be at most one filter referencing the marker
			assert(!found_mark_reference);
			found_mark_reference = true;
			// this filter references the marker
			// we can turn this into a SEMI join if the filter is on only the marker
			if (filters[i]->filter->type == ExpressionType::BOUND_COLUMN_REF) {
				// filter just references the marker: turn into semi join
				join.type = JoinType::SEMI;
				filters.erase(filters.begin() + i);
				i--;
				continue;
			}
			// if the filter is on NOT(marker) AND the join conditions are all set to "null_values_are_equal" we can turn this into an ANTI join
			// if all join conditions have null_values_are_equal=true, then the result of the MARK join is always TRUE or FALSE, and never NULL
			// this happens in the case of a correlated EXISTS clause
			if (filters[i]->filter->type == ExpressionType::OPERATOR_NOT) {
				auto &op_expr = (OperatorExpression&) *filters[i]->filter;
				if (op_expr.children[0]->type == ExpressionType::BOUND_COLUMN_REF) {
					// the filter is NOT(marker), check the join conditions
					bool all_null_values_are_equal = true;
					for(auto &cond : comp_join.conditions) {
						if (!cond.null_values_are_equal) {
							all_null_values_are_equal = false;
							break;
						}
					}
					if (all_null_values_are_equal) {
						// all null values are equal, convert to ANTI join
						join.type = JoinType::ANTI;
						filters.erase(filters.begin() + i);
						i--;
						continue;
					}
				}
			}
		}
	}
	op->children[0] = left_pushdown.Rewrite(move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(move(op->children[1]));
	return FinishPushdown(move(op));
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSingleJoin(unique_ptr<LogicalOperator> op,
                                                             unordered_set<size_t> &left_bindings,
                                                             unordered_set<size_t> &right_bindings) {
 	auto &join = (LogicalJoin &)*op;
	assert(join.type == JoinType::SINGLE);
	FilterPushdown left_pushdown, right_pushdown;
	// now check the set of filters
	for (size_t i = 0; i < filters.size(); i++) {
		auto side = GetExpressionSide(filters[i]->bindings, left_bindings, right_bindings);
		if (side == JoinSide::LEFT) {
			// bindings match left side: push into left
			left_pushdown.filters.push_back(move(filters[i]));
			// erase the filter from the list of filters
			filters.erase(filters.begin() + i);
			i--;
		}
	}
	op->children[0] = left_pushdown.Rewrite(move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(move(op->children[1]));
	return FinishPushdown(move(op));
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownJoin(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::COMPARISON_JOIN || op->type == LogicalOperatorType::ANY_JOIN || op->type == LogicalOperatorType::DELIM_JOIN);
	auto &join = (LogicalJoin &)*op;
	unordered_set<size_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);

	// inner join should not occur here
	// because explicit inner joins are turned into cross product + filters and then transformed into inner joins in the
	// filter pushdown phase
	switch (join.type) {
	case JoinType::INNER:
		return PushdownInnerJoin(move(op), left_bindings, right_bindings);
	case JoinType::LEFT:
		return PushdownLeftJoin(move(op), left_bindings, right_bindings);
	case JoinType::MARK:
		return PushdownMarkJoin(move(op), left_bindings, right_bindings);
	case JoinType::SINGLE:
		return PushdownSingleJoin(move(op), left_bindings, right_bindings);
	default:
		// unsupported join type: stop pushing down
		return FinishPushdown(move(op));
	}
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownCrossProduct(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::CROSS_PRODUCT);
	FilterPushdown left_pushdown, right_pushdown;
	vector<unique_ptr<Expression>> join_conditions;
	unordered_set<size_t> left_bindings, right_bindings;
	if (filters.size() > 0) {
		// check to see into which side we should push the filters
		// first get the LHS and RHS bindings
		LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
		LogicalJoin::GetTableReferences(*op->children[1], right_bindings);
		// now check the set of filters
		for (auto &f : filters) {
			auto side = GetExpressionSide(f->bindings, left_bindings, right_bindings);
			if (side == JoinSide::LEFT) {
				// bindings match left side: push into left
				left_pushdown.filters.push_back(move(f));
			} else if (side == JoinSide::RIGHT) {
				// bindings match right side: push into right
				right_pushdown.filters.push_back(move(f));
			} else {
				assert(side == JoinSide::BOTH);
				// bindings match both: turn into join condition
				join_conditions.push_back(move(f->filter));
			}
		}
	}
	op->children[0] = left_pushdown.Rewrite(move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(move(op->children[1]));

	if (join_conditions.size() > 0) {
		// join conditions found: turn into inner join
		return LogicalComparisonJoin::CreateJoin(JoinType::INNER, move(op->children[0]), move(op->children[1]),
		                                         left_bindings, right_bindings, join_conditions);
	} else {
		// no join conditions found: keep as cross product
		return op;
	}
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownFilter(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::FILTER);
	auto &filter = (LogicalFilter &)*op;
	// filter: gather the filters and remove the filter from the set of operations
	for (size_t i = 0; i < filter.expressions.size(); i++) {
		if (AddFilter(move(filter.expressions[i]))) {
			// filter statically evaluates to false, strip tree
			return make_unique<LogicalEmptyResult>(move(op));
		}
	}
	return Rewrite(move(filter.children[0]));
}

unique_ptr<LogicalOperator> FilterPushdown::FinishPushdown(unique_ptr<LogicalOperator> op) {
	// unhandled type, first perform filter pushdown in its children
	for (size_t i = 0; i < op->children.size(); i++) {
		FilterPushdown pushdown;
		op->children[i] = pushdown.Rewrite(move(op->children[i]));
	}
	// now push any existing filters
	if (filters.size() == 0) {
		// no filters to push
		return op;
	}
	auto filter = make_unique<LogicalFilter>();
	for (auto &f : filters) {
		filter->expressions.push_back(move(f->filter));
	}
	filter->children.push_back(move(op));
	return move(filter);
}


static void RewriteSubqueryExpressionBindings(Filter &filter, Expression &expr, LogicalSubquery &subquery) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)expr;
		assert(colref.binding.table_index == subquery.table_index);
		assert(colref.depth == 0);

		// rewrite the binding by looking into the bound_tables list of the subquery
		size_t column_index = colref.binding.column_index;
		for(size_t i = 0; i < subquery.bound_tables.size(); i++) {
			auto &table = subquery.bound_tables[i];
			if (column_index < table.column_count) {
				// the binding belongs to this table, update the column binding
				colref.binding.table_index = table.table_index;
				colref.binding.column_index = column_index;
				filter.bindings.insert(table.table_index);
				return;
			}
			column_index -= table.column_count;
		}
		// table could not be found!
		assert(0);
	}
	expr.EnumerateChildren([&](Expression *child) {
		RewriteSubqueryExpressionBindings(filter, *child, subquery);
	});
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSubquery(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::SUBQUERY);
	auto &subquery = (LogicalSubquery&) *op;
	// push filter through logical subquery
	// all the BoundColumnRefExpressions in the filter should refer to the LogicalSubquery
	// we need to rewrite them to refer to the underlying bound tables instead
	for(size_t i = 0; i < filters.size(); i++) {
		auto &f = *filters[i];
		assert(f.bindings.size() <= 1);
		f.bindings.clear();
		// rewrite the bindings within this subquery
		RewriteSubqueryExpressionBindings(f, *f.filter, subquery);
	}
	// now continue the pushdown into the child
	subquery.children[0] = Rewrite(move(subquery.children[0]));
	return op;
}

static unique_ptr<Expression> ReplaceProjectionBindings(LogicalProjection &proj, unique_ptr<Expression> expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = (BoundColumnRefExpression &)*expr;
		assert(colref.binding.table_index == proj.table_index);
		assert(colref.binding.column_index < proj.expressions.size());
		assert(colref.depth == 0);
		// replace the binding with a copy to the expression at the referenced index
		return proj.expressions[colref.binding.column_index]->Copy();
	}
	expr->EnumerateChildren([&](unique_ptr<Expression> child) -> unique_ptr<Expression> {
		return ReplaceProjectionBindings(proj, move(child));
	});
	return expr;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownProjection(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::PROJECTION);
	auto &proj = (LogicalProjection&) *op;
	// push filter through logical projection
	// all the BoundColumnRefExpressions in the filter should refer to the LogicalProjection
	// we can rewrite them by replacing those references with the expression of the LogicalProjection node
	for(size_t i = 0; i < filters.size(); i++) {
		auto &f = *filters[i];
		assert(f.bindings.size() <= 1);
		f.bindings.clear();
		// rewrite the bindings within this subquery
		f.filter = ReplaceProjectionBindings(proj, move(f.filter));
		// extract the bindings again
		GetExpressionBindings(*f.filter, f.bindings);
	}
	// now push into children
	proj.children[0] = Rewrite(move(proj.children[0]));
	return op;
}