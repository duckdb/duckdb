#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/in_clause_rewriter.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

static bool HasTransitiveJoinBounds(const vector<unique_ptr<Filter>> &filters) {
	unordered_set<TableIndex> bound_tables;
	for (auto &filter : filters) {
		if (filter->bindings.size() != 1) {
			continue;
		}
		auto &expr = *filter->filter;
		bool has_bound = false;
		if (BoundComparisonExpression::IsComparison(expr)) {
			auto &comparison = expr.Cast<BoundFunctionExpression>();
			has_bound = BoundComparisonExpression::Left(comparison).IsFoldable() ||
			            BoundComparisonExpression::Right(comparison).IsFoldable();
		} else if (expr.GetExpressionType() == ExpressionType::COMPARE_BETWEEN) {
			auto &between = expr.Cast<BoundFunctionExpression>();
			has_bound = BoundBetweenExpression::LowerBound(between).IsFoldable() ||
			            BoundBetweenExpression::UpperBound(between).IsFoldable();
		}
		if (has_bound) {
			bound_tables.insert(*filter->bindings.begin());
		}
	}
	if (bound_tables.empty()) {
		return false;
	}
	for (auto &filter : filters) {
		if (filter->bindings.size() < 2 || !BoundComparisonExpression::IsComparison(*filter->filter)) {
			continue;
		}
		auto &comparison = filter->filter->Cast<BoundFunctionExpression>();
		auto type = comparison.GetExpressionType();
		if (type != ExpressionType::COMPARE_GREATERTHAN && type != ExpressionType::COMPARE_GREATERTHANOREQUALTO &&
		    type != ExpressionType::COMPARE_LESSTHAN && type != ExpressionType::COMPARE_LESSTHANOREQUALTO) {
			continue;
		}
		auto &right = BoundComparisonExpression::Right(comparison);
		if (right.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF &&
		    bound_tables.count(right.Cast<BoundColumnRefExpression>().Binding().table_index)) {
			return true;
		}
	}
	return false;
}

//! The top projection of a stack of projections over a dummy scan (e.g. an inlined `WITH p AS (SELECT 1 AS x)`)
static optional_ptr<LogicalProjection> GetConstantRow(LogicalOperator &op) {
	reference<LogicalOperator> current(op);
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION && current.get().children.size() == 1) {
		current = *current.get().children[0];
	}
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION ||
	    current.get().type != LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return nullptr;
	}
	return &op.Cast<LogicalProjection>();
}

//! The foldable, non-volatile expression `binding` resolves to through the projections of a constant row
static optional_ptr<const Expression> ResolveConstant(LogicalProjection &row, ColumnBinding binding) {
	reference<LogicalProjection> proj(row);
	while (true) {
		if (binding.table_index != proj.get().table_index) {
			return nullptr;
		}
		auto &expr = proj.get().GetExpression(binding);
		if (expr.IsFoldable() && !expr.IsVolatile()) {
			return &expr;
		}
		auto &child = *proj.get().children[0];
		if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    child.type != LogicalOperatorType::LOGICAL_PROJECTION) {
			return nullptr;
		}
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		if (colref.Depth() != 0) {
			return nullptr;
		}
		binding = colref.Binding();
		proj = child.Cast<LogicalProjection>();
	}
}

//! Every row of the other side is paired with the one constant row, so its columns can be replaced by their values
static void ReplaceConstantRowBindings(LogicalProjection &row, Filter &filter) {
	bool changed = false;
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
	    filter.filter, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &expr) {
		    if (colref.Depth() != 0) {
			    return;
		    }
		    auto constant = ResolveConstant(row, colref.Binding());
		    if (!constant) {
			    return;
		    }
		    auto copy = constant->Copy();
		    if (!colref.GetAlias().empty()) {
			    copy->SetAlias(colref.GetAlias());
		    }
		    expr = std::move(copy);
		    changed = true;
	    });
	if (changed) {
		filter.ExtractBindings();
	}
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownCrossProduct(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->children.size() > 1);
	FilterPushdown left_pushdown(optimizer, projection_mode);
	FilterPushdown right_pushdown(optimizer, projection_mode);
	vector<unique_ptr<Expression>> join_expressions;
	vector<unique_ptr<Expression>> deferred_filters;
	auto join_ref_type = JoinRefType::REGULAR;
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		break;
	default:
		throw InternalException("Unsupported join type for cross product push down");
	}
	unordered_set<TableIndex> left_bindings, right_bindings;
	if (!filters.empty()) {
		// Recombine useful join bounds before distributing filters to the inputs.
		if (HasTransitiveJoinBounds(filters)) {
			if (PushFilters() == FilterResult::UNSATISFIABLE) {
				return make_uniq<LogicalEmptyResult>(std::move(op));
			}
			GenerateFilters();
		}
		// check to see into which side we should push the filters
		// first get the LHS and RHS bindings
		LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
		LogicalJoin::GetTableReferences(*op->children[1], right_bindings);
		// substitute the values of a constant one-row side into join filters, so they can be pushed into the other side
		for (idx_t child_idx = 0; child_idx < 2; child_idx++) {
			auto row = GetConstantRow(*op->children[child_idx]);
			if (!row) {
				continue;
			}
			for (auto &f : filters) {
				if (f->bindings.count(row->table_index) &&
				    JoinSide::GetJoinSide(f->bindings, left_bindings, right_bindings) == JoinSide::BOTH) {
					ReplaceConstantRowBindings(*row, *f);
				}
			}
		}
		// now check the set of filters
		for (auto &f : filters) {
			auto side = JoinSide::GetJoinSide(f->bindings, left_bindings, right_bindings);
			if (side == JoinSide::LEFT) {
				// bindings match left side: push into left
				left_pushdown.filters.push_back(std::move(f));
			} else if (side == JoinSide::RIGHT) {
				right_pushdown.filters.push_back(std::move(f));
			} else {
				D_ASSERT(side == JoinSide::BOTH || side == JoinSide::NONE);
				if (InClauseRewriter::HasRewritableInClause(*f->filter)) {
					deferred_filters.push_back(std::move(f->filter));
				} else {
					// bindings match both: turn into join condition
					join_expressions.push_back(std::move(f->filter));
				}
			}
		}
	}

	op->children[0] = left_pushdown.Rewrite(std::move(op->children[0]));
	op->children[1] = right_pushdown.Rewrite(std::move(op->children[1]));

	unique_ptr<LogicalOperator> result;
	if (!join_expressions.empty()) {
		// join conditions found: turn into inner join
		// extract join conditions
		vector<JoinCondition> conditions;
		const auto join_type = JoinType::INNER;
		LogicalComparisonJoin::ExtractJoinConditions(GetContext(), join_type, join_ref_type, op->children[0],
		                                             op->children[1], left_bindings, right_bindings, join_expressions,
		                                             conditions);
		// create the join from the join conditions
		result = LogicalComparisonJoin::CreateJoin(join_type, join_ref_type, std::move(op->children[0]),
		                                           std::move(op->children[1]), std::move(conditions));

		// possible cases are: AnyJoin, ComparisonJoin, or Filter + ComparisonJoin
		if (op->has_estimated_cardinality) {
			// set the estimated cardinality of the new operator
			result->SetEstimatedCardinality(op->estimated_cardinality);
			if (result->type == LogicalOperatorType::LOGICAL_FILTER) {
				// if the new operators are Filter + ComparisonJoin, also set the estimated cardinality for the join
				D_ASSERT(result->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN);
				result->children[0]->SetEstimatedCardinality(op->estimated_cardinality);
			}
		}
	} else {
		// no join conditions found: keep as cross product
		D_ASSERT(op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
		result = std::move(op);
	}
	if (deferred_filters.empty()) {
		return result;
	}
	auto filter = make_uniq<LogicalFilter>();
	filter->expressions = std::move(deferred_filters);
	filter->children.push_back(std::move(result));
	if (filter->children[0]->has_estimated_cardinality) {
		filter->SetEstimatedCardinality(filter->children[0]->estimated_cardinality);
	}
	return std::move(filter);
}

} // namespace duckdb
