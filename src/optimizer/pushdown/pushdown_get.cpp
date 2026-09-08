#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/in_clause_rewriter.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/constraints/bound_check_constraint.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_parameter_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/storage/storage_index.hpp"

namespace duckdb {

/**
 * When a BoundColumnRefExpression that's part of expr (a filter) arrives here, its
 * name will be set to the projection name i.e. "other" for SELECT col as other.
 * If CTE inlining optimizer collapses the CTE in
 * WITH cte AS (SELECT col AS other FROM reader()) SELECT * WHERE other > 0 FROM cte,
 * reader() will get a complex filter with "other" which doesn't exist.
 * Rename the columns back to their original names
 */
static void NormalizeColumnRefAliases(unique_ptr<Expression> &expr, const LogicalGet &get) {
	const vector<ColumnIndex> &column_ids = get.GetColumnIds();
	ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(expr, [&](auto &ref, auto &) {
		const ColumnBinding &binding = ref.Binding();
		if (binding.table_index != get.table_index || binding.column_index >= column_ids.size()) {
			return;
		}
		const ColumnIndex &col_idx = column_ids[binding.column_index];
		if (!col_idx.HasPrimaryIndex()) {
			ref.SetAlias(Identifier(col_idx.GetFieldName()));
			return;
		}
		const idx_t primary = col_idx.GetPrimaryIndex();
		if (col_idx.IsVirtualColumn()) {
			if (const auto it = get.virtual_columns.find(primary); it != get.virtual_columns.end()) {
				ref.SetAlias(Identifier(it->second.name.GetIdentifierName()));
			}
		} else if (primary < get.names.size()) {
			ref.SetAlias(Identifier(col_idx.GetName(get.names[primary].GetIdentifierName())));
		}
	});
}

//! Rewrite the column references of a bound CHECK expression (which are storage indexes) into bindings of the get
static bool RewriteCheckExpression(unique_ptr<Expression> &expr, const LogicalGet &get) {
	unordered_map<idx_t, idx_t> storage_to_binding;
	auto &column_ids = get.GetColumnIds();
	for (idx_t i = 0; i < column_ids.size(); i++) {
		StorageIndex storage_index;
		if (column_ids[i].HasChildren() || !get.TryGetStorageIndex(column_ids[i], storage_index)) {
			continue;
		}
		storage_to_binding[storage_index.GetPrimaryIndex()] = i;
	}
	bool success = true;
	ExpressionIterator::VisitExpressionMutable<BoundReferenceExpression>(
	    expr, [&](BoundReferenceExpression &ref, unique_ptr<Expression> &child) {
		    auto entry = storage_to_binding.find(ref.Index());
		    if (entry == storage_to_binding.end()) {
			    // the column is not scanned by the get
			    success = false;
			    return;
		    }
		    child = make_uniq<BoundColumnRefExpression>(ref.GetReturnType(),
		                                                ColumnBinding(get.table_index, ProjectionIndex(entry->second)));
	    });
	return success;
}

//! A CHECK constraint guarantees that its expression never evaluates to FALSE for any row in the table
//! a filter that is the negation of a CHECK expression can therefore never be satisfied
static bool IsNegation(const Expression &filter, const Expression &check) {
	if (filter.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
		return filter.Cast<BoundOperatorExpression>().GetChildren()[0]->Equals(check);
	}
	if (!BoundComparisonExpression::IsComparison(filter) || !BoundComparisonExpression::IsComparison(check) ||
	    NegateComparisonExpression(filter.GetExpressionType()) != check.GetExpressionType()) {
		return false;
	}
	auto &filter_comparison = filter.Cast<BoundFunctionExpression>();
	auto &check_comparison = check.Cast<BoundFunctionExpression>();
	return BoundComparisonExpression::Left(filter_comparison)
	           .Equals(BoundComparisonExpression::Left(check_comparison)) &&
	       BoundComparisonExpression::Right(filter_comparison)
	           .Equals(BoundComparisonExpression::Right(check_comparison));
}

//! Check if any of the filters contradicts a CHECK constraint of the scanned table
bool FilterPushdown::CheckConstraintsUnsatisfiable(const LogicalGet &get) {
	auto table = get.GetTable();
	if (filters.empty() || !table) {
		return false;
	}
	shared_ptr<Binder> binder;
	for (auto &constraint : table->GetConstraints()) {
		if (constraint->type != ConstraintType::CHECK) {
			continue;
		}
		if (!binder) {
			binder = Binder::CreateBinder(optimizer.context);
		}
		auto bound_constraint = binder->BindConstraint(*constraint, table->name, table->GetColumns());
		auto check_expression = std::move(bound_constraint->Cast<BoundCheckConstraint>().expression);
		if (check_expression->IsVolatile() || !check_expression->IsConsistent()) {
			continue;
		}
		// the CheckBinder casts the CHECK expression to INTEGER - strip the cast to get back the predicate
		if (!BoundCastExpression::IsCast(*check_expression) || !RewriteCheckExpression(check_expression, get)) {
			continue;
		}
		auto &check_predicate = BoundCastExpression::Child(check_expression->Cast<BoundFunctionExpression>());
		for (auto &filter : filters) {
			if (IsNegation(*filter->filter, check_predicate)) {
				return true;
			}
		}
	}
	return false;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownGet(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = op->Cast<LogicalGet>();

	for (auto &filter : filters) {
		NormalizeColumnRefAliases(filter->filter, get);
	}

	if (CheckConstraintsUnsatisfiable(get)) {
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}

	if (get.function.pushdown_complex_filter || get.function.filter_pushdown) {
		// this scan supports some form of filter push-down
		// check if there are any parameters
		// if there are, invalidate them to force a re-bind on execution
		for (auto &filter : filters) {
			if (filter->filter->HasParameter()) {
				// there is a parameter in the filters! invalidate it
				BoundParameterExpression::InvalidateRecursive(*filter->filter);
			}
		}
	}
	if (get.function.pushdown_complex_filter) {
		// for the remaining filters, check if we can push any of them into the scan as well
		vector<unique_ptr<Expression>> expressions;
		expressions.reserve(filters.size());
		for (auto &filter : filters) {
			expressions.push_back(std::move(filter->filter));
		}
		filters.clear();

		get.function.pushdown_complex_filter(optimizer.context, get, get.bind_data.get(), expressions);

		if (expressions.empty()) {
			return op;
		}
		// re-generate the filters
		for (auto &expr : expressions) {
			auto f = make_uniq<Filter>();
			f->filter = std::move(expr);
			f->ExtractBindings();
			filters.push_back(std::move(f));
		}
	}

	if (get.table_filters.HasFilters() || !get.function.filter_pushdown) {
		// the table function does not support filter pushdown: push a LogicalFilter on top
		return FinishPushdown(std::move(op));
	}
	if (PushFilters() == FilterResult::UNSATISFIABLE) {
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}

	auto &column_ids = get.GetColumnIds();
	//! We generate the table filters that will be executed during the table scan
	vector<FilterPushdownResult> pushdown_results;
	get.table_filters = combiner.GenerateTableScanFilters(column_ids, pushdown_results);

	GenerateFilters();

	for (idx_t i = pushdown_results.size(); i < filters.size(); ++i) {
		// any generated filters have not been pushed down yet
		pushdown_results.push_back(FilterPushdownResult::NO_PUSHDOWN);
	}
	// for any filters we did not manage to push into specialized table filters - try to push them as a generic
	// expression
	for (idx_t i = 0; i < filters.size(); ++i) {
		// get the previous pushdown result
		auto pushdown_result = pushdown_results[i];
		if (pushdown_result != FilterPushdownResult::NO_PUSHDOWN) {
			// this has already been (partially) pushed down - skip
			continue;
		}
		auto &expr = *filters[i]->filter;
		if (expr.IsVolatile()) {
			continue;
		}
		// IN with enough values benefits from a hash join and is handled by InClauseRewriter - skip pushdown.
		// Also skip throwing IN expressions: scan pushdown loses short-circuit evaluation semantics.
		if (expr.GetExpressionType() == ExpressionType::COMPARE_IN) {
			if (expr.CanThrow()) {
				continue;
			}
			auto &in_expr = expr.Cast<BoundOperatorExpression>();
			if (!in_expr.GetChildren().empty() &&
			    in_expr.GetChildren()[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    in_expr.GetChildren().size() - 1 >= InClauseRewriter::IN_CLAUSE_REWRITE_THRESHOLD) {
				continue;
			}
		}
		// Allow pushing down filters that can throw only if there is a single expression
		if (expr.CanThrow() && filters.size() > 1) {
			continue;
		}
		pushdown_result = combiner.TryPushdownGenericExpression(get, expr);
		if (pushdown_result == FilterPushdownResult::PUSHED_DOWN_FULLY) {
			filters.erase_at(i);
			pushdown_results.erase_at(i);
			i--;
		}
	}

	//! Now we try to pushdown the remaining filters to perform zonemap checking
	return FinishPushdown(std::move(op));
}

} // namespace duckdb
