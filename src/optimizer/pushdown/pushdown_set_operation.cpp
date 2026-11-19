#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

static void ReplaceSetOpBindings(vector<ColumnBinding> &bindings, Filter &filter, unique_ptr<Expression> &root_expr,
                                 LogicalSetOperation &setop) {
	root_expr = ExpressionIterator::PreorderReplace<BoundColumnRefExpression>(
	    std::move(root_expr), [&](const BoundColumnRefExpression &colref) {
		    D_ASSERT(colref.Binding().table_index == setop.table_index);
		    D_ASSERT(colref.Depth() == 0);

		    // rewrite the binding by looking into the bound_tables list of the subquery
		    auto &new_binding = bindings[colref.Binding().column_index];
		    filter.bindings.insert(new_binding.table_index);
		    return colref.WithBinding(new_binding);
	    });
}

//! Whether a filter on the left child of an EXCEPT / INTERSECT keeps or removes all the rows that the set operation
//! treats as equal. Only such a filter may run before the set operation, which returns some of the equal rows: it
//! reads only columns that are compared as they are, or it compares what a column is compared as (the column under
//! its collation) with constants. This is what a filter needs to pass the row_number() windows of CreateJoin as well.
static bool KeepsEqualRowsTogether(const Expression &filter, const vector<JoinCondition> &comparisons) {
	if (filter.IsVolatile()) {
		return false;
	}
	auto compared = FilterCombiner::GetComparedExpression(filter);
	if (compared) {
		for (auto &comparison : comparisons) {
			if (comparison.GetLHS().Equals(*compared)) {
				return true;
			}
		}
	}
	bool plain_columns = true;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(filter, [&](const BoundColumnRefExpression &colref) {
		bool compared_as_is = false;
		for (auto &comparison : comparisons) {
			auto &left = comparison.GetLHS();
			if (left.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    left.Cast<BoundColumnRefExpression>().Binding() == colref.Binding()) {
				compared_as_is = true;
			}
		}
		plain_columns = plain_columns && compared_as_is;
	});
	return plain_columns;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSetOperation(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_UNION || op->type == LogicalOperatorType::LOGICAL_EXCEPT ||
	         op->type == LogicalOperatorType::LOGICAL_INTERSECT);
	auto &setop = op->Cast<LogicalSetOperation>();

	if (op->type != LogicalOperatorType::LOGICAL_UNION) {
		vector<unique_ptr<Filter>> remaining_filters;
		// EXCEPT and INTERSECT return rows of their left child: a filter on the result applies to the left child if it
		// does not tell the rows apart that the set operation treats as equal, otherwise it stays above. A right row
		// that fails what the filters imply for it through the comparisons only has left rows as equals that the
		// filters remove, so the implied filters apply to the right child
		D_ASSERT(op->children.size() == 2);
		auto comparisons = setop.GetComparisons(optimizer.context);
		auto left_bindings = op->children[0]->GetColumnBindings();
		FilterPushdown left_pushdown(optimizer, convert_mark_joins, projection_mode);
		FilterPushdown right_pushdown(optimizer, convert_mark_joins, projection_mode);
		for (auto &original_filter : filters) {
			auto filter = make_uniq<Filter>();
			filter->filter = original_filter->filter->Copy();
			ReplaceSetOpBindings(left_bindings, *filter, filter->filter, setop);
			if (!KeepsEqualRowsTogether(*filter->filter, comparisons)) {
				remaining_filters.push_back(std::move(original_filter));
				continue;
			}
			filter->ExtractBindings();
			filter->ExtractBarrier();
			left_pushdown.filters.push_back(std::move(filter));
		}
		vector<reference<const Expression>> left_filters;
		for (auto &filter : left_pushdown.filters) {
			left_filters.push_back(*filter->filter);
		}
		if (!left_filters.empty()) {
			FilterCombiner::GenerateJoinFilters(
			    comparisons, left_filters, JoinSide::RIGHT,
			    [&](unique_ptr<Expression> right_filter) { right_pushdown.AddFilter(std::move(right_filter)); });
			right_pushdown.GenerateFilters();
		}
		op->children[0] = left_pushdown.Rewrite(std::move(op->children[0]));
		op->children[1] = right_pushdown.Rewrite(std::move(op->children[1]));
		filters = std::move(remaining_filters);
	}

	for (idx_t child_idx = 0; child_idx < op->children.size() && op->type == LogicalOperatorType::LOGICAL_UNION;
	     child_idx++) {
		auto &child = op->children[child_idx];
		auto child_bindings = child->GetColumnBindings();

		FilterPushdown child_pushdown(optimizer, convert_mark_joins, projection_mode);
		for (auto &original_filter : filters) {
			// first create a copy of the filter
			auto filter = make_uniq<Filter>();
			filter->filter = original_filter->filter->Copy();

			//  rewrite references to the result of the union into references to the child index
			ReplaceSetOpBindings(child_bindings, *filter, filter->filter, setop);

			// extract bindings again
			filter->ExtractBindings();

			// move the filters into the child pushdown nodes
			child_pushdown.filters.push_back(std::move(filter));
		}

		// pushdown into the child
		child = child_pushdown.Rewrite(std::move(child));
	}
	if (op->type == LogicalOperatorType::LOGICAL_UNION) {
		// every filter went into every child
		filters.clear();
	}
	bool all_empty = true;
	for (auto &child : op->children) {
		if (child->type != LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
			all_empty = false;
		}
	}
	if (all_empty) {
		// all sides are empty: the result must be empty
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}
	if (op->type == LogicalOperatorType::LOGICAL_UNION) {
		// for UNION (ALL) - delete all empty children and return
		for (idx_t i = 0; i < op->children.size(); i++) {
			if (op->children[i]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
				op->children.erase(op->children.begin() + static_cast<int64_t>(i));
				i--;
			}
		}
		return op;
	}
	bool left_empty = op->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	bool right_empty = op->children[1]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	if (left_empty && setop.setop_all) {
		// left child is empty result
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_EXCEPT:
			// except: if left child is empty, return empty result
		case LogicalOperatorType::LOGICAL_INTERSECT:
			// intersect: if any child is empty, return empty result itself
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			throw InternalException("Unsupported set operation");
		}
	} else if (right_empty && setop.setop_all) {
		// right child is empty result
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_EXCEPT:
			if (op->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				// union or except with empty right child: return left child
				auto &projection = op->children[0]->Cast<LogicalProjection>();
				projection.table_index = setop.table_index;
				return PushFinalFilters(std::move(op->children[0]));
			}
			break;
		case LogicalOperatorType::LOGICAL_INTERSECT:
			// intersect: if any child is empty, return empty result itself
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			throw InternalException("Unsupported set operation");
		}
	}
	return PushFinalFilters(std::move(op));
}

} // namespace duckdb
