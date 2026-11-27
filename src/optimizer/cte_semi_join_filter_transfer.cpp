#include "duckdb/optimizer/cte_filter_pusher.hpp"

#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

//! The search follows one column, but the equalities of an INNER join fork it: the bound keeps it linear
static constexpr idx_t MAX_SEARCH_STEPS = 256;

static optional_ptr<const BoundColumnRefExpression> PlainColumn(const Expression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	auto &column_ref = expr.Cast<BoundColumnRefExpression>();
	if (column_ref.Depth() != 0) {
		return nullptr;
	}
	return &column_ref;
}

static bool IsEquality(const JoinCondition &condition) {
	return condition.IsComparison() && (condition.GetComparisonType() == ExpressionType::COMPARE_EQUAL ||
	                                    condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM);
}

static bool HasBinding(LogicalOperator &op, const ColumnBinding &binding) {
	for (auto &op_binding : op.GetColumnBindings()) {
		if (op_binding == binding) {
			return true;
		}
	}
	return false;
}

//! Adds the conjuncts of the filter that compare `column` with constants and cannot throw
static void AddColumnFilters(const Expression &filter, const BoundColumnRefExpression &column,
                             vector<unique_ptr<Expression>> &result) {
	if (filter.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		for (auto &child : filter.Cast<BoundConjunctionExpression>().GetChildren()) {
			AddColumnFilters(*child, column, result);
		}
		return;
	}
	auto compared = FilterCombiner::GetComparedExpression(filter);
	if (compared && compared->Equals(column) && !filter.CanThrow()) {
		result.push_back(filter.Copy());
	}
}

//! Adds the filters that compare `column`, a column of `op`, with constants and that every row of `op` passes
static void GuaranteedColumnFilters(LogicalOperator &op, const BoundColumnRefExpression &column, idx_t &steps,
                                    vector<unique_ptr<Expression>> &result) {
	if (++steps > MAX_SEARCH_STEPS) {
		return;
	}
	// the filters on a column of the child that is passed on as `column`
	auto follow = [&](LogicalOperator &child, const BoundColumnRefExpression &source) {
		if (source.Binding() == column.Binding()) {
			GuaranteedColumnFilters(child, column, steps, result);
			return;
		}
		if (source.GetReturnType() != column.GetReturnType()) {
			return;
		}
		vector<unique_ptr<Expression>> source_filters;
		GuaranteedColumnFilters(child, source, steps, source_filters);
		for (auto &filter : source_filters) {
			result.push_back(ExpressionIterator::ReplaceExpression(*filter, source, column));
		}
	};
	auto &binding = column.Binding();
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		for (auto &expr : op.expressions) {
			AddColumnFilters(*expr, column, result);
		}
		follow(*op.children[0], column);
		break;
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		if (binding.table_index != projection.table_index || binding.column_index.GetIndex() >= op.expressions.size()) {
			break;
		}
		auto source = PlainColumn(*op.expressions[binding.column_index.GetIndex()]);
		if (source) {
			follow(*op.children[0], *source);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		// grouping sets add NULL groups that the filters below do not describe
		auto &aggregate = op.Cast<LogicalAggregate>();
		if (binding.table_index != aggregate.group_index || aggregate.grouping_sets.size() > 1 ||
		    binding.column_index.GetIndex() >= aggregate.groups.size()) {
			break;
		}
		auto source = PlainColumn(*aggregate.groups[binding.column_index.GetIndex()]);
		if (source) {
			follow(*op.children[0], *source);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (binding.table_index != get.table_index || !op.children.empty()) {
			break;
		}
		auto filter = get.table_filters.TryGetFilterByColumnIndex(binding.column_index);
		// an optional filter is a hint the scan may ignore
		if (!filter || ExpressionFilter::IsOptionalFilter(*filter)) {
			break;
		}
		AddColumnFilters(*filter->ToExpression(column), column, result);
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (op.children.size() != 2) {
			break;
		}
		optional_idx side;
		for (idx_t child_idx = 0; child_idx < 2; child_idx++) {
			if (HasBinding(*op.children[child_idx], binding)) {
				side = child_idx;
				break;
			}
		}
		if (!side.IsValid()) {
			break;
		}
		// the side that an outer join extends with NULLs has rows its filters do not describe
		const auto side_idx = side.GetIndex();
		bool preserved;
		switch (join.join_type) {
		case JoinType::INNER:
			preserved = true;
			break;
		case JoinType::LEFT:
		case JoinType::SEMI:
		case JoinType::ANTI:
		case JoinType::MARK:
			preserved = side_idx == 0;
			break;
		case JoinType::RIGHT:
			preserved = side_idx == 1;
			break;
		default:
			preserved = false;
			break;
		}
		if (!preserved) {
			break;
		}
		follow(*op.children[side_idx], column);
		if (join.join_type != JoinType::INNER) {
			break;
		}
		// the rows of an INNER join also pass what the filters on the other side imply through its equalities
		auto &other_child = *op.children[1 - side_idx];
		vector<unique_ptr<Expression>> other_filters;
		for (auto &condition : join.conditions) {
			if (!IsEquality(condition)) {
				continue;
			}
			auto own = PlainColumn(side_idx == 0 ? condition.GetLHS() : condition.GetRHS());
			auto other = PlainColumn(side_idx == 0 ? condition.GetRHS() : condition.GetLHS());
			if (own && other && own->Binding() == binding && HasBinding(other_child, other->Binding())) {
				GuaranteedColumnFilters(other_child, *other, steps, other_filters);
			}
		}
		vector<reference<const Expression>> filters;
		for (auto &filter : other_filters) {
			filters.push_back(*filter);
		}
		FilterCombiner::GenerateJoinFilters(join.conditions, filters, side_idx == 0 ? JoinSide::LEFT : JoinSide::RIGHT,
		                                    [&](unique_ptr<Expression> filter) {
			                                    // an equality of the other column with another column of this side
			                                    AddColumnFilters(*filter, column, result);
		                                    });
		break;
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		for (auto &child : op.children) {
			if (HasBinding(*child, binding)) {
				follow(*child, column);
				break;
			}
		}
		break;
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_DISTINCT:
	case LogicalOperatorType::LOGICAL_WINDOW:
		// rows are removed, reordered or kept, the columns of the child pass through under their own bindings
		if (op.children.size() == 1 && HasBinding(*op.children[0], binding)) {
			follow(*op.children[0], column);
		}
		break;
	default:
		// set operations and CTE references end the search
		break;
	}
}

//! The join that keeps only the left rows with a match: a SEMI join, or a MARK join below a filter on its bare mark
static optional_ptr<LogicalComparisonJoin> FilteringJoin(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		return join.join_type == JoinType::SEMI && op.children.size() == 2 ? &join : nullptr;
	}
	if (op.type != LogicalOperatorType::LOGICAL_FILTER ||
	    op.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return nullptr;
	}
	auto &join = op.children[0]->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::MARK || join.children.size() != 2) {
		return nullptr;
	}
	for (auto &expr : op.expressions) {
		auto column_ref = PlainColumn(*expr);
		if (column_ref && column_ref->Binding().table_index == join.mark_index) {
			return &join;
		}
	}
	return nullptr;
}

void CTEFilterPusher::GenerateSemiJoinFilters(LogicalOperator &op) {
	if (cte_info_map.empty()) {
		return;
	}
	// children first, so that a nested join has filtered its left side before the join above searches that side
	for (auto &child : op.children) {
		GenerateSemiJoinFilters(*child);
	}
	auto join = FilteringJoin(op);
	if (!join) {
		return;
	}
	// the left side is a reference of a materialized CTE whose other references are all filtered
	optional_ptr<LogicalOperator> ref_op = join->children[0].get();
	optional_ptr<LogicalOperator> ref_filter;
	if (ref_op->type == LogicalOperatorType::LOGICAL_FILTER && !ref_op->Cast<LogicalFilter>().HasProjectionMap()) {
		ref_filter = ref_op;
		ref_op = ref_op->children[0].get();
	}
	if (ref_op->type != LogicalOperatorType::LOGICAL_CTE_REF) {
		return;
	}
	auto &cte_ref = ref_op->Cast<LogicalCTERef>();
	auto entry = cte_info_map.find(to_string(cte_ref.cte_index.index));
	if (entry == cte_info_map.end() || cte_ref.is_recurring || cte_ref.correlated_columns != 0) {
		return;
	}
	auto &info = *entry->second;
	if (info.references.size() != info.filters.size() + (ref_filter ? 0 : 1)) {
		return;
	}

	// the filters on the join keys that every right row passes
	vector<unique_ptr<Expression>> right_filters;
	for (auto &condition : join->conditions) {
		if (!IsEquality(condition)) {
			continue;
		}
		auto right_key = PlainColumn(condition.GetRHS());
		if (right_key && HasBinding(*join->children[1], right_key->Binding())) {
			idx_t steps = 0;
			GuaranteedColumnFilters(*join->children[1], *right_key, steps, right_filters);
		}
	}
	if (right_filters.empty()) {
		return;
	}

	// a left row with a match passes what they imply for the left side
	vector<reference<const Expression>> filters;
	for (auto &filter : right_filters) {
		filters.push_back(*filter);
	}
	vector<unique_ptr<Expression>> left_filters;
	FilterCombiner::GenerateJoinFilters(join->conditions, filters, JoinSide::LEFT,
	                                    [&](unique_ptr<Expression> left_filter) {
		                                    bool exists = false;
		                                    if (ref_filter) {
			                                    for (auto &existing : ref_filter->expressions) {
				                                    exists = exists || existing->Equals(*left_filter);
			                                    }
		                                    }
		                                    for (auto &existing : left_filters) {
			                                    exists = exists || existing->Equals(*left_filter);
		                                    }
		                                    if (!exists) {
			                                    left_filters.push_back(std::move(left_filter));
		                                    }
	                                    });
	if (left_filters.empty()) {
		return;
	}
	if (ref_filter) {
		for (auto &left_filter : left_filters) {
			ref_filter->expressions.push_back(std::move(left_filter));
		}
		return;
	}
	// The CTE is restricted by the disjunction of the filters of its references, which this filter usually repeats in
	// part. As one flat conjunction it is a branch that the expression rewriter reduces: (A AND B AND C) OR A is A.
	auto filter = make_uniq<LogicalFilter>();
	if (left_filters.size() == 1) {
		filter->expressions = std::move(left_filters);
	} else {
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		conjunction->GetChildrenMutable() = std::move(left_filters);
		filter->expressions.push_back(std::move(conjunction));
	}
	filter->children.push_back(std::move(join->children[0]));
	info.filters.push_back(*filter);
	info.all_cte_refs_are_filtered = true;
	join->children[0] = std::move(filter);
}

} // namespace duckdb
