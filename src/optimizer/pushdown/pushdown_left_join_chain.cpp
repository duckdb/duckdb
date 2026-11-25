#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"

#include "duckdb/optimizer/distinctness.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/logical_operator_repeatability.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

struct LeftJoinChainKey {
	LeftJoinChainKey(unique_ptr<Expression> expression, ExpressionType comparison)
	    : expression(std::move(expression)), comparison(comparison) {
	}

	unique_ptr<Expression> expression;
	ExpressionType comparison;
};

struct LeftJoinChainSource {
	explicit LeftJoinChainSource(LogicalComparisonJoin &join) : join(join) {
	}

	LogicalComparisonJoin &join;
	//! Right-hand keys in the common left-hand key order
	vector<unique_ptr<Expression>> keys;
};

static bool IsLeftJoinChainColumn(const Expression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
	       expr.Cast<BoundColumnRefExpression>().Depth() == 0;
}

static bool MatchLeftJoinChainKeys(LeftJoinChainSource &source, vector<LeftJoinChainKey> &probe_keys) {
	auto &join = source.join;
	if (join.conditions.empty() || join.HasProjectionMap() || join.filter_pushdown) {
		return false;
	}
	const bool first = probe_keys.empty();
	if (!first && join.conditions.size() != probe_keys.size()) {
		return false;
	}
	source.keys.resize(join.conditions.size());
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() ||
		    (condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL &&
		     condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) ||
		    !IsLeftJoinChainColumn(condition.GetLHS()) || !IsLeftJoinChainColumn(condition.GetRHS()) ||
		    condition.GetLHS().GetReturnType() != condition.GetRHS().GetReturnType()) {
			return false;
		}
		idx_t position = 0;
		for (; position < probe_keys.size(); position++) {
			if (probe_keys[position].expression->Equals(condition.GetLHS())) {
				break;
			}
		}
		if (first) {
			if (position != probe_keys.size()) {
				return false;
			}
			probe_keys.emplace_back(condition.GetLHS().Copy(), condition.GetComparisonType());
		} else if (position == probe_keys.size() || probe_keys[position].comparison != condition.GetComparisonType()) {
			return false;
		}
		if (source.keys[position]) {
			return false;
		}
		source.keys[position] = condition.GetRHS().Copy();
	}
	return true;
}

static bool CanFilterLeftJoinChainInput(LogicalOperator &op) {
	if (ClassifyLogicalOperatorRepeatability(op) != LogicalOperatorRepeatability::REPEATABLE ||
	    op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		return false;
	}
	if ((op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_ANY_JOIN) &&
	    op.Cast<LogicalJoin>().join_type == JoinType::SINGLE) {
		return false;
	}
	bool can_filter = true;
	auto check_expression = [&](const Expression &expr) {
		can_filter &= !expr.IsVolatile() && !expr.CanThrow() && !ExpressionBarrier::Contains(expr);
	};
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) { check_expression(**expr); });
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto table = get.GetTable();
		// Table functions do not expose whether the scan itself can throw.
		if (!table || !table->TryGetDuckTableEntry()) {
			return false;
		}
		for (auto &entry : get.table_filters) {
			check_expression(*ExpressionFilter::GetExpressionFilter(entry.Filter(), "LEFT join chain").expr);
		}
		for (auto &filter : get.table_filters.GetMultiColumnFilters()) {
			check_expression(*ExpressionFilter::GetExpressionFilter(*filter, "LEFT join chain").expr);
		}
	}
	if (!can_filter) {
		return false;
	}
	for (auto &child : op.children) {
		if (!CanFilterLeftJoinChainInput(*child)) {
			return false;
		}
	}
	return true;
}

static unique_ptr<LogicalOperator> ReassociateLeftJoinChain(vector<LeftJoinChainSource> &sources,
                                                            vector<LeftJoinChainKey> &probe_keys) {
	auto &first = sources.back();
	auto left = std::move(first.join.children[0]);
	auto right = std::move(first.join.children[1]);
	auto combined_keys = std::move(first.keys);
	for (idx_t source_idx = sources.size() - 1; source_idx > 0; source_idx--) {
		auto &source = sources[source_idx - 1];
		auto full_join = make_uniq<LogicalComparisonJoin>(JoinType::OUTER);
		for (idx_t key_idx = 0; key_idx < probe_keys.size(); key_idx++) {
			full_join->conditions.emplace_back(combined_keys[key_idx]->Copy(), source.keys[key_idx]->Copy(),
			                                   probe_keys[key_idx].comparison);
			auto type = combined_keys[key_idx]->GetReturnType();
			combined_keys[key_idx] =
			    make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, std::move(type),
			                                       std::move(combined_keys[key_idx]), std::move(source.keys[key_idx]));
		}
		full_join->AddChild(std::move(right));
		full_join->AddChild(std::move(source.join.children[1]));
		right = std::move(full_join);
	}
	auto result = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	for (idx_t key_idx = 0; key_idx < probe_keys.size(); key_idx++) {
		result->conditions.emplace_back(std::move(probe_keys[key_idx].expression), std::move(combined_keys[key_idx]),
		                                probe_keys[key_idx].comparison);
	}
	result->AddChild(std::move(left));
	result->AddChild(std::move(right));
	return std::move(result);
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownLeftJoinChain(unique_ptr<LogicalOperator> op) {
	if (filters.empty() || op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return PushFinalFilters(std::move(op));
	}
	for (auto &filter : filters) {
		// Reassociation can push these predicates onto right-hand rows that never matched the left input.
		if (filter->has_barrier || filter->filter->IsVolatile() || filter->filter->CanThrow()) {
			return PushFinalFilters(std::move(op));
		}
	}
	vector<LeftJoinChainKey> probe_keys;
	vector<LeftJoinChainSource> sources;
	unordered_set<TableIndex> nullable_bindings;
	reference<LogicalOperator> current = *op;
	while (current.get().type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = current.get().Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::LEFT) {
			break;
		}
		LeftJoinChainSource source(join);
		if (!MatchLeftJoinChainKeys(source, probe_keys)) {
			break;
		}
		LogicalJoin::GetTableReferences(*join.children[1], nullable_bindings);
		sources.push_back(std::move(source));
		current = *join.children[0];
	}
	if (sources.size() < 2) {
		return PushFinalFilters(std::move(op));
	}
	auto left_bindings = current.get().GetColumnBindings();
	column_binding_set_t left_columns(left_bindings.begin(), left_bindings.end());
	for (auto &key : probe_keys) {
		if (!left_columns.count(key.expression->Cast<BoundColumnRefExpression>().Binding())) {
			return PushFinalFilters(std::move(op));
		}
	}
	bool removes_null = false;
	for (auto &filter : filters) {
		if (FilterRemovesNull(*filter->filter, nullable_bindings)) {
			removes_null = true;
			break;
		}
	}
	if (!removes_null) {
		return PushFinalFilters(std::move(op));
	}
	// The INNER join and its runtime filters can skip work in any of the inputs.
	if (!CanFilterLeftJoinChainInput(*op)) {
		return PushFinalFilters(std::move(op));
	}
	Distinctness distinctness(DistinctnessPropagation::UNARY_OPERATORS_AND_JOINS);
	bool has_duplicate_source = false;
	for (auto &source : sources) {
		if (!distinctness.IsDistinctOn(*source.join.children[1], source.keys)) {
			// Avoid multiplying right-hand rows for keys that might not occur in the left input.
			if (has_duplicate_source) {
				return PushFinalFilters(std::move(op));
			}
			has_duplicate_source = true;
		}
	}
	op = ReassociateLeftJoinChain(sources, probe_keys);
	return Rewrite(std::move(op));
}

} // namespace duckdb
