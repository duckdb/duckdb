//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain_candidate.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain_candidate.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/optimizer/join_order/join_order_optimizer.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>

namespace duckdb {

struct EquivalentBinding {
	EquivalentBinding(ColumnBinding left_p, ColumnBinding right_p) : left(left_p), right(right_p) {
	}

	ColumnBinding left;
	ColumnBinding right;
};

class BindingEquivalence {
public:
	void Add(ColumnBinding left, ColumnBinding right) {
		if (left != right) {
			edges.emplace_back(left, right);
		}
	}

	bool Contains(ColumnBinding source, ColumnBinding target) const {
		if (source == target) {
			return true;
		}
		column_binding_set_t visited;
		vector<ColumnBinding> pending {source};
		while (!pending.empty()) {
			auto current = pending.back();
			pending.pop_back();
			if (!visited.insert(current).second) {
				continue;
			}
			for (auto &edge : edges) {
				ColumnBinding next;
				if (edge.left == current) {
					next = edge.right;
				} else if (edge.right == current) {
					next = edge.left;
				} else {
					continue;
				}
				if (next == target) {
					return true;
				}
				pending.push_back(next);
			}
		}
		return false;
	}

private:
	vector<EquivalentBinding> edges;
};

static bool GetCandidateBinding(const Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &column = expr.Cast<BoundColumnRefExpression>();
	if (column.Depth() != 0) {
		return false;
	}
	binding = column.Binding();
	return true;
}

static bool IsCandidateEquivalenceComparison(const JoinCondition &condition) {
	if (!condition.IsComparison()) {
		return false;
	}
	auto comparison = condition.GetComparisonType();
	return comparison == ExpressionType::COMPARE_EQUAL || comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

static bool IsSafeInnerJoin(const LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	return join.join_type == JoinType::INNER && !join.HasProjectionMap();
}

static void CollectEquivalences(LogicalOperator &op, BindingEquivalence &equivalence) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		auto output_bindings = projection.GetColumnBindings();
		for (idx_t expression_idx = 0; expression_idx < projection.expressions.size(); expression_idx++) {
			ColumnBinding child_binding;
			if (GetCandidateBinding(*projection.expressions[expression_idx], child_binding)) {
				equivalence.Add(output_bindings[expression_idx], child_binding);
			}
		}
	} else if (IsSafeInnerJoin(op)) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &condition : join.conditions) {
			if (!IsCandidateEquivalenceComparison(condition)) {
				continue;
			}
			ColumnBinding left;
			ColumnBinding right;
			if (GetCandidateBinding(condition.GetLHS(), left) && GetCandidateBinding(condition.GetRHS(), right)) {
				equivalence.Add(left, right);
			}
		}
	}

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
		if (op.children.size() == 1) {
			CollectEquivalences(*op.children[0], equivalence);
		}
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		if (IsSafeInnerJoin(op)) {
			CollectEquivalences(*op.children[0], equivalence);
			CollectEquivalences(*op.children[1], equivalence);
		}
		break;
	default:
		break;
	}
}

static bool FindCandidateKeys(LogicalOperator &op, const vector<unique_ptr<Expression>> &keys,
                              const BindingEquivalence &equivalence, vector<idx_t> &key_indices) {
	auto bindings = op.GetColumnBindings();
	if (bindings.empty() || bindings.size() != op.types.size()) {
		return false;
	}
	key_indices.clear();
	key_indices.reserve(keys.size());
	for (auto &key : keys) {
		ColumnBinding key_binding;
		if (!GetCandidateBinding(*key, key_binding)) {
			return false;
		}
		optional_idx match;
		for (idx_t binding_idx = 0; binding_idx < bindings.size(); binding_idx++) {
			if (op.types[binding_idx] != key->GetReturnType() ||
			    !equivalence.Contains(key_binding, bindings[binding_idx])) {
				continue;
			}
			if (bindings[binding_idx] == key_binding) {
				match = binding_idx;
				break;
			}
			if (!match.IsValid()) {
				match = binding_idx;
			}
		}
		if (!match.IsValid()) {
			return false;
		}
		key_indices.push_back(match.GetIndex());
	}
	return true;
}

bool DuplicateEliminatedDomainCandidateFinder::IsSupportedSource(LogicalOperator &op) {
	if (op.HasSideEffects() || op.HasVolatileExpressions()) {
		return false;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return true;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		return (join.join_type == JoinType::INNER || join.join_type == JoinType::SEMI) && !join.HasProjectionMap();
	}
	default:
		return false;
	}
}

static bool OperatorOutputsBinding(LogicalOperator &op, ColumnBinding binding) {
	auto bindings = op.GetColumnBindings();
	return std::find(bindings.begin(), bindings.end(), binding) != bindings.end();
}

static bool HasSupportedKeyProvenance(LogicalOperator &op, ColumnBinding binding) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		return OperatorOutputsBinding(op, binding);
	case LogicalOperatorType::LOGICAL_FILTER:
		return op.children.size() == 1 && HasSupportedKeyProvenance(*op.children[0], binding);
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (op.children.size() != 1) {
			return false;
		}
		auto &projection = op.Cast<LogicalProjection>();
		auto bindings = projection.GetColumnBindings();
		auto binding_index = std::find(bindings.begin(), bindings.end(), binding);
		if (binding_index == bindings.end()) {
			return false;
		}
		auto expression_index = NumericCast<idx_t>(binding_index - bindings.begin());
		ColumnBinding child_binding;
		return GetCandidateBinding(*projection.expressions[expression_index], child_binding) &&
		       HasSupportedKeyProvenance(*projection.children[0], child_binding);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		auto bindings = aggregate.GetColumnBindings();
		auto binding_index = std::find(bindings.begin(), bindings.end(), binding);
		return binding_index != bindings.end() &&
		       NumericCast<idx_t>(binding_index - bindings.begin()) < aggregate.groups.size();
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		if (op.children.size() != 2) {
			return false;
		}
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::INNER) {
			return HasSupportedKeyProvenance(*op.children[0], binding) ||
			       HasSupportedKeyProvenance(*op.children[1], binding);
		}
		if (join.join_type == JoinType::SEMI) {
			return HasSupportedKeyProvenance(*op.children[0], binding);
		}
		return false;
	}
	default:
		return false;
	}
}

static bool CandidateKeysAreSupported(LogicalOperator &op, const vector<idx_t> &key_indices) {
	auto bindings = op.GetColumnBindings();
	for (auto key_index : key_indices) {
		if (key_index >= bindings.size() || !HasSupportedKeyProvenance(op, bindings[key_index])) {
			return false;
		}
	}
	return true;
}

static void FindCandidates(unique_ptr<LogicalOperator> &op, const vector<unique_ptr<Expression>> &keys,
                           const BindingEquivalence &equivalence, idx_t joins_above,
                           vector<DuplicateEliminatedDomainCandidate> &candidates, bool is_root = false) {
	if (!is_root && DuplicateEliminatedDomainCandidateFinder::IsSupportedSource(*op)) {
		vector<idx_t> key_indices;
		if (FindCandidateKeys(*op, keys, equivalence, key_indices) && CandidateKeysAreSupported(*op, key_indices)) {
			candidates.emplace_back(op, std::move(key_indices), joins_above);
		}
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
		if (op->children.size() == 1) {
			FindCandidates(op->children[0], keys, equivalence, joins_above, candidates);
		}
		break;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		if (!IsSafeInnerJoin(*op)) {
			break;
		}
		FindCandidates(op->children[0], keys, equivalence, joins_above + 1, candidates);
		FindCandidates(op->children[1], keys, equivalence, joins_above + 1, candidates);
		break;
	}
	default:
		break;
	}
}

static double GetTypeWidth(const LogicalType &type) {
	return LossyNumericCast<double>(MaxValue<idx_t>(GetTypeIdSize(type.InternalType()), 1));
}

static double GetOutputWidth(LogicalOperator &op) {
	double width = 0;
	for (auto &type : op.types) {
		width += GetTypeWidth(type);
	}
	return MaxValue<double>(width, 1);
}

static double GetKeyWidth(const vector<unique_ptr<Expression>> &keys) {
	double width = 0;
	for (auto &key : keys) {
		width += GetTypeWidth(key->GetReturnType());
	}
	return MaxValue<double>(width, 1);
}

static double EstimateDomainWork(LogicalOperator &source, idx_t rows, double key_width) {
	// The key-width term treats every input row as a possible distinct RHS group. This charges safe-superset
	// candidates for the additional groups they can introduce without requiring undeclared uniqueness.
	return LossyNumericCast<double>(rows) * (GetOutputWidth(source) + key_width);
}

static bool HasSelection(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		return true;
	}
	if (op.type == LogicalOperatorType::LOGICAL_GET && op.Cast<LogicalGet>().table_filters.HasFilters()) {
		return true;
	}
	if ((op.type == LogicalOperatorType::LOGICAL_PROJECTION ||
	     op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) &&
	    op.children.size() == 1) {
		return HasSelection(*op.children[0]);
	}
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::SEMI && !op.children.empty()) {
			return HasSelection(*op.children[0]);
		}
	}
	return false;
}

static optional_idx EstimateSimpleCardinality(ClientContext &context, LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		return RelationStatisticsHelper::ExtractGetStats(op.Cast<LogicalGet>(), context).cardinality;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		if (op.children.size() == 1) {
			return EstimateSimpleCardinality(context, *op.children[0]);
		}
		return optional_idx();
	case LogicalOperatorType::LOGICAL_FILTER: {
		if (op.children.size() != 1) {
			return optional_idx();
		}
		auto child_cardinality = EstimateSimpleCardinality(context, *op.children[0]);
		if (!child_cardinality.IsValid()) {
			return optional_idx();
		}
		auto filtered =
		    static_cast<double>(child_cardinality.GetIndex()) * RelationStatisticsHelper::DEFAULT_SELECTIVITY;
		return MaxValue<idx_t>(LossyNumericCast<idx_t>(filtered), 1);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		if (op.children.size() == 1) {
			return EstimateSimpleCardinality(context, *op.children[0]);
		}
		return optional_idx();
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::SEMI && !op.children.empty()) {
			return EstimateSimpleCardinality(context, *op.children[0]);
		}
		return optional_idx();
	}
	default:
		return optional_idx();
	}
}

static optional_idx FindBestCandidate(ClientContext &context, LogicalOperator &payload,
                                      const vector<unique_ptr<Expression>> &keys,
                                      vector<DuplicateEliminatedDomainCandidate> &candidates) {
	vector<reference<LogicalOperator>> estimated_operators;
	estimated_operators.reserve(candidates.size() + 1);
	for (auto &candidate : candidates) {
		estimated_operators.push_back(*candidate.source.get());
	}
	estimated_operators.push_back(payload);

	vector<idx_t> estimates;
	JoinOrderOptimizer estimator(context);
	bool has_join_estimates = estimator.EstimateCardinalitiesWithoutReordering(payload, estimated_operators, estimates);
	idx_t payload_rows = has_join_estimates ? estimates.back() : 0;
	auto key_width = GetKeyWidth(keys);
	double payload_work = EstimateDomainWork(payload, payload_rows, key_width);

	optional_idx best;
	double best_work = 0;
	for (idx_t candidate_idx = 0; candidate_idx < candidates.size(); candidate_idx++) {
		auto &candidate = candidates[candidate_idx];
		optional_idx estimated_rows;
		if (has_join_estimates) {
			estimated_rows = estimates[candidate_idx];
		} else {
			estimated_rows = EstimateSimpleCardinality(context, *candidate.source.get());
		}
		if (!estimated_rows.IsValid()) {
			continue;
		}
		auto candidate_rows = estimated_rows.GetIndex();
		auto candidate_work = EstimateDomainWork(*candidate.source.get(), candidate_rows, key_width);

		bool clearly_cheaper =
		    has_join_estimates && payload_rows > 0 && candidate_rows <= payload_rows && candidate_work < payload_work;
		bool bounded_small_domain = candidate_rows <= STANDARD_VECTOR_SIZE && candidate.joins_above > 0 &&
		                            HasSelection(*candidate.source.get());
		if (!clearly_cheaper && !bounded_small_domain) {
			continue;
		}
		if (!best.IsValid() || candidate_work < best_work) {
			best = candidate_idx;
			best_work = candidate_work;
		}
	}
	return best;
}

unique_ptr<DuplicateEliminatedDomainCandidate>
DuplicateEliminatedDomainCandidateFinder::FindBest(ClientContext &context, LogicalComparisonJoin &join) {
	if (join.children.empty() || join.duplicate_eliminated_columns.empty()) {
		return nullptr;
	}
	join.children[0]->ResolveOperatorTypes();
	BindingEquivalence equivalence;
	CollectEquivalences(*join.children[0], equivalence);
	vector<DuplicateEliminatedDomainCandidate> candidates;
	FindCandidates(join.children[0], join.duplicate_eliminated_columns, equivalence, 0, candidates, true);
	if (candidates.empty()) {
		return nullptr;
	}
	auto selected_index = FindBestCandidate(context, *join.children[0], join.duplicate_eliminated_columns, candidates);
	if (!selected_index.IsValid()) {
		return nullptr;
	}
	auto &selected = candidates[selected_index.GetIndex()];
	return make_uniq<DuplicateEliminatedDomainCandidate>(selected.source.get(), std::move(selected.key_indices),
	                                                     selected.joins_above);
}

unique_ptr<DuplicateEliminatedDomainCandidate>
DuplicateEliminatedDomainCandidateFinder::CreateForSource(LogicalComparisonJoin &join,
                                                          unique_ptr<LogicalOperator> &source, idx_t joins_above) {
	if (!IsSupportedSource(*source)) {
		return nullptr;
	}
	source->ResolveOperatorTypes();
	BindingEquivalence equivalence;
	CollectEquivalences(*join.children[0], equivalence);
	vector<idx_t> key_indices;
	if (!FindCandidateKeys(*source, join.duplicate_eliminated_columns, equivalence, key_indices) ||
	    !CandidateKeysAreSupported(*source, key_indices)) {
		return nullptr;
	}
	return make_uniq<DuplicateEliminatedDomainCandidate>(source, std::move(key_indices), joins_above);
}

} // namespace duckdb
