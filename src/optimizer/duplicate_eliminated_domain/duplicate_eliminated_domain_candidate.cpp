//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.hpp"

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/subquery/duplicate_eliminated_domain_properties.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>

namespace duckdb {

enum class KeyMatch : uint8_t { IDENTICAL, NULL_SAFE_EQUALITY, EQUALITY };

struct EquivalentBinding {
	EquivalentBinding(ColumnBinding left_p, ColumnBinding right_p, bool null_safe_p)
	    : left(left_p), right(right_p), null_safe(null_safe_p) {
	}

	ColumnBinding left;
	ColumnBinding right;
	bool null_safe;
};

class BindingEquivalence {
public:
	void Add(ColumnBinding left, ColumnBinding right, bool null_safe = true) {
		if (left != right) {
			edges.emplace_back(left, right, null_safe);
		}
	}

	bool FindMatch(ColumnBinding source, ColumnBinding target, KeyMatch &match) const {
		if (source == target) {
			match = KeyMatch::IDENTICAL;
			return true;
		}
		if (Contains(source, target, true)) {
			match = KeyMatch::NULL_SAFE_EQUALITY;
			return true;
		}
		if (Contains(source, target, false)) {
			match = KeyMatch::EQUALITY;
			return true;
		}
		return false;
	}

private:
	bool Contains(ColumnBinding source, ColumnBinding target, bool require_null_safe) const {
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
				if (require_null_safe && !edge.null_safe) {
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

	vector<EquivalentBinding> edges;
};

struct AnalyzedCandidate {
	AnalyzedCandidate(unique_ptr<LogicalOperator> &source_p, vector<idx_t> key_indices_p,
	                  DuplicateEliminatedDomainCoverage coverage_p, idx_t base_relation_count_p, idx_t depth_p,
	                  idx_t order_p)
	    : source(source_p), key_indices(std::move(key_indices_p)), coverage(coverage_p),
	      base_relation_count(base_relation_count_p), depth(depth_p), order(order_p) {
	}

	reference<unique_ptr<LogicalOperator>> source;
	vector<idx_t> key_indices;
	DuplicateEliminatedDomainCoverage coverage;
	idx_t base_relation_count;
	idx_t depth;
	idx_t order;
};

struct OperatorAnalysis {
	column_binding_set_t source_bindings;
	bool supported_source = false;
	idx_t base_relation_count = 0;
};

static bool GetBinding(const Expression &expr, ColumnBinding &binding) {
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

static bool IsUnprojectedInnerJoin(const LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	return join.join_type == JoinType::INNER && !join.HasProjectionMap();
}

static bool IsEquivalenceCondition(const JoinCondition &condition) {
	if (!condition.IsComparison()) {
		return false;
	}
	auto comparison = condition.GetComparisonType();
	return comparison == ExpressionType::COMPARE_EQUAL || comparison == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
}

static void CollectEquivalences(LogicalOperator &op, BindingEquivalence &equivalence) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		auto output_bindings = projection.GetColumnBindings();
		for (idx_t expression_idx = 0; expression_idx < projection.expressions.size(); expression_idx++) {
			ColumnBinding child_binding;
			if (GetBinding(*projection.expressions[expression_idx], child_binding)) {
				equivalence.Add(output_bindings[expression_idx], child_binding);
			}
		}
	} else if (IsUnprojectedInnerJoin(op)) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &condition : join.conditions) {
			if (!IsEquivalenceCondition(condition)) {
				continue;
			}
			ColumnBinding left;
			ColumnBinding right;
			if (GetBinding(condition.GetLHS(), left) && GetBinding(condition.GetRHS(), right)) {
				equivalence.Add(left, right,
				                condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM);
			}
		}
	}
	for (auto &child : op.children) {
		CollectEquivalences(*child, equivalence);
	}
}

static idx_t MatchRank(KeyMatch match) {
	switch (match) {
	case KeyMatch::IDENTICAL:
		return 0;
	case KeyMatch::NULL_SAFE_EQUALITY:
		return 1;
	case KeyMatch::EQUALITY:
		return 2;
	default:
		throw InternalException("Unknown duplicate-eliminated domain key match");
	}
}

static bool FindCandidateKeys(LogicalOperator &op, const vector<unique_ptr<Expression>> &keys,
                              const BindingEquivalence &equivalence, const column_binding_set_t &source_bindings,
                              vector<idx_t> &candidate_key_indices) {
	auto bindings = op.GetColumnBindings();
	if (bindings.empty() || bindings.size() != op.types.size()) {
		return false;
	}
	candidate_key_indices.clear();
	candidate_key_indices.reserve(keys.size());
	for (auto &key : keys) {
		ColumnBinding key_binding;
		if (!GetBinding(*key, key_binding)) {
			return false;
		}
		optional_idx match;
		KeyMatch best_match = KeyMatch::EQUALITY;
		for (idx_t binding_idx = 0; binding_idx < bindings.size(); binding_idx++) {
			if (op.types[binding_idx] != key->GetReturnType() ||
			    source_bindings.find(bindings[binding_idx]) == source_bindings.end()) {
				continue;
			}
			KeyMatch current_match;
			if (!equivalence.FindMatch(key_binding, bindings[binding_idx], current_match)) {
				continue;
			}
			if (!match.IsValid() || MatchRank(current_match) < MatchRank(best_match)) {
				match = binding_idx;
				best_match = current_match;
				if (current_match == KeyMatch::IDENTICAL) {
					break;
				}
			}
		}
		if (!match.IsValid()) {
			return false;
		}
		candidate_key_indices.push_back(match.GetIndex());
	}
	return true;
}

static bool EffectsPermitFactoring(const LogicalOperator &op) {
	return DuplicateEliminatedDomainSafety::CanFactorSource(op);
}

class CandidateAnalyzer {
public:
	CandidateAnalyzer(const vector<unique_ptr<Expression>> &keys_p, const BindingEquivalence &equivalence_p)
	    : keys(keys_p), equivalence(equivalence_p) {
	}

	vector<AnalyzedCandidate> Analyze(unique_ptr<LogicalOperator> &root) {
		Visit(root, DuplicateEliminatedDomainCoverage::EXACT, true, 0, true);
		return std::move(candidates);
	}

private:
	OperatorAnalysis Visit(unique_ptr<LogicalOperator> &op, DuplicateEliminatedDomainCoverage coverage, bool discover,
	                       idx_t depth, bool is_root = false) {
		vector<OperatorAnalysis> children;
		children.reserve(op->children.size());
		for (idx_t child_idx = 0; child_idx < op->children.size(); child_idx++) {
			auto child_coverage = coverage;
			auto discover_child = false;
			switch (op->type) {
			case LogicalOperatorType::LOGICAL_PROJECTION:
				discover_child = discover;
				break;
			case LogicalOperatorType::LOGICAL_FILTER:
				discover_child = discover;
				child_coverage = DuplicateEliminatedDomainCoverage::SUPERSET;
				break;
			case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
				discover_child = discover && IsUnprojectedInnerJoin(*op);
				if (discover_child) {
					child_coverage = DuplicateEliminatedDomainCoverage::SUPERSET;
				}
				break;
			default:
				break;
			}
			children.push_back(Visit(op->children[child_idx], child_coverage, discover_child, depth + 1));
		}

		OperatorAnalysis result;
		for (auto &child : children) {
			result.base_relation_count += child.base_relation_count;
		}
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_GET: {
			auto &get = op->Cast<LogicalGet>();
			result.supported_source = !get.HasTableInOutInput() && op->children.empty();
			auto bindings = op->GetColumnBindings();
			result.source_bindings.insert(bindings.begin(), bindings.end());
			result.base_relation_count = 1;
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER:
			if (children.size() == 1) {
				result = std::move(children[0]);
			}
			break;
		case LogicalOperatorType::LOGICAL_PROJECTION:
			if (children.size() == 1) {
				auto &projection = op->Cast<LogicalProjection>();
				auto output_bindings = projection.GetColumnBindings();
				for (idx_t expression_idx = 0; expression_idx < projection.expressions.size(); expression_idx++) {
					ColumnBinding child_binding;
					if (GetBinding(*projection.expressions[expression_idx], child_binding) &&
					    children[0].source_bindings.find(child_binding) != children[0].source_bindings.end()) {
						result.source_bindings.insert(output_bindings[expression_idx]);
					}
				}
				result.supported_source = children[0].supported_source;
				result.base_relation_count = children[0].base_relation_count;
			}
			break;
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggregate = op->Cast<LogicalAggregate>();
			auto bindings = aggregate.GetColumnBindings();
			for (idx_t group_idx = 0; group_idx < aggregate.groups.size(); group_idx++) {
				result.source_bindings.insert(bindings[group_idx]);
			}
			result.supported_source = op->children.size() == 1;
			break;
		}
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op->Cast<LogicalComparisonJoin>();
			if (children.size() != 2 || join.HasProjectionMap()) {
				break;
			}
			if (join.join_type == JoinType::INNER) {
				result.source_bindings = children[0].source_bindings;
				result.source_bindings.insert(children[1].source_bindings.begin(), children[1].source_bindings.end());
				result.supported_source = children[0].supported_source && children[1].supported_source;
			} else if (join.join_type == JoinType::SEMI) {
				result.source_bindings = children[0].source_bindings;
				result.supported_source = children[0].supported_source;
			}
			break;
		}
		default:
			break;
		}

		result.supported_source &= EffectsPermitFactoring(*op);
		if (discover && !is_root && result.supported_source) {
			vector<idx_t> key_indices;
			if (FindCandidateKeys(*op, keys, equivalence, result.source_bindings, key_indices)) {
				candidates.emplace_back(op, std::move(key_indices), coverage, result.base_relation_count, depth,
				                        next_order++);
			}
		}
		return result;
	}

private:
	const vector<unique_ptr<Expression>> &keys;
	const BindingEquivalence &equivalence;
	vector<AnalyzedCandidate> candidates;
	idx_t next_order = 0;
};

static optional_idx FindBestCandidate(ClientContext &context, LogicalOperator &payload,
                                      vector<AnalyzedCandidate> &candidates, bool allow_superset) {
	auto payload_estimate = MaxValue<idx_t>(payload.EstimateCardinality(context), 1);
	optional_idx best;
	idx_t best_rows = 0;
	idx_t best_base_relations = 0;
	idx_t best_depth = 0;
	idx_t best_order = 0;
	for (idx_t candidate_idx = 0; candidate_idx < candidates.size(); candidate_idx++) {
		auto &candidate = candidates[candidate_idx];
		if (!allow_superset && candidate.coverage == DuplicateEliminatedDomainCoverage::SUPERSET) {
			continue;
		}
		auto estimate = MaxValue<idx_t>(candidate.source.get()->EstimateCardinality(context), 1);
		if (estimate > payload_estimate ||
		    (estimate == payload_estimate &&
		     !DuplicateEliminatedDomainProperties::HasSelection(*candidate.source.get()))) {
			continue;
		}
		auto better = !best.IsValid() || estimate < best_rows ||
		              (estimate == best_rows && candidate.depth < best_depth) ||
		              (estimate == best_rows && candidate.depth == best_depth &&
		               candidate.base_relation_count < best_base_relations) ||
		              (estimate == best_rows && candidate.depth == best_depth &&
		               candidate.base_relation_count == best_base_relations && candidate.order < best_order);
		if (better) {
			best = candidate_idx;
			best_rows = estimate;
			best_base_relations = candidate.base_relation_count;
			best_depth = candidate.depth;
			best_order = candidate.order;
		}
	}
	return best;
}

optional<DuplicateEliminatedDomainCandidate>
DuplicateEliminatedDomainAnalyzer::FindBest(ClientContext &context, LogicalComparisonJoin &join,
                                            bool can_evaluate_additional_groups) {
	if (join.children.empty() || join.duplicate_eliminated_columns.empty()) {
		return {};
	}
	join.children[0]->ResolveOperatorTypes();
	BindingEquivalence equivalence;
	CollectEquivalences(*join.children[0], equivalence);
	CandidateAnalyzer analyzer(join.duplicate_eliminated_columns, equivalence);
	auto candidates = analyzer.Analyze(join.children[0]);
	auto selected_index = FindBestCandidate(context, *join.children[0], candidates, can_evaluate_additional_groups);
	if (!selected_index.IsValid()) {
		return {};
	}
	auto &selected = candidates[selected_index.GetIndex()];
	D_ASSERT(can_evaluate_additional_groups || selected.coverage == DuplicateEliminatedDomainCoverage::EXACT);
	return DuplicateEliminatedDomainCandidate(selected.source.get(), std::move(selected.key_indices),
	                                          selected.coverage);
}

} // namespace duckdb
