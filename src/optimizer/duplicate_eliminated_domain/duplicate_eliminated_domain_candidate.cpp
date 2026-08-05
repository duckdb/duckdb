//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.hpp"

#include "duckdb/optimizer/cte_inlining.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_cte_registry.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_extractor.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>

namespace duckdb {

enum class DuplicateEliminatedDomainCoverage : uint8_t { EXACT, SUPERSET };
enum class KeyMatch : uint8_t { IDENTICAL, NULL_SAFE_EQUALITY, EQUALITY };

struct EquivalentBinding {
public:
	EquivalentBinding(ColumnBinding left_p, ColumnBinding right_p, bool null_safe_p)
	    : left(left_p), right(right_p), null_safe(null_safe_p) {
	}

public:
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

private:
	vector<EquivalentBinding> edges;
};

struct AnalyzedCandidate {
public:
	AnalyzedCandidate(unique_ptr<LogicalOperator> &source_p, vector<idx_t> source_path_p, vector<idx_t> key_indices_p,
	                  DuplicateEliminatedDomainCoverage coverage_p, idx_t base_relation_count_p, idx_t depth_p,
	                  idx_t order_p)
	    : source(source_p), source_path(std::move(source_path_p)), key_indices(std::move(key_indices_p)),
	      coverage(coverage_p), base_relation_count(base_relation_count_p), depth(depth_p), order(order_p) {
	}

public:
	reference<unique_ptr<LogicalOperator>> source;
	vector<idx_t> source_path;
	vector<idx_t> key_indices;
	DuplicateEliminatedDomainCoverage coverage;
	idx_t base_relation_count;
	idx_t depth;
	idx_t order;
	idx_t source_cardinality = 0;
	idx_t domain_cardinality = 0;
};

struct OperatorAnalysis {
public:
	column_binding_set_t source_bindings;
	bool supported_source = false;
	idx_t base_relation_count = 0;
};

struct CandidateTraversal {
public:
	DuplicateEliminatedDomainCoverage coverage;
	bool discover;
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

static bool IsInnerJoin(const LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	return join.join_type == JoinType::INNER;
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
	} else if (IsInnerJoin(op)) {
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

static bool CanAddCTEReference(ClientContext &context, const DuplicateEliminatedDomainCTERegistry &cte_registry,
                               LogicalCTERef &cte_ref) {
	if (cte_ref.is_recurring) {
		return false;
	}
	auto definition = cte_registry.FindDefinition(cte_ref.cte_index);
	if (!definition) {
		return false;
	}
	if (cte_registry.IsAlwaysMaterialized(cte_ref.cte_index)) {
		return true;
	}
	if (!cte_registry.IsNeverMaterialized(cte_ref.cte_index) && !definition->HasVolatileExpressions() &&
	    CTEInlining::EndsInAggregateOrDistinct(*definition)) {
		// A multiply referenced default CTE ending in an aggregate, DISTINCT, or window remains shared.
		return true;
	}
	return DuplicateEliminatedDomainSafety::CanDuplicateSource(context, *definition);
}

class CandidateAnalyzer {
public:
	CandidateAnalyzer(ClientContext &context_p, const DuplicateEliminatedDomainCTERegistry &cte_registry_p,
	                  const vector<unique_ptr<Expression>> &keys_p, const BindingEquivalence &equivalence_p,
	                  bool allow_cte_candidates_p)
	    : context(context_p), cte_registry(cte_registry_p), keys(keys_p), equivalence(equivalence_p),
	      allow_cte_candidates(allow_cte_candidates_p) {
	}

public:
	vector<AnalyzedCandidate> Analyze(unique_ptr<LogicalOperator> &root) {
		vector<idx_t> path;
		Visit(root, DuplicateEliminatedDomainCoverage::EXACT, true, 0, path, true);
		return std::move(candidates);
	}

private:
	OperatorAnalysis Visit(unique_ptr<LogicalOperator> &op, DuplicateEliminatedDomainCoverage coverage, bool discover,
	                       idx_t depth, vector<idx_t> &path, bool is_root = false) {
		auto can_factor_operator = DuplicateEliminatedDomainSafety::CanFactorOperator(context, *op);
		auto children = AnalyzeChildren(op, coverage, discover, can_factor_operator, depth, path);

		OperatorAnalysis result;
		for (auto &child : children) {
			result.base_relation_count += child.base_relation_count;
		}
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_GET: {
			result.supported_source = can_factor_operator;
			auto bindings = op->GetColumnBindings();
			result.source_bindings.insert(bindings.begin(), bindings.end());
			result.base_relation_count = 1;
			break;
		}
		case LogicalOperatorType::LOGICAL_CTE_REF: {
			auto &cte_ref = op->Cast<LogicalCTERef>();
			result.supported_source =
			    allow_cte_candidates && can_factor_operator && CanAddCTEReference(context, cte_registry, cte_ref);
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
			AnalyzeProjection(op->Cast<LogicalProjection>(), children, result);
			break;
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggregate = op->Cast<LogicalAggregate>();
			auto bindings = aggregate.GetColumnBindings();
			for (idx_t group_idx = 0; group_idx < aggregate.groups.size(); group_idx++) {
				result.source_bindings.insert(bindings[group_idx]);
			}
			result.supported_source = children.size() == 1 && children[0].supported_source;
			break;
		}
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			AnalyzeComparisonJoin(op->Cast<LogicalComparisonJoin>(), children, result);
			break;
		}
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
			if (children.size() == 2) {
				result.source_bindings = children[0].source_bindings;
				result.source_bindings.insert(children[1].source_bindings.begin(), children[1].source_bindings.end());
				result.supported_source = children[0].supported_source && children[1].supported_source;
			}
			break;
		default:
			break;
		}

		result.supported_source &= can_factor_operator;
		if (discover && !is_root && result.supported_source) {
			vector<idx_t> key_indices;
			if (FindCandidateKeys(*op, keys, equivalence, result.source_bindings, key_indices)) {
				candidates.emplace_back(op, path, std::move(key_indices), coverage, result.base_relation_count, depth,
				                        next_order++);
			}
		}
		return result;
	}

	static CandidateTraversal GetChildTraversal(const LogicalOperator &op, idx_t child_idx,
	                                            DuplicateEliminatedDomainCoverage coverage, bool discover,
	                                            bool can_factor_operator) {
		CandidateTraversal result {coverage, false};
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION:
			result.discover = discover && can_factor_operator;
			break;
		case LogicalOperatorType::LOGICAL_FILTER:
			result.discover = discover && can_factor_operator;
			result.coverage = DuplicateEliminatedDomainCoverage::SUPERSET;
			break;
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op.Cast<LogicalComparisonJoin>();
			result.discover = discover && can_factor_operator &&
			                  (join.join_type == JoinType::INNER ||
			                   (join.join_type == JoinType::SEMI && !join.HasProjectionMap() && child_idx == 0));
			if (result.discover) {
				result.coverage = DuplicateEliminatedDomainCoverage::SUPERSET;
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
			result.discover = discover && can_factor_operator;
			if (result.discover) {
				result.coverage = DuplicateEliminatedDomainCoverage::SUPERSET;
			}
			break;
		default:
			break;
		}
		return result;
	}

	vector<OperatorAnalysis> AnalyzeChildren(unique_ptr<LogicalOperator> &op,
	                                         DuplicateEliminatedDomainCoverage coverage, bool discover,
	                                         bool can_factor_operator, idx_t depth, vector<idx_t> &path) {
		vector<OperatorAnalysis> result;
		result.reserve(op->children.size());
		for (idx_t child_idx = 0; child_idx < op->children.size(); child_idx++) {
			auto traversal = GetChildTraversal(*op, child_idx, coverage, discover, can_factor_operator);
			path.push_back(child_idx);
			result.push_back(Visit(op->children[child_idx], traversal.coverage, traversal.discover, depth + 1, path));
			path.pop_back();
		}
		return result;
	}

	static void AnalyzeProjection(LogicalProjection &projection, const vector<OperatorAnalysis> &children,
	                              OperatorAnalysis &result) {
		if (children.size() != 1) {
			return;
		}
		auto &child = children[0];
		auto output_bindings = projection.GetColumnBindings();
		for (idx_t expression_idx = 0; expression_idx < projection.expressions.size(); expression_idx++) {
			ColumnBinding child_binding;
			if (GetBinding(*projection.expressions[expression_idx], child_binding) &&
			    child.source_bindings.find(child_binding) != child.source_bindings.end()) {
				result.source_bindings.insert(output_bindings[expression_idx]);
			}
		}
		result.supported_source = child.supported_source;
		result.base_relation_count = child.base_relation_count;
	}

	static void AnalyzeComparisonJoin(LogicalComparisonJoin &join, const vector<OperatorAnalysis> &children,
	                                  OperatorAnalysis &result) {
		if (children.size() != 2) {
			return;
		}
		if (join.join_type == JoinType::INNER) {
			result.source_bindings = children[0].source_bindings;
			result.source_bindings.insert(children[1].source_bindings.begin(), children[1].source_bindings.end());
			if (join.HasProjectionMap()) {
				column_binding_set_t output_bindings;
				for (auto &binding : join.GetColumnBindings()) {
					output_bindings.insert(binding);
				}
				for (auto entry = result.source_bindings.begin(); entry != result.source_bindings.end();) {
					if (output_bindings.find(*entry) == output_bindings.end()) {
						entry = result.source_bindings.erase(entry);
					} else {
						entry++;
					}
				}
			}
			result.supported_source = children[0].supported_source && children[1].supported_source;
		} else if (join.join_type == JoinType::SEMI && !join.HasProjectionMap()) {
			result.source_bindings = children[0].source_bindings;
			result.supported_source = children[0].supported_source && children[1].supported_source;
		}
	}

private:
	ClientContext &context;
	const DuplicateEliminatedDomainCTERegistry &cte_registry;
	const vector<unique_ptr<Expression>> &keys;
	const BindingEquivalence &equivalence;
	bool allow_cte_candidates;
	vector<AnalyzedCandidate> candidates;
	idx_t next_order = 0;
};

static optional_idx TryEstimateDomainCardinality(RelationStatsExtractor &stats_extractor, LogicalOperator &op,
                                                 const vector<idx_t> &key_indices,
                                                 bool require_reliable_payload_stats) {
	auto stats = stats_extractor.Extract(op);
	if (!stats || stats->columns.size() != op.GetColumnBindings().size()) {
		return {};
	}
	vector<DistinctCount> key_distinct_counts;
	key_distinct_counts.reserve(key_indices.size());
	for (auto key_idx : key_indices) {
		if (key_idx >= stats->columns.size()) {
			return {};
		}
		auto distinct_count = stats->columns[key_idx].distinct_count;
		if (require_reliable_payload_stats && distinct_count.source == DistinctCountSource::CARDINALITY) {
			return {};
		}
		distinct_count.distinct_count = MinValue(distinct_count.distinct_count, stats->cardinality);
		key_distinct_counts.push_back(distinct_count);
	}
	return RelationStatisticsHelper::EstimateDistinctCardinality(key_distinct_counts, stats->cardinality);
}

static idx_t DistinctCountRank(DistinctCountSource source) {
	switch (source) {
	case DistinctCountSource::EXACT:
		return 0;
	case DistinctCountSource::HLL:
		return 1;
	case DistinctCountSource::MIN_MAX:
		return 2;
	case DistinctCountSource::CARDINALITY:
		return 3;
	default:
		throw InternalException("Unknown distinct-count source");
	}
}

static bool FindPayloadKeyIndices(LogicalOperator &op, const vector<unique_ptr<Expression>> &keys,
                                  const BindingEquivalence &equivalence, const RelationStats &stats,
                                  vector<idx_t> &key_indices) {
	auto bindings = op.GetColumnBindings();
	if (bindings.size() != stats.columns.size()) {
		return false;
	}
	key_indices.clear();
	key_indices.reserve(keys.size());
	for (auto &key : keys) {
		ColumnBinding key_binding;
		if (!GetBinding(*key, key_binding)) {
			return false;
		}
		optional_idx best;
		idx_t best_distinct_rank = 0;
		idx_t best_match_rank = 0;
		for (idx_t binding_idx = 0; binding_idx < bindings.size(); binding_idx++) {
			if (op.types[binding_idx] != key->GetReturnType()) {
				continue;
			}
			KeyMatch match;
			if (!equivalence.FindMatch(key_binding, bindings[binding_idx], match)) {
				continue;
			}
			auto distinct_rank = DistinctCountRank(stats.columns[binding_idx].distinct_count.source);
			auto match_rank = MatchRank(match);
			if (!best.IsValid() || distinct_rank < best_distinct_rank ||
			    (distinct_rank == best_distinct_rank && match_rank < best_match_rank)) {
				best = binding_idx;
				best_distinct_rank = distinct_rank;
				best_match_rank = match_rank;
			}
		}
		if (!best.IsValid()) {
			return false;
		}
		key_indices.push_back(best.GetIndex());
	}
	return true;
}

struct CTEKeySource {
public:
	TableIndex cte_index;
	vector<idx_t> column_indices;
};

static optional<CTEKeySource> TraceBindingsToCTE(LogicalOperator &op, vector<ColumnBinding> bindings) {
	reference<LogicalOperator> current = op;
	while (true) {
		if (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = current.get().Cast<LogicalProjection>();
			if (projection.children.size() != 1) {
				return {};
			}
			auto output_bindings = projection.GetColumnBindings();
			for (auto &binding : bindings) {
				auto entry = std::find(output_bindings.begin(), output_bindings.end(), binding);
				if (entry == output_bindings.end()) {
					return {};
				}
				auto expression_idx = NumericCast<idx_t>(entry - output_bindings.begin());
				if (!GetBinding(*projection.expressions[expression_idx], binding)) {
					return {};
				}
			}
			current = *projection.children[0];
			continue;
		}
		if (current.get().type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT || current.get().children.size() != 2) {
			break;
		}
		optional_idx source_child;
		for (auto &binding : bindings) {
			optional_idx binding_child;
			for (idx_t child_idx = 0; child_idx < current.get().children.size(); child_idx++) {
				auto child_bindings = current.get().children[child_idx]->GetColumnBindings();
				if (std::find(child_bindings.begin(), child_bindings.end(), binding) != child_bindings.end()) {
					if (binding_child.IsValid()) {
						return {};
					}
					binding_child = child_idx;
				}
			}
			if (!binding_child.IsValid()) {
				return {};
			}
			if (source_child.IsValid() && source_child != binding_child) {
				return {};
			}
			source_child = binding_child;
		}
		if (!source_child.IsValid()) {
			return {};
		}
		current = *current.get().children[source_child.GetIndex()];
	}
	if (current.get().type != LogicalOperatorType::LOGICAL_CTE_REF) {
		return {};
	}
	auto &cte_ref = current.get().Cast<LogicalCTERef>();
	vector<idx_t> column_indices;
	column_indices.reserve(bindings.size());
	for (auto &binding : bindings) {
		if (binding.table_index != cte_ref.table_index || binding.column_index >= cte_ref.chunk_types.size()) {
			return {};
		}
		column_indices.push_back(binding.column_index);
	}
	return CTEKeySource {cte_ref.cte_index, std::move(column_indices)};
}

struct DomainKeyMapping {
public:
	idx_t domain_index;
	ColumnBinding retained_binding;
};

class EquivalentSourceDomainAnalyzer {
public:
	EquivalentSourceDomainAnalyzer(CTEKeySource payload_source_p, TableIndex domain_cte_index_p)
	    : payload_source(std::move(payload_source_p)), domain_cte_index(domain_cte_index_p) {
	}

public:
	bool Analyze(LogicalOperator &rhs) {
		return Analyze(rhs, false) && found_grouped_restriction;
	}

private:
	optional_ptr<LogicalCTERef> GetGeneratedRef(LogicalOperator &op) const {
		if (op.type != LogicalOperatorType::LOGICAL_CTE_REF) {
			return nullptr;
		}
		auto &cte_ref = op.Cast<LogicalCTERef>();
		return cte_ref.cte_index == domain_cte_index ? optional_ptr<LogicalCTERef>(cte_ref) : nullptr;
	}

	bool ValidateMappings(LogicalOperator &retained, LogicalCTERef &domain_ref,
	                      const vector<DomainKeyMapping> &mappings) const {
		if (domain_ref.chunk_types.size() != payload_source.column_indices.size() ||
		    mappings.size() != payload_source.column_indices.size()) {
			return false;
		}
		vector<ColumnBinding> retained_bindings(mappings.size());
		vector<bool> found(mappings.size(), false);
		for (auto &mapping : mappings) {
			if (mapping.domain_index >= found.size() || found[mapping.domain_index]) {
				return false;
			}
			found[mapping.domain_index] = true;
			retained_bindings[mapping.domain_index] = mapping.retained_binding;
		}
		auto retained_source = TraceBindingsToCTE(retained, std::move(retained_bindings));
		return retained_source && retained_source->cte_index == payload_source.cte_index &&
		       retained_source->column_indices == payload_source.column_indices;
	}

	bool AnalyzeComparisonJoin(LogicalComparisonJoin &join, bool under_grouping_operator) {
		if (join.children.size() != 2) {
			return false;
		}
		auto left_domain = GetGeneratedRef(*join.children[0]);
		auto right_domain = GetGeneratedRef(*join.children[1]);
		if (bool(left_domain) == bool(right_domain)) {
			return false;
		}
		auto domain_idx = left_domain ? 0 : 1;
		auto &domain_ref = left_domain ? *left_domain : *right_domain;
		auto &retained = *join.children[1 - domain_idx];
		if (under_grouping_operator) {
			vector<DomainKeyMapping> mappings;
			mappings.reserve(join.conditions.size());
			for (auto &condition : join.conditions) {
				if (!condition.IsComparison() ||
				    (condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL &&
				     condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM)) {
					return false;
				}
				ColumnBinding left_binding;
				ColumnBinding right_binding;
				if (!GetBinding(condition.GetLHS(), left_binding) || !GetBinding(condition.GetRHS(), right_binding)) {
					return false;
				}
				auto left_generated = left_binding.table_index == domain_ref.table_index;
				auto right_generated = right_binding.table_index == domain_ref.table_index;
				if (left_generated == right_generated) {
					return false;
				}
				auto &generated = left_generated ? left_binding : right_binding;
				mappings.push_back({generated.column_index, left_generated ? right_binding : left_binding});
			}
			if (!ValidateMappings(retained, domain_ref, mappings)) {
				return false;
			}
			found_grouped_restriction = true;
		}
		return Analyze(retained, under_grouping_operator);
	}

	bool AnalyzeFilterCrossProduct(LogicalFilter &filter, bool under_grouping_operator) {
		if (filter.HasProjectionMap() || filter.children.size() != 1 ||
		    filter.children[0]->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT ||
		    filter.children[0]->children.size() != 2) {
			return false;
		}
		auto &cross_product = *filter.children[0];
		auto left_domain = GetGeneratedRef(*cross_product.children[0]);
		auto right_domain = GetGeneratedRef(*cross_product.children[1]);
		if (bool(left_domain) == bool(right_domain)) {
			return false;
		}
		auto domain_idx = left_domain ? 0 : 1;
		auto &domain_ref = left_domain ? *left_domain : *right_domain;
		auto &retained = *cross_product.children[1 - domain_idx];
		if (under_grouping_operator) {
			vector<DomainKeyMapping> mappings;
			mappings.reserve(filter.expressions.size());
			for (auto &expression : filter.expressions) {
				if (!BoundComparisonExpression::IsComparison(*expression) ||
				    (expression->GetExpressionType() != ExpressionType::COMPARE_EQUAL &&
				     expression->GetExpressionType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM)) {
					return false;
				}
				auto &comparison = expression->Cast<BoundFunctionExpression>();
				ColumnBinding left_binding;
				ColumnBinding right_binding;
				if (!GetBinding(BoundComparisonExpression::Left(comparison), left_binding) ||
				    !GetBinding(BoundComparisonExpression::Right(comparison), right_binding)) {
					return false;
				}
				auto left_generated = left_binding.table_index == domain_ref.table_index;
				auto right_generated = right_binding.table_index == domain_ref.table_index;
				if (left_generated == right_generated) {
					return false;
				}
				auto &generated = left_generated ? left_binding : right_binding;
				mappings.push_back({generated.column_index, left_generated ? right_binding : left_binding});
			}
			if (!ValidateMappings(retained, domain_ref, mappings)) {
				return false;
			}
			found_grouped_restriction = true;
		}
		return Analyze(retained, under_grouping_operator);
	}

	bool Analyze(LogicalOperator &op, bool under_grouping_operator) {
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN && op.children.size() == 2 &&
		    (GetGeneratedRef(*op.children[0]) || GetGeneratedRef(*op.children[1]))) {
			return AnalyzeComparisonJoin(op.Cast<LogicalComparisonJoin>(), under_grouping_operator);
		}
		if (op.type == LogicalOperatorType::LOGICAL_FILTER && op.children.size() == 1 &&
		    op.children[0]->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT &&
		    op.children[0]->children.size() == 2 &&
		    (GetGeneratedRef(*op.children[0]->children[0]) || GetGeneratedRef(*op.children[0]->children[1]))) {
			return AnalyzeFilterCrossProduct(op.Cast<LogicalFilter>(), under_grouping_operator);
		}
		if (GetGeneratedRef(op)) {
			return false;
		}
		auto child_under_grouping =
		    under_grouping_operator || op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY;
		for (auto &child : op.children) {
			if (!Analyze(*child, child_under_grouping)) {
				return false;
			}
		}
		return true;
	}

private:
	CTEKeySource payload_source;
	TableIndex domain_cte_index;
	bool found_grouped_restriction = false;
};

bool DuplicateEliminatedDomainAnalyzer::CanEliminateEquivalentSourceDomain(LogicalComparisonJoin &join,
                                                                           LogicalOperator &rhs,
                                                                           TableIndex domain_cte_index) {
	if (join.children.empty() || join.duplicate_eliminated_columns.empty()) {
		return false;
	}
	vector<ColumnBinding> payload_key_bindings;
	payload_key_bindings.reserve(join.duplicate_eliminated_columns.size());
	for (auto &key : join.duplicate_eliminated_columns) {
		ColumnBinding binding;
		if (!GetBinding(*key, binding)) {
			return false;
		}
		payload_key_bindings.push_back(binding);
	}
	auto payload_source = TraceBindingsToCTE(*join.children[0], std::move(payload_key_bindings));
	if (!payload_source) {
		return false;
	}
	EquivalentSourceDomainAnalyzer analyzer(std::move(*payload_source), domain_cte_index);
	return analyzer.Analyze(rhs);
}

struct CandidateScore {
public:
	bool IsBetterThan(const CandidateScore &other) const {
		if (source_cardinality != other.source_cardinality) {
			return source_cardinality < other.source_cardinality;
		}
		if (domain_cardinality != other.domain_cardinality) {
			return domain_cardinality < other.domain_cardinality;
		}
		if (base_relation_count != other.base_relation_count) {
			return base_relation_count < other.base_relation_count;
		}
		if (depth != other.depth) {
			return depth < other.depth;
		}
		return order < other.order;
	}

public:
	idx_t source_cardinality;
	idx_t domain_cardinality;
	idx_t base_relation_count;
	idx_t depth;
	idx_t order;
};

static optional<CandidateScore> TryScoreCandidate(RelationStatsExtractor &stats_extractor,
                                                  AnalyzedCandidate &candidate) {
	auto source_stats = stats_extractor.Extract(*candidate.source.get());
	auto domain_estimate =
	    TryEstimateDomainCardinality(stats_extractor, *candidate.source.get(), candidate.key_indices, false);
	if (!source_stats || !source_stats->stats_initialized || !domain_estimate.IsValid()) {
		return {};
	}
	return CandidateScore {MaxValue<idx_t>(source_stats->cardinality, 1),
	                       MaxValue<idx_t>(domain_estimate.GetIndex(), 1), candidate.base_relation_count,
	                       candidate.depth, candidate.order};
}

static optional_idx FindBestCandidate(RelationStatsExtractor &stats_extractor, LogicalOperator &payload,
                                      const vector<unique_ptr<Expression>> &keys, const BindingEquivalence &equivalence,
                                      vector<AnalyzedCandidate> &candidates, bool allow_superset,
                                      idx_t &payload_cardinality, idx_t &payload_domain_cardinality) {
	auto payload_stats = stats_extractor.Extract(payload);
	if (!payload_stats || !payload_stats->stats_initialized) {
		return {};
	}
	vector<idx_t> payload_key_indices;
	if (!FindPayloadKeyIndices(payload, keys, equivalence, *payload_stats, payload_key_indices)) {
		return {};
	}
	auto payload_domain = TryEstimateDomainCardinality(stats_extractor, payload, payload_key_indices, true);
	if (!payload_domain.IsValid()) {
		return {};
	}
	payload_cardinality = MaxValue<idx_t>(payload_stats->cardinality, 1);
	payload_domain_cardinality = MaxValue<idx_t>(payload_domain.GetIndex(), 1);
	optional_idx best;
	optional<CandidateScore> best_score;
	for (idx_t candidate_idx = 0; candidate_idx < candidates.size(); candidate_idx++) {
		auto &candidate = candidates[candidate_idx];
		if (!allow_superset && candidate.coverage == DuplicateEliminatedDomainCoverage::SUPERSET) {
			continue;
		}
		auto score = TryScoreCandidate(stats_extractor, candidate);
		if (!score) {
			continue;
		}
		if (score->source_cardinality >= payload_cardinality ||
		    score->domain_cardinality > payload_domain_cardinality) {
			continue;
		}
		if (best_score && !score->IsBetterThan(*best_score)) {
			continue;
		}
		best = candidate_idx;
		best_score = *score;
		candidate.source_cardinality = score->source_cardinality;
		candidate.domain_cardinality = score->domain_cardinality;
	}
	return best;
}

optional<DuplicateEliminatedDomainCandidate>
DuplicateEliminatedDomainAnalyzer::FindBest(ClientContext &context,
                                            const DuplicateEliminatedDomainCTERegistry &cte_registry,
                                            LogicalComparisonJoin &join, bool can_evaluate_additional_groups) {
	if (join.children.empty() || join.duplicate_eliminated_columns.empty()) {
		return {};
	}
	join.children[0]->ResolveOperatorTypes();
	BindingEquivalence equivalence;
	CollectEquivalences(*join.children[0], equivalence);
	// Reusing a CTE as a MARK domain removes the generated pair domain and lets the ordinary MARK-to-SEMI rewrite
	// fire. Other join types can evaluate many additional groups without that compensating topology improvement.
	CandidateAnalyzer analyzer(context, cte_registry, join.duplicate_eliminated_columns, equivalence,
	                           join.join_type == JoinType::MARK);
	auto candidates = analyzer.Analyze(join.children[0]);
	RelationStatsExtractor stats_extractor(
	    context, [&](TableIndex cte_index) { return cte_registry.FindDefinition(cte_index); });
	idx_t payload_cardinality;
	idx_t payload_domain_cardinality;
	auto selected_index =
	    FindBestCandidate(stats_extractor, *join.children[0], join.duplicate_eliminated_columns, equivalence,
	                      candidates, can_evaluate_additional_groups, payload_cardinality, payload_domain_cardinality);
	if (!selected_index.IsValid()) {
		return {};
	}
	auto &selected = candidates[selected_index.GetIndex()];
	D_ASSERT(can_evaluate_additional_groups || selected.coverage == DuplicateEliminatedDomainCoverage::EXACT);
	return DuplicateEliminatedDomainCandidate(
	    *selected.source.get(), std::move(selected.source_path), std::move(selected.key_indices),
	    selected.source_cardinality, selected.domain_cardinality, payload_cardinality, payload_domain_cardinality);
}

optional_ptr<unique_ptr<LogicalOperator>>
DuplicateEliminatedDomainCandidate::TryResolveSource(unique_ptr<LogicalOperator> &payload) const {
	reference<unique_ptr<LogicalOperator>> source = payload;
	for (auto child_idx : source_path) {
		if (child_idx >= source.get()->children.size()) {
			return nullptr;
		}
		source = source.get()->children[child_idx];
	}
	source.get()->ResolveOperatorTypes();
	if (source.get()->type != source_type || source.get()->types != source_types ||
	    source.get()->GetColumnBindings() != source_bindings) {
		return nullptr;
	}
	for (auto key_idx : key_indices) {
		if (key_idx >= source_types.size()) {
			return nullptr;
		}
	}
	return source.get();
}

} // namespace duckdb
