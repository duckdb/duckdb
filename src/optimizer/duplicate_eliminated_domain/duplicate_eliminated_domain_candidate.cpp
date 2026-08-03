//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_candidate.hpp"

#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_safety.hpp"
#include "duckdb/optimizer/duplicate_eliminated_domain/duplicate_eliminated_domain_cte_registry.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/common/operator/multiply.hpp"
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
	AnalyzedCandidate(unique_ptr<LogicalOperator> &source_p, vector<idx_t> source_path_p, vector<idx_t> key_indices_p,
	                  DuplicateEliminatedDomainCoverage coverage_p, idx_t base_relation_count_p, idx_t depth_p,
	                  idx_t order_p)
	    : source(source_p), source_path(std::move(source_path_p)), key_indices(std::move(key_indices_p)),
	      coverage(coverage_p), base_relation_count(base_relation_count_p), depth(depth_p), order(order_p) {
	}

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

class CandidateAnalyzer {
public:
	CandidateAnalyzer(ClientContext &context_p, const vector<unique_ptr<Expression>> &keys_p,
	                  const BindingEquivalence &equivalence_p)
	    : context(context_p), keys(keys_p), equivalence(equivalence_p) {
	}

	vector<AnalyzedCandidate> Analyze(unique_ptr<LogicalOperator> &root) {
		vector<idx_t> path;
		Visit(root, DuplicateEliminatedDomainCoverage::EXACT, true, 0, path, true);
		return std::move(candidates);
	}

private:
	OperatorAnalysis Visit(unique_ptr<LogicalOperator> &op, DuplicateEliminatedDomainCoverage coverage, bool discover,
	                       idx_t depth, vector<idx_t> &path, bool is_root = false) {
		auto can_factor_operator = DuplicateEliminatedDomainSafety::CanFactorOperator(context, *op);
		vector<OperatorAnalysis> children;
		children.reserve(op->children.size());
		for (idx_t child_idx = 0; child_idx < op->children.size(); child_idx++) {
			auto child_coverage = coverage;
			auto discover_child = false;
			switch (op->type) {
			case LogicalOperatorType::LOGICAL_PROJECTION:
				discover_child = discover && can_factor_operator;
				break;
			case LogicalOperatorType::LOGICAL_FILTER:
				discover_child = discover && can_factor_operator;
				child_coverage = DuplicateEliminatedDomainCoverage::SUPERSET;
				break;
			case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
				if (IsInnerJoin(*op)) {
					discover_child = discover && can_factor_operator;
				} else {
					auto &join = op->Cast<LogicalComparisonJoin>();
					discover_child = discover && can_factor_operator && join.join_type == JoinType::SEMI &&
					                 !join.HasProjectionMap() && child_idx == 0;
				}
				if (discover_child) {
					child_coverage = DuplicateEliminatedDomainCoverage::SUPERSET;
				}
				break;
			default:
				break;
			}
			path.push_back(child_idx);
			children.push_back(Visit(op->children[child_idx], child_coverage, discover_child, depth + 1, path));
			path.pop_back();
		}

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
			result.supported_source = children.size() == 1 && children[0].supported_source;
			break;
		}
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
			auto &join = op->Cast<LogicalComparisonJoin>();
			if (children.size() != 2) {
				break;
			}
			if (join.join_type == JoinType::INNER) {
				result.source_bindings = children[0].source_bindings;
				result.source_bindings.insert(children[1].source_bindings.begin(), children[1].source_bindings.end());
				if (join.HasProjectionMap()) {
					column_binding_set_t output_bindings;
					for (auto &binding : op->GetColumnBindings()) {
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
			break;
		}
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

private:
	ClientContext &context;
	const vector<unique_ptr<Expression>> &keys;
	const BindingEquivalence &equivalence;
	vector<AnalyzedCandidate> candidates;
	idx_t next_order = 0;
};

class RelationStatsExtractor {
public:
	RelationStatsExtractor(ClientContext &context_p, const DuplicateEliminatedDomainCTERegistry &cte_registry_p)
	    : context(context_p), cte_registry(cte_registry_p) {
	}

	optional<RelationStats> Extract(LogicalOperator &op) {
		unordered_set<TableIndex> visiting_ctes;
		return ExtractInternal(op, visiting_ctes);
	}

private:
	optional<RelationStats> ExtractCTERef(LogicalCTERef &cte_ref, unordered_set<TableIndex> &visiting_ctes) {
		if (cte_ref.is_recurring) {
			return {};
		}
		auto cached = cte_stats.find(cte_ref.cte_index);
		if (cached != cte_stats.end()) {
			return cached->second;
		}
		auto definition = cte_registry.FindDefinition(cte_ref.cte_index);
		if (!definition || !visiting_ctes.insert(cte_ref.cte_index).second) {
			return {};
		}
		auto result = ExtractInternal(*definition, visiting_ctes);
		visiting_ctes.erase(cte_ref.cte_index);
		if (!result || result->column_distinct_count.size() < cte_ref.chunk_types.size()) {
			return {};
		}
		result->column_distinct_count.erase(result->column_distinct_count.begin() + cte_ref.chunk_types.size(),
		                                    result->column_distinct_count.end());
		if (result->column_names.size() < cte_ref.chunk_types.size()) {
			result->column_names.resize(cte_ref.chunk_types.size(), Identifier("duplicate_eliminated_cte"));
		} else {
			result->column_names.erase(result->column_names.begin() + cte_ref.chunk_types.size(),
			                           result->column_names.end());
		}
		cte_stats.emplace(cte_ref.cte_index, *result);
		return result;
	}

	optional<RelationStats> ExtractInternal(LogicalOperator &op, unordered_set<TableIndex> &visiting_ctes) {
		switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		return RelationStatisticsHelper::ExtractGetStats(op.Cast<LogicalGet>(), context, false);
	case LogicalOperatorType::LOGICAL_CTE_REF:
		return ExtractCTERef(op.Cast<LogicalCTERef>(), visiting_ctes);
	case LogicalOperatorType::LOGICAL_FILTER: {
		if (op.children.size() != 1) {
			return {};
		}
		return ExtractInternal(*op.children[0], visiting_ctes);
	}
		case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (op.children.size() != 1) {
			return {};
		}
			auto child = ExtractInternal(*op.children[0], visiting_ctes);
			if (!child) {
				return {};
		}
		return RelationStatisticsHelper::ExtractProjectionStats(op.Cast<LogicalProjection>(), *child);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		if (op.children.size() != 1) {
			return {};
		}
			auto child = ExtractInternal(*op.children[0], visiting_ctes);
			if (!child) {
				return {};
		}
		return RelationStatisticsHelper::ExtractAggregationStats(op.Cast<LogicalAggregate>(), *child);
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (op.children.size() != 2 || (join.join_type != JoinType::INNER && join.join_type != JoinType::SEMI)) {
			return {};
		}
			auto left = ExtractInternal(*op.children[0], visiting_ctes);
			auto right = ExtractInternal(*op.children[1], visiting_ctes);
			if (!left || !right) {
				return {};
		}
		RelationStats result;
		result.cardinality = join.join_type == JoinType::SEMI
		                         ? left->cardinality
		                         : MaxValue<idx_t>(left->cardinality, right->cardinality);
		auto output_bindings = op.GetColumnBindings();
		auto left_bindings = op.children[0]->GetColumnBindings();
		auto right_bindings = op.children[1]->GetColumnBindings();
		for (auto &output_binding : output_bindings) {
			optional<DistinctCount> distinct_count;
			for (idx_t binding_idx = 0; binding_idx < left_bindings.size(); binding_idx++) {
				if (left_bindings[binding_idx] == output_binding && binding_idx < left->column_distinct_count.size()) {
					distinct_count = left->column_distinct_count[binding_idx];
					break;
				}
			}
			if (!distinct_count) {
				for (idx_t binding_idx = 0; binding_idx < right_bindings.size(); binding_idx++) {
					if (right_bindings[binding_idx] == output_binding &&
					    binding_idx < right->column_distinct_count.size()) {
						distinct_count = right->column_distinct_count[binding_idx];
						break;
					}
				}
			}
			if (!distinct_count) {
				return {};
			}
			distinct_count->distinct_count = MinValue(distinct_count->distinct_count, result.cardinality);
			result.column_distinct_count.push_back(*distinct_count);
			result.column_names.emplace_back("duplicate_eliminated_domain");
		}
			result.stats_initialized = true;
			return result;
		}
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
			if (op.children.size() != 2) {
				return {};
			}
			auto left = ExtractInternal(*op.children[0], visiting_ctes);
			auto right = ExtractInternal(*op.children[1], visiting_ctes);
			if (!left || !right) {
				return {};
			}
			auto result = RelationStatisticsHelper::CombineStatsOfNonReorderableOperator(op, {*left, *right});
			if (!TryMultiplyOperator::Operation(left->cardinality, right->cardinality, result.cardinality)) {
				result.cardinality = NumericLimits<idx_t>::Maximum();
			}
			return result;
		}
		default:
			return {};
	}
	}

private:
	ClientContext &context;
	const DuplicateEliminatedDomainCTERegistry &cte_registry;
	unordered_map<TableIndex, RelationStats> cte_stats;
};

static optional_idx TryEstimateDomainCardinality(RelationStatsExtractor &stats_extractor, LogicalOperator &op,
                                                 const vector<idx_t> &key_indices,
                                                 bool require_reliable_payload_stats) {
	auto stats = stats_extractor.Extract(op);
	if (!stats || !stats->stats_initialized || stats->column_distinct_count.size() != op.GetColumnBindings().size()) {
		return {};
	}
	vector<DistinctCount> key_distinct_counts;
	key_distinct_counts.reserve(key_indices.size());
	for (auto key_idx : key_indices) {
		if (key_idx >= stats->column_distinct_count.size()) {
			return {};
		}
		auto distinct_count = stats->column_distinct_count[key_idx];
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
	if (bindings.size() != stats.column_distinct_count.size()) {
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
			auto distinct_rank = DistinctCountRank(stats.column_distinct_count[binding_idx].source);
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

static optional_idx FindBestCandidate(RelationStatsExtractor &stats_extractor, LogicalOperator &payload,
                                      const vector<unique_ptr<Expression>> &keys,
                                      const BindingEquivalence &equivalence,
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
	idx_t best_rows = 0;
	idx_t best_domain_rows = 0;
	idx_t best_base_relations = 0;
	idx_t best_depth = 0;
	idx_t best_order = 0;
	for (idx_t candidate_idx = 0; candidate_idx < candidates.size(); candidate_idx++) {
		auto &candidate = candidates[candidate_idx];
		if (!allow_superset && candidate.coverage == DuplicateEliminatedDomainCoverage::SUPERSET) {
			continue;
		}
		auto source_stats = stats_extractor.Extract(*candidate.source.get());
		auto domain_estimate = TryEstimateDomainCardinality(stats_extractor, *candidate.source.get(), candidate.key_indices,
		                                                         false);
		if (!source_stats || !source_stats->stats_initialized || !domain_estimate.IsValid()) {
			continue;
		}
		auto estimate = MaxValue<idx_t>(source_stats->cardinality, 1);
		auto domain_rows = MaxValue<idx_t>(domain_estimate.GetIndex(), 1);
		if (estimate >= payload_cardinality || domain_rows > payload_domain_cardinality) {
			continue;
		}
		auto better = !best.IsValid() || estimate < best_rows ||
		              (estimate == best_rows && domain_rows < best_domain_rows) ||
		              (estimate == best_rows && domain_rows == best_domain_rows &&
		               candidate.base_relation_count < best_base_relations) ||
		              (estimate == best_rows && domain_rows == best_domain_rows &&
		               candidate.base_relation_count == best_base_relations && candidate.depth < best_depth) ||
		              (estimate == best_rows && domain_rows == best_domain_rows &&
		               candidate.base_relation_count == best_base_relations && candidate.depth == best_depth &&
		               candidate.order < best_order);
		if (better) {
			best = candidate_idx;
			best_rows = estimate;
			best_domain_rows = domain_rows;
			best_base_relations = candidate.base_relation_count;
			best_depth = candidate.depth;
			best_order = candidate.order;
			candidate.source_cardinality = estimate;
			candidate.domain_cardinality = domain_rows;
		}
	}
	return best;
}

optional<DuplicateEliminatedDomainCandidate>
DuplicateEliminatedDomainAnalyzer::FindBest(ClientContext &context,
                                            const DuplicateEliminatedDomainCTERegistry &cte_registry,
                                            LogicalComparisonJoin &join,
                                            bool can_evaluate_additional_groups) {
	if (join.children.empty() || join.duplicate_eliminated_columns.empty()) {
		return {};
	}
	join.children[0]->ResolveOperatorTypes();
	BindingEquivalence equivalence;
	CollectEquivalences(*join.children[0], equivalence);
	CandidateAnalyzer analyzer(context, join.duplicate_eliminated_columns, equivalence);
	auto candidates = analyzer.Analyze(join.children[0]);
	RelationStatsExtractor stats_extractor(context, cte_registry);
	idx_t payload_cardinality;
	idx_t payload_domain_cardinality;
	auto selected_index = FindBestCandidate(stats_extractor, *join.children[0], join.duplicate_eliminated_columns,
	                                        equivalence, candidates,
	                                        can_evaluate_additional_groups, payload_cardinality,
	                                        payload_domain_cardinality);
	if (!selected_index.IsValid()) {
		return {};
	}
	auto &selected = candidates[selected_index.GetIndex()];
	D_ASSERT(can_evaluate_additional_groups || selected.coverage == DuplicateEliminatedDomainCoverage::EXACT);
	return DuplicateEliminatedDomainCandidate(
	    *selected.source.get(), std::move(selected.source_path), std::move(selected.key_indices), selected.coverage,
	    selected.source_cardinality, selected.domain_cardinality, payload_cardinality, payload_domain_cardinality,
	    DuplicateEliminatedDomainProperties::HasNonJoinSelection(*selected.source.get()));
}

optional_ptr<unique_ptr<LogicalOperator>>
DuplicateEliminatedDomainCandidate::TryResolveSource(unique_ptr<LogicalOperator> &payload) const {
	auto source = &payload;
	for (auto child_idx : source_path) {
		if (child_idx >= (*source)->children.size()) {
			return nullptr;
		}
		source = &(*source)->children[child_idx];
	}
	(*source)->ResolveOperatorTypes();
	if ((*source)->type != source_type || (*source)->types != source_types ||
	    (*source)->GetColumnBindings() != source_bindings) {
		return nullptr;
	}
	for (auto key_idx : key_indices) {
		if (key_idx >= source_types.size()) {
			return nullptr;
		}
	}
	return source;
}

} // namespace duckdb
