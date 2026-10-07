#include "duckdb/optimizer/relation_statistics/relation_statistics_extractor.hpp"

#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"

namespace duckdb {

RelationStatsExtractor::RelationStatsExtractor(ClientContext &context) : context(context) {
}

RelationStatsExtractor::RelationStatsExtractor(ClientContext &context, relation_stats_cte_callback_t cte_callback)
    : context(context), cte_callback(std::move(cte_callback)) {
}

optional_ptr<const RelationStats> RelationStatsExtractor::Extract(LogicalOperator &op) {
	auto cache_entry = cache.find(op);
	if (cache_entry != cache.end()) {
		return cache_entry->second;
	}
	if (failed_operators.find(op) != failed_operators.end() || !active_operators.insert(op).second) {
		return nullptr;
	}

	extracted_operator_count++;
	auto result = ExtractInternal(op);
	active_operators.erase(op);
	if (!result || !result->stats_initialized || !result->MatchesBindings(op.GetColumnBindings())) {
		failed_operators.insert(op);
		return nullptr;
	}
	result->Verify(op.GetColumnBindings());
	auto inserted = cache.emplace(op, std::move(*result));
	return inserted.first->second;
}

idx_t RelationStatsExtractor::ExtractedOperatorCount() const {
	return extracted_operator_count;
}

optional<RelationStats> RelationStatsExtractor::ExtractInternal(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		return ExtractCTERef(op.Cast<LogicalCTERef>());
	}
	if (op.type == LogicalOperatorType::LOGICAL_EXPLAIN) {
		return RelationStatisticsHelper::ExtractExplainStats(op);
	}

	vector<reference<const RelationStats>> child_stats;
	child_stats.reserve(op.children.size());
	for (auto &child : op.children) {
		auto stats = Extract(*child);
		if (!stats) {
			return {};
		}
		child_stats.push_back(*stats);
	}
	return RelationStatisticsHelper::ExtractOperatorStats(op, context, child_stats);
}

optional<RelationStats> RelationStatsExtractor::ExtractCTERef(LogicalCTERef &cte_ref) {
	if (cte_ref.is_recurring || !cte_callback) {
		return {};
	}
	auto definition = cte_callback(cte_ref.cte_index);
	if (!definition) {
		return {};
	}
	auto definition_stats = Extract(*definition);
	if (!definition_stats) {
		return {};
	}
	return RelationStatisticsHelper::RebindOutputStats(*definition_stats, cte_ref);
}

} // namespace duckdb
