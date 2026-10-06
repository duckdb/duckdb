#include "duckdb/optimizer/cte_join_filter_pusher.hpp"
#include "duckdb/optimizer/cte_join_filter_collector.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_extractor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"

#include <cmath>

namespace duckdb {

CTEJoinFilterPusher::CTEJoinFilterPusher(Optimizer &optimizer) : optimizer(optimizer) {
}

bool CTEJoinFilterPusher::HasValidDependency(const MaterializedCTEInfo &info) {
	auto &cte = info.materialized_cte.Cast<LogicalMaterializedCTE>();
	return cte.filter_dependency && cte.filter_dependency->MatchesConsumers(info.references);
}

void CTEJoinFilterPusher::FindCandidates(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte = op.Cast<LogicalMaterializedCTE>();
		auto info = make_uniq<MaterializedCTEInfo>(op);
		info->ancestors = available_ctes;
		cte_info_map.insert(to_string(cte.table_index.index), std::move(info));
		FindCandidates(*op.children[0]);
		available_ctes.push_back(cte.table_index);
		FindCandidates(*op.children[1]);
		available_ctes.pop_back();
		return;
	}
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &ref = op.Cast<LogicalCTERef>();
		auto entry = cte_info_map.find(to_string(ref.cte_index.index));
		if (entry != cte_info_map.end()) {
			entry->second->references.push_back(ref);
		}
		return;
	}
	for (auto &child : op.children) {
		FindCandidates(*child);
	}
}

void CTEJoinFilterPusher::Optimize(LogicalOperator &op) {
	// Bound the extra scan/hash-build cost and require a substantial reduction in retained keys.
	static constexpr double MAX_SOURCE_RATIO = 0.25;
	static constexpr double MAX_KEY_RATIO = 0.5;
	unordered_map<TableIndex, TableIndex> join_targets;
	cte_info_map.clear();
	FindCandidates(op);
	for (auto &entry : cte_info_map) {
		if (HasValidDependency(*entry.second)) {
			auto &cte = entry.second->materialized_cte.Cast<LogicalMaterializedCTE>();
			join_targets.emplace(cte.table_index, cte.filter_dependency->row_scan);
		}
	}
	if (join_targets.empty()) {
		return;
	}
	auto join_filters = CTEJoinFilterCollector::Collect(op, join_targets);
	if (join_filters.empty()) {
		return;
	}

	cte_info_map.clear();
	FindCandidates(op);
	vector<TableIndex> targets;
	for (auto &entry : cte_info_map) {
		targets.push_back(entry.second->materialized_cte.Cast<LogicalMaterializedCTE>().table_index);
	}
	unordered_map<TableIndex, RelationStats> restricted_stats;
	for (auto target : targets) {
		// Producer rewrites invalidate operator references and cached statistics.
		cte_info_map.clear();
		FindCandidates(op);
		auto target_entry = cte_info_map.find(to_string(target.index));
		if (target_entry == cte_info_map.end() || !HasValidDependency(*target_entry->second)) {
			continue;
		}
		auto &info = *target_entry->second;
		auto &cte = info.materialized_cte.Cast<LogicalMaterializedCTE>();
		RelationStatsExtractor extractor(optimizer.context, [&](TableIndex index) -> optional_ptr<LogicalOperator> {
			auto entry = cte_info_map.find(to_string(index.index));
			return entry == cte_info_map.end() ? nullptr : entry->second->materialized_cte.children[0].get();
		});
		auto target_stats = extractor.Extract(*cte.children[0]);
		if (!target_stats || target_stats->cardinality == 0) {
			continue;
		}
		optional_ptr<CTEJoinFilter> best;
		double best_fraction = 1;
		idx_t best_source_count = 0;
		for (auto &filter : join_filters) {
			if (filter.target != target || filter.target_scan != cte.filter_dependency->row_scan ||
			    std::find(info.ancestors.begin(), info.ancestors.end(), filter.source) == info.ancestors.end()) {
				continue;
			}
			auto source_entry = cte_info_map.find(to_string(filter.source.index));
			if (source_entry == cte_info_map.end()) {
				continue;
			}
			auto &source_cte = source_entry->second->materialized_cte;
			auto cached = restricted_stats.find(filter.source);
			optional_ptr<const RelationStats> source_stats =
			    cached == restricted_stats.end() ? extractor.Extract(*source_cte.children[0]) : &cached->second;
			// An extra scan and hash build should be small relative to the input being restricted.
			if (!source_stats || static_cast<double>(source_stats->cardinality) >
			                         static_cast<double>(target_stats->cardinality) * MAX_SOURCE_RATIO) {
				continue;
			}
			double fraction = 1;
			bool valid_columns = true;
			for (idx_t i = 0; i < filter.comparisons.size(); i++) {
				auto source_col = filter.source_columns[i].GetIndex();
				auto target_col = filter.target_columns[i].GetIndex();
				if (source_col >= source_stats->columns.size() || target_col >= target_stats->columns.size()) {
					valid_columns = false;
					break;
				}
				auto &source_distinct = source_stats->columns[source_col].distinct_count;
				auto &target_distinct = target_stats->columns[target_col].distinct_count;
				if (target_distinct.source == DistinctCountSource::CARDINALITY || target_distinct.distinct_count == 0) {
					continue;
				}
				auto source_count = MinValue(source_distinct.distinct_count, source_stats->cardinality);
				auto target_count = MinValue(target_distinct.distinct_count, target_stats->cardinality);
				fraction = MinValue(fraction, static_cast<double>(source_count) / static_cast<double>(target_count));
			}
			if (valid_columns && fraction <= MAX_KEY_RATIO &&
			    (!best || fraction < best_fraction ||
			     (fraction == best_fraction && source_stats->cardinality < best_source_count))) {
				best = filter;
				best_fraction = fraction;
				best_source_count = source_stats->cardinality;
			}
		}
		if (!best) {
			continue;
		}
		auto &source_cte = cte_info_map[to_string(best->source.index)]->materialized_cte;
		source_cte.children[0]->ResolveOperatorTypes();
		auto &source_types = source_cte.children[0]->types;
		vector<Identifier> names;
		for (idx_t i = 0; i < source_types.size(); i++) {
			names.emplace_back("key_" + to_string(i));
		}
		auto source_ref = make_uniq<LogicalCTERef>(optimizer.binder.GenerateTableIndex(), best->source, source_types,
		                                           std::move(names));
		auto source_bindings = source_ref->GetColumnBindings();
		auto target_bindings = cte.children[0]->GetColumnBindings();
		cte.children[0]->ResolveOperatorTypes();
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);
		for (idx_t i = 0; i < best->comparisons.size(); i++) {
			auto source_col = best->source_columns[i].GetIndex();
			auto target_col = best->target_columns[i].GetIndex();
			join->conditions.emplace_back(
			    make_uniq<BoundColumnRefExpression>(cte.children[0]->types[target_col], target_bindings[target_col]),
			    make_uniq<BoundColumnRefExpression>(source_types[source_col], source_bindings[source_col]),
			    best->comparisons[i]);
		}
		auto stats = *target_stats;
		stats.cardinality = LossyNumericCast<idx_t>(std::ceil(static_cast<double>(stats.cardinality) * best_fraction));
		for (auto &column : stats.columns) {
			column.distinct_count.distinct_count = MinValue(column.distinct_count.distinct_count, stats.cardinality);
		}
		restricted_stats.emplace(target, std::move(stats));
		join->children.push_back(std::move(cte.children[0]));
		join->children.push_back(std::move(source_ref));
		cte.children[0] = std::move(join);
	}
	return;
}

} // namespace duckdb
