#include "duckdb/optimizer/cte_filter_pusher.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_extractor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"

#include <cmath>

namespace duckdb {

CTEFilterPusher::MaterializedCTEInfo::MaterializedCTEInfo(LogicalOperator &materialized_cte_p)
    : materialized_cte(materialized_cte_p), all_cte_refs_are_filtered(true),
      has_filter_dependency(materialized_cte_p.Cast<LogicalMaterializedCTE>().filter_dependency != nullptr) {
}

CTEFilterPusher::CTEFilterPusher(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

unique_ptr<LogicalOperator> CTEFilterPusher::Optimize(unique_ptr<LogicalOperator> op) {
	FindCandidates(*op);
	if (cte_info_map.empty()) {
		return op;
	}
	// Scalar producer pushdown must preserve markers referenced by CTE consumers.
	FilterPushdown pushdown(optimizer);
	unordered_set<TableIndex> referenced_bindings;
	pushdown.CheckMarkToSemi(*op, referenced_bindings);
	auto ctes = std::move(cte_info_map);

	// Iterate once over all materialized CTEs
	for (auto it = ctes.rbegin(); it != ctes.rend(); it++) {
		if (it->second->filters.empty() ||
		    (!it->second->all_cte_refs_are_filtered && !it->second->has_filter_dependency)) {
			continue;
		}

		// The cte_info_map must be reconstructed each time.
		// Changes to the plan otherwise break the non-unique_ptr references.
		cte_info_map = InsertionOrderPreservingMap<unique_ptr<MaterializedCTEInfo>>();
		FindCandidates(*op);

		auto &info = *cte_info_map[it->first];
		if (CanPushFilter(info)) {
			PushFilterIntoCTE(info);
		}
	}
	return PushJoinFilters(std::move(op));
}

void CTEFilterPusher::ClearDependencies(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		op.Cast<LogicalMaterializedCTE>().filter_dependency.reset();
	}
	for (auto &child : op.children) {
		ClearDependencies(*child);
	}
}

bool CTEFilterPusher::CanPushFilter(const MaterializedCTEInfo &info) {
	if (info.all_cte_refs_are_filtered) {
		return true;
	}
	if (info.filters.size() != 1 || !HasValidDependency(info)) {
		return false;
	}
	auto &cte = info.materialized_cte.Cast<LogicalMaterializedCTE>();
	D_ASSERT(info.filters[0].get().children[0]->type == LogicalOperatorType::LOGICAL_CTE_REF);
	auto &filtered_ref = info.filters[0].get().children[0]->Cast<LogicalCTERef>();
	return filtered_ref.table_index == cte.filter_dependency->row_scan;
}

bool CTEFilterPusher::HasValidDependency(const MaterializedCTEInfo &info) {
	auto &cte = info.materialized_cte.Cast<LogicalMaterializedCTE>();
	if (!cte.filter_dependency || info.references.size() != 2) {
		return false;
	}
	idx_t row_scans = 0;
	idx_t domain_scans = 0;
	for (auto &ref : info.references) {
		row_scans += ref.get().table_index == cte.filter_dependency->row_scan;
		domain_scans += ref.get().table_index == cte.filter_dependency->domain_scan;
	}
	return row_scans == 1 && domain_scans == 1;
}

void CTEFilterPusher::FindCandidates(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		// We encountered a new CTE, add it to the map
		auto key = to_string(op.Cast<LogicalMaterializedCTE>().table_index.index);
		auto value = make_uniq<MaterializedCTEInfo>(op);

		value->ancestors = available_ctes;
		cte_info_map.insert(key, std::move(value));
		FindCandidates(*op.children[0]);
		available_ctes.push_back(op.Cast<LogicalMaterializedCTE>().table_index);
		FindCandidates(*op.children[1]);
		available_ctes.pop_back();
		return;
	} else if (op.type == LogicalOperatorType::LOGICAL_FILTER &&
	           op.children[0]->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		// We encountered a filtered CTE ref, update the according CTE info
		auto &cte_ref = op.children[0]->Cast<LogicalCTERef>();
		auto it = cte_info_map.find(to_string(cte_ref.cte_index.index));
		if (it != cte_info_map.end()) {
			it->second->filters.push_back(op);
			it->second->references.push_back(cte_ref);
		}
		return;
	} else if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		// We encountered a CTE ref without a filter on top, so we can't do the optimization
		auto &cte_ref = op.Cast<LogicalCTERef>();
		auto it = cte_info_map.find(to_string(cte_ref.cte_index.index));
		if (it != cte_info_map.end()) {
			it->second->all_cte_refs_are_filtered = false;
			it->second->references.push_back(cte_ref);
		}
		return;
	}
	for (auto &child : op.children) {
		FindCandidates(*child);
	}
}

void CTEFilterPusher::PushFilterIntoCTE(MaterializedCTEInfo &info) {
	D_ASSERT(info.materialized_cte.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE);
	if (info.filters.empty()) {
		return;
	}

	// Create an OR expression with all the filters on all references of the CTE
	unique_ptr<Expression> outer_expr;
	for (auto &filter : info.filters) {
		D_ASSERT(filter.get().type == LogicalOperatorType::LOGICAL_FILTER);

		auto old_bindings = filter.get().children[0]->GetColumnBindings();
		auto new_bindings = info.materialized_cte.children[0]->GetColumnBindings();
		D_ASSERT(old_bindings.size() == new_bindings.size());

		ColumnBindingReplacer replacer;
		replacer.replacement_bindings.reserve(old_bindings.size());
		for (idx_t i = 0; i < old_bindings.size(); i++) {
			replacer.replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
		}

		bool all_conjuncts_repeatable = true;
		for (auto &expr : filter.get().expressions) {
			all_conjuncts_repeatable &= !expr->IsVolatile() && !ExpressionBarrier::Contains(*expr);
		}

		// We copy the filters and replace the CTE reference bindings with the bindings in the CTE definition
		unique_ptr<Expression> inner_expr;
		for (auto &filter_expr : filter.get().expressions) {
			// Dropping a conjunct must not expose errors it previously short-circuited.
			if (filter_expr->IsVolatile() || ExpressionBarrier::Contains(*filter_expr) ||
			    (!all_conjuncts_repeatable && filter_expr->CanThrow())) {
				continue;
			}
			auto filter_expr_copy = filter_expr->Copy();
			replacer.VisitExpression(&filter_expr_copy);
			if (inner_expr) {
				inner_expr = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
				                                                   std::move(inner_expr), std::move(filter_expr_copy));
			} else {
				inner_expr = std::move(filter_expr_copy);
			}
		}

		// An unrestricted consumer makes the disjunction true.
		if (!inner_expr) {
			return;
		}

		if (outer_expr) {
			outer_expr = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(outer_expr),
			                                                   std::move(inner_expr));
		} else {
			outer_expr = std::move(inner_expr);
		}
	}

	// Add the filter on top of the CTE definition and split the predicates
	auto new_cte = make_uniq_base<LogicalOperator, LogicalFilter>(std::move(outer_expr));
	LogicalFilter::SplitPredicates(new_cte->Cast<LogicalFilter>().expressions);

	// Rewrite the operator expressions before adding the child op (children should be rewritten already)
	optimizer.rewriter.VisitOperator(*new_cte);
	new_cte->children.push_back(std::move(info.materialized_cte.children[0]));

	// Push down the filter
	FilterPushdown pushdown(optimizer, true, FilterPushdown::ProjectionMode::PRESERVE_COMPUTED_EXPRESSIONS);
	new_cte = pushdown.Rewrite(std::move(new_cte));

	info.materialized_cte.children[0] = std::move(new_cte);
}

void CTEFilterPusher::AddJoinFilter(const LogicalCTERef &source, const LogicalCTERef &target,
                                    const vector<ColumnBinding> &source_keys, const vector<ColumnBinding> &target_keys,
                                    const vector<ExpressionType> &comparisons) {
	D_ASSERT(!comparisons.empty() && source_keys.size() == comparisons.size() &&
	         target_keys.size() == comparisons.size());
	auto target_entry = join_targets.find(target.cte_index);
	if (source.is_recurring || target.is_recurring || source.cte_index == target.cte_index ||
	    target_entry == join_targets.end() || target_entry->second != target.table_index) {
		return;
	}
	JoinFilter filter;
	filter.source = source.cte_index;
	filter.target = target.cte_index;
	filter.target_scan = target.table_index;
	filter.comparisons = comparisons;
	for (idx_t i = 0; i < comparisons.size(); i++) {
		D_ASSERT(source_keys[i].table_index == source.table_index);
		D_ASSERT(target_keys[i].table_index == target.table_index);
		filter.source_columns.push_back(source_keys[i].column_index);
		filter.target_columns.push_back(target_keys[i].column_index);
	}
	for (auto &existing : join_filters) {
		if (existing.source == filter.source && existing.target == filter.target &&
		    existing.target_scan == filter.target_scan && existing.source_columns == filter.source_columns &&
		    existing.target_columns == filter.target_columns && existing.comparisons == filter.comparisons) {
			return;
		}
	}
	join_filters.push_back(std::move(filter));
}

unique_ptr<LogicalOperator> CTEFilterPusher::PushJoinFilters(unique_ptr<LogicalOperator> op) {
	// Bound the extra scan/hash-build cost and require a substantial reduction in retained keys.
	static constexpr double MAX_SOURCE_RATIO = 0.25;
	static constexpr double MAX_KEY_RATIO = 0.5;
	cte_info_map.clear();
	FindCandidates(*op);
	for (auto &entry : cte_info_map) {
		if (HasValidDependency(*entry.second)) {
			auto &cte = entry.second->materialized_cte.Cast<LogicalMaterializedCTE>();
			join_targets.emplace(cte.table_index, cte.filter_dependency->row_scan);
		}
	}
	if (join_targets.empty()) {
		return op;
	}
	FilterPushdown::CollectCTEJoinFilters(*op, *this);
	if (join_filters.empty()) {
		return op;
	}

	cte_info_map.clear();
	FindCandidates(*op);
	vector<TableIndex> targets;
	for (auto &entry : cte_info_map) {
		targets.push_back(entry.second->materialized_cte.Cast<LogicalMaterializedCTE>().table_index);
	}
	unordered_map<TableIndex, RelationStats> restricted_stats;
	for (auto target : targets) {
		// Producer rewrites invalidate operator references and cached statistics.
		cte_info_map.clear();
		FindCandidates(*op);
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
		optional_ptr<JoinFilter> best;
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
	return op;
}

} // namespace duckdb
