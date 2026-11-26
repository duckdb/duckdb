#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"

#include "duckdb/common/map.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/optimizer/join_order/join_predicate.hpp"
#include "duckdb/planner/operator/list.hpp"

#include <algorithm>

namespace duckdb {

static void CopyBoundValueRestrictions(RelationStats &result, const RelationStats &child) {
	vector<optional_idx> source_columns;
	source_columns.reserve(result.columns.size());
	for (auto &column : result.columns) {
		source_columns.push_back(child.FindColumn(column.binding));
	}
	result.CopyValueRestrictions(child, source_columns);
}

static optional<RelationStats> ProjectChildStats(LogicalOperator &op,
                                                 const vector<reference<const RelationStats>> &children,
                                                 CardinalityValue cardinality, optional_idx maximum = {},
                                                 const vector<bool> &null_extended = {}) {
	D_ASSERT(null_extended.empty() || null_extended.size() == children.size());
	RelationStats result;
	result.cardinality = cardinality;
	result.max_cardinality = maximum;
	result.stats_initialized = true;
	result.table_name = Identifier(op.GetName());
	struct ProjectedChildColumn {
		reference<const RelationColumnStats> source;
		bool can_have_null;
	};
	column_binding_map_t<ProjectedChildColumn> child_columns;
	for (idx_t i = 0; i < children.size(); i++) {
		auto extended = !null_extended.empty() && null_extended[i];
		for (auto &column : children[i].get().columns) {
			child_columns.emplace(column.binding, ProjectedChildColumn {column, column.can_have_null || extended});
		}
	}
	for (auto &binding : op.GetColumnBindings()) {
		auto entry = child_columns.find(binding);
		if (entry == child_columns.end()) {
			return {};
		}
		auto &source = entry->second.source.get();
		result.columns.emplace_back(binding, source.total_domain, source.current_domain, source.name);
		result.columns.back().can_have_null = entry->second.can_have_null;
	}
	for (auto &child : children) {
		CopyBoundValueRestrictions(result, child);
	}
	result.CapCurrentDomainsToCardinality();
	result.Verify(op.GetColumnBindings());
	return result;
}

static CardinalityValue JoinCardinality(LogicalComparisonJoin &join, const RelationStats &left,
                                        const RelationStats &right) {
	switch (join.join_type) {
	case JoinType::RIGHT_ANTI:
	case JoinType::RIGHT_SEMI:
		return right.GetCardinality();
	case JoinType::ANTI:
	case JoinType::SEMI:
	case JoinType::SINGLE:
	case JoinType::MARK:
		return left.GetCardinality();
	default:
		return MaxValue(left.GetCardinality(), right.GetCardinality());
	}
}

static optional_ptr<const RelationColumnStats> GetJoinColumn(const Expression &expression, const RelationStats &stats) {
	auto binding = JoinOrderUtil::GetEquivalenceBinding(expression);
	return binding.table_index.IsValid() ? stats.GetColumnStats(binding) : nullptr;
}

static vector<SemiAntiJoinDomain> ExtractSemiAntiJoinDomains(LogicalComparisonJoin &join,
                                                             const RelationStats &preserved, const RelationStats &rhs,
                                                             bool &has_residual) {
	vector<SemiAntiJoinDomain> domains;
	has_residual = false;
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() || (condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL &&
		                                  condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM)) {
			has_residual = true;
			continue;
		}
		auto lhs_preserved = GetJoinColumn(condition.GetLHS(), preserved);
		auto rhs_column = GetJoinColumn(condition.GetRHS(), rhs);
		if (!lhs_preserved || !rhs_column) {
			lhs_preserved = GetJoinColumn(condition.GetRHS(), preserved);
			rhs_column = GetJoinColumn(condition.GetLHS(), rhs);
		}
		if (!lhs_preserved || !rhs_column) {
			has_residual = true;
			continue;
		}
		bool seen_domain = false;
		for (auto &domain : domains) {
			if (domain.preserved_binding == lhs_preserved->binding && domain.rhs_binding == rhs_column->binding) {
				seen_domain = true;
				break;
			}
		}
		if (seen_domain) {
			continue;
		}
		auto rhs_current_domain = rhs_column->GetSupportedSemiAntiDomainSize();
		if (!rhs_current_domain) {
			has_residual = true;
			continue;
		}
		auto total_domain =
		    RelationStatisticsHelper::SelectTotalDomain({lhs_preserved->total_domain, rhs_column->total_domain});
		if (!total_domain || total_domain->distinct_count == 0) {
			has_residual = true;
			continue;
		}
		// Row-count fallbacks bound domains but do not estimate membership coverage.
		if (!total_domain->has_usable_domain) {
			has_residual = true;
		}
		domains.push_back(
		    {total_domain->distinct_count, *rhs_current_domain, lhs_preserved->binding, rhs_column->binding});
	}
	return domains;
}

static SemiAntiJoinCardinalityEstimate EstimateSemiAntiJoin(LogicalComparisonJoin &join, const RelationStats &preserved,
                                                            const RelationStats &rhs,
                                                            const vector<SemiAntiJoinDomain> &domains,
                                                            bool has_residual) {
	auto estimate = RelationStatisticsHelper::EstimateSemiAntiJoinCardinality(
	    preserved.GetCardinality(), rhs.GetCardinality(), rhs.row_retention, join.join_type, domains, has_residual);
	auto is_semi = join.join_type == JoinType::SEMI || join.join_type == JoinType::RIGHT_SEMI;
	if (!is_semi || join.children.size() != 2) {
		return estimate;
	}
	auto rhs_index = join.join_type == JoinType::RIGHT_SEMI ? 0 : 1;
	optional_ptr<const LogicalOperator> rhs_op = join.children[rhs_index].get();
	while (rhs_op->type == LogicalOperatorType::LOGICAL_PROJECTION && rhs_op->children.size() == 1) {
		rhs_op = rhs_op->children[0].get();
	}
	if (rhs_op->type == LogicalOperatorType::LOGICAL_CHUNK_GET) {
		// Constant-list domains do not describe the frequency of matching values.
		auto fallback = RelationStatisticsHelper::EstimateSemiAntiJoinFallback(preserved.GetCardinality(),
		                                                                       rhs.GetCardinality(), join.join_type);
		if (estimate.cardinality < fallback.cardinality) {
			return fallback;
		}
	}
	return estimate;
}

SemiAntiJoinCardinalityEstimate RelationStatisticsHelper::EstimateSemiAntiJoinCardinality(LogicalComparisonJoin &join,
                                                                                          const RelationStats &left,
                                                                                          const RelationStats &right) {
	auto preserve_right = join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI;
	auto &preserved = preserve_right ? right : left;
	auto &rhs = preserve_right ? left : right;
	bool has_residual;
	auto domains = ExtractSemiAntiJoinDomains(join, preserved, rhs, has_residual);
	return EstimateSemiAntiJoin(join, preserved, rhs, domains, has_residual);
}

void RelationStatisticsHelper::ApplySemiAntiJoinStats(LogicalComparisonJoin &join, const RelationStats &left,
                                                      const RelationStats &right, RelationStats &result) {
	auto preserve_right = join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI;
	auto &preserved = preserve_right ? right : left;
	auto &rhs = preserve_right ? left : right;
	D_ASSERT(join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI ||
	         join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI);
	bool has_residual;
	auto domains = ExtractSemiAntiJoinDomains(join, preserved, rhs, has_residual);
	auto estimate = EstimateSemiAntiJoin(join, preserved, rhs, domains, has_residual);
	result.row_retention = preserved.row_retention;
	result.cardinality = preserved.GetCardinality();
	result.max_cardinality = JoinCardinalityBound(join.join_type, left.max_cardinality, right.max_cardinality);
	for (auto &column : result.columns) {
		auto source = preserved.GetColumnStats(column.binding);
		if (source) {
			column.semi_anti_join_domain_evidence = source->semi_anti_join_domain_evidence;
			column.can_have_null = source->can_have_null;
		}
	}
	ApplyCardinality(result, estimate.cardinality);
	if (join.join_type == JoinType::SEMI || join.join_type == JoinType::RIGHT_SEMI) {
		map<idx_t, vector<shared_ptr<const ColumnValueRestriction>>> restrictions;
		map<idx_t, vector<idx_t>> witnessed_aliases;
		auto rhs_aliases = rhs.GetValueAliases();
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL ||
			    condition.GetLHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
			    condition.GetRHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
			    !condition.GetLHS().GetReturnType().EqualsIncludingCollation(condition.GetRHS().GetReturnType())) {
				continue;
			}
			auto column = GetJoinColumn(condition.GetLHS(), preserved);
			auto witness = GetJoinColumn(condition.GetRHS(), rhs);
			if (!column || !witness) {
				column = GetJoinColumn(condition.GetRHS(), preserved);
				witness = GetJoinColumn(condition.GetLHS(), rhs);
			}
			auto index = column ? result.FindColumn(column->binding) : optional_idx();
			if (index.IsValid() && witness) {
				restrictions[index.GetIndex()].push_back(witness->value_restriction);
				auto rhs_index = rhs.FindColumn(witness->binding).GetIndex();
				witnessed_aliases[rhs_aliases[rhs_index]].push_back(index.GetIndex());
			}
		}
		for (auto &entry : restrictions) {
			auto &column = result.columns[entry.first];
			entry.second.push_back(column.value_restriction);
			column.value_restriction =
			    ColumnValueRestriction::Combine(entry.second, RestrictionCombination::CONJUNCTION);
		}
		for (auto &entry : witnessed_aliases) {
			auto &group = entry.second;
			std::sort(group.begin(), group.end());
			group.erase(std::unique(group.begin(), group.end()), group.end());
			if (group.size() > 1) {
				result.value_equivalent_columns.push_back(std::move(group));
			}
		}
		for (auto &domain : domains) {
			auto column_index = result.FindColumn(domain.preserved_binding);
			if (!column_index.IsValid()) {
				continue;
			}
			auto &column = result.columns[column_index.GetIndex()];
			column.current_domain =
			    DistinctCount(MinValue(column.current_domain.distinct_count, domain.rhs_current_domain),
			                  DistinctCountSource::CARDINALITY);
		}
	}
}

static optional<RelationStats> ExtractGetWithChildStats(LogicalGet &get, ClientContext &context,
                                                        const RelationStats &child_stats) {
	auto result = RelationStatisticsHelper::ExtractGetStats(get, context);
	result.cardinality = child_stats.cardinality;
	// A table-in/table-out function can expand its input.
	result.max_cardinality = optional_idx();
	RelationColumnStatsLookup child_columns(child_stats);
	for (auto &binding : get.GetColumnBindings()) {
		if (binding.table_index == get.table_index) {
			continue;
		}
		auto child_column = child_columns.Find(binding);
		if (!child_column) {
			return {};
		}
		result.columns.emplace_back(binding, child_column->total_domain, child_column->current_domain,
		                            child_column->name);
		result.columns.back().can_have_null = child_column->can_have_null;
	}
	CopyBoundValueRestrictions(result, child_stats);
	result.CapCurrentDomainsToCardinality();
	result.Verify(get.GetColumnBindings());
	return result;
}

static optional<RelationStats> ExtractUnnestStats(LogicalOperator &op, const RelationStats &child_stats) {
	auto &unnest = op.Cast<LogicalUnnest>();
	RelationStats result;
	result.cardinality = child_stats.cardinality;
	result.stats_initialized = true;
	result.table_name = Identifier(op.GetName());
	RelationColumnStatsLookup child_columns(child_stats);
	for (auto &binding : op.GetColumnBindings()) {
		auto child_column = child_columns.Find(binding);
		if (child_column) {
			result.columns.emplace_back(binding, child_column->total_domain, child_column->current_domain,
			                            child_column->name);
			result.columns.back().can_have_null = child_column->can_have_null;
		} else if (binding.table_index == unnest.unnest_index) {
			result.columns.emplace_back(binding, DistinctCount(result.cardinality, DistinctCountSource::CARDINALITY),
			                            Identifier("unnest"));
		} else {
			return {};
		}
	}
	CopyBoundValueRestrictions(result, child_stats);
	result.Verify(op.GetColumnBindings());
	return result;
}

static optional<RelationStats> ExtractComparisonJoinStats(LogicalComparisonJoin &join,
                                                          const vector<reference<const RelationStats>> &child_stats) {
	if (child_stats.size() != 2) {
		return {};
	}
	if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI ||
	    join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI) {
		auto preserve_right = join.join_type == JoinType::RIGHT_SEMI || join.join_type == JoinType::RIGHT_ANTI;
		auto &preserved = child_stats[preserve_right ? 1 : 0].get();
		auto result = ProjectChildStats(join, child_stats, preserved.cardinality);
		if (!result) {
			return {};
		}
		RelationStatisticsHelper::ApplySemiAntiJoinStats(join, child_stats[0], child_stats[1], *result);
		result->Verify(join.GetColumnBindings());
		return result;
	}
	auto cardinality = JoinCardinality(join, child_stats[0], child_stats[1]);
	auto maximum = RelationStatisticsHelper::JoinCardinalityBound(join.join_type, child_stats[0].get().max_cardinality,
	                                                              child_stats[1].get().max_cardinality);
	if (join.join_type != JoinType::MARK) {
		vector<bool> null_extended {join.join_type == JoinType::RIGHT || join.join_type == JoinType::OUTER,
		                            join.join_type == JoinType::LEFT || join.join_type == JoinType::OUTER ||
		                                join.join_type == JoinType::SINGLE};
		return ProjectChildStats(join, child_stats, cardinality, maximum, null_extended);
	}

	RelationStats mark_result;
	mark_result.cardinality = cardinality;
	mark_result.max_cardinality = maximum;
	mark_result.row_retention = child_stats[0].get().row_retention;
	mark_result.stats_initialized = true;
	mark_result.table_name = Identifier(join.GetName());
	RelationColumnStatsLookup left_columns(child_stats[0].get());
	for (auto &binding : join.GetColumnBindings()) {
		auto column = left_columns.Find(binding);
		if (column) {
			mark_result.columns.push_back(*column);
		} else if (binding.table_index == join.mark_index) {
			mark_result.columns.emplace_back(binding, DistinctCount(3, DistinctCountSource::CARDINALITY),
			                                 Identifier("mark"));
		} else {
			return {};
		}
	}
	vector<optional_idx> source_columns;
	for (auto &column : mark_result.columns) {
		source_columns.push_back(child_stats[0].get().FindColumn(column.binding));
	}
	mark_result.CopyEvidence(child_stats[0], source_columns);
	mark_result.CapCurrentDomainsToCardinality();
	mark_result.Verify(join.GetColumnBindings());
	return mark_result;
}

static optional<RelationStats> ExtractCrossProductStats(LogicalOperator &op,
                                                        const vector<reference<const RelationStats>> &child_stats) {
	if (child_stats.size() != 2) {
		return {};
	}
	auto &left = child_stats[0].get();
	auto &right = child_stats[1].get();
	auto cardinality = left.GetCardinality() * right.GetCardinality();
	auto maximum = RelationStatisticsHelper::MultiplyCardinalityBounds(left.max_cardinality, right.max_cardinality);
	return ProjectChildStats(op, child_stats, cardinality, maximum);
}

static optional<RelationStats> ExtractSetOperationStats(LogicalSetOperation &setop,
                                                        const vector<reference<const RelationStats>> &child_stats) {
	if (child_stats.empty()) {
		return {};
	}

	RelationStats result;
	result.stats_initialized = true;
	result.table_name = Identifier(setop.GetName());
	switch (setop.type) {
	case LogicalOperatorType::LOGICAL_UNION:
		result.cardinality = 0;
		result.max_cardinality = optional_idx(0);
		for (auto &child : child_stats) {
			result.cardinality += child.get().cardinality;
			result.max_cardinality =
			    RelationStatisticsHelper::AddCardinalityBounds(result.max_cardinality, child.get().max_cardinality);
		}
		break;
	case LogicalOperatorType::LOGICAL_INTERSECT:
		result.cardinality = child_stats[0].get().cardinality;
		for (auto &child : child_stats) {
			result.cardinality = MinValue(result.cardinality, child.get().cardinality);
			auto maximum = child.get().max_cardinality;
			if (maximum.IsValid()) {
				result.max_cardinality = optional_idx(
				    result.max_cardinality.IsValid() ? MinValue(result.max_cardinality.GetIndex(), maximum.GetIndex())
				                                     : maximum.GetIndex());
			}
		}
		break;
	case LogicalOperatorType::LOGICAL_EXCEPT:
		result.cardinality = child_stats[0].get().cardinality;
		result.max_cardinality = child_stats[0].get().max_cardinality;
		break;
	default:
		return {};
	}

	auto bindings = setop.GetColumnBindings();
	for (idx_t column_idx = 0; column_idx < bindings.size(); column_idx++) {
		if (column_idx >= child_stats[0].get().columns.size()) {
			return {};
		}
		auto &first_column = child_stats[0].get().columns[column_idx];
		auto total_domain = first_column.total_domain;
		auto current_domain = first_column.current_domain;
		auto population_bound = first_column.PopulationBound();
		vector<shared_ptr<const ColumnValueRestriction>> restrictions;
		if (setop.type != LogicalOperatorType::LOGICAL_UNION || !child_stats[0].get().IsEmpty()) {
			restrictions.push_back(first_column.value_restriction);
		}
		auto is_empty_population = [&](idx_t child_idx) {
			auto bound = child_stats[child_idx].get().columns[column_idx].PopulationBound();
			return child_stats[child_idx].get().IsEmpty() && bound.IsValid() && bound.GetIndex() == 0;
		};
		bool empty_population = is_empty_population(0);
		for (idx_t child_idx = 1; child_idx < child_stats.size(); child_idx++) {
			if (column_idx >= child_stats[child_idx].get().columns.size()) {
				return {};
			}
			auto &child_column = child_stats[child_idx].get().columns[column_idx];
			if (setop.type == LogicalOperatorType::LOGICAL_INTERSECT ||
			    (setop.type == LogicalOperatorType::LOGICAL_UNION && !child_stats[child_idx].get().IsEmpty())) {
				restrictions.push_back(child_column.value_restriction);
			}
			switch (setop.type) {
			case LogicalOperatorType::LOGICAL_UNION:
				population_bound =
				    RelationStatisticsHelper::AddCardinalityBounds(population_bound, child_column.PopulationBound());
				if (is_empty_population(child_idx)) {
					break;
				}
				if (empty_population) {
					total_domain = child_column.total_domain;
					current_domain = child_column.current_domain;
					empty_population = false;
					break;
				}
				total_domain.distinct_count += child_column.total_domain.distinct_count;
				current_domain.distinct_count += child_column.current_domain.distinct_count;
				total_domain.source = DistinctCountSource::DERIVED;
				total_domain.has_usable_domain &= child_column.total_domain.has_usable_domain;
				current_domain.has_usable_domain &= child_column.current_domain.has_usable_domain;
				current_domain.source = DistinctCountSource::CARDINALITY;
				break;
			case LogicalOperatorType::LOGICAL_INTERSECT:
				if (child_column.PopulationBound().IsValid()) {
					population_bound = population_bound.IsValid() ? MinValue(population_bound.GetIndex(),
					                                                         child_column.PopulationBound().GetIndex())
					                                              : child_column.PopulationBound();
				}
				total_domain.distinct_count =
				    MinValue(total_domain.distinct_count, child_column.total_domain.distinct_count);
				current_domain.distinct_count =
				    MinValue(current_domain.distinct_count, child_column.current_domain.distinct_count);
				total_domain.source = DistinctCountSource::DERIVED;
				total_domain.has_usable_domain &= child_column.total_domain.has_usable_domain;
				current_domain.has_usable_domain &= child_column.current_domain.has_usable_domain;
				current_domain.source = DistinctCountSource::CARDINALITY;
				break;
			case LogicalOperatorType::LOGICAL_EXCEPT:
				break;
			default:
				return {};
			}
		}
		total_domain.distinct_count = MaxValue(total_domain.distinct_count, current_domain.distinct_count);
		result.columns.emplace_back(bindings[column_idx], total_domain, current_domain, first_column.name);
		result.columns.back().value_restriction = ColumnValueRestriction::Combine(
		    restrictions, setop.type != LogicalOperatorType::LOGICAL_UNION ? RestrictionCombination::CONJUNCTION
		                                                                   : RestrictionCombination::DISJUNCTION);
		if (population_bound.IsValid()) {
			result.columns.back().TightenPopulationBound(population_bound.GetIndex());
		}
	}
	result.CapCurrentDomainsToCardinality();
	result.Verify(bindings);
	return result;
}

RelationStats RelationStatisticsHelper::ExtractExplainStats(LogicalOperator &op) {
	RelationStats result;
	result.cardinality = 3;
	result.max_cardinality = optional_idx(3);
	result.stats_initialized = true;
	result.table_name = Identifier(op.GetName());
	for (auto &binding : op.GetColumnBindings()) {
		result.columns.emplace_back(binding, DistinctCount(result.cardinality, DistinctCountSource::CARDINALITY),
		                            Identifier("explain"));
	}
	result.Verify(op.GetColumnBindings());
	return result;
}

optional<RelationStats>
RelationStatisticsHelper::ExtractOperatorStats(LogicalOperator &op, ClientContext &context,
                                               const vector<reference<const RelationStats>> &child_stats) {
	if (child_stats.size() != op.children.size()) {
		return {};
	}
	for (idx_t child_idx = 0; child_idx < child_stats.size(); child_idx++) {
		auto &stats = child_stats[child_idx].get();
		if (!stats.stats_initialized || !stats.MatchesBindings(op.children[child_idx]->GetColumnBindings())) {
			return {};
		}
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		if (child_stats.empty()) {
			return ExtractGetStats(op.Cast<LogicalGet>(), context);
		}
		return child_stats.size() == 1 ? ExtractGetWithChildStats(op.Cast<LogicalGet>(), context, child_stats[0].get())
		                               : optional<RelationStats>();
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		return ExtractDelimGetStats(op.Cast<LogicalDelimGet>(), context);
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		return ExtractDummyScanStats(op.Cast<LogicalDummyScan>(), context);
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		return ExtractExpressionGetStats(op.Cast<LogicalExpressionGet>(), context);
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		return ExtractColumnDataGetStats(op.Cast<LogicalColumnDataGet>(), context);
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return ExtractProjectionStats(op.Cast<LogicalProjection>(), child_stats[0].get());
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return ExtractAggregationStats(op.Cast<LogicalAggregate>(), child_stats[0].get(), context);
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return ExtractDistinctStats(op.Cast<LogicalDistinct>(), child_stats[0].get(), context);
	case LogicalOperatorType::LOGICAL_WINDOW:
		return ExtractWindowStats(op.Cast<LogicalWindow>(), child_stats[0].get());
	case LogicalOperatorType::LOGICAL_FILTER: {
		if (child_stats.size() != 1) {
			return {};
		}
		return ExtractFilterStats(op.Cast<LogicalFilter>(), child_stats[0].get());
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return child_stats.size() == 1 ? ProjectOutputStats(child_stats[0].get(), op) : optional<RelationStats>();
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		return child_stats.size() == 2 ? ProjectOutputStats(child_stats[1].get(), op) : optional<RelationStats>();
	case LogicalOperatorType::LOGICAL_UNNEST:
		return ExtractUnnestStats(op, child_stats[0].get());
	case LogicalOperatorType::LOGICAL_LIMIT: {
		if (child_stats.size() != 1) {
			return {};
		}
		return ExtractLimitStats(op.Cast<LogicalLimit>(), child_stats[0].get());
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return ExtractComparisonJoinStats(op.Cast<LogicalComparisonJoin>(), child_stats);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return ExtractCrossProductStats(op, child_stats);
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_EXCEPT:
		return ExtractSetOperationStats(op.Cast<LogicalSetOperation>(), child_stats);
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return ExtractEmptyResultStats(op.Cast<LogicalEmptyResult>());
	case LogicalOperatorType::LOGICAL_EXPLAIN:
		return ExtractExplainStats(op);
	default:
		return {};
	}
}

} // namespace duckdb
