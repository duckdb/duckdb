#include "duckdb/optimizer/join_order/join_order_source_stats.hpp"

#include "duckdb/optimizer/join_order/join_predicate.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"

namespace duckdb {

DistinctCount JoinDomainEstimate::Resolve(const vector<ColumnBinding> &bindings) const {
	auto domain = estimate;
	for (auto binding : bindings) {
		auto entry = local_fallbacks.find(binding);
		if (entry != local_fallbacks.end()) {
			domain = domain ? RelationStatisticsHelper::SelectTotalDomain({*domain, entry->second}) : entry->second;
		}
	}
	if (!domain) {
		return DistinctCount(CardinalityValue(NumericLimits<idx_t>::Maximum()), DistinctCountSource::CARDINALITY);
	}
	if (!estimate || !estimate->has_usable_domain || reliable_source_count != 1 ||
	    (estimate->source != DistinctCountSource::HLL && estimate->source != DistinctCountSource::EXACT)) {
		return *domain;
	}
	optional<CardinalityValue> key_domain;
	for (auto candidate : min_max_candidates) {
		if (candidate > domain->distinct_count && candidate <= reliable_source_cardinality) {
			key_domain = key_domain ? MinValue(*key_domain, candidate) : candidate;
		}
	}
	if (key_domain) {
		domain->distinct_count = *key_domain;
	}
	return *domain;
}

optional<CardinalityValue> JoinDomainEstimate::UnknownScanBasis() const {
	return estimate && !estimate->has_usable_domain ? optional<CardinalityValue>(estimate->distinct_count)
	                                                : optional<CardinalityValue>();
}

void JoinDomainEstimate::AddSource(const JoinColumnSource &source) {
	if (!sources.insert(source.binding).second) {
		return;
	}
	if (source.domain.has_usable_domain &&
	    (source.domain.source == DistinctCountSource::HLL || source.domain.source == DistinctCountSource::EXACT)) {
		reliable_source_cardinality = MinValue(reliable_source_cardinality, source.source_cardinality);
		reliable_source_count++;
	}
}

void JoinDomainEstimate::Update(ColumnBinding binding, const DistinctCount &distinct_count,
                                optional_ptr<const JoinColumnSources> column_sources) {
	auto observation = distinct_count;
	if (distinct_count.source == DistinctCountSource::DERIVED) {
		// Source ranges cannot uplift a population already conditioned at a relational boundary.
		reliable_source_cardinality = 0;
	}
	if (column_sources) {
		AddSource(column_sources->direct);
		for (auto &source : column_sources->equivalents) {
			AddSource(source);
		}
		if (distinct_count.source == DistinctCountSource::DERIVED || !distinct_count.has_usable_domain) {
			observation = column_sources->direct.domain;
		}
		if (!distinct_count.has_usable_domain) {
			// Preserve P separately from the original scan fallback S: discount S before restricting to P.
			local_fallbacks.emplace(binding, distinct_count);
		}
	} else if (!distinct_count.has_usable_domain || distinct_count.source == DistinctCountSource::DERIVED) {
		// A boundary's conditioned population is not an observation of another input's raw source.
		local_fallbacks.emplace(binding, distinct_count);
		return;
	} else if (distinct_count.source == DistinctCountSource::HLL ||
	           distinct_count.source == DistinctCountSource::EXACT) {
		// A reliable observation without source provenance has no known population for the uplift guard.
		reliable_source_cardinality = 0;
	}
	if (column_sources && observation.source == DistinctCountSource::MIN_MAX) {
		min_max_candidates.push_back(MinValue(observation.distinct_count, column_sources->direct.scan_cardinality));
	}
	estimate = estimate ? RelationStatisticsHelper::SelectTotalDomain({*estimate, observation}) : observation;
}

JoinOrderSourceStats::JoinOrderSourceStats(ClientContext &context, LogicalOperator &root) {
	AddPlan(context, root);
}

void JoinOrderSourceStats::AddPlan(ClientContext &context, LogicalOperator &root) {
	vector<reference<LogicalOperator>> pending {root};
	vector<reference<LogicalProjection>> pending_projections;
	while (!pending.empty()) {
		auto &op = pending.back().get();
		pending.pop_back();
		if (op.type == LogicalOperatorType::LOGICAL_GET && op.children.empty()) {
			auto &get = op.Cast<LogicalGet>();
			if (scans.find(get.table_index) == scans.end()) {
				auto source = get.EstimateSourceStatistics(context);
				column_binding_map_t<DistinctCount> source_domains;
				auto stats = RelationStatisticsHelper::ExtractGetStats(get, context, source, source_domains);
				scans.emplace(get.table_index,
				              ScanStats {source.estimated_cardinality, std::move(stats), std::move(source_domains)});
			}
		}
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			pending_projections.push_back(op.Cast<LogicalProjection>());
		}
		for (auto &child : op.children) {
			pending.push_back(*child);
		}
	}
	// Capture children first, reusing their summaries when resolving each parent.
	for (auto it = pending_projections.rbegin(); it != pending_projections.rend(); ++it) {
		auto &projection = it->get();
		if (projections.find(projection.table_index) == projections.end()) {
			projections.emplace(projection.table_index, Resolve(projection));
		}
	}
}

optional_ptr<const RelationStats> JoinOrderSourceStats::FindScanStats(const LogicalGet &get) const {
	auto entry = scans.find(get.table_index);
	return entry == scans.end() ? nullptr : optional_ptr<const RelationStats>(entry->second.stats);
}

static idx_t SourceChildCount(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return op.children.size();
	case LogicalOperatorType::LOGICAL_SECURE_VIEW:
		return op.Cast<LogicalSecureView>().propagate_statistics ? op.children.size() : 0;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		switch (op.Cast<LogicalComparisonJoin>().join_type) {
		case JoinType::INNER:
			return 2;
		case JoinType::LEFT:
		case JoinType::SEMI:
		case JoinType::ANTI:
		case JoinType::MARK:
			return 1;
		default:
			return 0;
		}
	default:
		return 0;
	}
}

static void AddSourceEquality(JoinEqualityGroups &groups, ExpressionType type, const Expression &left,
                              const Expression &right) {
	if (type != ExpressionType::COMPARE_EQUAL && type != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		return;
	}
	auto lhs = JoinOrderUtil::GetEquivalenceBinding(left);
	auto rhs = JoinOrderUtil::GetEquivalenceBinding(right);
	if (lhs.table_index.IsValid() && rhs.table_index.IsValid()) {
		groups.AddEquality(lhs, rhs);
	}
}

static void AddSourceEqualities(JoinEqualityGroups &groups, const Expression &expression) {
	if (expression.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		for (auto &child : expression.Cast<BoundConjunctionExpression>().GetChildren()) {
			AddSourceEqualities(groups, *child);
		}
	} else if (BoundComparisonExpression::IsComparison(expression)) {
		auto &comparison = expression.Cast<BoundFunctionExpression>();
		AddSourceEquality(groups, expression.GetExpressionType(), BoundComparisonExpression::Left(comparison),
		                  BoundComparisonExpression::Right(comparison));
	}
}

static void MergeEquivalentSources(LogicalOperator &op, join_column_sources_t &inputs) {
	auto is_inner = op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	                op.Cast<LogicalComparisonJoin>().join_type == JoinType::INNER;
	if (!is_inner && op.type != LogicalOperatorType::LOGICAL_FILTER) {
		return;
	}
	JoinEqualityGroups groups;
	column_binding_map_t<ColumnBinding> source_outputs;
	for (auto &entry : inputs) {
		groups.AddEquality(entry.first, entry.first);
		auto first = source_outputs.emplace(entry.second.direct.binding, entry.first);
		groups.AddEquality(entry.first, first.first->second);
		for (auto &source : entry.second.equivalents) {
			auto first = source_outputs.emplace(source.binding, entry.first);
			groups.AddEquality(entry.first, first.first->second);
		}
	}
	if (is_inner) {
		for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
			if (condition.IsComparison()) {
				AddSourceEquality(groups, condition.GetComparisonType(), condition.GetLHS(), condition.GetRHS());
			}
		}
	} else {
		for (auto &expression : op.expressions) {
			AddSourceEqualities(groups, *expression);
		}
	}
	unordered_map<idx_t, column_binding_map_t<JoinColumnSource>> contributors;
	for (auto &entry : inputs) {
		auto &group = contributors[groups.GetGroup(entry.first)];
		group.emplace(entry.second.direct.binding, entry.second.direct);
		for (auto &source : entry.second.equivalents) {
			group.emplace(source.binding, source);
		}
	}
	for (auto &entry : inputs) {
		auto &group = contributors.at(groups.GetGroup(entry.first));
		entry.second.equivalents.clear();
		for (auto &source : group) {
			if (source.first != entry.second.direct.binding) {
				entry.second.equivalents.push_back(source.second);
			}
		}
	}
}

join_column_sources_t JoinOrderSourceStats::Resolve(LogicalOperator &root) const {
	struct Frame {
		explicit Frame(LogicalOperator &op) : op(op) {
		}

		reference<LogicalOperator> op;
		idx_t next_child = 0;
		join_column_sources_t inputs;
	};
	vector<Frame> stack;
	stack.emplace_back(root);
	while (true) {
		auto &frame = stack.back();
		auto &op = frame.op.get();
		auto cached = op.type == LogicalOperatorType::LOGICAL_PROJECTION
		                  ? projections.find(op.Cast<LogicalProjection>().table_index)
		                  : projections.end();
		if (cached == projections.end() && frame.next_child < SourceChildCount(op)) {
			auto &child = *op.children[frame.next_child++];
			stack.emplace_back(child);
			continue;
		}
		MergeEquivalentSources(op, frame.inputs);
		join_column_sources_t output;
		if (cached != projections.end()) {
			output = cached->second;
		} else if (op.type == LogicalOperatorType::LOGICAL_GET && op.children.empty()) {
			auto &get = op.Cast<LogicalGet>();
			auto entry = scans.find(get.table_index);
			if (entry != scans.end() && !get.extra_info.sample_options) {
				auto &scan = entry->second;
				for (auto &column : scan.stats.columns) {
					output.emplace(column.binding,
					               JoinColumnSources({column.binding, scan.cardinality, scan.stats.cardinality,
					                                  scan.source_domains.at(column.binding)}));
				}
			}
		} else if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto bindings = op.GetColumnBindings();
			for (idx_t i = 0; i < op.expressions.size(); i++) {
				auto &expr = *op.expressions[i];
				if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
					continue;
				}
				auto &column = expr.Cast<BoundColumnRefExpression>();
				auto entry = frame.inputs.find(column.Binding());
				if (column.Depth() == 0 && entry != frame.inputs.end()) {
					output.emplace(bindings[i], entry->second);
				}
			}
		} else {
			for (auto &binding : op.GetColumnBindings()) {
				auto entry = frame.inputs.find(binding);
				if (entry != frame.inputs.end()) {
					output.emplace(binding, entry->second);
				}
			}
		}
		stack.pop_back();
		if (stack.empty()) {
			return output;
		}
		for (auto &entry : output) {
			stack.back().inputs.emplace(entry.first, std::move(entry.second));
		}
	}
}

} // namespace duckdb
