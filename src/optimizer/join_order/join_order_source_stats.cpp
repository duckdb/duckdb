#include "duckdb/optimizer/join_order/join_order_source_stats.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/optimizer/join_order/join_predicate.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"

namespace duckdb {

JoinOrderSourceStats::JoinOrderSourceStats(ClientContext &context, LogicalOperator &root) {
	AddPlan(context, root);
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

void JoinOrderSourceStats::AddPlan(ClientContext &context, LogicalOperator &root) {
	const auto sample_size = Settings::Get<FilterSelectivitySampleSizeSetting>(context);
	bool has_sampled_filter = false;
	bool has_inner_join = false;
	vector<reference<LogicalOperator>> pending {root};
	vector<reference<LogicalProjection>> pending_projections;
	vector<reference<LogicalOperator>> source_roots {root};
	unordered_map<TableIndex, reference<LogicalGet>> new_scans;
	while (!pending.empty()) {
		auto &op = pending.back().get();
		pending.pop_back();
		if (op.type == LogicalOperatorType::LOGICAL_GET && op.children.empty()) {
			auto &get = op.Cast<LogicalGet>();
			if (scans.find(get.table_index) == scans.end()) {
				auto cardinality = get.EstimateSourceCardinality(context);
				auto stats = RelationStatisticsHelper::ExtractGetStats(get, context, cardinality);
				scans.emplace(get.table_index, ScanStats {cardinality, std::move(stats)});
				new_scans.emplace(get.table_index, get);
				auto table = get.GetTable();
				if (sample_size && table && table->TryGetDuckTableEntry()) {
					for (auto &entry : get.table_filters) {
						has_sampled_filter |= !ExpressionFilter::IsOptionalFilter(entry.Filter());
					}
				}
			}
		}
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			pending_projections.push_back(op.Cast<LogicalProjection>());
		}
		has_inner_join |= op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		                  op.Cast<LogicalComparisonJoin>().join_type == JoinType::INNER;
		auto source_child_count = SourceChildCount(op);
		for (idx_t i = 0; i < op.children.size(); i++) {
			auto &child = *op.children[i];
			pending.push_back(child);
			if (i >= source_child_count) {
				source_roots.push_back(child);
			}
		}
	}
	vector<pair<ColumnBinding, ColumnBinding>> join_pairs;
	if (has_sampled_filter && has_inner_join) {
		for (auto &source_root : source_roots) {
			ResolveInternal(source_root, join_pairs);
		}
	}
	column_binding_map_t<column_binding_set_t> considered;
	for (auto &join_pair : join_pairs) {
		for (bool reverse : {false, true}) {
			auto filtered = reverse ? join_pair.second : join_pair.first;
			auto population = reverse ? join_pair.first : join_pair.second;
			auto filtered_get = new_scans.find(filtered.table_index);
			auto population_get = new_scans.find(population.table_index);
			// Previously captured scans may already have been optimized.
			if (filtered_get == new_scans.end() || population_get == new_scans.end() ||
			    !considered[filtered].insert(population).second) {
				continue;
			}
			auto factor = RelationStatisticsHelper::ConditionalFilterFactor(
			    context, filtered_get->second.get(), filtered.column_index, population_get->second.get(),
			    population.column_index);
			if (factor) {
				scans.at(filtered.table_index).filter_factors[filtered].emplace(population, *factor);
			}
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
	return ResolveInternal(root, nullptr);
}

join_column_sources_t
JoinOrderSourceStats::ResolveInternal(LogicalOperator &root,
                                      optional_ptr<vector<pair<ColumnBinding, ColumnBinding>>> join_pairs) const {
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
		if (join_pairs && op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		    op.Cast<LogicalComparisonJoin>().join_type == JoinType::INNER) {
			for (auto &condition : op.Cast<LogicalComparisonJoin>().conditions) {
				if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
					continue;
				}
				auto left = frame.inputs.find(JoinOrderUtil::GetEquivalenceBinding(condition.GetLHS()));
				auto right = frame.inputs.find(JoinOrderUtil::GetEquivalenceBinding(condition.GetRHS()));
				if (left != frame.inputs.end() && right != frame.inputs.end()) {
					join_pairs->emplace_back(left->second.direct.binding, right->second.direct.binding);
				}
			}
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
					JoinColumnSources source(
					    {column.binding, scan.cardinality, scan.stats.cardinality, column.total_domain});
					auto factors = scan.filter_factors.find(column.binding);
					if (factors != scan.filter_factors.end()) {
						source.filter_factors = factors->second;
					}
					output.emplace(column.binding, std::move(source));
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
