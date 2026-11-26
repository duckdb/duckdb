#include "duckdb/optimizer/join_order/join_order_source_stats.hpp"

#include "duckdb/optimizer/join_order/join_predicate.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"
#include "duckdb/common/profiler.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/planner/table_filter_set.hpp"

namespace duckdb {

struct JoinStatisticsColumnTarget {
	reference<LogicalGet> get;
	ProjectionIndex projection_index;
};

class JoinStatisticsColumnCollector {
public:
	explicit JoinStatisticsColumnCollector(LogicalOperator &root) {
		RegisterCTEs(root);
		Collect(root);
	}

	const vector<ProjectionIndex> &GetColumns(TableIndex table_index) const {
		auto entry = columns.find(table_index);
		return entry == columns.end() ? empty_columns : entry->second;
	}

private:
	static bool ContainsBinding(LogicalOperator &op, const ColumnBinding &binding) {
		auto bindings = op.GetColumnBindings();
		return std::find(bindings.begin(), bindings.end(), binding) != bindings.end();
	}

	void RegisterCTEs(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			auto &cte = op.Cast<LogicalMaterializedCTE>();
			cte_definitions[cte.table_index] = cte.children[0].get();
		}
		for (auto &child : op.children) {
			RegisterCTEs(*child);
		}
	}

	void AddTarget(vector<JoinStatisticsColumnTarget> &result, LogicalGet &get, ProjectionIndex projection_index) {
		for (auto &target : result) {
			if (target.get.get().table_index == get.table_index && target.projection_index == projection_index) {
				return;
			}
		}
		result.push_back({get, projection_index});
	}

	void ResolveExpression(LogicalOperator &op, const Expression &expression,
	                       vector<JoinStatisticsColumnTarget> &result) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expression,
		                                                              [&](const BoundColumnRefExpression &ref) {
			                                                              if (ref.Depth() == 0) {
				                                                              ResolveBinding(op, ref.Binding(), result);
			                                                              }
		                                                              });
	}

	void ResolveBinding(LogicalOperator &op, const ColumnBinding &binding, vector<JoinStatisticsColumnTarget> &result) {
		if (op.type == LogicalOperatorType::LOGICAL_GET && op.children.empty()) {
			auto &get = op.Cast<LogicalGet>();
			if (binding.table_index == get.table_index &&
			    binding.column_index.GetIndex() < MaxValue<idx_t>(1, get.GetColumnIds().size())) {
				AddTarget(result, get, binding.column_index);
			}
			return;
		}
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = op.Cast<LogicalProjection>();
			if (binding.table_index == projection.table_index &&
			    binding.column_index.GetIndex() < projection.expressions.size()) {
				ResolveExpression(*projection.children[0], projection.GetExpression(binding), result);
				return;
			}
		}
		if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			auto &aggregate = op.Cast<LogicalAggregate>();
			if (binding.table_index == aggregate.group_index &&
			    binding.column_index.GetIndex() < aggregate.groups.size()) {
				ResolveExpression(*aggregate.children[0], aggregate.GetGroupExpression(binding.column_index), result);
			}
			return;
		}
		if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto &cte_ref = op.Cast<LogicalCTERef>();
			if (binding.table_index != cte_ref.table_index || cte_ref.is_recurring || cte_ref.correlated_columns > 0) {
				return;
			}
			auto definition = cte_definitions.find(cte_ref.cte_index);
			if (definition == cte_definitions.end() || active_ctes.find(cte_ref.cte_index) != active_ctes.end()) {
				return;
			}
			auto bindings = definition->second->GetColumnBindings();
			if (binding.column_index.GetIndex() >= bindings.size()) {
				return;
			}
			active_ctes.insert(cte_ref.cte_index);
			ResolveBinding(*definition->second, bindings[binding.column_index.GetIndex()], result);
			active_ctes.erase(cte_ref.cte_index);
			return;
		}
		for (auto &child : op.children) {
			if (ContainsBinding(*child, binding)) {
				ResolveBinding(*child, binding, result);
			}
		}
	}

	static bool IsEquality(ExpressionType type) {
		return type == ExpressionType::COMPARE_EQUAL || type == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	}

	static bool ConnectsDifferentScans(const vector<JoinStatisticsColumnTarget> &left,
	                                   const vector<JoinStatisticsColumnTarget> &right) {
		for (auto &lhs : left) {
			for (auto &rhs : right) {
				if (lhs.get.get().table_index != rhs.get.get().table_index) {
					return true;
				}
			}
		}
		return false;
	}

	void AddEqualityTargets(vector<JoinStatisticsColumnTarget> &left, vector<JoinStatisticsColumnTarget> &right) {
		if (!ConnectsDifferentScans(left, right)) {
			return;
		}
		for (auto &target : left) {
			AddColumn(target);
		}
		for (auto &target : right) {
			AddColumn(target);
		}
	}

	void AddColumn(const JoinStatisticsColumnTarget &target) {
		auto &entries = columns[target.get.get().table_index];
		if (std::find(entries.begin(), entries.end(), target.projection_index) == entries.end()) {
			entries.push_back(target.projection_index);
		}
	}

	void CollectFilterEquality(LogicalOperator &input, const Expression &expression) {
		if (expression.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
			for (auto &child : expression.Cast<BoundConjunctionExpression>().GetChildren()) {
				CollectFilterEquality(input, *child);
			}
			return;
		}
		if (!BoundComparisonExpression::IsComparison(expression) || !IsEquality(expression.GetExpressionType())) {
			return;
		}
		auto &comparison = expression.Cast<BoundFunctionExpression>();
		vector<JoinStatisticsColumnTarget> left;
		vector<JoinStatisticsColumnTarget> right;
		ResolveExpression(input, BoundComparisonExpression::Left(comparison), left);
		ResolveExpression(input, BoundComparisonExpression::Right(comparison), right);
		AddEqualityTargets(left, right);
	}

	void Collect(LogicalOperator &op) {
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN && op.children.size() == 2) {
			auto &join = op.Cast<LogicalComparisonJoin>();
			for (auto &condition : join.conditions) {
				if (!condition.IsComparison() || !IsEquality(condition.GetComparisonType())) {
					continue;
				}
				vector<JoinStatisticsColumnTarget> left;
				vector<JoinStatisticsColumnTarget> right;
				ResolveExpression(*op.children[0], condition.GetLHS(), left);
				ResolveExpression(*op.children[1], condition.GetRHS(), right);
				if (left.empty() || right.empty()) {
					left.clear();
					right.clear();
					ResolveExpression(*op.children[1], condition.GetLHS(), left);
					ResolveExpression(*op.children[0], condition.GetRHS(), right);
				}
				AddEqualityTargets(left, right);
			}
		} else if (op.type == LogicalOperatorType::LOGICAL_FILTER && op.children.size() == 1) {
			for (auto &expression : op.expressions) {
				CollectFilterEquality(*op.children[0], *expression);
			}
		}
		for (auto &child : op.children) {
			Collect(*child);
		}
	}

private:
	unordered_map<TableIndex, vector<ProjectionIndex>> columns;
	unordered_map<TableIndex, optional_ptr<LogicalOperator>> cte_definitions;
	unordered_set<TableIndex> active_ctes;
	vector<ProjectionIndex> empty_columns;
};

JoinOrderSourceStats::JoinOrderSourceStats(ClientContext &context, LogicalOperator &root,
                                           FilterConditionedStatisticsMode mode) {
	AddPlan(context, root, mode == FilterConditionedStatisticsMode::COLLECT);
}

void JoinOrderSourceStats::AddPlan(ClientContext &context, LogicalOperator &root) {
	AddPlan(context, root, false);
}

void JoinOrderSourceStats::AddPlan(ClientContext &context, LogicalOperator &root,
                                   bool collect_filter_conditioned_statistics) {
	struct PendingScan {
		PendingScan(LogicalGet &get_p, idx_t cardinality_p) : get(get_p), cardinality(cardinality_p) {
		}

		reference<LogicalGet> get;
		idx_t cardinality;
		vector<ColumnIndex> statistics_columns;
		vector<ColumnIndex> scan_columns;
		vector<vector<ProjectionIndex>> statistics_projections;
		TableFilterSet filters;
		unique_ptr<TableFunctionFilterConditionedStatistics> filter_conditioned_statistics;
	};
	vector<PendingScan> pending_scans;
	vector<reference<LogicalOperator>> pending {root};
	vector<reference<LogicalProjection>> pending_projections;
	while (!pending.empty()) {
		auto &op = pending.back().get();
		pending.pop_back();
		if (op.type == LogicalOperatorType::LOGICAL_GET && op.children.empty()) {
			auto &get = op.Cast<LogicalGet>();
			if (scans.find(get.table_index) == scans.end()) {
				pending_scans.emplace_back(get, get.EstimateSourceCardinality(context));
			}
		}
		if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			pending_projections.push_back(op.Cast<LogicalProjection>());
		}
		for (auto &child : op.children) {
			pending.push_back(*child);
		}
	}

	JoinStatisticsColumnCollector requested_columns(root);
	struct ProviderBatch {
		table_function_collect_filter_conditioned_statistics_batch_t callback;
		vector<idx_t> scan_indexes;
	};
	vector<ProviderBatch> batches;
	idx_t request_count = 0;
	for (idx_t scan_index = 0; scan_index < pending_scans.size(); scan_index++) {
		auto &scan = pending_scans[scan_index];
		auto &get = scan.get.get();
		auto &projections = requested_columns.GetColumns(get.table_index);
		if (!collect_filter_conditioned_statistics || projections.empty() || !get.bind_data ||
		    !get.function.collect_filter_conditioned_statistics_batch) {
			continue;
		}
		auto add_scan_column = [&](const ColumnIndex &column) {
			auto entry = std::find(scan.scan_columns.begin(), scan.scan_columns.end(), column);
			if (entry != scan.scan_columns.end()) {
				return ProjectionIndex(NumericCast<idx_t>(entry - scan.scan_columns.begin()));
			}
			scan.scan_columns.push_back(column);
			return ProjectionIndex(scan.scan_columns.size() - 1);
		};
		for (auto projection : projections) {
			ColumnBinding binding(get.table_index, projection);
			auto column = get.GetColumnIndex(binding);
			auto entry = std::find(scan.statistics_columns.begin(), scan.statistics_columns.end(), column);
			if (entry == scan.statistics_columns.end()) {
				scan.statistics_columns.push_back(column);
				scan.statistics_projections.push_back({projection});
			} else {
				scan.statistics_projections[NumericCast<idx_t>(entry - scan.statistics_columns.begin())].push_back(
				    projection);
			}
			add_scan_column(column);
		}
		for (auto &entry : get.table_filters) {
			auto column = get.GetColumnIndex(ColumnBinding(get.table_index, entry.GetIndex()));
			auto compact_index = add_scan_column(column);
			scan.filters.PushFilter(
			    compact_index, ExpressionFilter::GetExpressionFilter(entry.Filter(), "JoinOrderSourceStats").Copy());
		}
		for (auto &filter : get.table_filters.GetMultiColumnFilters()) {
			auto copy = ExpressionFilter::GetExpressionFilter(*filter, "JoinOrderSourceStats").Copy();
			for (auto &projection : copy->column_indexes) {
				auto column = get.GetColumnIndex(ColumnBinding(get.table_index, projection));
				projection = add_scan_column(column);
			}
			scan.filters.PushMultiColumnFilter(std::move(copy));
		}
		if (get.function.prepare_filter_conditioned_statistics) {
			auto &bind_data = get.bind_data.write();
			TableFunctionFilterConditionedStatisticsInput input(&bind_data, scan.statistics_columns, scan.scan_columns,
			                                                    scan.filters);
			if (!get.function.prepare_filter_conditioned_statistics(context, bind_data, input)) {
				continue;
			}
		}
		auto callback = get.function.collect_filter_conditioned_statistics_batch;
		auto batch = std::find_if(batches.begin(), batches.end(),
		                          [&](const ProviderBatch &entry) { return entry.callback == callback; });
		if (batch == batches.end()) {
			batches.push_back({callback, {scan_index}});
		} else {
			batch->scan_indexes.push_back(scan_index);
		}
		request_count++;
	}

	Profiler filter_conditioned_statistics_timer;
	filter_conditioned_statistics_timer.Start();
	idx_t physical_groups = 0;
	idx_t compressed_bytes = 0;
	idx_t uncompressed_bytes = 0;
	idx_t rows_collected = 0;
	for (auto &batch : batches) {
		vector<TableFunctionFilterConditionedStatisticsInput> inputs;
		inputs.reserve(batch.scan_indexes.size());
		for (auto scan_index : batch.scan_indexes) {
			auto &scan = pending_scans[scan_index];
			auto &get = scan.get.get();
			inputs.emplace_back(get.bind_data.get(), scan.statistics_columns, scan.scan_columns, scan.filters);
		}
		auto batch_result = batch.callback(context, inputs);
		if (!batch_result) {
			continue;
		}
		if (batch_result->statistics.size() != batch.scan_indexes.size()) {
			throw InternalException(
			    "Table function returned %llu results for a %llu-request filter-conditioned statistics batch",
			    batch_result->statistics.size(), batch.scan_indexes.size());
		}
		for (idx_t i = 0; i < batch.scan_indexes.size(); i++) {
			auto &scan = pending_scans[batch.scan_indexes[i]];
			auto &get = scan.get.get();
			scan.filter_conditioned_statistics = std::move(batch_result->statistics[i]);
			if (!scan.filter_conditioned_statistics) {
				continue;
			}
			auto &statistics = *scan.filter_conditioned_statistics;
			if (statistics.filtered_column_statistics.size() != scan.statistics_columns.size() ||
			    (!statistics.filtered_distinct_statistics.empty() &&
			     statistics.filtered_distinct_statistics.size() != scan.statistics_columns.size())) {
				throw InternalException("Table function returned invalid filter-conditioned column statistics");
			}
			auto full_count = MaxValue<idx_t>(1, get.GetColumnIds().size());
			vector<unique_ptr<BaseStatistics>> full_statistics(full_count);
			vector<unique_ptr<DistinctStatistics>> full_distinct_statistics(full_count);
			for (idx_t column_idx = 0; column_idx < scan.statistics_projections.size(); column_idx++) {
				for (auto projection : scan.statistics_projections[column_idx]) {
					if (statistics.filtered_column_statistics[column_idx]) {
						full_statistics[projection.GetIndex()] =
						    statistics.filtered_column_statistics[column_idx]->ToUnique();
					}
					if (!statistics.filtered_distinct_statistics.empty() &&
					    statistics.filtered_distinct_statistics[column_idx]) {
						full_distinct_statistics[projection.GetIndex()] =
						    statistics.filtered_distinct_statistics[column_idx]->Copy();
					}
				}
			}
			scan.filter_conditioned_statistics = make_uniq<TableFunctionFilterConditionedStatistics>(
			    statistics.filtered_cardinality, std::move(full_statistics), std::move(full_distinct_statistics));
		}
		physical_groups += batch_result->physical_groups;
		compressed_bytes += batch_result->compressed_bytes;
		uncompressed_bytes += batch_result->uncompressed_bytes;
		rows_collected += batch_result->rows_collected;
	}
	filter_conditioned_statistics_timer.End();
	if (request_count > 0) {
		auto &profiler = QueryProfiler::Get(context);
		profiler.AddToMetricTiming("optimizer.filter_conditioned_statistics_time",
		                           filter_conditioned_statistics_timer.ElapsedNanos());
		profiler.AddToMetricCounter("optimizer.filter_conditioned_statistics_requests", request_count);
		profiler.AddToMetricCounter("optimizer.filter_conditioned_statistics_groups", physical_groups);
		profiler.AddToMetricCounter("optimizer.filter_conditioned_statistics_compressed_bytes", compressed_bytes);
		profiler.AddToMetricCounter("optimizer.filter_conditioned_statistics_uncompressed_bytes", uncompressed_bytes);
		profiler.AddToMetricCounter("optimizer.filter_conditioned_statistics_rows_collected", rows_collected);
	}

	for (auto &scan : pending_scans) {
		auto &get = scan.get.get();
		auto stats = RelationStatisticsHelper::ExtractGetStats(get, context, scan.cardinality,
		                                                       scan.filter_conditioned_statistics.get());
		scans.emplace(get.table_index, ScanStats {scan.cardinality, std::move(stats)});
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
					                                  column.total_domain, column.filtered_join_domain}));
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
