#include "duckdb/optimizer/aggregate_reuse.hpp"

#include "duckdb/optimizer/aggregate_rewrite_helper.hpp"
#include "duckdb/optimizer/key_properties.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

struct TableColumnOrigin {
	reference<TableCatalogEntry> table;
	ColumnIndex column;
};

struct AggregateJoinCandidate {
	idx_t owner_child;
	idx_t probe_child;
	idx_t owner_key;
	idx_t probe_key;
	vector<ColumnBinding> owner_groups;
};

static optional_idx GetDirectReferenceIndex(const Expression &expression, LogicalOperator &input) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto index = expression.Cast<BoundReferenceExpression>().Index();
		return index < input.GetColumnBindings().size() ? optional_idx(index) : optional_idx();
	}
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return optional_idx();
	}
	auto binding = expression.Cast<BoundColumnRefExpression>().Binding();
	auto bindings = input.GetColumnBindings();
	for (idx_t index = 0; index < bindings.size(); index++) {
		if (bindings[index] == binding) {
			return optional_idx(index);
		}
	}
	return optional_idx();
}

static optional_idx GetInvertibleReferenceIndex(const Expression &expression, LogicalOperator &input) {
	auto result = GetDirectReferenceIndex(expression, input);
	if (result.IsValid() || !BoundCastExpression::IsCast(expression)) {
		return result;
	}
	auto &cast = expression.Cast<BoundFunctionExpression>();
	if (!BoundCastExpression::CastIsInvertible(BoundCastExpression::SourceType(cast),
	                                           BoundCastExpression::TargetType(cast))) {
		return optional_idx();
	}
	return GetInvertibleReferenceIndex(BoundCastExpression::Child(cast), input);
}

static optional<TableColumnOrigin> GetTableColumnOrigin(LogicalOperator &op, idx_t output_idx,
                                                        bool allow_filtered = false) {
	if (output_idx >= op.GetColumnBindings().size()) {
		return nullopt;
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		if (projection.children.size() != 1 || output_idx >= projection.expressions.size()) {
			return nullopt;
		}
		auto child_idx = allow_filtered
		                     ? GetInvertibleReferenceIndex(*projection.expressions[output_idx], *projection.children[0])
		                     : GetDirectReferenceIndex(*projection.expressions[output_idx], *projection.children[0]);
		return child_idx.IsValid() ? GetTableColumnOrigin(*projection.children[0], child_idx.GetIndex(), allow_filtered)
		                           : nullopt;
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		if (!allow_filtered) {
			return nullopt;
		}
		auto &filter = op.Cast<LogicalFilter>();
		if (filter.children.size() != 1) {
			return nullopt;
		}
		if (!filter.projection_map.empty()) {
			if (output_idx >= filter.projection_map.size()) {
				return nullopt;
			}
			output_idx = filter.projection_map[output_idx].GetIndex();
		}
		return GetTableColumnOrigin(*filter.children[0], output_idx, allow_filtered);
	}
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		if (!allow_filtered) {
			return nullopt;
		}
		auto binding = op.GetColumnBindings()[output_idx];
		for (auto &child : op.children) {
			auto bindings = child->GetColumnBindings();
			auto entry = std::find(bindings.begin(), bindings.end(), binding);
			if (entry != bindings.end()) {
				return GetTableColumnOrigin(*child, NumericCast<idx_t>(entry - bindings.begin()), allow_filtered);
			}
		}
		return nullopt;
	}
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return nullopt;
	}
	auto &get = op.Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table || !get.children.empty() || !get.scan_partition_indices.empty() ||
	    (!allow_filtered &&
	     (get.table_filters.HasFilters() || (get.dynamic_filters && get.dynamic_filters->HasFilters())))) {
		return nullopt;
	}
	return TableColumnOrigin {*table, get.GetColumnIndex(ProjectionIndex(output_idx))};
}

static optional<TableColumnOrigin> GetTableColumnOrigin(LogicalOperator &op, const Expression &expression,
                                                        bool allow_filtered = false) {
	auto output_idx = GetDirectReferenceIndex(expression, op);
	return output_idx.IsValid() ? GetTableColumnOrigin(op, output_idx.GetIndex(), allow_filtered) : nullopt;
}

static bool SameOrigin(const TableColumnOrigin &left, const TableColumnOrigin &right) {
	return &left.table.get() == &right.table.get() && left.column == right.column;
}

static optional_idx GetJoinOutputReference(const Expression &expression, LogicalComparisonJoin &join,
                                           idx_t &child_index) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto index = expression.Cast<BoundReferenceExpression>().Index();
		auto left_count = join.children[0]->GetColumnBindings().size();
		if (index < left_count) {
			child_index = 0;
			return optional_idx(index);
		}
		index -= left_count;
		if (index < join.children[1]->GetColumnBindings().size()) {
			child_index = 1;
			return optional_idx(index);
		}
		return optional_idx();
	}
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return optional_idx();
	}
	for (idx_t side = 0; side < 2; side++) {
		auto index = GetDirectReferenceIndex(expression, *join.children[side]);
		if (index.IsValid()) {
			child_index = side;
			return index;
		}
	}
	return optional_idx();
}

static bool HasCompleteGrouping(const LogicalAggregate &aggregate) {
	if (aggregate.groups.empty() || !aggregate.grouping_functions.empty() || aggregate.grouping_sets.size() > 1) {
		return false;
	}
	if (aggregate.grouping_sets.empty()) {
		return true;
	}
	auto &grouping_set = aggregate.grouping_sets[0];
	if (grouping_set.size() != aggregate.groups.size()) {
		return false;
	}
	for (idx_t group_idx = 0; group_idx < aggregate.groups.size(); group_idx++) {
		if (grouping_set.find(ProjectionIndex(group_idx)) == grouping_set.end()) {
			return false;
		}
	}
	return true;
}

static optional<AggregateJoinCandidate> GetCandidate(LogicalAggregate &aggregate, LogicalComparisonJoin &join) {
	if (join.join_type != JoinType::INNER || join.children.size() != 2 || join.conditions.size() != 1 ||
	    join.HasProjectionMap() || join.HasArbitraryConditions() || aggregate.expressions.size() != 1 ||
	    !HasCompleteGrouping(aggregate)) {
		return nullopt;
	}
	auto &condition = join.conditions[0];
	if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL ||
	    condition.GetLHS().GetReturnType() != condition.GetRHS().GetReturnType()) {
		return nullopt;
	}
	auto left_key = GetDirectReferenceIndex(condition.GetLHS(), *join.children[0]);
	auto right_key = GetDirectReferenceIndex(condition.GetRHS(), *join.children[1]);
	if (!left_key.IsValid() || !right_key.IsValid()) {
		return nullopt;
	}
	auto &outer_aggregate = aggregate.expressions[0]->Cast<BoundAggregateExpression>();
	if (outer_aggregate.IsDistinct() || outer_aggregate.IsVolatile() ||
	    outer_aggregate.StateExportMode() != AggregateStateExportMode::NONE || outer_aggregate.GetFilter() ||
	    outer_aggregate.GetOrderBys() || outer_aggregate.GetChildren().size() != 1) {
		return nullopt;
	}
	for (idx_t owner_child = 0; owner_child < 2; owner_child++) {
		const auto probe_child = 1 - owner_child;
		if (!GetDirectReferenceIndex(*outer_aggregate.GetChildren()[0], *join.children[probe_child]).IsValid()) {
			continue;
		}
		const auto owner_key = owner_child == 0 ? left_key.GetIndex() : right_key.GetIndex();
		const auto probe_key = probe_child == 0 ? left_key.GetIndex() : right_key.GetIndex();
		auto key_property = GetUniqueKeyProperty(*join.children[owner_child], {owner_key});
		if (!key_property) {
			continue;
		}
		vector<ColumnBinding> owner_groups;
		auto owner_bindings = join.children[owner_child]->GetColumnBindings();
		bool valid = true;
		bool groups_by_owner_key = false;
		for (auto &group : aggregate.groups) {
			idx_t group_child;
			auto group_index = GetJoinOutputReference(*group, join, group_child);
			if (!group_index.IsValid() || group_child != owner_child ||
			    !key_property->FunctionallyDetermines(*join.children[owner_child], group_index.GetIndex())) {
				valid = false;
				break;
			}
			groups_by_owner_key = groups_by_owner_key || group_index.GetIndex() == owner_key;
			owner_groups.push_back(owner_bindings[group_index.GetIndex()]);
		}
		if (valid && groups_by_owner_key) {
			return AggregateJoinCandidate {owner_child, probe_child, owner_key, probe_key, std::move(owner_groups)};
		}
	}
	return nullopt;
}

static optional_idx FindTracedOutput(LogicalOperator &op, const ColumnBinding &target) {
	auto bindings = op.GetColumnBindings();
	for (idx_t output_idx = 0; output_idx < bindings.size(); output_idx++) {
		if (bindings[output_idx] == target) {
			return optional_idx(output_idx);
		}
	}
	if (op.children.size() != 1) {
		return optional_idx();
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		auto traced = FindTracedOutput(*filter.children[0], target);
		if (!traced.IsValid() || filter.projection_map.empty()) {
			return traced;
		}
		for (idx_t output_idx = 0; output_idx < filter.projection_map.size(); output_idx++) {
			if (filter.projection_map[output_idx].GetIndex() == traced.GetIndex()) {
				return optional_idx(output_idx);
			}
		}
		return optional_idx();
	}
	if (op.type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return optional_idx();
	}
	auto &projection = op.Cast<LogicalProjection>();
	auto traced = FindTracedOutput(*projection.children[0], target);
	if (!traced.IsValid()) {
		return optional_idx();
	}
	for (idx_t output_idx = 0; output_idx < projection.expressions.size(); output_idx++) {
		auto child_idx = GetInvertibleReferenceIndex(*projection.expressions[output_idx], *projection.children[0]);
		if (child_idx == traced) {
			return optional_idx(output_idx);
		}
	}
	return optional_idx();
}

static optional_ptr<LogicalAggregate> FindUnaryAggregate(LogicalOperator &op) {
	reference<LogicalOperator> current(op);
	while (current.get().type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		if (current.get().children.size() != 1 || (current.get().type != LogicalOperatorType::LOGICAL_PROJECTION &&
		                                           current.get().type != LogicalOperatorType::LOGICAL_FILTER)) {
			return nullptr;
		}
		current = *current.get().children[0];
	}
	return current.get().Cast<LogicalAggregate>();
}

static bool SameReusableAggregate(const BoundAggregateExpression &outer, LogicalOperator &outer_input,
                                  const BoundAggregateExpression &inner, LogicalOperator &inner_input) {
	if (outer.IsDistinct() || inner.IsDistinct() || outer.Function() != inner.Function() ||
	    outer.GetReturnType() != inner.GetReturnType() || outer.StateExportMode() != inner.StateExportMode() ||
	    outer.GetFilter() || inner.GetFilter() || outer.GetOrderBys() || inner.GetOrderBys() ||
	    outer.GetChildren().size() != 1 || inner.GetChildren().size() != 1 ||
	    !FunctionData::Equals(outer.BindInfo().get(), inner.BindInfo().get())) {
		return false;
	}
	auto outer_origin = GetTableColumnOrigin(outer_input, *outer.GetChildren()[0]);
	auto inner_origin = GetTableColumnOrigin(inner_input, *inner.GetChildren()[0]);
	return outer_origin && inner_origin && SameOrigin(*outer_origin, *inner_origin);
}

static optional<ColumnBinding> PromoteReusableAggregate(unique_ptr<LogicalOperator> &op, ColumnBinding key_binding,
                                                        LogicalOperator &outer_probe,
                                                        const TableColumnOrigin &outer_key_origin,
                                                        const BoundAggregateExpression &outer_aggregate, bool apply) {
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op->Cast<LogicalProjection>();
		auto bindings = projection.GetColumnBindings();
		auto key_entry = std::find(bindings.begin(), bindings.end(), key_binding);
		if (projection.children.size() != 1 || key_entry == bindings.end()) {
			return nullopt;
		}
		auto key_idx = NumericCast<idx_t>(key_entry - bindings.begin());
		auto child_idx = GetDirectReferenceIndex(*projection.expressions[key_idx], *projection.children[0]);
		if (!child_idx.IsValid()) {
			return nullopt;
		}
		auto child_binding = projection.children[0]->GetColumnBindings()[child_idx.GetIndex()];
		auto payload = PromoteReusableAggregate(projection.children[0], child_binding, outer_probe, outer_key_origin,
		                                        outer_aggregate, apply);
		if (!payload) {
			return nullopt;
		}
		const auto result = ColumnBinding(projection.table_index, ProjectionIndex(projection.expressions.size()));
		if (apply) {
			projection.expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(outer_aggregate.GetReturnType(), *payload));
			projection.ResolveOperatorTypes();
		}
		return result;
	}
	if (op->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op->Cast<LogicalFilter>();
		auto bindings = filter.GetColumnBindings();
		auto key_entry = std::find(bindings.begin(), bindings.end(), key_binding);
		if (filter.children.size() != 1 || key_entry == bindings.end()) {
			return nullopt;
		}
		auto child_bindings = filter.children[0]->GetColumnBindings();
		idx_t child_key_idx = NumericCast<idx_t>(key_entry - bindings.begin());
		if (!filter.projection_map.empty()) {
			child_key_idx = filter.projection_map[child_key_idx].GetIndex();
		}
		auto payload = PromoteReusableAggregate(filter.children[0], child_bindings[child_key_idx], outer_probe,
		                                        outer_key_origin, outer_aggregate, apply);
		if (!payload) {
			return nullopt;
		}
		if (apply && !filter.projection_map.empty()) {
			child_bindings = filter.children[0]->GetColumnBindings();
			auto payload_entry = std::find(child_bindings.begin(), child_bindings.end(), *payload);
			if (payload_entry == child_bindings.end()) {
				throw InternalException("Validated aggregate payload is not exposed by its child plan");
			}
			filter.projection_map.push_back(
			    ProjectionIndex(NumericCast<idx_t>(payload_entry - child_bindings.begin())));
			filter.ResolveOperatorTypes();
		}
		return payload;
	}
	if (op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return nullopt;
	}
	auto &join = op->Cast<LogicalComparisonJoin>();
	if (join.children.size() != 2 || join.HasProjectionMap()) {
		return nullopt;
	}
	if (join.join_type == JoinType::SEMI) {
		if (join.conditions.size() != 1 || join.conditions[0].GetComparisonType() != ExpressionType::COMPARE_EQUAL ||
		    join.children[1]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
			return nullopt;
		}
		auto left_key = GetDirectReferenceIndex(join.conditions[0].GetLHS(), *join.children[0]);
		auto right_key = GetDirectReferenceIndex(join.conditions[0].GetRHS(), *join.children[1]);
		if (!left_key.IsValid() || !right_key.IsValid() ||
		    join.children[0]->GetColumnBindings()[left_key.GetIndex()] != key_binding) {
			return nullopt;
		}
		auto &right_projection = join.children[1]->Cast<LogicalProjection>();
		auto inner_aggregate = FindUnaryAggregate(*right_projection.children[0]);
		if (!inner_aggregate || inner_aggregate->children.size() != 1 || inner_aggregate->groups.size() != 1 ||
		    inner_aggregate->grouping_sets.size() > 1 || !inner_aggregate->grouping_functions.empty()) {
			return nullopt;
		}
		if (!inner_aggregate->grouping_sets.empty() &&
		    (inner_aggregate->grouping_sets[0].size() != 1 ||
		     inner_aggregate->grouping_sets[0].find(ProjectionIndex(0)) == inner_aggregate->grouping_sets[0].end())) {
			return nullopt;
		}
		auto group_binding = ColumnBinding(inner_aggregate->group_index, ProjectionIndex(0));
		auto traced_group = FindTracedOutput(*right_projection.children[0], group_binding);
		auto projected_key = GetInvertibleReferenceIndex(*right_projection.expressions[right_key.GetIndex()],
		                                                 *right_projection.children[0]);
		if (!traced_group.IsValid() || projected_key != traced_group) {
			return nullopt;
		}
		auto right_key_origin = GetTableColumnOrigin(*inner_aggregate->children[0], *inner_aggregate->groups[0]);
		if (!right_key_origin || !SameOrigin(*right_key_origin, outer_key_origin)) {
			return nullopt;
		}
		idx_t aggregate_idx = DConstants::INVALID_INDEX;
		for (idx_t idx = 0; idx < inner_aggregate->expressions.size(); idx++) {
			auto &inner = inner_aggregate->expressions[idx]->Cast<BoundAggregateExpression>();
			if (!SameReusableAggregate(outer_aggregate, outer_probe, inner, *inner_aggregate->children[0])) {
				continue;
			}
			if (aggregate_idx != DConstants::INVALID_INDEX) {
				return nullopt;
			}
			aggregate_idx = idx;
		}
		if (aggregate_idx == DConstants::INVALID_INDEX) {
			return nullopt;
		}
		auto aggregate_binding = ColumnBinding(inner_aggregate->aggregate_index, ProjectionIndex(aggregate_idx));
		auto traced_output = FindTracedOutput(*right_projection.children[0], aggregate_binding);
		if (!traced_output.IsValid()) {
			return nullopt;
		}
		auto payload_binding = right_projection.children[0]->GetColumnBindings()[traced_output.GetIndex()];
		const auto result =
		    ColumnBinding(right_projection.table_index, ProjectionIndex(right_projection.expressions.size()));
		if (apply) {
			right_projection.expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(outer_aggregate.GetReturnType(), payload_binding));
			join.join_type = JoinType::INNER;
			right_projection.ResolveOperatorTypes();
			join.ResolveOperatorTypes();
		}
		return result;
	}
	if (join.join_type != JoinType::INNER) {
		return nullopt;
	}
	for (auto &child : join.children) {
		auto child_bindings = child->GetColumnBindings();
		if (std::find(child_bindings.begin(), child_bindings.end(), key_binding) == child_bindings.end()) {
			continue;
		}
		auto payload =
		    PromoteReusableAggregate(child, key_binding, outer_probe, outer_key_origin, outer_aggregate, apply);
		if (payload) {
			if (apply) {
				join.ResolveOperatorTypes();
			}
			return payload;
		}
	}
	return nullopt;
}

struct RelationEdge {
	TableColumnOrigin left;
	TableColumnOrigin right;
	ExpressionType comparison;
};

struct RelationGraph {
	vector<reference<LogicalGet>> sources;
	vector<RelationEdge> edges;
};

static bool ContainsSource(const RelationGraph &graph, TableCatalogEntry &table) {
	for (auto &source : graph.sources) {
		if (source.get().GetTable().get() == &table) {
			return true;
		}
	}
	return false;
}

static optional<TableColumnOrigin> FindBindingOrigin(LogicalOperator &op, const ColumnBinding &binding) {
	auto bindings = op.GetColumnBindings();
	auto entry = std::find(bindings.begin(), bindings.end(), binding);
	if (entry != bindings.end()) {
		auto result = GetTableColumnOrigin(op, NumericCast<idx_t>(entry - bindings.begin()), true);
		if (result) {
			return result;
		}
	}
	for (auto &child : op.children) {
		auto result = FindBindingOrigin(*child, binding);
		if (result) {
			return result;
		}
	}
	return nullopt;
}

static bool SameEdge(const RelationEdge &left, const RelationEdge &right) {
	if (left.comparison != right.comparison) {
		return false;
	}
	return (SameOrigin(left.left, right.left) && SameOrigin(left.right, right.right)) ||
	       (SameOrigin(left.left, right.right) && SameOrigin(left.right, right.left));
}

static bool CollectInnerGraph(LogicalOperator &op, optional_ptr<LogicalComparisonJoin> domain_semi,
                              RelationGraph &graph) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		if (projection.children.size() != 1) {
			return false;
		}
		for (auto &expression : projection.expressions) {
			if (expression->IsVolatile()) {
				return false;
			}
		}
		return CollectInnerGraph(*projection.children[0], domain_semi, graph);
	}
	if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = op.Cast<LogicalFilter>();
		if (filter.children.size() != 1) {
			return false;
		}
		for (auto &expression : filter.expressions) {
			if (expression->IsVolatile()) {
				return false;
			}
		}
		return filter.expressions.empty() && CollectInnerGraph(*filter.children[0], domain_semi, graph);
	}
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		if (join.HasProjectionMap() || join.HasArbitraryConditions() || join.children.size() != 2) {
			return false;
		}
		if (domain_semi && &join == domain_semi.get()) {
			const auto core_child = join.join_type == JoinType::RIGHT_SEMI ? idx_t(1) : idx_t(0);
			return CollectInnerGraph(*join.children[core_child], domain_semi, graph);
		}
		if (join.join_type != JoinType::INNER) {
			return false;
		}
		for (auto &condition : join.conditions) {
			if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
				return false;
			}
			auto left = GetTableColumnOrigin(*join.children[0], condition.GetLHS(), true);
			auto right = GetTableColumnOrigin(*join.children[1], condition.GetRHS(), true);
			if (!left || !right) {
				return false;
			}
			graph.edges.push_back({*left, *right, condition.GetComparisonType()});
		}
		return CollectInnerGraph(*join.children[0], domain_semi, graph) &&
		       CollectInnerGraph(*join.children[1], domain_semi, graph);
	}
	if (op.type != LogicalOperatorType::LOGICAL_GET) {
		return false;
	}
	auto &get = op.Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table || !get.children.empty() || ContainsSource(graph, *table)) {
		return false;
	}
	graph.sources.push_back(get);
	return true;
}

static optional_ptr<LogicalCTERef> GetDomainCTERef(LogicalOperator &op) {
	reference<LogicalOperator> current(op);
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION && current.get().children.size() == 1) {
		auto &projection = current.get().Cast<LogicalProjection>();
		for (auto &expression : projection.expressions) {
			if (expression->IsVolatile()) {
				return nullptr;
			}
		}
		current = *projection.children[0];
	}
	return current.get().type == LogicalOperatorType::LOGICAL_CTE_REF ? current.get().Cast<LogicalCTERef>()
	                                                                  : optional_ptr<LogicalCTERef>();
}

static optional_idx TraceDomainOutput(LogicalOperator &op, idx_t output_idx, TableIndex cte_index) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &ref = op.Cast<LogicalCTERef>();
		return ref.cte_index == cte_index ? optional_idx(output_idx) : optional_idx();
	}
	if (op.children.size() != 1) {
		return optional_idx();
	}
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = op.Cast<LogicalProjection>();
		if (output_idx >= projection.expressions.size()) {
			return optional_idx();
		}
		auto child_idx = GetInvertibleReferenceIndex(*projection.expressions[output_idx], *op.children[0]);
		return child_idx.IsValid() ? TraceDomainOutput(*op.children[0], child_idx.GetIndex(), cte_index)
		                           : optional_idx();
	}
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &aggregate = op.Cast<LogicalAggregate>();
		if (output_idx >= aggregate.groups.size()) {
			return optional_idx();
		}
		auto child_idx = GetInvertibleReferenceIndex(*aggregate.groups[output_idx], *op.children[0]);
		return child_idx.IsValid() ? TraceDomainOutput(*op.children[0], child_idx.GetIndex(), cte_index)
		                           : optional_idx();
	}
	return optional_idx();
}

struct DomainSemiInfo {
	reference<LogicalComparisonJoin> join;
	reference<LogicalCTERef> cte_ref;
	vector<TableColumnOrigin> core_keys;
	vector<idx_t> cte_columns;
};

static optional<DomainSemiInfo> FindDomainSemi(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION || op.type == LogicalOperatorType::LOGICAL_FILTER) {
		return op.children.size() == 1 ? FindDomainSemi(*op.children[0]) : nullopt;
	}
	if (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return nullopt;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	if (join.join_type == JoinType::INNER) {
		auto left = FindDomainSemi(*join.children[0]);
		auto right = FindDomainSemi(*join.children[1]);
		return left && !right ? left : right && !left ? right : nullopt;
	}
	if ((join.join_type != JoinType::SEMI && join.join_type != JoinType::RIGHT_SEMI) || join.HasProjectionMap() ||
	    join.HasArbitraryConditions() || join.conditions.empty()) {
		return nullopt;
	}
	const auto core_child = join.join_type == JoinType::RIGHT_SEMI ? idx_t(1) : idx_t(0);
	const auto domain_child = 1 - core_child;
	auto grouped = FindUnaryAggregate(*join.children[domain_child]);
	optional_ptr<LogicalOperator> domain_input;
	if (grouped) {
		if (!grouped->expressions.empty() || grouped->groups.size() != join.conditions.size() ||
		    grouped->children.size() != 1 || grouped->grouping_sets.size() > 1 ||
		    !grouped->grouping_functions.empty()) {
			return nullopt;
		}
		domain_input = grouped->children[0].get();
	} else {
		domain_input = join.children[domain_child].get();
	}
	auto cte_ref = GetDomainCTERef(*domain_input);
	if (!cte_ref) {
		return nullopt;
	}
	DomainSemiInfo result {join, *cte_ref, {}, {}};
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
			return nullopt;
		}
		auto &core_expression = core_child == 0 ? condition.GetLHS() : condition.GetRHS();
		auto &domain_expression = domain_child == 0 ? condition.GetLHS() : condition.GetRHS();
		auto core_key = GetTableColumnOrigin(*join.children[core_child], core_expression, true);
		auto domain_idx = GetDirectReferenceIndex(domain_expression, *join.children[domain_child]);
		if (!core_key || !domain_idx.IsValid()) {
			return nullopt;
		}
		auto cte_col = TraceDomainOutput(*join.children[domain_child], domain_idx.GetIndex(), cte_ref->cte_index);
		if (!cte_col.IsValid()) {
			return nullopt;
		}
		result.core_keys.push_back(*core_key);
		result.cte_columns.push_back(cte_col.GetIndex());
	}
	return result;
}

static optional_ptr<LogicalGet> FindSource(const RelationGraph &graph, TableCatalogEntry &table) {
	for (auto &source : graph.sources) {
		if (source.get().GetTable().get() == &table) {
			return source.get();
		}
	}
	return nullptr;
}

static optional_idx FindSourceColumn(LogicalGet &get, const ColumnIndex &column) {
	auto bindings = get.GetColumnBindings();
	for (idx_t idx = 0; idx < bindings.size(); idx++) {
		if (get.GetColumnIndex(ProjectionIndex(idx)) == column) {
			return optional_idx(idx);
		}
	}
	return optional_idx();
}

static optional_ptr<const TableFilter> FindSourceFilter(LogicalGet &get, const ColumnIndex &column) {
	for (auto &entry : get.table_filters) {
		if (get.GetColumnIndex(entry.GetIndex()) == column) {
			return entry.Filter();
		}
	}
	return nullptr;
}

static bool SameSourceFilter(const TableFilter &left, const TableFilter &right) {
	if (left.filter_type != TableFilterType::EXPRESSION_FILTER ||
	    right.filter_type != TableFilterType::EXPRESSION_FILTER) {
		return false;
	}
	return Expression::Equals(*left.Cast<ExpressionFilter>().expr, *right.Cast<ExpressionFilter>().expr);
}

static bool IsDomainCorrelationFilter(const TableFilter &filter, const TableColumnOrigin &subject,
                                      LogicalOperator &producer, const vector<TableColumnOrigin> &core_keys,
                                      const vector<TableColumnOrigin> &domain_keys) {
	if (filter.filter_type != TableFilterType::EXPRESSION_FILTER) {
		return false;
	}
	auto &expression = *filter.Cast<ExpressionFilter>().expr;
	if (expression.GetExpressionType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM || expression.IsVolatile()) {
		return false;
	}
	vector<reference<const Expression>> children;
	ExpressionIterator::EnumerateChildren(expression, [&](const Expression &child) { children.push_back(child); });
	if (children.size() == 2 && Expression::Equals(children[0].get(), children[1].get())) {
		return true;
	}
	if (children.size() == 2 && children[0].get().GetExpressionClass() == ExpressionClass::BOUND_REF &&
	    children[1].get().GetExpressionClass() == ExpressionClass::BOUND_REF &&
	    children[0].get().Cast<BoundReferenceExpression>().Index() ==
	        children[1].get().Cast<BoundReferenceExpression>().Index()) {
		return true;
	}
	optional<ColumnBinding> external_binding;
	idx_t reference_count = 0;
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expression, [&](const BoundColumnRefExpression &ref) {
		external_binding = ref.Binding();
		reference_count++;
	});
	if (reference_count != 1 || !external_binding) {
		return false;
	}
	auto external = FindBindingOrigin(producer, *external_binding);
	if (!external) {
		return false;
	}
	for (idx_t idx = 0; idx < domain_keys.size(); idx++) {
		if (SameOrigin(subject, core_keys[idx]) && SameOrigin(*external, domain_keys[idx])) {
			return true;
		}
	}
	return false;
}

static bool SourceFiltersMatch(const RelationGraph &core, const RelationGraph &producer, LogicalOperator &producer_op,
                               const vector<TableColumnOrigin> &core_keys,
                               const vector<TableColumnOrigin> &domain_keys) {
	for (auto &core_source_ref : core.sources) {
		auto &core_source = core_source_ref.get();
		auto table = core_source.GetTable();
		auto producer_source = table ? FindSource(producer, *table) : optional_ptr<LogicalGet>();
		if (!producer_source) {
			return false;
		}
		for (auto &entry : producer_source->table_filters) {
			auto column = producer_source->GetColumnIndex(entry.GetIndex());
			auto core_filter = FindSourceFilter(core_source, column);
			if (!core_filter || !SameSourceFilter(*core_filter, entry.Filter())) {
				return false;
			}
		}
		for (auto &entry : core_source.table_filters) {
			auto column = core_source.GetColumnIndex(entry.GetIndex());
			auto producer_filter = FindSourceFilter(*producer_source, column);
			if (producer_filter && SameSourceFilter(entry.Filter(), *producer_filter)) {
				continue;
			}
			TableColumnOrigin subject {*table, column};
			bool is_core_key = std::find_if(core_keys.begin(), core_keys.end(), [&](const TableColumnOrigin &key) {
				                   return SameOrigin(subject, key);
			                   }) != core_keys.end();
			if (!is_core_key ||
			    !IsDomainCorrelationFilter(entry.Filter(), subject, producer_op, core_keys, domain_keys)) {
				return false;
			}
		}
	}
	return true;
}

static optional_idx FindProducerOutput(LogicalOperator &producer, const TableColumnOrigin &origin) {
	auto bindings = producer.GetColumnBindings();
	for (idx_t idx = 0; idx < bindings.size(); idx++) {
		auto candidate = GetTableColumnOrigin(producer, idx, true);
		if (candidate && SameOrigin(*candidate, origin)) {
			return optional_idx(idx);
		}
	}
	return optional_idx();
}

static unique_ptr<Expression> RewriteToCTE(const Expression &expression, LogicalOperator &scope,
                                           LogicalOperator &producer, LogicalCTERef &cte_ref,
                                           const vector<pair<TableColumnOrigin, TableColumnOrigin>> &key_map,
                                           bool &success) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF ||
	    expression.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		auto output_idx = GetDirectReferenceIndex(expression, scope);
		if (!output_idx.IsValid()) {
			success = false;
			return nullptr;
		}
		if (scope.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = scope.Cast<LogicalProjection>();
			if (projection.children.size() != 1 || output_idx.GetIndex() >= projection.expressions.size()) {
				success = false;
				return nullptr;
			}
			return RewriteToCTE(*projection.expressions[output_idx.GetIndex()], *projection.children[0], producer,
			                    cte_ref, key_map, success);
		}
		auto origin = GetTableColumnOrigin(scope, output_idx.GetIndex(), true);
		if (!origin) {
			success = false;
			return nullptr;
		}
		for (auto &entry : key_map) {
			if (SameOrigin(*origin, entry.first)) {
				origin = entry.second;
				break;
			}
		}
		auto producer_idx = FindProducerOutput(producer, *origin);
		if (!producer_idx.IsValid() || producer_idx.GetIndex() >= cte_ref.types.size()) {
			success = false;
			return nullptr;
		}
		return make_uniq<BoundColumnRefExpression>(
		    cte_ref.types[producer_idx.GetIndex()],
		    ColumnBinding(cte_ref.table_index, ProjectionIndex(producer_idx.GetIndex())));
	}
	auto result = expression.Copy();
	ExpressionIterator::EnumerateChildren(*result, [&](unique_ptr<Expression> &child) {
		if (success) {
			child = RewriteToCTE(*child, scope, producer, cte_ref, key_map, success);
		}
	});
	return success ? std::move(result) : nullptr;
}

AggregateReuseOptimizer::AggregateReuseOptimizer(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

void AggregateReuseOptimizer::CollectCTEs(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte = op.Cast<LogicalMaterializedCTE>();
		cte_definitions.emplace(cte.table_index.index, cte);
	}
	for (auto &child : op.children) {
		CollectCTEs(*child);
	}
}

void AggregateReuseOptimizer::VisitOperator(unique_ptr<LogicalOperator> &op) {
	LogicalOperatorVisitor::VisitOperator(op);
	if (!TryReuseMaterializedAggregate(op)) {
		TryRewrite(op);
	}
}

unique_ptr<Expression> AggregateReuseOptimizer::VisitReplace(BoundColumnRefExpression &expression,
                                                             unique_ptr<Expression> *expression_ptr) {
	auto entry = replacement_map.find(expression.Binding());
	if (entry != replacement_map.end()) {
		expression.BindingMutable() = entry->second;
	}
	return nullptr;
}

bool AggregateReuseOptimizer::TryReuseMaterializedAggregate(unique_ptr<LogicalOperator> &op) {
	if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return false;
	}
	auto &aggregate = op->Cast<LogicalAggregate>();
	if (aggregate.children.size() != 1 || aggregate.expressions.empty() || !HasCompleteGrouping(aggregate)) {
		return false;
	}
	for (auto &group : aggregate.groups) {
		if (group->IsVolatile()) {
			return false;
		}
	}
	for (auto &expression : aggregate.expressions) {
		if (expression->IsVolatile()) {
			return false;
		}
	}

	auto domain = FindDomainSemi(*aggregate.children[0]);
	if (!domain || domain->core_keys.size() != aggregate.groups.size()) {
		return false;
	}
	auto cte_entry = cte_definitions.find(domain->cte_ref.get().cte_index.index);
	if (cte_entry == cte_definitions.end()) {
		return false;
	}
	auto &cte = cte_entry->second.get();
	if (cte.children.size() != 2 || cte.children[0]->types.size() != cte.column_count) {
		return false;
	}
	auto &producer_op = *cte.children[0];

	RelationGraph core_graph;
	RelationGraph producer_graph;
	auto core_ok = CollectInnerGraph(*aggregate.children[0], domain->join.get(), core_graph);
	auto producer_ok = CollectInnerGraph(producer_op, nullptr, producer_graph);
	if (!core_ok || !producer_ok || core_graph.sources.empty() ||
	    producer_graph.sources.size() != core_graph.sources.size() + 1) {
		return false;
	}
	for (auto &source : core_graph.sources) {
		auto table = source.get().GetTable();
		if (!table || !ContainsSource(producer_graph, *table)) {
			return false;
		}
	}
	optional_ptr<LogicalGet> extra_source;
	for (auto &source : producer_graph.sources) {
		auto table = source.get().GetTable();
		if (table && !ContainsSource(core_graph, *table)) {
			extra_source = source.get();
		}
	}
	if (!extra_source || !extra_source->GetTable()) {
		return false;
	}

	vector<bool> matched_core_edges(core_graph.edges.size(), false);
	vector<RelationEdge> cross_edges;
	for (auto &edge : producer_graph.edges) {
		const auto left_core = ContainsSource(core_graph, edge.left.table);
		const auto right_core = ContainsSource(core_graph, edge.right.table);
		if (left_core && right_core) {
			bool matched = false;
			for (idx_t idx = 0; idx < core_graph.edges.size(); idx++) {
				if (!matched_core_edges[idx] && SameEdge(edge, core_graph.edges[idx])) {
					matched_core_edges[idx] = true;
					matched = true;
					break;
				}
			}
			if (!matched) {
				return false;
			}
		} else if (left_core != right_core) {
			cross_edges.push_back(edge);
		} else {
			return false;
		}
	}
	if (std::find(matched_core_edges.begin(), matched_core_edges.end(), false) != matched_core_edges.end() ||
	    cross_edges.size() != domain->core_keys.size()) {
		return false;
	}

	vector<TableColumnOrigin> domain_keys;
	vector<pair<TableColumnOrigin, TableColumnOrigin>> key_map;
	vector<idx_t> extra_key_indices;
	vector<bool> matched_cross_edges(cross_edges.size(), false);
	for (idx_t key_idx = 0; key_idx < domain->core_keys.size(); key_idx++) {
		if (domain->cte_columns[key_idx] >= producer_op.GetColumnBindings().size()) {
			return false;
		}
		auto domain_key = GetTableColumnOrigin(producer_op, domain->cte_columns[key_idx], true);
		auto group_key = GetTableColumnOrigin(*aggregate.children[0], *aggregate.groups[key_idx], true);
		if (!domain_key || !group_key || !SameOrigin(*group_key, domain->core_keys[key_idx]) ||
		    &domain_key->table.get() != extra_source->GetTable().get()) {
			return false;
		}
		bool matched_cross = false;
		for (idx_t edge_idx = 0; edge_idx < cross_edges.size(); edge_idx++) {
			if (matched_cross_edges[edge_idx]) {
				continue;
			}
			auto &edge = cross_edges[edge_idx];
			if ((SameOrigin(edge.left, domain->core_keys[key_idx]) && SameOrigin(edge.right, *domain_key)) ||
			    (SameOrigin(edge.right, domain->core_keys[key_idx]) && SameOrigin(edge.left, *domain_key))) {
				matched_cross = true;
				matched_cross_edges[edge_idx] = true;
				break;
			}
		}
		if (!matched_cross) {
			return false;
		}
		auto extra_idx = FindSourceColumn(*extra_source, domain_key->column);
		if (!extra_idx.IsValid()) {
			return false;
		}
		domain_keys.push_back(*domain_key);
		extra_key_indices.push_back(extra_idx.GetIndex());
		key_map.emplace_back(domain->core_keys[key_idx], *domain_key);
	}
	auto unique = GetUniqueKeyProperty(*extra_source, extra_key_indices);
	auto filters_match = SourceFiltersMatch(core_graph, producer_graph, producer_op, domain->core_keys, domain_keys);
	if (!unique || !filters_match) {
		return false;
	}

	auto cte_ref_index = optimizer.binder.GenerateTableIndex();
	auto names = AggregateRewriteHelper::GenerateColumnNames("__aggregate_reuse", cte.column_count);
	auto replacement = make_uniq<LogicalCTERef>(cte_ref_index, cte.table_index, producer_op.types, std::move(names));
	replacement->estimated_cardinality = producer_op.estimated_cardinality;
	replacement->has_estimated_cardinality = producer_op.has_estimated_cardinality;
	replacement->ResolveOperatorTypes();
	bool success = true;
	vector<unique_ptr<Expression>> groups;
	vector<unique_ptr<Expression>> expressions;
	for (auto &group : aggregate.groups) {
		groups.push_back(RewriteToCTE(*group, *aggregate.children[0], producer_op, *replacement, key_map, success));
	}
	for (auto &expression : aggregate.expressions) {
		expressions.push_back(
		    RewriteToCTE(*expression, *aggregate.children[0], producer_op, *replacement, key_map, success));
	}
	if (!success) {
		return false;
	}
	aggregate.groups = std::move(groups);
	aggregate.expressions = std::move(expressions);
	aggregate.children[0] = std::move(replacement);
	aggregate.ResolveOperatorTypes();
	return true;
}

bool AggregateReuseOptimizer::TryRewrite(unique_ptr<LogicalOperator> &op) {
	if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return false;
	}
	auto &aggregate = op->Cast<LogicalAggregate>();
	if (aggregate.children.size() != 1 || aggregate.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = aggregate.children[0]->Cast<LogicalComparisonJoin>();
	auto candidate = GetCandidate(aggregate, join);
	if (!candidate) {
		return false;
	}
	auto &outer_aggregate = aggregate.expressions[0]->Cast<BoundAggregateExpression>();
	auto &owner = join.children[candidate->owner_child];
	auto &probe = join.children[candidate->probe_child];
	auto owner_bindings = owner->GetColumnBindings();
	auto outer_key_origin = GetTableColumnOrigin(*probe, candidate->probe_key);
	if (!outer_key_origin || candidate->owner_key >= owner_bindings.size()) {
		return false;
	}
	auto planned_payload = PromoteReusableAggregate(owner, owner_bindings[candidate->owner_key], *probe,
	                                                *outer_key_origin, outer_aggregate, false);
	if (!planned_payload) {
		return false;
	}
	auto payload_binding = PromoteReusableAggregate(owner, owner_bindings[candidate->owner_key], *probe,
	                                                *outer_key_origin, outer_aggregate, true);
	if (!payload_binding || *payload_binding != *planned_payload) {
		throw InternalException("Aggregate payload changed after validating the owner plan");
	}
	owner_bindings = owner->GetColumnBindings();
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(aggregate.groups.size() + 1);
	for (idx_t group_idx = 0; group_idx < candidate->owner_groups.size(); group_idx++) {
		auto owner_entry = std::find(owner_bindings.begin(), owner_bindings.end(), candidate->owner_groups[group_idx]);
		if (owner_entry == owner_bindings.end()) {
			throw InternalException("Aggregate owner group changed after validation");
		}
		auto owner_idx = NumericCast<idx_t>(owner_entry - owner_bindings.begin());
		expressions.push_back(make_uniq<BoundColumnRefExpression>(aggregate.groups[group_idx]->GetAlias(),
		                                                          aggregate.groups[group_idx]->GetReturnType(),
		                                                          owner_bindings[owner_idx]));
	}
	expressions.push_back(make_uniq<BoundColumnRefExpression>(outer_aggregate.GetAlias(),
	                                                          outer_aggregate.GetReturnType(), *planned_payload));
	replacement_map[ColumnBinding(aggregate.aggregate_index, ProjectionIndex(0))] =
	    ColumnBinding(aggregate.group_index, ProjectionIndex(aggregate.groups.size()));
	auto projection = make_uniq<LogicalProjection>(aggregate.group_index, std::move(expressions));
	projection->estimated_cardinality = aggregate.estimated_cardinality;
	projection->has_estimated_cardinality = aggregate.has_estimated_cardinality;
	projection->children.push_back(std::move(owner));
	projection->ResolveOperatorTypes();
	op = std::move(projection);
	return true;
}

} // namespace duckdb
