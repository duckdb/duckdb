#include "duckdb/optimizer/distinct_aggregate_rewriter.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/optimizer/aggregate_rewrite_helper.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

DistinctAggregateRewriter::DistinctAggregateRewriter(Optimizer &optimizer_p) : optimizer(optimizer_p) {
}

namespace {

struct DistinctAggregateSet {
	explicit DistinctAggregateSet(idx_t source_index) : source_index(source_index) {
		aggregate_indices.push_back(source_index);
	}

	idx_t source_index;
	vector<idx_t> aggregate_indices;
	vector<unique_ptr<Expression>> order_expressions;
};

struct BranchResult {
	unique_ptr<LogicalOperator> plan;
	TableIndex table_index;
	vector<idx_t> aggregate_indices;
};

static optional_idx FindExpression(const vector<unique_ptr<Expression>> &expressions, const Expression &needle) {
	for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
		if (Expression::Equals(*expressions[expr_idx], needle)) {
			return expr_idx;
		}
	}
	return optional_idx();
}

static void AddOrderExpressions(DistinctAggregateSet &set, const BoundAggregateExpression &aggregate) {
	if (!aggregate.GetOrderBys()) {
		return;
	}
	for (auto &order : aggregate.GetOrderBys()->orders) {
		if (FindExpression(aggregate.GetChildren(), *order.expression).IsValid()) {
			continue;
		}
		if (!FindExpression(set.order_expressions, *order.expression).IsValid()) {
			set.order_expressions.push_back(order.expression->Copy());
		}
	}
}

static unique_ptr<BoundAggregateExpression> CreateFinalAggregate(const BoundAggregateExpression &source,
                                                                 const DistinctAggregateSet &set,
                                                                 TableIndex input_table, idx_t input_column_offset,
                                                                 idx_t order_column_offset,
                                                                 optional_idx filter_column_offset = optional_idx()) {
	auto result = unique_ptr_cast<Expression, BoundAggregateExpression>(source.Copy());
	result->GetAggregateTypeMutable() = AggregateType::NON_DISTINCT;
	result->GetChildrenMutable().clear();
	for (idx_t child_idx = 0; child_idx < source.GetChildren().size(); child_idx++) {
		auto &child = source.GetChildren()[child_idx];
		result->GetChildrenMutable().push_back(make_uniq<BoundColumnRefExpression>(
		    child->GetReturnType(), ColumnBinding(input_table, ProjectionIndex(input_column_offset + child_idx))));
	}
	if (source.GetOrderBys()) {
		result->GetOrderBysMutable() = make_uniq<BoundOrderModifier>();
		for (auto &order : source.GetOrderBys()->orders) {
			auto order_idx = FindExpression(source.GetChildren(), *order.expression);
			idx_t column_offset;
			if (order_idx.IsValid()) {
				column_offset = input_column_offset + order_idx.GetIndex();
			} else {
				order_idx = FindExpression(set.order_expressions, *order.expression);
				D_ASSERT(order_idx.IsValid());
				column_offset = order_column_offset + order_idx.GetIndex();
			}
			result->GetOrderBysMutable()->orders.emplace_back(
			    order.type, order.null_order,
			    make_uniq<BoundColumnRefExpression>(order.expression->GetReturnType(),
			                                        ColumnBinding(input_table, ProjectionIndex(column_offset))));
		}
	} else {
		result->GetOrderBysMutable().reset();
	}
	if (filter_column_offset.IsValid()) {
		result->GetFilterMutable() = make_uniq<BoundColumnRefExpression>(
		    LogicalType::BOOLEAN, ColumnBinding(input_table, ProjectionIndex(filter_column_offset.GetIndex())));
	} else {
		result->GetFilterMutable().reset();
	}
	return result;
}

static unique_ptr<LogicalOperator> CreateProjection(Optimizer &optimizer, unique_ptr<LogicalOperator> child,
                                                    TableIndex child_group_index, TableIndex child_aggregate_index,
                                                    const vector<unique_ptr<Expression>> &groups,
                                                    const vector<unique_ptr<Expression>> &aggregates,
                                                    const vector<idx_t> &aggregate_indices,
                                                    TableIndex &projection_index) {
	vector<unique_ptr<Expression>> projection_expressions;
	projection_expressions.reserve(groups.size() + aggregate_indices.size());
	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		projection_expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    groups[group_idx]->GetReturnType(), ColumnBinding(child_group_index, ProjectionIndex(group_idx))));
	}
	for (idx_t aggregate_idx = 0; aggregate_idx < aggregate_indices.size(); aggregate_idx++) {
		auto &source = aggregates[aggregate_indices[aggregate_idx]];
		projection_expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    source->GetReturnType(), ColumnBinding(child_aggregate_index, ProjectionIndex(aggregate_idx))));
	}
	projection_index = optimizer.binder.GenerateTableIndex();
	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(projection_expressions));
	projection->children.push_back(std::move(child));
	return std::move(projection);
}

static BranchResult CreateDistinctBranch(Optimizer &optimizer, LogicalAggregate &aggr, const DistinctAggregateSet &set,
                                         unique_ptr<LogicalOperator> input,
                                         const column_binding_map_t<ColumnBinding> &input_replacements) {
	const idx_t group_count = aggr.groups.size();
	auto &source_aggregate = aggr.expressions[set.source_index]->Cast<BoundAggregateExpression>();

	vector<unique_ptr<Expression>> distinct_groups;
	distinct_groups.reserve(group_count + source_aggregate.GetChildren().size() + set.order_expressions.size() +
	                        (source_aggregate.GetFilter() ? 1 : 0));
	for (auto &group : aggr.groups) {
		distinct_groups.push_back(AggregateRewriteHelper::CopyAndRebind(*group, input_replacements));
	}
	for (auto &child : source_aggregate.GetChildren()) {
		distinct_groups.push_back(AggregateRewriteHelper::CopyAndRebind(*child, input_replacements));
	}
	for (auto &order_expr : set.order_expressions) {
		distinct_groups.push_back(AggregateRewriteHelper::CopyAndRebind(*order_expr, input_replacements));
	}
	optional_idx filter_column_offset;
	if (source_aggregate.GetFilter()) {
		// Keeping FILTER as a deduplication key preserves argument evaluation and groups with no qualifying rows.
		filter_column_offset = distinct_groups.size();
		distinct_groups.push_back(
		    AggregateRewriteHelper::CopyAndRebind(*source_aggregate.GetFilter(), input_replacements));
	}

	auto distinct_group_index = optimizer.binder.GenerateTableIndex();
	auto distinct_aggregate_index = optimizer.binder.GenerateTableIndex();
	auto distinct =
	    make_uniq<LogicalAggregate>(distinct_group_index, distinct_aggregate_index, vector<unique_ptr<Expression>>());
	distinct->groups = std::move(distinct_groups);
	distinct->children.push_back(std::move(input));

	vector<unique_ptr<Expression>> final_groups;
	final_groups.reserve(group_count);
	for (idx_t group_idx = 0; group_idx < group_count; group_idx++) {
		final_groups.push_back(make_uniq<BoundColumnRefExpression>(
		    aggr.groups[group_idx]->GetReturnType(), ColumnBinding(distinct_group_index, ProjectionIndex(group_idx))));
	}

	vector<unique_ptr<Expression>> final_aggregates;
	final_aggregates.reserve(set.aggregate_indices.size());
	const auto order_column_offset = group_count + source_aggregate.GetChildren().size();
	for (auto aggregate_idx : set.aggregate_indices) {
		auto &aggregate = aggr.expressions[aggregate_idx]->Cast<BoundAggregateExpression>();
		final_aggregates.push_back(CreateFinalAggregate(aggregate, set, distinct_group_index, group_count,
		                                                order_column_offset, filter_column_offset));
	}

	auto final_group_index = optimizer.binder.GenerateTableIndex();
	auto final_aggregate_index = optimizer.binder.GenerateTableIndex();
	auto final = make_uniq<LogicalAggregate>(final_group_index, final_aggregate_index, std::move(final_aggregates));
	final->groups = std::move(final_groups);
	final->children.push_back(std::move(distinct));
	if (aggr.has_estimated_cardinality) {
		final->SetEstimatedCardinality(aggr.estimated_cardinality);
	}

	BranchResult result;
	result.aggregate_indices = set.aggregate_indices;
	result.plan = CreateProjection(optimizer, std::move(final), final_group_index, final_aggregate_index, aggr.groups,
	                               aggr.expressions, result.aggregate_indices, result.table_index);
	return result;
}

static BranchResult CreateRegularBranch(Optimizer &optimizer, LogicalAggregate &aggr,
                                        const vector<idx_t> &aggregate_indices, unique_ptr<LogicalOperator> input,
                                        const column_binding_map_t<ColumnBinding> &input_replacements) {
	vector<unique_ptr<Expression>> regular_groups;
	regular_groups.reserve(aggr.groups.size());
	for (auto &group : aggr.groups) {
		regular_groups.push_back(AggregateRewriteHelper::CopyAndRebind(*group, input_replacements));
	}

	vector<unique_ptr<Expression>> regular_aggregates;
	regular_aggregates.reserve(aggregate_indices.size());
	for (auto aggregate_idx : aggregate_indices) {
		regular_aggregates.push_back(
		    AggregateRewriteHelper::CopyAndRebind(*aggr.expressions[aggregate_idx], input_replacements));
	}

	auto regular_group_index = optimizer.binder.GenerateTableIndex();
	auto regular_aggregate_index = optimizer.binder.GenerateTableIndex();
	auto regular =
	    make_uniq<LogicalAggregate>(regular_group_index, regular_aggregate_index, std::move(regular_aggregates));
	regular->groups = std::move(regular_groups);
	regular->children.push_back(std::move(input));
	if (aggr.has_estimated_cardinality) {
		regular->SetEstimatedCardinality(aggr.estimated_cardinality);
	}

	BranchResult result;
	result.aggregate_indices = aggregate_indices;
	result.plan = CreateProjection(optimizer, std::move(regular), regular_group_index, regular_aggregate_index,
	                               aggr.groups, aggr.expressions, result.aggregate_indices, result.table_index);
	return result;
}

static unique_ptr<LogicalOperator> JoinBranches(const vector<BranchResult> &branches,
                                                vector<unique_ptr<LogicalOperator>> branch_plans,
                                                const vector<unique_ptr<Expression>> &groups) {
	D_ASSERT(!branch_plans.empty());
	if (groups.empty()) {
		// Ungrouped aggregate branches each produce one row, including for empty inputs.
		auto result = std::move(branch_plans[0]);
		for (idx_t branch_idx = 1; branch_idx < branch_plans.size(); branch_idx++) {
			result = LogicalCrossProduct::Create(std::move(result), std::move(branch_plans[branch_idx]));
		}
		return result;
	}

	// Every branch consumes the same rows and retains every original group; null-safe inner joins preserve NULL keys.
	auto result = std::move(branch_plans[0]);
	const auto anchor_table = branches[0].table_index;
	for (idx_t branch_idx = 1; branch_idx < branch_plans.size(); branch_idx++) {
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
			auto left = make_uniq<BoundColumnRefExpression>(groups[group_idx]->GetReturnType(),
			                                                ColumnBinding(anchor_table, ProjectionIndex(group_idx)));
			auto right = make_uniq<BoundColumnRefExpression>(
			    groups[group_idx]->GetReturnType(),
			    ColumnBinding(branches[branch_idx].table_index, ProjectionIndex(group_idx)));
			join->conditions.emplace_back(std::move(left), std::move(right), ExpressionType::COMPARE_NOT_DISTINCT_FROM);
		}
		join->children.push_back(std::move(result));
		join->children.push_back(std::move(branch_plans[branch_idx]));
		result = std::move(join);
	}
	return result;
}

} // namespace

bool DistinctAggregateRewriter::TryRewrite(unique_ptr<LogicalOperator> &op) {
	if (op->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY || op->children.size() != 1) {
		return false;
	}
	auto &aggr = op->Cast<LogicalAggregate>();
	if (aggr.grouping_sets.size() > 1 || aggr.expressions.empty()) {
		return false;
	}
	if (aggr.grouping_sets.size() == 1) {
		if (aggr.grouping_sets[0].size() != aggr.groups.size()) {
			return false;
		}
		for (idx_t group_idx = 0; group_idx < aggr.groups.size(); group_idx++) {
			if (aggr.grouping_sets[0].find(ProjectionIndex(group_idx)) == aggr.grouping_sets[0].end()) {
				return false;
			}
		}
	}

	bool has_distinct = false;
	for (auto &expr : aggr.expressions) {
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			return false;
		}
		auto &aggregate = expr->Cast<BoundAggregateExpression>();
		if (aggregate.IsDistinct()) {
			has_distinct = true;
		}
	}
	if (!has_distinct) {
		return false;
	}

	AggregateRewriteHelper::StageVolatileAggregateInputs(optimizer, aggr, op->children[0]);

	vector<DistinctAggregateSet> distinct_sets;
	vector<idx_t> regular_aggregates;
	for (idx_t aggregate_idx = 0; aggregate_idx < aggr.expressions.size(); aggregate_idx++) {
		auto &expr = aggr.expressions[aggregate_idx];
		auto &aggregate = expr->Cast<BoundAggregateExpression>();
		if (!aggregate.IsDistinct()) {
			regular_aggregates.push_back(aggregate_idx);
			continue;
		}
		bool found_match = false;
		for (auto &set : distinct_sets) {
			auto &other = aggr.expressions[set.source_index]->Cast<BoundAggregateExpression>();
			if (Expression::ListEquals(aggregate.GetChildren(), other.GetChildren()) &&
			    Expression::Equals(aggregate.GetFilter(), other.GetFilter())) {
				set.aggregate_indices.push_back(aggregate_idx);
				AddOrderExpressions(set, aggregate);
				found_match = true;
				break;
			}
		}
		if (!found_match) {
			distinct_sets.emplace_back(aggregate_idx);
			AddOrderExpressions(distinct_sets.back(), aggregate);
		}
	}
	if (distinct_sets.empty()) {
		return false;
	}

	const bool needs_cte = distinct_sets.size() + (regular_aggregates.empty() ? 0 : 1) > 1;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;
	vector<ColumnBinding> input_bindings;
	TableIndex cte_index;
	if (needs_cte) {
		op->children[0]->ResolveOperatorTypes();
		input_types = op->children[0]->types;
		input_names = AggregateRewriteHelper::GenerateColumnNames("__distinct_input", input_types.size());
		input_bindings = op->children[0]->GetColumnBindings();
		cte_index = optimizer.binder.GenerateTableIndex();
	}

	vector<BranchResult> branches;
	vector<unique_ptr<LogicalOperator>> branch_plans;
	auto CreateBranchInput = [&](column_binding_map_t<ColumnBinding> &input_replacements) {
		if (!needs_cte) {
			return std::move(op->children[0]);
		}
		return AggregateRewriteHelper::CreateCTERef(optimizer, cte_index, input_types, input_names, input_bindings,
		                                            input_replacements);
	};

	for (auto &set : distinct_sets) {
		column_binding_map_t<ColumnBinding> input_replacements;
		auto branch =
		    CreateDistinctBranch(optimizer, aggr, set, CreateBranchInput(input_replacements), input_replacements);
		branch_plans.push_back(std::move(branch.plan));
		branches.push_back(std::move(branch));
	}
	if (!regular_aggregates.empty()) {
		column_binding_map_t<ColumnBinding> input_replacements;
		auto branch = CreateRegularBranch(optimizer, aggr, regular_aggregates, CreateBranchInput(input_replacements),
		                                  input_replacements);
		branch_plans.push_back(std::move(branch.plan));
		branches.push_back(std::move(branch));
	}

	vector<ColumnBinding> aggregate_bindings(aggr.expressions.size());
	for (auto &branch : branches) {
		for (idx_t local_idx = 0; local_idx < branch.aggregate_indices.size(); local_idx++) {
			aggregate_bindings[branch.aggregate_indices[local_idx]] =
			    ColumnBinding(branch.table_index, ProjectionIndex(aggr.groups.size() + local_idx));
		}
	}

	auto joined = JoinBranches(branches, std::move(branch_plans), aggr.groups);

	vector<unique_ptr<Expression>> projection_expressions;
	projection_expressions.reserve(aggr.groups.size() + aggr.expressions.size() + aggr.grouping_functions.size());
	const auto final_projection_index = optimizer.binder.GenerateTableIndex();
	for (idx_t group_idx = 0; group_idx < aggr.groups.size(); group_idx++) {
		projection_expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(aggr.groups[group_idx]->GetReturnType(),
		                                        ColumnBinding(branches[0].table_index, ProjectionIndex(group_idx))));
		replacement_map[ColumnBinding(aggr.group_index, ProjectionIndex(group_idx))] =
		    ColumnBinding(final_projection_index, ProjectionIndex(group_idx));
	}
	for (idx_t aggregate_idx = 0; aggregate_idx < aggr.expressions.size(); aggregate_idx++) {
		projection_expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    aggr.expressions[aggregate_idx]->GetReturnType(), aggregate_bindings[aggregate_idx]));
		replacement_map[ColumnBinding(aggr.aggregate_index, ProjectionIndex(aggregate_idx))] =
		    ColumnBinding(final_projection_index, ProjectionIndex(aggr.groups.size() + aggregate_idx));
	}
	for (idx_t grouping_idx = 0; grouping_idx < aggr.grouping_functions.size(); grouping_idx++) {
		projection_expressions.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
		replacement_map[ColumnBinding(aggr.groupings_index, ProjectionIndex(grouping_idx))] = ColumnBinding(
		    final_projection_index, ProjectionIndex(aggr.groups.size() + aggr.expressions.size() + grouping_idx));
	}

	unique_ptr<LogicalOperator> result =
	    make_uniq<LogicalProjection>(final_projection_index, std::move(projection_expressions));
	result->children.push_back(std::move(joined));
	if (aggr.has_estimated_cardinality) {
		result->SetEstimatedCardinality(aggr.estimated_cardinality);
	}

	if (needs_cte) {
		// DEFAULT keeps the shared input eligible for direct streaming fan-out during CTE planning.
		auto cte_name = Identifier(StringUtil::Format("__distinct_aggregate_cte_%llu", cte_index.index));
		result = make_uniq<LogicalMaterializedCTE>(std::move(cte_name), cte_index, input_types.size(),
		                                           std::move(op->children[0]), std::move(result),
		                                           CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
		if (aggr.has_estimated_cardinality) {
			result->SetEstimatedCardinality(aggr.estimated_cardinality);
		}
	}

	result->ResolveOperatorTypes();
	op = std::move(result);
	return true;
}

void DistinctAggregateRewriter::VisitOperator(unique_ptr<LogicalOperator> &op) {
	LogicalOperatorVisitor::VisitOperator(op);
	TryRewrite(op);
}

unique_ptr<Expression> DistinctAggregateRewriter::VisitReplace(BoundColumnRefExpression &expr,
                                                               unique_ptr<Expression> *expr_ptr) {
	auto entry = replacement_map.find(expr.Binding());
	if (entry != replacement_map.end()) {
		expr.BindingMutable() = entry->second;
	}
	return nullptr;
}

} // namespace duckdb
