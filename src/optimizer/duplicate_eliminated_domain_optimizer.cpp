#include "duckdb/optimizer/duplicate_eliminated_domain_optimizer.hpp"

#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/optimizer/aggregate_rewrite_helper.hpp"
#include "duckdb/optimizer/expression_heuristics.hpp"
#include "duckdb/optimizer/join_filter_pushdown_optimizer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_extractor.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_deep_copy.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/logical_operator_repeatability.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_column_data_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/parser/expression_map.hpp"

#include <cmath>
#include <limits>

namespace duckdb {

DuplicateEliminatedDomainOptimizer::DuplicateEliminatedDomainOptimizer(Optimizer &optimizer_p)
    : optimizer(optimizer_p) {
}

static bool HasImplicitBitstringAggregate(const LogicalOperator &op) {
	bool found = false;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](const unique_ptr<Expression> *expression) {
		ExpressionIterator::VisitExpression<BoundAggregateExpression>(**expression, [&](const auto &aggregate) {
			found |= aggregate.Function().GetName() == "bitstring_agg" && aggregate.Function().HasStatisticsCallback();
		});
		ExpressionIterator::VisitExpression<BoundWindowExpression>(**expression, [&](const auto &window) {
			auto &function = window.AggregateFunction();
			found |= function && function->GetName() == "bitstring_agg" && function->HasStatisticsCallback();
		});
	});
	if (found) {
		return true;
	}
	for (auto &child : op.children) {
		if (HasImplicitBitstringAggregate(*child)) {
			return true;
		}
	}
	return false;
}

void DuplicateEliminatedDomainOptimizer::Optimize(unique_ptr<LogicalOperator> &op) {
	// Implicit bitstring bounds depend on statistics, including those propagated through CTE references.
	if (!HasImplicitBitstringAggregate(*op)) {
		VisitOperator(op);
		op->ResolveOperatorTypes();
	}
}

static bool IsWorkReference(const Expression &expression) {
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF:
		return expression.Cast<BoundColumnRefExpression>().Depth() == 0;
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_PARAMETER:
		return true;
	default:
		return false;
	}
}

static optional_idx ReferenceIndex(const Expression &expression, LogicalOperator &input) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
	    expression.Cast<BoundColumnRefExpression>().Depth() != 0) {
		return {};
	}
	return AggregateRewriteHelper::GetDirectReferenceIndex(expression, input);
}

static optional_idx ReferenceIndex(const Expression &expression, LogicalOperator &input, const LogicalType &type) {
	if (!expression.GetReturnType().EqualsIncludingCollation(type)) {
		return {};
	}
	return ReferenceIndex(expression, input);
}

static bool HasSimpleGrouping(const LogicalAggregate &aggregate) {
	return !aggregate.groups.empty() && aggregate.grouping_functions.empty() &&
	       (aggregate.grouping_sets.empty() ||
	        (aggregate.grouping_sets.size() == 1 && aggregate.grouping_sets[0].size() == aggregate.groups.size()));
}

//! One full grouping set preserves the directly referenced key and its comparison semantics.
static optional_idx GroupKeyIndex(LogicalAggregate &aggregate, idx_t key) {
	if (!HasSimpleGrouping(aggregate) || key >= aggregate.groups.size()) {
		return {};
	}
	return ReferenceIndex(*aggregate.groups[key], *aggregate.children[0]);
}

static optional_idx GroupKeyIndex(LogicalAggregate &aggregate, idx_t key, const LogicalType &type) {
	auto index = GroupKeyIndex(aggregate, key);
	if (!index.IsValid() || !aggregate.groups[key]->GetReturnType().EqualsIncludingCollation(type)) {
		return {};
	}
	return index;
}

struct AggregateKeyMatch {
	reference<LogicalAggregate> aggregate;
	vector<idx_t> group_indices;
};

//! Trace output positions to the complete grouping key.
static optional<AggregateKeyMatch> FindAggregate(LogicalOperator &op, vector<idx_t> keys) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		for (auto &expression : op.expressions) {
			if (expression->CanThrow() || expression->IsVolatile()) {
				return {};
			}
		}
		for (auto &key : keys) {
			if (key >= op.expressions.size()) {
				return {};
			}
			auto index = ReferenceIndex(*op.expressions[key], *op.children[0]);
			if (!index.IsValid()) {
				return {};
			}
			key = index.GetIndex();
		}
		return FindAggregate(*op.children[0], std::move(keys));
	}
	if (op.type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return {};
	}
	auto &aggregate = op.Cast<LogicalAggregate>();
	if (aggregate.preserve_input_order || aggregate.groups.size() != keys.size() || !HasSimpleGrouping(aggregate)) {
		return {};
	}
	vector<bool> matched(keys.size(), false);
	for (auto key : keys) {
		if (key >= keys.size() || matched[key] ||
		    !ReferenceIndex(*aggregate.groups[key], *aggregate.children[0]).IsValid()) {
			return {};
		}
		matched[key] = true;
	}
	for (auto &expression : aggregate.expressions) {
		bool supported = true;
		ExpressionIterator::EnumerateChildren(*expression,
		                                      [&](const Expression &child) { supported &= IsWorkReference(child); });
		if (!supported) {
			return {};
		}
	}
	if (!LogicalSubtreeIsRepeatable(aggregate)) {
		return {};
	}
	return AggregateKeyMatch {aggregate, std::move(keys)};
}

static optional_ptr<LogicalAggregate> FindAggregate(LogicalOperator &op, idx_t key) {
	auto match = FindAggregate(op, vector<idx_t> {key});
	return match ? &match->aggregate.get() : nullptr;
}

//! An upper bound from the actual closed input, not a cardinality estimate.
static optional_idx FixedRowCount(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		return 1;
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return 0;
	case LogicalOperatorType::LOGICAL_CHUNK_GET: {
		auto &get = op.Cast<LogicalColumnDataGet>();
		return get.collection ? optional_idx(get.collection->Count()) : optional_idx();
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		if (op.children.size() == 1 && op.children[0]->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
			return op.Cast<LogicalExpressionGet>().expressions.size();
		}
		return {};
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		// Empty grouping sets can produce a row from empty input.
		if (aggregate.grouping_sets.size() != 1 || aggregate.grouping_sets[0].empty()) {
			return {};
		}
		return FixedRowCount(*op.children[0]);
	}
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
		return FixedRowCount(*op.children[0]);
	default:
		return {};
	}
}

static bool AppendFixedKey(vector<vector<unique_ptr<Expression>>> &rows, const Expression &expression,
                           const LogicalType &type) {
	if ((expression.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT &&
	     expression.GetExpressionClass() != ExpressionClass::BOUND_PARAMETER) ||
	    !expression.GetReturnType().EqualsIncludingCollation(type)) {
		return false;
	}
	vector<unique_ptr<Expression>> row;
	row.push_back(expression.Copy());
	rows.push_back(std::move(row));
	return true;
}

//! Copy keys already represented by expressions, leaving columnar inputs on the runtime path.
//! The caller's payload and its evaluation remain in place.
static bool CopyFixedKeys(LogicalOperator &op, idx_t key, const LogicalType &type,
                          vector<vector<unique_ptr<Expression>>> &rows) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &expression = *op.expressions[key];
		auto index = ReferenceIndex(expression, *op.children[0]);
		if (index.IsValid()) {
			return CopyFixedKeys(*op.children[0], index.GetIndex(), type, rows);
		}
		auto count = FixedRowCount(*op.children[0]);
		return count.IsValid() && (count.GetIndex() == 0 || AppendFixedKey(rows, expression, type));
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		auto binding = op.GetColumnBindings()[key];
		BoundColumnRefExpression reference(type, binding);
		auto index = ReferenceIndex(reference, *op.children[0]);
		return index.IsValid() && CopyFixedKeys(*op.children[0], index.GetIndex(), type, rows);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto index = GroupKeyIndex(op.Cast<LogicalAggregate>(), key, type);
		return index.IsValid() && CopyFixedKeys(*op.children[0], index.GetIndex(), type, rows);
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		if (!FixedRowCount(op).IsValid()) {
			return false;
		}
		for (auto &row : op.Cast<LogicalExpressionGet>().expressions) {
			if (!AppendFixedKey(rows, *row[key], type)) {
				return false;
			}
		}
		return true;
	}
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return true;
	default:
		return false;
	}
}

//! The old restriction must be proven to contain every demanded key.
static bool CopyExactFixedKeys(LogicalOperator &op, idx_t key, const LogicalType &type,
                               vector<vector<unique_ptr<Expression>>> &rows) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &expression = *op.expressions[key];
		auto index = ReferenceIndex(expression, *op.children[0], type);
		if (index.IsValid()) {
			return CopyExactFixedKeys(*op.children[0], index.GetIndex(), type, rows);
		}
		return op.children[0]->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN &&
		       AppendFixedKey(rows, expression, type);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto index = GroupKeyIndex(op.Cast<LogicalAggregate>(), key, type);
		return index.IsValid() && CopyExactFixedKeys(*op.children[0], index.GetIndex(), type, rows);
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &join = op.Cast<LogicalJoin>();
		if (join.join_type != JoinType::LEFT) {
			return false;
		}
		BoundColumnRefExpression reference(type, op.GetColumnBindings()[key]);
		auto index = ReferenceIndex(reference, *op.children[0]);
		return index.IsValid() && CopyExactFixedKeys(*op.children[0], index.GetIndex(), type, rows);
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return CopyFixedKeys(op, key, type, rows);
	default:
		return false;
	}
}

enum class RestrictionAction { KEEP, REPLACE, REMOVE };

//! The chosen action and the work saved if the restriction is removed.
struct RestrictionDecision {
	RestrictionAction action = RestrictionAction::KEEP;
	double removal_savings = 0;
};

static unique_ptr<LogicalAggregate> CreateKeyDomain(Optimizer &optimizer, unique_ptr<LogicalOperator> input,
                                                    unique_ptr<Expression> key) {
	auto domain = make_uniq<LogicalAggregate>(optimizer.binder.GenerateTableIndex(),
	                                          optimizer.binder.GenerateTableIndex(), vector<unique_ptr<Expression>>());
	domain->groups.push_back(std::move(key));
	domain->grouping_sets.push_back({ProjectionIndex(0)});
	domain->children.push_back(std::move(input));
	return domain;
}

static unique_ptr<Expression> DomainKey(const LogicalAggregate &domain) {
	return make_uniq<BoundColumnRefExpression>(domain.groups[0]->GetReturnType(),
	                                           ColumnBinding(domain.group_index, ProjectionIndex(0)));
}

static optional_ptr<const LogicalMaterializedCTE>
FindVisibleOwner(TableIndex index, const vector<reference<LogicalMaterializedCTE>> &cte_scopes) {
	for (auto &owner : cte_scopes) {
		if (owner.get().table_index == index) {
			return owner.get();
		}
	}
	return nullptr;
}

//! Guaranteed bounds valid before and after expanding a domain; callbacks only modify expression copies.
class DomainAggregateBounds {
public:
	DomainAggregateBounds(ClientContext &context_p, const vector<reference<LogicalMaterializedCTE>> &scopes_p)
	    : context(context_p), scopes(scopes_p) {
	}

	bool CanEvaluate(LogicalAggregate &aggregate) const {
		for (auto &expression : aggregate.expressions) {
			if (!expression->CanThrow()) {
				continue;
			}
			if (expression->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				return false;
			}
			auto copy = expression->Copy();
			column_binding_set_t visited;
			AggregateStats(copy->Cast<BoundAggregateExpression>(), *aggregate.children[0], visited);
			if (copy->CanThrow()) {
				return false;
			}
		}
		return true;
	}

private:
	optional_idx MaxRows(LogicalOperator &input, unordered_set<TableIndex> &visited) const {
		switch (input.type) {
		case LogicalOperatorType::LOGICAL_GET: {
			auto &get = input.Cast<LogicalGet>();
			if (!get.children.empty() || !get.function.cardinality || !get.bind_data) {
				return {};
			}
			auto stats = get.function.cardinality(context, get.bind_data.get());
			return stats && stats->has_max_cardinality ? optional_idx(stats->max_cardinality) : optional_idx();
		}
		case LogicalOperatorType::LOGICAL_PROJECTION:
		case LogicalOperatorType::LOGICAL_FILTER:
			return MaxRows(*input.children[0], visited);
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggregate = input.Cast<LogicalAggregate>();
			if (aggregate.groups.empty() && aggregate.grouping_sets.size() <= 1) {
				return 1;
			}
			return HasSimpleGrouping(aggregate) ? MaxRows(*input.children[0], visited) : optional_idx();
		}
		case LogicalOperatorType::LOGICAL_CTE_REF: {
			auto &reference = input.Cast<LogicalCTERef>();
			auto owner = FindVisibleOwner(reference.cte_index, scopes);
			// Reject repeated definitions as well as cycles: a CTE DAG can otherwise require exponential work.
			if (!owner || reference.correlated_columns || reference.is_recurring ||
			    !visited.insert(reference.cte_index).second) {
				return {};
			}
			return MaxRows(*owner->children[0], visited);
		}
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
			if (input.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
				auto &join = input.Cast<LogicalComparisonJoin>();
				if (join.join_type == JoinType::SEMI || join.join_type == JoinType::ANTI) {
					return MaxRows(*input.children[0], visited);
				}
				if (join.join_type != JoinType::INNER) {
					return {};
				}
			}
			auto left = MaxRows(*input.children[0], visited);
			auto right = MaxRows(*input.children[1], visited);
			idx_t product;
			return left.IsValid() && right.IsValid() &&
			               TryMultiplyOperator::Operation(left.GetIndex(), right.GetIndex(), product)
			           ? optional_idx(product)
			           : optional_idx();
		}
		default:
			return FixedRowCount(input);
		}
	}

	unique_ptr<BaseStatistics> AggregateStats(BoundAggregateExpression &copy, LogicalOperator &input,
	                                          column_binding_set_t &visited) const {
		if (!copy.Function().HasStatisticsCallback()) {
			return nullptr;
		}
		unordered_set<TableIndex> row_visits;
		auto maximum = MaxRows(input, row_visits);
		if (!maximum.IsValid()) {
			return nullptr;
		}
		vector<BaseStatistics> children;
		for (auto &child : copy.GetChildren()) {
			auto stats = ExpressionStats(*child, input, visited);
			children.push_back(stats ? stats->Copy() : BaseStatistics::CreateUnknown(child->GetReturnType()));
		}
		NodeStatistics rows(maximum.GetIndex(), maximum.GetIndex());
		AggregateStatisticsInput statistics(copy.BindInfoMutable(), children, &rows);
		return copy.Function().GetCallbacks().GetStatisticsCallback()(context, copy, statistics);
	}

	unique_ptr<BaseStatistics> ExpressionStats(const Expression &expression, LogicalOperator &input,
	                                           column_binding_set_t &visited) const {
		if (expression.GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE) {
			auto copy = expression.Copy();
			auto stats = AggregateStats(copy->Cast<BoundAggregateExpression>(), input, visited);
			return copy->CanThrow() ? nullptr : std::move(stats);
		}
		auto index = ReferenceIndex(expression, input);
		if (index.IsValid()) {
			switch (input.type) {
			case LogicalOperatorType::LOGICAL_PROJECTION: {
				auto &projected = *input.expressions[index.GetIndex()];
				if (!expression.GetReturnType().EqualsIncludingCollation(projected.GetReturnType())) {
					return nullptr;
				}
				return ExpressionStats(projected, *input.children[0], visited);
			}
			case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
				auto &aggregate = input.Cast<LogicalAggregate>();
				if (HasSimpleGrouping(aggregate)) {
					auto binding = expression.Cast<BoundColumnRefExpression>().Binding();
					auto &source = aggregate.GetExpression(binding);
					if (!expression.GetReturnType().EqualsIncludingCollation(source.GetReturnType())) {
						return nullptr;
					}
					return ExpressionStats(source, *input.children[0], visited);
				}
				break;
			}
			case LogicalOperatorType::LOGICAL_CTE_REF: {
				auto &reference = input.Cast<LogicalCTERef>();
				auto owner = FindVisibleOwner(reference.cte_index, scopes);
				if (!owner || reference.is_recurring || reference.correlated_columns ||
				    !visited.emplace(reference.cte_index, ProjectionIndex(index.GetIndex())).second) {
					break;
				}
				auto bindings = owner->children[0]->GetColumnBindings();
				if (index.GetIndex() >= bindings.size() || index.GetIndex() >= reference.chunk_types.size() ||
				    !expression.GetReturnType().EqualsIncludingCollation(reference.chunk_types[index.GetIndex()])) {
					break;
				}
				BoundColumnRefExpression source(expression.GetReturnType(), bindings[index.GetIndex()]);
				return ExpressionStats(source, *owner->children[0], visited);
			}
			case LogicalOperatorType::LOGICAL_FILTER:
			case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
			case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
				for (auto &child : input.children) {
					if (ReferenceIndex(expression, *child).IsValid()) {
						return ExpressionStats(expression, *child, visited);
					}
				}
				break;
			default:
				break;
			}
		}
		// Declared DECIMAL endpoints remain valid without evaluating predicates or propagating statistics.
		auto &type = expression.GetReturnType();
		if (type.id() != LogicalTypeId::DECIMAL) {
			return nullptr;
		}
		auto stats = NumericStats::CreateUnknown(type).ToUnique();
		NumericStats::SetMin(*stats, Value::MinimumValue(type));
		NumericStats::SetMax(*stats, Value::MaximumValue(type));
		return stats;
	}

private:
	ClientContext &context;
	const vector<reference<LogicalMaterializedCTE>> &scopes;
};

//! A matched input restriction and the projections between it and its aggregate.
//! Borrows the plan for one decision. Apply may destroy the matched join.
class DomainRestriction {
public:
	DomainRestriction(ClientContext &context, const vector<reference<LogicalMaterializedCTE>> &scopes,
	                  LogicalAggregate &aggregate_p, ExpressionType consumer_comparison_p)
	    : DomainRestriction(context, scopes, aggregate_p, vector<ExpressionType> {consumer_comparison_p}) {
	}

	DomainRestriction(ClientContext &context, const vector<reference<LogicalMaterializedCTE>> &scopes,
	                  LogicalAggregate &aggregate_p, const vector<ExpressionType> &comparisons)
	    : aggregate(aggregate_p), consumer_comparisons(comparisons), bounds(context, scopes) {
		vector<reference<const Expression>> keys;
		for (auto &group : aggregate.groups) {
			keys.push_back(*group);
		}
		D_ASSERT(keys.size() == comparisons.size());
		join = Find(*aggregate.children[0], keys);
	}

public:
	optional_ptr<LogicalComparisonJoin> GetJoin() const {
		return join;
	}

	bool HasSimpleProjections() const;
	bool CanRemove() const;
	const JoinCondition &GetCondition(idx_t key) const {
		return join->conditions[conditions[key]];
	}
	bool FiltersNullKey(idx_t key) const {
		return GetCondition(key).GetComparisonType() == ExpressionType::COMPARE_EQUAL &&
		       consumer_comparisons[key] == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	}
	idx_t NullFilterCount() const {
		idx_t count = 0;
		for (idx_t key = 0; key < conditions.size(); key++) {
			count += FiltersNullKey(key);
		}
		return count;
	}
	void Apply(unique_ptr<LogicalAggregate> &domain, RestrictionAction action);
	void Remove();

private:
	optional_ptr<LogicalComparisonJoin> Find(LogicalOperator &input, const vector<reference<const Expression>> &keys);

private:
	LogicalAggregate &aggregate;
	vector<ExpressionType> consumer_comparisons;
	vector<idx_t> conditions;
	DomainAggregateBounds bounds;
	optional_ptr<LogicalComparisonJoin> join;
	vector<reference<LogicalProjection>> projections;
};

optional_ptr<LogicalComparisonJoin> DomainRestriction::Find(LogicalOperator &input,
                                                            const vector<reference<const Expression>> &keys) {
	if (input.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		vector<reference<const Expression>> projected;
		for (auto &key : keys) {
			auto index = ReferenceIndex(key, input);
			if (!index.IsValid()) {
				return nullptr;
			}
			projected.push_back(*input.expressions[index.GetIndex()]);
		}
		projections.push_back(input.Cast<LogicalProjection>());
		return Find(*input.children[0], projected);
	}
	if (input.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return nullptr;
	}
	auto &join = input.Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::SEMI || join.conditions.size() != keys.size() || !join.expressions.empty() ||
	    !join.duplicate_eliminated_columns.empty() || join.filter_pushdown) {
		return nullptr;
	}
	vector<bool> matched(keys.size(), false);
	for (auto &key : keys) {
		optional_idx found;
		for (idx_t index = 0; index < join.conditions.size(); index++) {
			auto &condition = join.conditions[index];
			if (matched[index] || !condition.IsComparison() ||
			    (condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL &&
			     condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) ||
			    !key.get().Equals(condition.GetLHS()) || !ReferenceIndex(key, *join.children[0]).IsValid() ||
			    !ReferenceIndex(condition.GetRHS(), *join.children[1]).IsValid()) {
				continue;
			}
			found = index;
			break;
		}
		if (!found.IsValid()) {
			return nullptr;
		}
		matched[found.GetIndex()] = true;
		conditions.push_back(found.GetIndex());
	}
	return join;
}

bool DomainRestriction::HasSimpleProjections() const {
	for (auto &projection : projections) {
		for (auto &expression : projection.get().expressions) {
			if (!IsWorkReference(*expression)) {
				return false;
			}
		}
	}
	return true;
}

bool DomainRestriction::CanRemove() const {
	if (!HasSimpleProjections()) {
		return false;
	}
	return bounds.CanEvaluate(aggregate);
}

void DomainRestriction::Apply(unique_ptr<LogicalAggregate> &domain, RestrictionAction action) {
	auto &restriction = *join;
	auto &condition = restriction.conditions[0];
	if (action == RestrictionAction::REPLACE) {
		D_ASSERT(conditions.size() == 1 && domain);
		condition.RightReference() = DomainKey(*domain);
		restriction.children[1] = std::move(domain);
	} else {
		D_ASSERT(action == RestrictionAction::REMOVE);
		Remove();
	}
}

void DomainRestriction::Remove() {
	auto &restriction = *join;
	auto &slot = projections.empty() ? aggregate.children[0] : projections.back().get().children[0];
	D_ASSERT(slot.get() == &restriction);
	auto input = std::move(restriction.children[0]);
	auto filter = make_uniq<LogicalFilter>();
	for (idx_t key = 0; key < conditions.size(); key++) {
		if (FiltersNullKey(key)) {
			filter->expressions.push_back(make_uniq<BoundOperatorExpression>(
			    ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN, GetCondition(key).GetLHS().Copy()));
		}
	}
	if (!filter->expressions.empty()) {
		filter->children.push_back(std::move(input));
		input = std::move(filter);
	}
	slot = LogicalJoin::PromoteJoinSide(std::move(input), std::move(restriction.left_projection_map));
	join = nullptr;
}

static double EstimateFixedDomainWork(double key_rows, double distinct_key_rows) {
	return 2 * key_rows + distinct_key_rows + 3;
}

static double EstimateKeyProductionWork(RelationStatsExtractor &extractor, LogicalOperator &input) {
	if (input.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		return EstimateKeyProductionWork(extractor, *input.children[0]);
	}
	auto rows = extractor.Extract(input);
	if (!rows) {
		return 0;
	}
	if (input.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		// This reader disappears; its shared definition may still be used elsewhere.
		return rows->cardinality.GetDouble() * static_cast<double>(input.GetColumnBindings().size());
	}
	if (input.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto child_rows = extractor.Extract(*input.children[0]);
		if (child_rows) {
			return EstimateFixedDomainWork(child_rows->cardinality.GetDouble(), rows->cardinality.GetDouble()) +
			       EstimateKeyProductionWork(extractor, *input.children[0]);
		}
	}
	return 0;
}

struct ReleasedDomainWork {
	optional_ptr<LogicalOperator> production;
	optional_ptr<LogicalOperator> source;
};

//! Compare alternatives using the same statistics source and domain-production costs.
//! Statistics are extracted for each decision, never retained across a plan rewrite.
class DomainCostModel {
public:
	DomainCostModel(RelationStatsExtractor &extractor_p, LogicalAggregate &aggregate_p)
	    : extractor(extractor_p), aggregate(aggregate_p) {
	}

	DomainCostModel(RelationStatsExtractor &extractor_p, LogicalAggregate &aggregate_p, double key_input_rows_p,
	                double source_read_write_work_p)
	    : extractor(extractor_p), aggregate(aggregate_p), key_input_rows(key_input_rows_p),
	      source_read_write_work(source_read_write_work_p) {
	}

public:
	bool IsProfitable(LogicalComparisonJoin &restriction, LogicalOperator &domain);
	//! Keeping and safe removal are always considered; a supplied domain also permits scalar replacement.
	RestrictionDecision ChooseRestrictionAction(const DomainRestriction &existing,
	                                            optional_ptr<LogicalAggregate> domain, bool keys_may_expand = false);
	//! Price only producers that the caller has proven will stop.
	static double EstimateReleasedWork(RelationStatsExtractor &extractor, ReleasedDomainWork released);
	void SetReleasedWork(const ReleasedDomainWork &released);
	static bool IsRecoveryProfitable(double outer_rows, const RelationStats &domain, double expression_work,
	                                 idx_t single_row_joins, double released_work, double restriction_savings);

private:
	static bool IsCheaper(double candidate, double original);
	static double EstimateSemiJoinWork(double input_rows, double output_rows, double key_rows);
	static double EstimateRecoveryDomainRows(const RelationStats &domain);
	static double EstimateRecoveryRowWork(double expression_work, idx_t single_row_joins);
	double EstimateAggregateWork(double input_rows, double group_count) const;
	double EstimateAggregateWork(const RelationStats &input, const vector<ColumnBinding> &group_bindings) const;
	optional<TupleDomainRestrictionEstimate> EstimateRestriction(const DomainRestriction &existing,
	                                                             const RelationStats &input, const RelationStats &keys);

private:
	RelationStatsExtractor &extractor;
	LogicalAggregate &aggregate;
	double key_input_rows = 0;
	double source_read_write_work = 0;
	double released_work = 0;
};

double DomainCostModel::EstimateReleasedWork(RelationStatsExtractor &extractor, ReleasedDomainWork released) {
	// The unit of this model is one row through a hash table. Producing the key domain and materializing its source
	// are counted per value by the formulas, but measured they cost about one such unit per source row each (0.25 to
	// 0.46 of the formula for sources of 3 columns and 1 column), so the released work is scaled down.
	static constexpr double RELEASED_WORK_SCALE = 0.3;
	auto production_work = released.production ? EstimateKeyProductionWork(extractor, *released.production) : 0;
	double source_values = 0;
	if (released.source) {
		auto stats = extractor.Extract(*released.source);
		if (stats) {
			source_values =
			    stats->cardinality.GetDouble() * static_cast<double>(released.source->GetColumnBindings().size());
		}
	}
	return RELEASED_WORK_SCALE * (production_work + source_values);
}

void DomainCostModel::SetReleasedWork(const ReleasedDomainWork &released) {
	released_work = EstimateReleasedWork(extractor, released);
}

bool DomainCostModel::IsCheaper(double candidate, double original) {
	auto tolerance = 32 * std::numeric_limits<double>::epsilon() * MaxValue(original, candidate);
	return std::isfinite(original) && std::isfinite(candidate) && original - candidate > tolerance;
}

double DomainCostModel::EstimateAggregateWork(double input_rows, double group_count) const {
	auto aggregate_work_per_row = static_cast<double>(aggregate.expressions.size()) + 1;
	return aggregate_work_per_row * input_rows + (aggregate_work_per_row + 1) * group_count;
}

double DomainCostModel::EstimateSemiJoinWork(double input_rows, double output_rows, double key_rows) {
	return key_rows + input_rows + output_rows;
}

double DomainCostModel::EstimateRecoveryDomainRows(const RelationStats &domain) {
	CardinalityValue distinct_count(1);
	for (auto &column : domain.columns) {
		// Key independence and range-based estimates can hide repeated tuples.
		if (column.total_domain.source == DistinctCountSource::HLL ||
		    column.total_domain.source == DistinctCountSource::EXACT) {
			distinct_count = MaxValue(distinct_count, column.current_domain.distinct_count);
		}
	}
	return MinValue(distinct_count, domain.cardinality).GetDouble();
}

double DomainCostModel::EstimateRecoveryRowWork(double expression_work, idx_t single_row_joins) {
	// Scalar-expression scores and conditionless row processing are cheaper than hash-table row work.
	static constexpr double EXPRESSION_WORK_PER_HASH_ROW = 64;
	static constexpr double SINGLE_ROW_JOIN_WORK = 0.3;
	return expression_work / EXPRESSION_WORK_PER_HASH_ROW +
	       static_cast<double>(single_row_joins) * SINGLE_ROW_JOIN_WORK;
}

bool DomainCostModel::IsRecoveryProfitable(double outer_rows, const RelationStats &domain, double expression_work,
                                           idx_t single_row_joins, double released_work, double restriction_savings) {
	auto domain_rows = EstimateRecoveryDomainRows(domain);
	if (outer_rows <= domain_rows) {
		return true;
	}
	// Cancelling unchanged joins leaves one domain-sized build, probe and output.
	auto saved_work = EstimateSemiJoinWork(domain_rows, domain_rows, domain_rows) + released_work + restriction_savings;
	auto added_work = (outer_rows - domain_rows) * EstimateRecoveryRowWork(expression_work, single_row_joins);
	return std::isfinite(saved_work) && std::isfinite(added_work) && added_work <= saved_work;
}

double DomainCostModel::EstimateAggregateWork(const RelationStats &input,
                                              const vector<ColumnBinding> &group_bindings) const {
	vector<DistinctCount> domains;
	for (auto &binding : group_bindings) {
		auto column = input.GetColumnStats(binding);
		if (!column) {
			return std::numeric_limits<double>::quiet_NaN();
		}
		domains.push_back(column->current_domain);
	}
	auto group_count = RelationStatisticsHelper::EstimateDistinctCardinality(domains, input.cardinality);
	return EstimateAggregateWork(input.cardinality.GetDouble(), group_count.GetDouble());
}

static optional<RelationStats> EstimateRestrictionStats(LogicalComparisonJoin &restriction, const RelationStats &input,
                                                        const RelationStats &keys) {
	auto estimate = RelationStatisticsHelper::EstimateSemiAntiJoinCardinality(restriction, input, keys);
	if (!estimate.used_domain_evidence) {
		return {};
	}
	auto restricted = input;
	RelationStatisticsHelper::ApplySemiAntiJoinStats(restriction, input, keys, restricted);
	return restricted;
}

optional<TupleDomainRestrictionEstimate> DomainCostModel::EstimateRestriction(const DomainRestriction &existing,
                                                                              const RelationStats &input,
                                                                              const RelationStats &keys) {
	auto &restriction = *existing.GetJoin();
	if (aggregate.groups.size() == 1) {
		auto restricted = EstimateRestrictionStats(restriction, input, keys);
		if (!restricted) {
			return {};
		}
		auto binding = existing.GetCondition(0).GetLHS().Cast<BoundColumnRefExpression>().Binding();
		auto column = restricted->GetColumnStats(binding);
		if (!column) {
			return {};
		}
		auto groups =
		    RelationStatisticsHelper::EstimateDistinctCardinality({column->current_domain}, restricted->cardinality);
		return TupleDomainRestrictionEstimate {restricted->cardinality.GetDouble(), groups.GetDouble()};
	}
	return RelationStatisticsHelper::EstimateTupleDomainRestriction(restriction, input, keys, extractor);
}

bool DomainCostModel::IsProfitable(LogicalComparisonJoin &restriction, LogicalOperator &domain) {
	auto input = extractor.Extract(*aggregate.children[0]);
	auto groups = extractor.Extract(aggregate);
	auto keys = extractor.Extract(domain);
	if (!input || !groups || !keys) {
		return false;
	}
	auto restricted = EstimateRestrictionStats(restriction, *input, *keys);
	if (!restricted) {
		return false;
	}
	auto column = restricted->GetColumnStats(aggregate.groups[0]->Cast<BoundColumnRefExpression>().Binding());
	if (!column) {
		return false;
	}
	// The inserted SEMI is the new aggregate input; old input-shape refinements no longer apply.
	auto group_count =
	    RelationStatisticsHelper::EstimateDistinctCardinality({column->current_domain}, restricted->cardinality);
	auto input_rows = input->cardinality.GetDouble();
	auto restricted_rows = restricted->cardinality.GetDouble();
	auto original_groups = groups->cardinality.GetDouble();
	auto restricted_groups = group_count.GetDouble();
	auto distinct_key_rows = keys->cardinality.GetDouble();
	// Cancel unchanged input/caller work and the consumer's identical output.
	auto canonical = EstimateAggregateWork(input_rows, original_groups);
	auto candidate = EstimateAggregateWork(restricted_rows, restricted_groups) +
	                 EstimateSemiJoinWork(input_rows, restricted_rows, distinct_key_rows) +
	                 EstimateFixedDomainWork(key_input_rows, distinct_key_rows) + source_read_write_work;
	return IsCheaper(candidate, canonical);
}

RestrictionDecision DomainCostModel::ChooseRestrictionAction(const DomainRestriction &existing,
                                                             optional_ptr<LogicalAggregate> domain,
                                                             bool keys_may_expand) {
	if (keys_may_expand && !existing.CanRemove()) {
		return {};
	}
	auto &restriction = *existing.GetJoin();
	auto input = extractor.Extract(*restriction.children[0]);
	auto old_keys = extractor.Extract(*restriction.children[1]);
	if (!input || !old_keys) {
		return {};
	}
	auto old_input = EstimateRestriction(existing, *input, *old_keys);
	auto old_key_rows = old_keys->cardinality.GetDouble();
	auto old_producer_work = EstimateKeyProductionWork(extractor, *restriction.children[1]);
	vector<ColumnBinding> group_bindings;
	for (idx_t key = 0; key < aggregate.groups.size(); key++) {
		group_bindings.push_back(existing.GetCondition(key).GetLHS().Cast<BoundColumnRefExpression>().Binding());
	}
	auto input_rows = input->cardinality.GetDouble();
	auto best_work = old_input ? EstimateAggregateWork(old_input->rows, old_input->groups) +
	                                 EstimateSemiJoinWork(input_rows, old_input->rows, old_key_rows) +
	                                 old_producer_work + released_work
	                           : std::numeric_limits<double>::quiet_NaN();
	auto original_work = best_work;
	auto action = RestrictionAction::KEEP;
	auto consider = [&](double work, RestrictionAction alternative) {
		if (IsCheaper(work, best_work)) {
			best_work = work;
			action = alternative;
		}
	};
	if (domain) {
		D_ASSERT(aggregate.groups.size() == 1);
		auto new_keys = extractor.Extract(*domain);
		if (new_keys) {
			auto &condition = existing.GetCondition(0);
			LogicalComparisonJoin replacement(JoinType::SEMI);
			replacement.conditions.emplace_back(condition.GetLHS().Copy(), DomainKey(*domain),
			                                    condition.GetComparisonType());
			auto restricted = EstimateRestrictionStats(replacement, *input, *new_keys);
			auto distinct_key_rows = new_keys->cardinality.GetDouble();
			auto new_producer_work =
			    EstimateFixedDomainWork(key_input_rows, distinct_key_rows) + source_read_write_work;
			auto new_key_work = distinct_key_rows + new_producer_work;
			if (!old_input) {
				// Containment can justify cheaper key production, but missing selectivity cannot justify removal.
				if (!keys_may_expand && IsCheaper(new_key_work, old_key_rows + old_producer_work)) {
					return {RestrictionAction::REPLACE, 0};
				}
				return {};
			}
			if (restricted) {
				consider(EstimateAggregateWork(*restricted, group_bindings) +
				             EstimateSemiJoinWork(input_rows, restricted->cardinality.GetDouble(), distinct_key_rows) +
				             new_producer_work,
				         RestrictionAction::REPLACE);
			} else if (!keys_may_expand) {
				consider(best_work - old_key_rows - old_producer_work + new_key_work, RestrictionAction::REPLACE);
			}
		}
	}
	if (old_input && (keys_may_expand || existing.CanRemove())) {
		// Charge each required NULL filter without taking credit for rows it removes.
		consider(EstimateAggregateWork(*input, group_bindings) +
		             static_cast<double>(existing.NullFilterCount()) * input_rows,
		         RestrictionAction::REMOVE);
	}
	return {action, action == RestrictionAction::REMOVE ? original_work - best_work : 0};
}

static bool HasSharedSource(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF || op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		return true;
	}
	for (auto &child : op.children) {
		if (HasSharedSource(*child)) {
			return true;
		}
	}
	return false;
}

static bool HasRuntimeFilterTargets(LogicalAggregate &aggregate) {
	JoinFilterPushdownColumn column;
	if (!JoinFilterPushdownUtil::PushdownJoinFilterExpression(*aggregate.groups[0], column)) {
		return false;
	}
	vector<JoinFilterPushdownColumn> columns;
	columns.push_back(std::move(column));
	vector<PushdownFilterTarget> targets;
	JoinFilterPushdownOptimizer::GetPushdownFilterTargets(*aggregate.children[0], columns, targets,
	                                                      EqualityPropagation::ENABLED);
	return !targets.empty();
}

static bool TryOptimizeRestriction(Optimizer &optimizer, LogicalAggregate &aggregate,
                                   unique_ptr<LogicalAggregate> &domain, ExpressionType consumer_comparison,
                                   const vector<reference<LogicalMaterializedCTE>> &cte_scopes) {
	DomainRestriction existing(optimizer.context, cte_scopes, aggregate, consumer_comparison);
	auto restriction = existing.GetJoin();
	if (!restriction || HasSharedSource(*restriction->children[1])) {
		return false;
	}
	auto &condition = restriction->conditions[0];
	auto key = ReferenceIndex(condition.GetRHS(), *restriction->children[1]).GetIndex();
	vector<vector<unique_ptr<Expression>>> original_rows;
	if (!CopyExactFixedKeys(*restriction->children[1], key, condition.GetRHS().GetReturnType(), original_rows)) {
		return false;
	}
	expression_set_t original_keys;
	for (auto &row : original_rows) {
		original_keys.insert(*row[0]);
	}
	auto &fixed = domain->children[0]->Cast<LogicalExpressionGet>();
	for (auto &row : fixed.expressions) {
		if (!original_keys.count(*row[0])) {
			return false;
		}
	}
	RelationStatsExtractor extractor(optimizer.context);
	DomainCostModel costs(extractor, aggregate, static_cast<double>(fixed.expressions.size()), 0);
	auto action = costs.ChooseRestrictionAction(existing, *domain).action;
	if (action == RestrictionAction::KEEP) {
		return false;
	}
	existing.Apply(domain, action);
	return true;
}

static optional_ptr<LogicalCTERef> FindSharedKey(LogicalOperator &op, idx_t &key) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto index = ReferenceIndex(*op.expressions[key], *op.children[0]);
		if (!index.IsValid()) {
			return nullptr;
		}
		key = index.GetIndex();
		return FindSharedKey(*op.children[0], key);
	}
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &aggregate = op.Cast<LogicalAggregate>();
		auto index = GroupKeyIndex(aggregate, key);
		if (!index.IsValid()) {
			return nullptr;
		}
		key = index.GetIndex();
		return FindSharedKey(*op.children[0], key);
	}
	if (op.type != LogicalOperatorType::LOGICAL_CTE_REF) {
		return nullptr;
	}
	auto &reference = op.Cast<LogicalCTERef>();
	return reference.is_recurring || reference.correlated_columns ? nullptr : &reference;
}

struct RuntimeDomainSource {
	optional_ptr<LogicalOperator> input;
	optional_ptr<const LogicalMaterializedCTE> owner;
	idx_t key = 0;
};

static bool ReferencesVisibleOwners(LogicalOperator &op, const vector<reference<LogicalMaterializedCTE>> &cte_scopes) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte = op.Cast<LogicalCTERef>();
		return !cte.is_recurring && !cte.correlated_columns && FindVisibleOwner(cte.cte_index, cte_scopes);
	}
	for (auto &child : op.children) {
		if (!ReferencesVisibleOwners(*child, cte_scopes)) {
			return false;
		}
	}
	return true;
}

static bool CanChangeRuntimeKeyReaders(LogicalOperator &op, const vector<reference<LogicalMaterializedCTE>> &cte_scopes,
                                       unordered_map<TableIndex, bool> &checked_owners) {
	if (ClassifyLogicalOperatorRepeatability(op) != LogicalOperatorRepeatability::REPEATABLE) {
		return false;
	}
	// A SINGLE join can throw when additional rows are evaluated.
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    op.Cast<LogicalComparisonJoin>().join_type == JoinType::SINGLE) {
		return false;
	}
	// Reader changes and materialization can expose rows that later filters or limits would skip.
	bool can_throw = false;
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](unique_ptr<Expression> *expression) { can_throw |= (*expression)->CanThrow(); });
	// Pushed-down scan predicates are not enumerated by the logical operator visitor.
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		for (auto &entry : op.Cast<LogicalGet>().table_filters) {
			can_throw |=
			    ExpressionFilter::GetExpressionFilter(entry.Filter(), "CanChangeRuntimeKeyReaders").expr->CanThrow();
		}
	}
	if (can_throw) {
		return false;
	}
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte = op.Cast<LogicalCTERef>();
		if (cte.is_recurring || cte.correlated_columns) {
			return false;
		}
		auto checked = checked_owners.find(cte.cte_index);
		if (checked != checked_owners.end()) {
			return checked->second;
		}
		auto owner = FindVisibleOwner(cte.cte_index, cte_scopes);
		if (!owner) {
			return false;
		}
		checked_owners.emplace(cte.cte_index, false);
		// Reader changes can expose or prevent producer filters and alter volatile payload evaluation.
		auto repeatable = CanChangeRuntimeKeyReaders(*owner->children[0], cte_scopes, checked_owners);
		checked_owners[cte.cte_index] = repeatable;
		return repeatable;
	}
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		// A materialized CTE inside the subtree is read by the subtree only: its definition is checked here, once,
		// for all of its references.
		if (!CanChangeRuntimeKeyReaders(*op.children[0], cte_scopes, checked_owners)) {
			return false;
		}
		checked_owners[op.Cast<LogicalMaterializedCTE>().table_index] = true;
		return CanChangeRuntimeKeyReaders(*op.children[1], cte_scopes, checked_owners);
	}
	for (auto &child : op.children) {
		if (!CanChangeRuntimeKeyReaders(*child, cte_scopes, checked_owners)) {
			return false;
		}
	}
	return true;
}

static bool TryGetRuntimeDomainSource(LogicalOperator &demand, idx_t demand_index,
                                      const vector<reference<LogicalMaterializedCTE>> &cte_scopes,
                                      RuntimeDomainSource &source) {
	source.key = demand_index;
	auto shared = FindSharedKey(demand, source.key);
	unordered_map<TableIndex, bool> checked_owners;
	if (shared) {
		auto owner = FindVisibleOwner(shared->cte_index, cte_scopes);
		// Statistics can make the original reader filterable after this pass.
		if (!owner || !CanChangeRuntimeKeyReaders(*shared, cte_scopes, checked_owners)) {
			return false;
		}
		source.input = *owner->children[0];
		source.owner = owner;
	} else {
		if (!CanChangeRuntimeKeyReaders(demand, cte_scopes, checked_owners)) {
			return false;
		}
		source.input = demand;
		source.key = demand_index;
	}
	return true;
}

using SharedKeyTuples = unordered_map<TableIndex, vector<idx_t>>;

static void TraceSharedKeys(LogicalOperator &op, vector<idx_t> keys, const vector<LogicalType> &types,
                            const vector<reference<LogicalMaterializedCTE>> &cte_scopes, bool allow_filters,
                            SharedKeyTuples &tuples) {
	optional_ptr<LogicalOperator> current = op;
	while (true) {
		auto bindings = current->GetColumnBindings();
		vector<idx_t> child_keys;
		for (idx_t key = 0; key < keys.size(); key++) {
			if (keys[key] >= bindings.size()) {
				return;
			}
			optional_idx index;
			switch (current->type) {
			case LogicalOperatorType::LOGICAL_PROJECTION:
				index = ReferenceIndex(*current->expressions[keys[key]], *current->children[0], types[key]);
				break;
			case LogicalOperatorType::LOGICAL_FILTER: {
				if (!allow_filters) {
					return;
				}
				BoundColumnRefExpression reference(types[key], bindings[keys[key]]);
				index = ReferenceIndex(reference, *current->children[0]);
				break;
			}
			case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
				index = GroupKeyIndex(current->Cast<LogicalAggregate>(), keys[key], types[key]);
				break;
			case LogicalOperatorType::LOGICAL_CTE_REF: {
				auto &cte = current->Cast<LogicalCTERef>();
				if (keys[key] >= cte.chunk_types.size() ||
				    !types[key].EqualsIncludingCollation(cte.chunk_types[keys[key]])) {
					return;
				}
				index = keys[key];
				break;
			}
			default:
				return;
			}
			if (!index.IsValid()) {
				return;
			}
			child_keys.push_back(index.GetIndex());
		}
		if (current->type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto &cte = current->Cast<LogicalCTERef>();
			auto owner = FindVisibleOwner(cte.cte_index, cte_scopes);
			if (!owner || cte.is_recurring || cte.correlated_columns ||
			    !tuples.emplace(cte.cte_index, child_keys).second) {
				return;
			}
			current = *owner->children[0];
		} else {
			current = *current->children[0];
		}
		keys = std::move(child_keys);
	}
}

static bool RuntimeRestrictionContainsDemand(const DomainRestriction &existing, LogicalOperator &demand,
                                             const vector<idx_t> &demand_indices,
                                             const vector<reference<LogicalMaterializedCTE>> &cte_scopes) {
	auto restriction = existing.GetJoin();
	if (!restriction || !existing.HasSimpleProjections()) {
		return false;
	}
	unordered_map<TableIndex, bool> checked_owners;
	if (!CanChangeRuntimeKeyReaders(*restriction->children[1], cte_scopes, checked_owners)) {
		return false;
	}
	vector<idx_t> keys;
	vector<LogicalType> types;
	for (idx_t key = 0; key < demand_indices.size(); key++) {
		auto &condition = existing.GetCondition(key);
		keys.push_back(ReferenceIndex(condition.GetRHS(), *restriction->children[1]).GetIndex());
		types.push_back(condition.GetRHS().GetReturnType());
	}
	SharedKeyTuples demanded_keys, original_keys;
	TraceSharedKeys(demand, demand_indices, types, cte_scopes, true, demanded_keys);
	TraceSharedKeys(*restriction->children[1], std::move(keys), types, cte_scopes, false, original_keys);
	bool contains_demand = false;
	for (auto &entry : original_keys) {
		auto found = demanded_keys.find(entry.first);
		contains_demand |= found != demanded_keys.end() && found->second == entry.second;
	}
	return contains_demand;
}

static RestrictionAction TryOptimizeRuntimeRestriction(ClientContext &context, DomainCostModel &costs,
                                                       LogicalAggregate &aggregate, LogicalOperator &demand,
                                                       idx_t demand_index, unique_ptr<LogicalAggregate> &domain,
                                                       ExpressionType consumer_comparison,
                                                       const vector<reference<LogicalMaterializedCTE>> &cte_scopes,
                                                       bool keys_may_expand = false) {
	DomainRestriction existing(context, cte_scopes, aggregate, consumer_comparison);
	if (!RuntimeRestrictionContainsDemand(existing, demand, {demand_index}, cte_scopes)) {
		return RestrictionAction::KEEP;
	}
	auto action = costs.ChooseRestrictionAction(existing, domain.get(), keys_may_expand).action;
	if (action != RestrictionAction::KEEP) {
		existing.Apply(domain, action);
	}
	return action;
}

struct ScanKeySource {
	optional_ptr<const unique_ptr<LogicalOperator>> input;
	idx_t key = 0;
	double rows = 0;
	double scan_rows = 0;
};

static optional_ptr<LogicalGet> FindClosedScan(LogicalOperator &input) {
	if (input.type == LogicalOperatorType::LOGICAL_FILTER || input.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		return FindClosedScan(*input.children[0]);
	}
	if (input.type != LogicalOperatorType::LOGICAL_GET || !input.children.empty()) {
		return nullptr;
	}
	auto &get = input.Cast<LogicalGet>();
	return get.GetTable() ? &get : nullptr;
}

//! An INNER equality lets either input supply a superset of the joined key domain.
static void FindScanKeySource(const unique_ptr<LogicalOperator> &input, idx_t key, const LogicalType &type,
                              RelationStatsExtractor &extractor, ClientContext &context, ScanKeySource &source) {
	if (auto scan = FindClosedScan(*input)) {
		auto stats = extractor.Extract(*input);
		if (stats && (!source.input || stats->cardinality.GetDouble() < source.rows)) {
			source.input = input;
			source.key = key;
			source.rows = stats->cardinality.GetDouble();
			source.scan_rows = static_cast<double>(scan->EstimateSourceCardinality(context));
		}
		return;
	}
	if (input->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto index = ReferenceIndex(*input->expressions[key], *input->children[0], type);
		if (index.IsValid()) {
			FindScanKeySource(input->children[0], index.GetIndex(), type, extractor, context, source);
		}
		return;
	}
	if (input->type == LogicalOperatorType::LOGICAL_FILTER) {
		BoundColumnRefExpression reference(type, input->GetColumnBindings()[key]);
		auto index = ReferenceIndex(reference, *input->children[0]);
		if (index.IsValid()) {
			FindScanKeySource(input->children[0], index.GetIndex(), type, extractor, context, source);
		}
		return;
	}
	if (input->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return;
	}
	auto &join = input->Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::INNER || !join.duplicate_eliminated_columns.empty()) {
		return;
	}
	BoundColumnRefExpression reference(type, join.GetColumnBindings()[key]);
	for (idx_t side = 0; side < 2; side++) {
		auto index = ReferenceIndex(reference, *join.children[side]);
		for (auto &condition : join.conditions) {
			if (index.IsValid()) {
				break;
			}
			if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			auto &other = side == 0 ? condition.GetRHS() : condition.GetLHS();
			auto &candidate = side == 0 ? condition.GetLHS() : condition.GetRHS();
			if (reference.Equals(other)) {
				index = ReferenceIndex(candidate, *join.children[side], type);
			}
		}
		if (index.IsValid()) {
			FindScanKeySource(join.children[side], index.GetIndex(), type, extractor, context, source);
		}
	}
}

//! A join whose RHS recovers the keys without a group of a correlated aggregate: a LEFT join of the
//! duplicate-eliminated keys of the join's own LHS against the grouped aggregate (FindOuterDomainJoin).
struct OuterDomainJoin {
	//! The number of operators between the join and the LEFT join against the key domain
	idx_t chain_length = 0;
	//! The LEFT join against the key domain
	optional_ptr<LogicalComparisonJoin> domain_join;
	//! Per key: the binding of the domain column and the binding of the LHS column it duplicates
	vector<pair<ColumnBinding, ColumnBinding>> keys;
	//! Demand indices and comparisons in aggregate grouping-key order
	vector<idx_t> demand_indices;
	vector<ExpressionType> comparisons;
	//! The materialized CTE both the LHS and the key domain read
	TableIndex source;
	//! The aggregate, when the restriction decision can handle the operators above it
	optional_ptr<LogicalAggregate> aggregate;
};

//! The (CTE, column) an output column reads from a materialized CTE through plain projections. With `distinct` the
//! column must pass exactly one duplicate elimination of `group_count` groups (inline or as a visible CTE); `group`
//! is the group of that duplicate elimination the column reads.
static bool TraceSourceColumn(LogicalOperator &op, idx_t column,
                              const vector<reference<LogicalMaterializedCTE>> &cte_scopes, bool distinct,
                              idx_t group_count, TableIndex &source, idx_t &source_column, idx_t &group) {
	reference<LogicalOperator> current = op;
	bool seen_distinct = false;
	while (true) {
		auto &node = current.get();
		switch (node.type) {
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			if (column >= node.expressions.size()) {
				return false;
			}
			auto index = ReferenceIndex(*node.expressions[column], *node.children[0]);
			if (!index.IsValid()) {
				return false;
			}
			column = index.GetIndex();
			current = *node.children[0];
			break;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggregate = node.Cast<LogicalAggregate>();
			if (!distinct || seen_distinct || !aggregate.expressions.empty() ||
			    aggregate.groups.size() != group_count) {
				return false;
			}
			auto index = GroupKeyIndex(aggregate, column);
			if (!index.IsValid()) {
				return false;
			}
			group = column;
			column = index.GetIndex();
			seen_distinct = true;
			current = *node.children[0];
			break;
		}
		case LogicalOperatorType::LOGICAL_CTE_REF: {
			auto &reference = node.Cast<LogicalCTERef>();
			if (reference.is_recurring || reference.correlated_columns) {
				return false;
			}
			if (distinct == seen_distinct) {
				source = reference.cte_index;
				source_column = column;
				return true;
			}
			auto owner = FindVisibleOwner(reference.cte_index, cte_scopes);
			if (!owner) {
				return false;
			}
			current = *owner->children[0];
			break;
		}
		default:
			return false;
		}
	}
}

//! `depends` caches, for one walk, whether the definition of a visible CTE reads the source: every reference of such a
//! CTE counts, but its definition is walked once.
static idx_t CountSourceReaders(LogicalOperator &op, TableIndex source,
                                const vector<reference<LogicalMaterializedCTE>> &cte_scopes,
                                unordered_map<TableIndex, bool> &depends) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &reference = op.Cast<LogicalCTERef>();
		if (reference.cte_index == source) {
			return 1;
		}
		auto known = depends.find(reference.cte_index);
		if (known != depends.end()) {
			return known->second ? 1 : 0;
		}
		// The owner is an ancestor of `op`: its definition cannot contain `op`.
		auto owner = FindVisibleOwner(reference.cte_index, cte_scopes);
		bool reads_source = owner && CountSourceReaders(*owner->children[0], source, cte_scopes, depends) > 0;
		depends[reference.cte_index] = reads_source;
		return reads_source ? 1 : 0;
	}
	idx_t count = 0;
	for (auto &child : op.children) {
		count += CountSourceReaders(*child, source, cte_scopes, depends);
	}
	return count;
}

//! The number of references below `op` that read the materialized CTE `source`, directly or through a visible CTE
//! whose definition does.
static idx_t CountSourceReaders(LogicalOperator &op, TableIndex source,
                                const vector<reference<LogicalMaterializedCTE>> &cte_scopes) {
	unordered_map<TableIndex, bool> depends;
	return CountSourceReaders(op, source, cte_scopes, depends);
}

//! An aggregate without groups below plain projections.
static bool IsSingleRow(LogicalOperator &op) {
	reference<LogicalOperator> current = op;
	while (current.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		current = *current.get().children[0];
	}
	if (current.get().type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return false;
	}
	auto &aggregate = current.get().Cast<LogicalAggregate>();
	return aggregate.groups.empty() && aggregate.grouping_sets.size() <= 1;
}

//! The RHS of the equality join is grouped by (a subset of) the join keys.
static bool JoinRHSIsDeduplicated(LogicalComparisonJoin &join) {
	vector<ColumnBinding> bindings;
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() || condition.GetRHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    condition.GetRHS().Cast<BoundColumnRefExpression>().Depth() != 0) {
			return false;
		}
		bindings.push_back(condition.GetRHS().Cast<BoundColumnRefExpression>().Binding());
	}
	return LogicalAggregate::KeysAreUnique(*join.children[1], std::move(bindings));
}

static bool ProjectionOnlyPassesKey(LogicalOperator &op, idx_t passthrough, ColumnBinding key) {
	for (idx_t index = 0; index < op.expressions.size(); index++) {
		if (index == passthrough) {
			continue;
		}
		bool observes_key = false;
		ExpressionIterator::EnumerateExpression(op.expressions[index], [&](const Expression &child) {
			switch (child.GetExpressionClass()) {
			case ExpressionClass::BOUND_COLUMN_REF: {
				auto &column = child.Cast<BoundColumnRefExpression>();
				observes_key |= column.Depth() != 0 || column.Binding() == key;
				break;
			}
			case ExpressionClass::BOUND_LAMBDA:
			case ExpressionClass::BOUND_LAMBDA_REF:
			case ExpressionClass::BOUND_SUBQUERY:
			case ExpressionClass::BOUND_REF:
				observes_key = true;
				break;
			default:
				break;
			}
		});
		if (observes_key) {
			return false;
		}
	}
	return true;
}

//! Matches, at an INNER / LEFT / SINGLE join,
//!
//!   JOIN (l.k IS NOT DISTINCT FROM p.k)
//!     L: a reference of a materialized CTE
//!     PROJECTION p (.., d.k)                    the empty-input substitution
//!       LEFT JOIN (no condition)                one row of empty-input aggregate values
//!         LEFT JOIN (d.k = a.k)
//!           d: the distinct keys of the CTE L reads
//!           a: an aggregate grouped by the keys
//!
//! Only operators that compute one row from one row of the inner LEFT join may sit above it.
static bool FindOuterDomainJoin(LogicalOperator &op, const vector<reference<LogicalMaterializedCTE>> &cte_scopes,
                                OuterDomainJoin &result) {
	auto &join = op.Cast<LogicalComparisonJoin>();
	if ((join.join_type != JoinType::SINGLE && join.join_type != JoinType::LEFT && join.join_type != JoinType::INNER) ||
	    !join.left_projection_map.empty() || !join.expressions.empty() || !join.duplicate_eliminated_columns.empty() ||
	    join.filter_pushdown || join.children[1]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	vector<reference<LogicalOperator>> chain;
	reference<LogicalOperator> current = *join.children[1];
	while (true) {
		auto &node = current.get();
		if (node.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			for (auto &expression : node.expressions) {
				if (expression->CanThrow() || expression->IsVolatile()) {
					return false;
				}
			}
		} else if (node.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &inner = node.Cast<LogicalComparisonJoin>();
			if (inner.join_type != JoinType::LEFT || inner.HasProjectionMap() || !inner.expressions.empty() ||
			    !inner.duplicate_eliminated_columns.empty() || inner.filter_pushdown) {
				return false;
			}
			if (!inner.conditions.empty()) {
				break;
			}
			if (!IsSingleRow(*inner.children[1])) {
				return false;
			}
		} else {
			return false;
		}
		chain.push_back(node);
		current = *node.children[0];
	}
	auto &domain_join = current.get().Cast<LogicalComparisonJoin>();
	auto &domain = *domain_join.children[0];
	auto domain_bindings = domain.GetColumnBindings();
	auto key_count = domain_bindings.size();
	if (key_count == 0 || domain_join.conditions.size() != key_count || join.conditions.size() != key_count ||
	    !JoinRHSIsDeduplicated(domain_join)) {
		return false;
	}

	// The inner LEFT join compares every domain column with a group of the aggregate. The domain columns are the
	// groups of the duplicate elimination, each of them exactly once: a group that is not output, or one that is output
	// twice in place of another, leaves several domain rows per key and would multiply the rows of the LHS.
	vector<idx_t> sources;
	vector<bool> domain_groups(key_count, false);
	for (idx_t index = 0; index < key_count; index++) {
		idx_t compared = 0;
		for (auto &condition : domain_join.conditions) {
			if (!condition.IsComparison() ||
			    (condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL &&
			     condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM)) {
				continue;
			}
			auto &left = condition.GetLHS();
			if (left.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    left.Cast<BoundColumnRefExpression>().Depth() == 0 &&
			    left.Cast<BoundColumnRefExpression>().Binding() == domain_bindings[index]) {
				compared++;
			}
		}
		TableIndex cte;
		idx_t column;
		idx_t group = 0;
		if (compared != 1 || !TraceSourceColumn(domain, index, cte_scopes, true, key_count, cte, column, group) ||
		    group >= key_count || domain_groups[group]) {
			return false;
		}
		domain_groups[group] = true;
		if (index == 0) {
			result.source = cte;
		} else if (result.source != cte) {
			return false;
		}
		sources.push_back(column);
	}

	// The outer join compares the LHS column each domain column duplicates with that column, passed through.
	vector<bool> matched(key_count, false);
	result.keys.resize(key_count);
	vector<idx_t> demand_indices(key_count);
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() || condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM ||
		    condition.GetLHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    condition.GetRHS().GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
		    condition.GetRHS().Cast<BoundColumnRefExpression>().Depth() != 0) {
			return false;
		}
		auto left_index = ReferenceIndex(condition.GetLHS(), *join.children[0]);
		TableIndex cte;
		idx_t column;
		idx_t group = 0;
		if (!left_index.IsValid() ||
		    !TraceSourceColumn(*join.children[0], left_index.GetIndex(), cte_scopes, false, 0, cte, column, group)) {
			return false;
		}
		auto binding = condition.GetRHS().Cast<BoundColumnRefExpression>().Binding();
		auto &type = condition.GetLHS().GetReturnType();
		const auto sensitive_key = !BoundComparisonExpression::EqualityPreservesValueRepresentation(type);
		if (sensitive_key) {
			auto outputs = join.GetColumnBindings();
			if (!type.IsFloating() || std::find(outputs.begin(), outputs.end(), binding) != outputs.end()) {
				return false;
			}
		}
		for (auto &above : chain) {
			if (above.get().type != LogicalOperatorType::LOGICAL_PROJECTION) {
				continue;
			}
			auto outputs = above.get().GetColumnBindings();
			auto entry = std::find(outputs.begin(), outputs.end(), binding);
			if (entry == outputs.end()) {
				return false;
			}
			auto projection_index = NumericCast<idx_t>(entry - outputs.begin());
			auto &expression = *above.get().expressions[projection_index];
			if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
			    expression.Cast<BoundColumnRefExpression>().Depth() != 0) {
				return false;
			}
			binding = expression.Cast<BoundColumnRefExpression>().Binding();
			if (sensitive_key && !ProjectionOnlyPassesKey(above.get(), projection_index, binding)) {
				return false;
			}
		}
		auto entry = std::find(domain_bindings.begin(), domain_bindings.end(), binding);
		if (entry == domain_bindings.end()) {
			return false;
		}
		auto index = NumericCast<idx_t>(entry - domain_bindings.begin());
		if (cte != result.source || column != sources[index] || matched[index]) {
			return false;
		}
		matched[index] = true;
		result.keys[index] = make_pair(binding, condition.GetLHS().Cast<BoundColumnRefExpression>().Binding());
		demand_indices[index] = left_index.GetIndex();
	}

	auto &aggregate_side = *domain_join.children[1];
	result.chain_length = chain.size();
	result.domain_join = &domain_join;
	vector<idx_t> aggregate_indices, domain_indices;
	for (auto &condition : domain_join.conditions) {
		auto index = ReferenceIndex(condition.GetRHS(), aggregate_side);
		auto domain_index = ReferenceIndex(condition.GetLHS(), domain);
		if (!index.IsValid() || !domain_index.IsValid()) {
			return true;
		}
		aggregate_indices.push_back(index.GetIndex());
		domain_indices.push_back(domain_index.GetIndex());
	}
	auto aggregate_match = FindAggregate(aggregate_side, std::move(aggregate_indices));
	if (aggregate_match) {
		result.aggregate = aggregate_match->aggregate.get();
		result.demand_indices.resize(key_count);
		result.comparisons.resize(key_count);
		for (idx_t index = 0; index < key_count; index++) {
			auto group = aggregate_match->group_indices[index];
			result.demand_indices[group] = demand_indices[domain_indices[index]];
			result.comparisons[group] = domain_join.conditions[index].GetComparisonType();
		}
	}
	return true;
}

static BindingReplacementGraph MaterializeRuntimeDomain(Optimizer &optimizer, unique_ptr<LogicalOperator> &op,
                                                        idx_t demand_side, const LogicalCTERef &domain) {
	BindingReplacementGraph replacements;
	unique_ptr<LogicalOperator> definition;
	ColumnBindingRewrite::RewriteChild(
	    op, demand_side, [&](unique_ptr<LogicalOperator> &child, BindingReplacementGraph &bindings) {
		    auto reference = make_uniq<LogicalCTERef>(optimizer.binder.GenerateTableIndex(), domain.cte_index,
		                                              domain.chunk_types, domain.bound_columns);
		    auto old_bindings = child->GetColumnBindings();
		    auto new_bindings = reference->GetColumnBindings();
		    for (idx_t index = 0; index < old_bindings.size(); index++) {
			    if (!bindings.Find(old_bindings[index])) {
				    bindings.Add(old_bindings[index], new_bindings[index]);
			    }
		    }
		    replacements.Merge(bindings);
		    definition = std::move(child);
		    child = std::move(reference);
	    });
	op =
	    make_uniq<LogicalMaterializedCTE>("domain_source", domain.cte_index, domain.chunk_types.size(),
	                                      std::move(definition), std::move(op), CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	return replacements;
}

bool DuplicateEliminatedDomainOptimizer::TryOptimizeRuntimeDomain(unique_ptr<LogicalOperator> &op,
                                                                  LogicalAggregate &aggregate, idx_t aggregate_side,
                                                                  idx_t demand_index, ExpressionType comparison) {
	auto &join = op->Cast<LogicalComparisonJoin>();
	auto &demand = *join.children[1 - aggregate_side];
	if (!ReferencesVisibleOwners(demand, cte_scopes) || !ReferencesVisibleOwners(aggregate, cte_scopes)) {
		return false;
	}
	RuntimeDomainSource source;
	if (!TryGetRuntimeDomainSource(demand, demand_index, cte_scopes, source)) {
		return false;
	}
	// The extractor caches by operator address: retain rejected candidates through fallback costing.
	unique_ptr<LogicalAggregate> scan_domain;
	auto owner_index = source.owner ? source.owner->table_index : optimizer.binder.GenerateTableIndex();
	RelationStatsExtractor extractor(optimizer.context, [&](TableIndex index) -> optional_ptr<LogicalOperator> {
		if (index == owner_index) {
			return source.input;
		}
		auto owner = FindVisibleOwner(index, cte_scopes);
		return owner ? owner->children[0].get() : nullptr;
	});
	auto source_stats = extractor.Extract(*source.input);
	if (!source_stats) {
		return false;
	}
	if (source.owner) {
		ScanKeySource scan_source;
		auto &type = aggregate.groups[0]->GetReturnType();
		FindScanKeySource(source.owner->children[0], source.key, type, extractor, optimizer.context, scan_source);
		if (scan_source.input) {
			auto parameters = optimizer.binder.GetParameters();
			LogicalOperatorDeepCopy copier(optimizer.binder, parameters ? parameters->GetParametersPtr() : nullptr);
			auto scan = copier.DeepCopy(*scan_source.input);
			auto key = make_uniq<BoundColumnRefExpression>(type, scan->GetColumnBindings()[scan_source.key]);
			scan_domain = CreateKeyDomain(optimizer, std::move(scan), std::move(key));
			DomainCostModel costs(extractor, aggregate, scan_source.rows, scan_source.scan_rows);
			// A scan supplies the keys: the demand's source is released either way.
			if (TryOptimizeRuntimeRestriction(optimizer.context, costs, aggregate, demand, demand_index, scan_domain,
			                                  comparison, cte_scopes, true) != RestrictionAction::KEEP) {
				return true;
			}
		}
	}
	source.input->ResolveOperatorTypes();
	auto &source_types = source.input->types;
	auto key_reference =
	    make_uniq<LogicalCTERef>(optimizer.binder.GenerateTableIndex(), owner_index, source_types,
	                             AggregateRewriteHelper::GenerateColumnNames("domain", source_types.size()));
	auto &domain_reference = *key_reference;
	auto key_expression =
	    make_uniq<BoundColumnRefExpression>(source_types[source.key], key_reference->GetColumnBindings()[source.key]);
	auto domain = CreateKeyDomain(optimizer, std::move(key_reference), std::move(key_expression));
	auto restriction = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);
	restriction->conditions.emplace_back(aggregate.groups[0]->Copy(), DomainKey(*domain), comparison);
	auto source_rows = source_stats->cardinality.GetDouble();
	// CTE readers scan the full row. A new owner also writes it and replaces the consumer with another reader.
	auto source_read_write_work = (source.owner ? 1 : 3) * source_rows * static_cast<double>(source_types.size());
	DomainCostModel costs(extractor, aggregate, source_rows, source_read_write_work);
	auto action = TryOptimizeRuntimeRestriction(optimizer.context, costs, aggregate, demand, demand_index, domain,
	                                            comparison, cte_scopes);
	if (action != RestrictionAction::KEEP) {
		if (action == RestrictionAction::REPLACE && !source.owner) {
			replacements.Merge(MaterializeRuntimeDomain(optimizer, op, 1 - aggregate_side, domain_reference));
		}
		return true;
	}
	if (join.join_type != JoinType::SINGLE && HasRuntimeFilterTargets(aggregate)) {
		return false;
	}
	if (!costs.IsProfitable(*restriction, *domain)) {
		return false;
	}
	restriction->children.push_back(std::move(aggregate.children[0]));
	restriction->children.push_back(std::move(domain));
	aggregate.children[0] = std::move(restriction);
	if (!source.owner) {
		replacements.Merge(MaterializeRuntimeDomain(optimizer, op, 1 - aggregate_side, domain_reference));
	}
	return true;
}

//! The number of references of the materialized CTE `cte` below `op`, nested CTE definitions included.
static idx_t CountReferences(LogicalOperator &op, TableIndex cte) {
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF) {
		return op.Cast<LogicalCTERef>().cte_index == cte ? 1 : 0;
	}
	idx_t count = 0;
	for (auto &child : op.children) {
		count += CountReferences(*child, cte);
	}
	return count;
}

//! Which producers stop when the restriction of the aggregate input to the keys of `domain` is removed and the outer
//! side `demand` takes the place of the domain. The restriction keys and the domain go away with the rewrite; their
//! producers only stop when nothing else reads them:
//! - the duplicate elimination behind the domain, when it is inline or a CTE that only the domain and the restriction
//!   keys reference;
//! - the materialization of the source, when the demand is its only remaining reference and the CTE is not
//!   materialized on request (it is then inlined).
static ReleasedDomainWork FindReleasedDomainWork(LogicalOperator &demand, LogicalOperator &domain,
                                                 optional_ptr<LogicalOperator> restriction_keys, TableIndex source,
                                                 const vector<reference<LogicalMaterializedCTE>> &cte_scopes) {
	ReleasedDomainWork released;
	reference<LogicalOperator> production = domain;
	while (production.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
		production = *production.get().children[0];
	}
	auto released_source_references =
	    CountReferences(domain, source) + (restriction_keys ? CountReferences(*restriction_keys, source) : 0);
	bool production_stops = true;
	if (production.get().type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto index = production.get().Cast<LogicalCTERef>().cte_index;
		auto owner = FindVisibleOwner(index, cte_scopes);
		production_stops =
		    owner && index != source &&
		    CountReferences(*owner->children[1], index) ==
		        CountReferences(domain, index) + (restriction_keys ? CountReferences(*restriction_keys, index) : 0);
		if (production_stops) {
			production = *owner->children[0];
			released_source_references += CountReferences(production, source);
		}
	}
	if (production_stops) {
		released.production = production.get();
	}
	auto owner = FindVisibleOwner(source, cte_scopes);
	if (owner && owner->materialize != CTEMaterialize::CTE_MATERIALIZE_ALWAYS && CountReferences(demand, source) == 1 &&
	    CountReferences(*owner->children[1], source) == 1 + released_source_references) {
		released.source = *owner->children[0];
	}
	return released;
}

static bool RecoveryIsProfitable(LogicalComparisonJoin &join, const OuterDomainJoin &matched,
                                 RelationStatsExtractor &extractor, const ReleasedDomainWork &released,
                                 double restriction_savings) {
	double expression_work = 0;
	idx_t single_row_joins = 0;
	optional_ptr<LogicalOperator> current = *join.children[1];
	for (idx_t depth = 0; depth < matched.chain_length; depth++) {
		if (current->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			for (auto &expression : current->expressions) {
				if (!IsWorkReference(*expression)) {
					expression_work += static_cast<double>(ExpressionHeuristics::Cost(*expression));
				}
			}
		} else {
			single_row_joins++;
		}
		current = *current->children[0];
	}
	if (expression_work == 0 && single_row_joins == 0) {
		return true;
	}
	auto outer = extractor.Extract(*join.children[0]);
	auto domain = extractor.Extract(*matched.domain_join->children[0]);
	if (!outer || !domain) {
		return false;
	}
	auto outer_rows = outer->cardinality.GetDouble();
	return DomainCostModel::IsRecoveryProfitable(outer_rows, *domain, expression_work, single_row_joins,
	                                             DomainCostModel::EstimateReleasedWork(extractor, released),
	                                             restriction_savings);
}

//! Replace the recovery domain by the outer rows when the saved join pays for additional evaluation.
bool DuplicateEliminatedDomainOptimizer::TryPullUpOuterDomainJoin(unique_ptr<LogicalOperator> &op) {
	OuterDomainJoin matched;
	if (!FindOuterDomainJoin(*op, cte_scopes, matched)) {
		return false;
	}
	RelationStatsExtractor extractor(optimizer.context, [&](TableIndex index) -> optional_ptr<LogicalOperator> {
		auto owner = FindVisibleOwner(index, cte_scopes);
		return owner ? owner->children[0].get() : nullptr;
	});
	auto collapse_released = FindReleasedDomainWork(*op->children[0], *matched.domain_join->children[0], nullptr,
	                                                matched.source, cte_scopes);
	optional<DomainRestriction> restriction;
	RestrictionDecision decision;
	if (matched.aggregate) {
		restriction.emplace(optimizer.context, cte_scopes, *matched.aggregate, matched.comparisons);
		auto restriction_join = restriction->GetJoin();
		auto &aggregate_side = *matched.domain_join->children[1];
		if (restriction_join &&
		    CountSourceReaders(aggregate_side, matched.source, cte_scopes) ==
		        CountSourceReaders(*restriction_join->children[1], matched.source, cte_scopes) &&
		    RuntimeRestrictionContainsDemand(*restriction, *op->children[0], matched.demand_indices, cte_scopes)) {
			auto released = FindReleasedDomainWork(*op->children[0], *matched.domain_join->children[0],
			                                       *restriction_join->children[1], matched.source, cte_scopes);
			// Only credit work that survives collapse with the restriction kept.
			if (collapse_released.production) {
				released.production = nullptr;
			}
			if (collapse_released.source) {
				released.source = nullptr;
			}
			DomainCostModel costs(extractor, *matched.aggregate);
			costs.SetReleasedWork(released);
			decision = costs.ChooseRestrictionAction(*restriction, nullptr);
		}
	}
	if (!RecoveryIsProfitable(op->Cast<LogicalComparisonJoin>(), matched, extractor, collapse_released,
	                          decision.removal_savings)) {
		return false;
	}
	if (restriction && decision.action == RestrictionAction::REMOVE) {
		restriction->Remove();
	}

	auto old_bindings = op->GetColumnBindings();
	auto &join = op->Cast<LogicalComparisonJoin>();
	join.children[0]->ResolveOperatorTypes();
	auto left_types = join.children[0]->types;
	auto left_bindings = join.children[0]->GetColumnBindings();
	auto right_projection_map = std::move(join.right_projection_map);
	join.right_projection_map.clear();

	// The join takes the place of the LEFT join against the key domain.
	ColumnBindingReplacer replacer;
	for (auto &key : matched.keys) {
		replacer.replacement_bindings.emplace_back(key.first, key.second);
	}
	auto chain_top = std::move(join.children[1]);
	vector<reference<LogicalOperator>> chain;
	reference<unique_ptr<LogicalOperator>> slot = chain_top;
	for (idx_t depth = 0; depth < matched.chain_length; depth++) {
		chain.push_back(*slot.get());
		slot = slot.get()->children[0];
	}
	auto &domain_join = slot.get()->Cast<LogicalComparisonJoin>();
	join.children[1] = std::move(domain_join.children[1]);
	join.conditions = std::move(domain_join.conditions);
	for (auto &condition : join.conditions) {
		replacer.VisitExpression(&condition.LeftReference());
	}
	join.join_type = JoinType::LEFT;
	slot.get() = std::move(op);

	// The operators above read the LHS keys instead of the domain and pass the LHS through.
	auto current_left = left_bindings;
	idx_t top_column_count = 0;
	for (idx_t depth = chain.size(); depth-- > 0;) {
		auto &node = chain[depth].get();
		if (node.type != LogicalOperatorType::LOGICAL_PROJECTION) {
			continue;
		}
		auto table_index = node.Cast<LogicalProjection>().table_index;
		for (auto &expression : node.expressions) {
			replacer.VisitExpression(&expression);
		}
		top_column_count = node.expressions.size();
		for (idx_t column = 0; column < current_left.size(); column++) {
			auto passed = ColumnBinding::PushExpression(
			    node.expressions, make_uniq<BoundColumnRefExpression>(left_types[column], current_left[column]));
			current_left[column] = ColumnBinding(table_index, passed);
		}
	}

	// Restore the column order of the join: its LHS, then its RHS.
	auto projection = LogicalProjection::CreateIdentity(optimizer.binder.GenerateTableIndex(), std::move(chain_top));
	auto right_expressions = std::move(projection->expressions);
	projection->expressions.clear();
	for (idx_t column = top_column_count; column < right_expressions.size(); column++) {
		projection->expressions.push_back(std::move(right_expressions[column]));
	}
	if (right_projection_map.empty()) {
		for (idx_t column = 0; column < top_column_count; column++) {
			projection->expressions.push_back(std::move(right_expressions[column]));
		}
	} else {
		for (auto &index : right_projection_map) {
			projection->expressions.push_back(right_expressions[index.GetIndex()]->Copy());
		}
	}
	projection->ResolveOperatorTypes();
	auto new_bindings = projection->GetColumnBindings();
	op = std::move(projection);
	for (idx_t column = 0; column < old_bindings.size(); column++) {
		if (old_bindings[column] != new_bindings[column] && !replacements.Find(old_bindings[column])) {
			replacements.Add(old_bindings[column], new_bindings[column]);
		}
	}
	return true;
}

bool DuplicateEliminatedDomainOptimizer::TryOptimizeDomain(unique_ptr<LogicalOperator> &op, idx_t aggregate_side,
                                                           idx_t condition_index) {
	auto &join = op->Cast<LogicalComparisonJoin>();
	auto &condition = join.conditions[condition_index];
	auto &aggregate_key = aggregate_side == 0 ? condition.GetLHS() : condition.GetRHS();
	auto &demand_key = aggregate_side == 0 ? condition.GetRHS() : condition.GetLHS();
	auto aggregate_index = ReferenceIndex(aggregate_key, *join.children[aggregate_side]);
	auto demand_index = ReferenceIndex(demand_key, *join.children[1 - aggregate_side]);
	if (!aggregate_index.IsValid() || !demand_index.IsValid() ||
	    !aggregate_key.GetReturnType().EqualsIncludingCollation(demand_key.GetReturnType())) {
		return false;
	}
	auto aggregate = FindAggregate(*join.children[aggregate_side], aggregate_index.GetIndex());
	if (!aggregate) {
		return false;
	}
	vector<vector<unique_ptr<Expression>>> rows;
	if (!CopyFixedKeys(*join.children[1 - aggregate_side], demand_index.GetIndex(), demand_key.GetReturnType(), rows)) {
		return TryOptimizeRuntimeDomain(op, *aggregate, aggregate_side, demand_index.GetIndex(),
		                                condition.GetComparisonType());
	}
	auto key_index = optimizer.binder.GenerateTableIndex();
	auto fixed =
	    make_uniq<LogicalExpressionGet>(key_index, vector<LogicalType> {demand_key.GetReturnType()}, std::move(rows));
	fixed->children.push_back(make_uniq<LogicalDummyScan>(optimizer.binder.GenerateTableIndex()));
	auto key_expression =
	    make_uniq<BoundColumnRefExpression>(demand_key.GetReturnType(), ColumnBinding(key_index, ProjectionIndex(0)));
	auto domain = CreateKeyDomain(optimizer, std::move(fixed), std::move(key_expression));
	if (TryOptimizeRestriction(optimizer, *aggregate, domain, condition.GetComparisonType(), cte_scopes)) {
		return true;
	}
	// Replacing an existing restriction keeps its filter opportunity. A new one may duplicate the consumer's work.
	if (join.join_type != JoinType::SINGLE && HasRuntimeFilterTargets(*aggregate)) {
		return false;
	}
	auto restriction = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);
	restriction->conditions.emplace_back(aggregate->groups[0]->Copy(), DomainKey(*domain),
	                                     condition.GetComparisonType());
	RelationStatsExtractor extractor(optimizer.context);
	auto key_input_rows = static_cast<double>(domain->children[0]->Cast<LogicalExpressionGet>().expressions.size());
	DomainCostModel costs(extractor, *aggregate, key_input_rows, 0);
	if (!costs.IsProfitable(*restriction, *domain)) {
		return false;
	}
	// All analysis borrows have expired. Preserve every original binding and payload column.
	restriction->children.push_back(std::move(aggregate->children[0]));
	restriction->children.push_back(std::move(domain));
	aggregate->children[0] = std::move(restriction);
	return true;
}

void DuplicateEliminatedDomainOptimizer::VisitOperator(unique_ptr<LogicalOperator> &op) {
	replacements = BindingReplacementGraph();
	if (op->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE) {
		return;
	}
	BindingReplacementGraph child_replacements;
	// Visit only original children; generated domain operators are never reconsidered.
	for (idx_t child_index = 0; child_index < op->children.size(); child_index++) {
		auto publish_owner = op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && child_index == 1;
		if (publish_owner) {
			cte_scopes.push_back(op->Cast<LogicalMaterializedCTE>());
		}
		ColumnBindingRewrite::RewriteChild(op, child_index,
		                                   [&](unique_ptr<LogicalOperator> &child, BindingReplacementGraph &bindings) {
			                                   VisitOperator(child);
			                                   bindings = std::move(replacements);
			                                   child_replacements.Merge(bindings);
		                                   });
		if (publish_owner) {
			cte_scopes.pop_back();
		}
	}
	replacements = std::move(child_replacements);
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN && !TryPullUpOuterDomainJoin(op)) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		bool safe_conditions = true;
		LogicalOperatorVisitor::EnumerateExpressions(join, [&](const unique_ptr<Expression> *expression) {
			safe_conditions &= !(*expression)->CanThrow() && !(*expression)->IsVolatile();
		});
		if (safe_conditions && join.expressions.empty() && join.duplicate_eliminated_columns.empty()) {
			for (idx_t condition_index = 0; condition_index < join.conditions.size(); condition_index++) {
				auto &condition = join.conditions[condition_index];
				if (condition.IsComparison() &&
				    (condition.GetComparisonType() == ExpressionType::COMPARE_EQUAL ||
				     condition.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM)) {
					auto join_type = join.join_type;
					auto rewritten = join_type == JoinType::INNER && TryOptimizeDomain(op, 0, condition_index);
					if (!rewritten && (join_type == JoinType::INNER || join_type == JoinType::LEFT ||
					                   join_type == JoinType::SINGLE)) {
						rewritten = TryOptimizeDomain(op, 1, condition_index);
					}
					if (rewritten) {
						break;
					}
				}
			}
		}
	}
	if (!replacements.Empty()) {
		replacements = replacements.ScopedToBoundary(op->GetColumnBindings());
	}
}

} // namespace duckdb
