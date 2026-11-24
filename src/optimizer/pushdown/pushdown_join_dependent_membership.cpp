#include "duckdb/optimizer/filter_pushdown.hpp"

#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_extractor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/expression_barrier.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_repeatability.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

struct MembershipKey {
	MembershipKey(unique_ptr<Expression> expression, ExpressionType comparison)
	    : expression(std::move(expression)), comparison(comparison) {
	}

	unique_ptr<Expression> expression;
	ExpressionType comparison;
};

struct MembershipSource {
	explicit MembershipSource(LogicalProjection &projection) : projection(projection) {
	}

	LogicalProjection &projection;
	//! Payload projection columns in the common probe key order
	vector<ProjectionIndex> key_columns;
};

struct MembershipTarget {
	MembershipTarget(LogicalAggregate &aggregate, vector<unique_ptr<Expression>> keys)
	    : aggregate(aggregate), keys(std::move(keys)) {
	}

	LogicalAggregate &aggregate;
	//! Probe keys expressed at the aggregate input
	vector<unique_ptr<Expression>> keys;
};

static bool IsMembershipColumn(const Expression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
	       expr.Cast<BoundColumnRefExpression>().Depth() == 0;
}

static bool CanCrossMembershipOperator(LogicalOperator &op) {
	if (ClassifyLogicalOperatorRepeatability(op) != LogicalOperatorRepeatability::REPEATABLE) {
		return false;
	}
	bool has_barrier = false;
	LogicalOperatorVisitor::EnumerateExpressions(
	    op, [&](unique_ptr<Expression> *expr) { has_barrier |= ExpressionBarrier::Contains(**expr); });
	return !has_barrier;
}

static bool CanShareMembershipSource(LogicalOperator &op) {
	// Existing CTEs require producer scope and policy that this local rewrite does not own.
	// Rejecting their readers also prevents applying this rewrite again to the same chain.
	if (op.type == LogicalOperatorType::LOGICAL_CTE_REF || op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE ||
	    !CanCrossMembershipOperator(op)) {
		return false;
	}
	column_binding_set_t inputs;
	for (auto &child : op.children) {
		if (!CanShareMembershipSource(*child)) {
			return false;
		}
		for (auto &binding : child->GetColumnBindings()) {
			inputs.insert(binding);
		}
	}
	bool closed = true;
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(**expr, [&](const BoundColumnRefExpression &ref) {
			closed &= ref.Depth() == 0 && inputs.count(ref.Binding()) != 0;
		});
	});
	return closed;
}

static bool MatchMembershipKeys(LogicalComparisonJoin &join, MembershipSource &source,
                                vector<MembershipKey> &probe_keys) {
	if (join.conditions.empty()) {
		return false;
	}
	auto &projection = source.projection;
	source.key_columns.resize(join.conditions.size());
	const bool first = probe_keys.empty();
	if (!first && join.conditions.size() != probe_keys.size()) {
		return false;
	}
	for (auto &condition : join.conditions) {
		if (!condition.IsComparison() ||
		    (condition.GetComparisonType() != ExpressionType::COMPARE_EQUAL &&
		     condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) ||
		    !IsMembershipColumn(condition.GetLHS()) || !IsMembershipColumn(condition.GetRHS()) ||
		    condition.GetLHS().GetReturnType() != condition.GetRHS().GetReturnType()) {
			return false;
		}
		auto &right_ref = condition.GetRHS().Cast<BoundColumnRefExpression>();
		if (right_ref.Binding().table_index != projection.table_index ||
		    right_ref.Binding().column_index >= projection.expressions.size()) {
			return false;
		}
		auto &key = projection.GetExpression(right_ref.Binding());
		if (!IsMembershipColumn(key) || key.GetReturnType() != condition.GetRHS().GetReturnType()) {
			return false;
		}
		idx_t position = 0;
		for (; position < probe_keys.size(); position++) {
			if (probe_keys[position].expression->Equals(condition.GetLHS())) {
				break;
			}
		}
		if (first) {
			if (position != probe_keys.size()) {
				return false;
			}
			probe_keys.emplace_back(condition.GetLHS().Copy(), condition.GetComparisonType());
		} else if (position == probe_keys.size() || probe_keys[position].comparison != condition.GetComparisonType()) {
			return false;
		}
		if (source.key_columns[position].IsValid()) {
			return false;
		}
		source.key_columns[position] = right_ref.Binding().column_index;
	}
	return true;
}

static unique_ptr<MembershipTarget> FindMembershipTarget(LogicalOperator &root,
                                                         const vector<MembershipKey> &probe_keys) {
	vector<unique_ptr<Expression>> input_keys;
	for (auto &key : probe_keys) {
		input_keys.push_back(key.expression->Copy());
	}
	reference<LogicalOperator> current = root;
	while (CanCrossMembershipOperator(current)) {
		auto &op = current.get();
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_FILTER:
			break;
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &projection = op.Cast<LogicalProjection>();
			for (auto &key : input_keys) {
				auto binding = key->Cast<BoundColumnRefExpression>().Binding();
				if (binding.table_index != projection.table_index ||
				    binding.column_index >= projection.expressions.size()) {
					return nullptr;
				}
				auto &expr = projection.GetExpression(binding);
				// Wrappers such as signbit can split an equal-valued group.
				if (!IsMembershipColumn(expr) || expr.GetReturnType() != key->GetReturnType()) {
					return nullptr;
				}
				key = expr.Copy();
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggr = op.Cast<LogicalAggregate>();
			if (aggr.groups.empty() || aggr.grouping_sets.size() > 1 || !aggr.grouping_functions.empty() ||
			    aggr.preserve_input_order ||
			    (!aggr.grouping_sets.empty() && aggr.grouping_sets[0].size() != aggr.groups.size())) {
				return nullptr;
			}
			// Discarding a group must not suppress errors in grouping expressions outside the join key.
			for (auto &group : aggr.groups) {
				if (group->CanThrow()) {
					return nullptr;
				}
			}
			for (auto &key : input_keys) {
				auto binding = key->Cast<BoundColumnRefExpression>().Binding();
				if (binding.table_index != aggr.group_index || binding.column_index >= aggr.groups.size()) {
					return nullptr;
				}
				auto &group = aggr.GetGroupExpression(binding.column_index);
				if (group.GetReturnType() != key->GetReturnType()) {
					return nullptr;
				}
				key = group.Copy();
			}
			return make_uniq<MembershipTarget>(aggr, std::move(input_keys));
		}
		default:
			return nullptr;
		}
		current = *op.children[0];
	}
	return nullptr;
}

static bool IsMembershipLikelySelective(ClientContext &context, MembershipTarget &target,
                                        const vector<MembershipSource> &sources) {
	RelationStatsExtractor extractor(context);
	auto input = extractor.Extract(*target.aggregate.children[0]);
	if (!input) {
		return false;
	}
	idx_t largest_domain = 0;
	for (auto &key : target.keys) {
		if (!IsMembershipColumn(*key)) {
			return false;
		}
		auto column = input->GetColumnStats(key->Cast<BoundColumnRefExpression>().Binding());
		if (!column) {
			return false;
		}
		largest_domain = MaxValue(largest_domain, column->current_domain.distinct_count);
	}
	double build_rows = 0;
	for (auto &source : sources) {
		auto stats = extractor.Extract(source.projection);
		if (!stats) {
			return false;
		}
		build_rows += static_cast<double>(stats->cardinality);
	}
	// Correlated keys can cover the whole input despite a large product of their distinct counts.
	return build_rows < static_cast<double>(largest_domain);
}

static unique_ptr<LogicalCTERef> CreateMembershipReader(Binder &binder, TableIndex cte_index,
                                                        const vector<LogicalType> &types) {
	vector<Identifier> names;
	for (idx_t i = 0; i < types.size(); i++) {
		names.emplace_back("column" + to_string(i));
	}
	return make_uniq<LogicalCTERef>(binder.GenerateTableIndex(), cte_index, types, std::move(names));
}

static unique_ptr<LogicalOperator> AddMembershipRestriction(Binder &binder, unique_ptr<LogicalOperator> root,
                                                            vector<MembershipSource> &sources,
                                                            const vector<MembershipKey> &probe_keys,
                                                            MembershipTarget &target) {
	vector<unique_ptr<LogicalOperator>> key_readers;
	for (auto &source : sources) {
		auto &projection = source.projection;
		auto producer = std::move(projection.children[0]);
		producer->ResolveOperatorTypes();
		auto old_bindings = producer->GetColumnBindings();
		auto column_count = producer->types.size();
		auto cte_index = binder.GenerateTableIndex();
		auto payload_reader = CreateMembershipReader(binder, cte_index, producer->types);
		ColumnBindingReplacer payload_replacer;
		payload_replacer.AddReplacements(old_bindings, payload_reader->GetColumnBindings());
		payload_replacer.VisitOperatorBindings(projection);
		projection.children[0] = std::move(payload_reader);

		auto key_reader = CreateMembershipReader(binder, cte_index, producer->types);
		vector<unique_ptr<Expression>> keys;
		for (auto column : source.key_columns) {
			auto &key = projection.expressions[column]->Cast<BoundColumnRefExpression>();
			keys.push_back(key.WithBinding(ColumnBinding(key_reader->table_index, key.Binding().column_index)));
		}
		auto key_projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(keys));
		key_projection->AddChild(std::move(key_reader));
		key_readers.push_back(std::move(key_projection));
		root = make_uniq<LogicalMaterializedCTE>("membership_source", cte_index, column_count, std::move(producer),
		                                         std::move(root), CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	}
	auto key_union = make_uniq<LogicalSetOperation>(binder.GenerateTableIndex(), probe_keys.size(),
	                                                std::move(key_readers), LogicalOperatorType::LOGICAL_UNION, true);
	auto restriction = make_uniq<LogicalComparisonJoin>(JoinType::SEMI);
	for (idx_t i = 0; i < probe_keys.size(); i++) {
		auto build_key = make_uniq<BoundColumnRefExpression>(target.keys[i]->GetReturnType(),
		                                                     ColumnBinding(key_union->table_index, ProjectionIndex(i)));
		restriction->conditions.emplace_back(std::move(target.keys[i]), std::move(build_key), probe_keys[i].comparison);
	}
	restriction->AddChild(std::move(target.aggregate.children[0]));
	restriction->AddChild(std::move(key_union));
	target.aggregate.children[0] = std::move(restriction);
	root->ResolveOperatorTypes();
	return root;
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownJoinDependentMembership(unique_ptr<LogicalOperator> op) {
	if (filters.empty() || op->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return op;
	}
	for (auto &filter : filters) {
		if (filter->has_barrier || filter->filter->IsVolatile()) {
			return op;
		}
	}
	vector<MembershipKey> probe_keys;
	vector<MembershipSource> sources;
	unordered_set<TableIndex> nullable_bindings;
	reference<LogicalOperator> current = *op;
	while (current.get().type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = current.get().Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::LEFT || join.children[1]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
			return op;
		}
		MembershipSource source(join.children[1]->Cast<LogicalProjection>());
		if (!MatchMembershipKeys(join, source, probe_keys)) {
			return op;
		}
		nullable_bindings.insert(source.projection.table_index);
		sources.push_back(std::move(source));
		current = *join.children[0];
	}
	if (sources.size() < 2) {
		return op;
	}
	bool removes_null = false;
	for (auto &filter : filters) {
		if (FilterRemovesNull(*filter->filter, nullable_bindings)) {
			removes_null = true;
			break;
		}
	}
	if (!removes_null) {
		return op;
	}
	auto target = FindMembershipTarget(current, probe_keys);
	if (!target) {
		return op;
	}
	for (auto &source : sources) {
		if (!CanShareMembershipSource(source.projection)) {
			return op;
		}
	}
	if (!IsMembershipLikelySelective(optimizer.context, *target, sources)) {
		return op;
	}
	return AddMembershipRestriction(optimizer.binder, std::move(op), sources, probe_keys, *target);
}

} // namespace duckdb
