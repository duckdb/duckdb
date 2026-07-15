#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/subquery/column_binding_layout.hpp"
#include "duckdb/planner/subquery/recursive_dependent_join_planner.hpp"

namespace duckdb {

// Join-condition subqueries that reference both inputs cannot be planned against
// one child. Non-FULL joins are lowered to the existing dependent/lateral join
// representation. FULL OUTER joins need the paper's match-domain rewrite so
// unmatched rows from both inputs can be reconstructed after the predicate has
// been decorrelated by the normal RecursiveDependentJoinPlanner/FlattenDependentJoins path.

static vector<Identifier> GenerateInternalColumnNames(idx_t column_count, const string &prefix) {
	vector<Identifier> result;
	result.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result.push_back(Identifier(prefix + to_string(i)));
	}
	return result;
}

// FULL OUTER pair-dependent predicates use the paper's D_R x D_S construction. Each side is projected to the
// bindings referenced by the predicate and independently deduplicated before the domain product is formed.

struct PairDependentJoinSide {
	TableIndex cte_index;
	vector<ColumnBinding> bindings;
	vector<LogicalType> types;
	vector<Identifier> names;
	vector<idx_t> domain_positions;
	bool domain_is_duplicate_free = false;
	unique_ptr<LogicalOperator> source;
};

struct PairDependentDomain {
	unique_ptr<LogicalOperator> plan;
	vector<ColumnBinding> original_bindings;
	vector<ColumnBinding> bindings;
	vector<LogicalType> types;
};

struct PairDependentSideRef {
	unique_ptr<LogicalOperator> plan;
	vector<ColumnBinding> payload_bindings;
	vector<ColumnBinding> domain_bindings;
	ColumnBindingLayout output;
};

struct PairDependentFullOuterJoinResult {
	unique_ptr<LogicalOperator> plan;
	vector<ReplacementBinding> replacements;
};

static void AddCurrentJoinBinding(const ColumnBinding &binding, const unordered_set<TableIndex> &left_bindings,
                                  const unordered_set<TableIndex> &right_bindings,
                                  column_binding_set_t &left_references, column_binding_set_t &right_references) {
	if (left_bindings.count(binding.table_index)) {
		left_references.insert(binding);
	} else if (right_bindings.count(binding.table_index)) {
		right_references.insert(binding);
	}
}

static void CollectPairDependentBindings(Expression &expression, const unordered_set<TableIndex> &left_bindings,
                                         const unordered_set<TableIndex> &right_bindings,
                                         column_binding_set_t &left_references,
                                         column_binding_set_t &right_references) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		if (colref.Depth() == 0) {
			AddCurrentJoinBinding(colref.Binding(), left_bindings, right_bindings, left_references, right_references);
		}
		return;
	}
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		auto &subquery = expression.Cast<BoundSubqueryExpression>();
		for (auto &column : subquery.GetBinder()->correlated_columns) {
			if (column.depth == 1) {
				AddCurrentJoinBinding(column.binding, left_bindings, right_bindings, left_references, right_references);
			}
		}
	}
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &child) {
		CollectPairDependentBindings(child, left_bindings, right_bindings, left_references, right_references);
	});
}

static bool ContainsAllBindings(const column_binding_set_t &bindings, const vector<ColumnBinding> &required) {
	for (auto &binding : required) {
		if (!bindings.count(binding)) {
			return false;
		}
	}
	return true;
}

static bool ReturnsAtMostOneRow(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		return true;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return op.Cast<LogicalAggregate>().groups.empty() && op.Cast<LogicalAggregate>().grouping_sets.size() <= 1;
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = op.Cast<LogicalLimit>();
		return limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE && limit.limit_val.GetConstantValue() <= 1;
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return op.children.size() == 1 && ReturnsAtMostOneRow(*op.children[0]);
	default:
		return false;
	}
}

static bool IsDuplicateFreeOn(LogicalOperator &op, const column_binding_set_t &bindings) {
	if (ReturnsAtMostOneRow(op)) {
		return true;
	}
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggregate = op.Cast<LogicalAggregate>();
		if (!aggregate.grouping_functions.empty() || aggregate.grouping_sets.size() > 1) {
			return false;
		}
		vector<ColumnBinding> group_bindings;
		group_bindings.reserve(aggregate.groups.size());
		for (idx_t i = 0; i < aggregate.groups.size(); i++) {
			group_bindings.emplace_back(aggregate.group_index, ProjectionIndex(i));
		}
		return ContainsAllBindings(bindings, group_bindings);
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		vector<ColumnBinding> distinct_bindings;
		if (distinct.distinct_targets.empty()) {
			distinct_bindings = op.GetColumnBindings();
		} else {
			for (auto &target : distinct.distinct_targets) {
				if (target->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
					return false;
				}
				distinct_bindings.push_back(target->Cast<BoundColumnRefExpression>().Binding());
			}
		}
		return ContainsAllBindings(bindings, distinct_bindings);
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		auto &set_operation = op.Cast<LogicalSetOperation>();
		return !set_operation.setop_all && ContainsAllBindings(bindings, op.GetColumnBindings());
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		auto &projection = op.Cast<LogicalProjection>();
		column_binding_set_t child_bindings;
		for (auto &binding : bindings) {
			if (binding.table_index != projection.table_index ||
			    binding.column_index.GetIndex() >= projection.expressions.size()) {
				return false;
			}
			auto &expression = projection.expressions[binding.column_index.GetIndex()];
			if (expression->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return false;
			}
			child_bindings.insert(expression->Cast<BoundColumnRefExpression>().Binding());
		}
		return IsDuplicateFreeOn(*op.children[0], child_bindings);
	}
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
		return op.children.size() == 1 && IsDuplicateFreeOn(*op.children[0], bindings);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		column_binding_set_t left_bindings;
		column_binding_set_t right_bindings;
		auto left_output = ColumnBindingLayout(op.children[0]->GetColumnBindings());
		auto right_output = ColumnBindingLayout(op.children[1]->GetColumnBindings());
		for (auto &binding : bindings) {
			if (left_output.positions.count(binding)) {
				left_bindings.insert(binding);
			} else if (right_output.positions.count(binding)) {
				right_bindings.insert(binding);
			} else {
				return false;
			}
		}
		auto left_unique = left_bindings.empty() ? ReturnsAtMostOneRow(*op.children[0])
		                                         : IsDuplicateFreeOn(*op.children[0], left_bindings);
		auto right_unique = right_bindings.empty() ? ReturnsAtMostOneRow(*op.children[1])
		                                           : IsDuplicateFreeOn(*op.children[1], right_bindings);
		return left_unique && right_unique;
	}
	default:
		return false;
	}
}

static PairDependentJoinSide PreparePairDependentJoinSide(Binder &binder, unique_ptr<LogicalOperator> source,
                                                          const column_binding_set_t &domain_bindings,
                                                          const string &name_prefix) {
	source->ResolveOperatorTypes();
	PairDependentJoinSide result;
	result.bindings = source->GetColumnBindings();
	result.types = source->types;
	result.names = GenerateInternalColumnNames(result.types.size(), name_prefix);
	result.cte_index = binder.GenerateTableIndex();
	result.source = std::move(source);
	for (idx_t i = 0; i < result.bindings.size(); i++) {
		if (domain_bindings.count(result.bindings[i])) {
			result.domain_positions.push_back(i);
		}
	}
	if (result.domain_positions.empty()) {
		throw InternalException("Pair-dependent FULL OUTER join side has no domain bindings");
	}
	column_binding_set_t projected_domain_bindings;
	for (auto position : result.domain_positions) {
		projected_domain_bindings.insert(result.bindings[position]);
	}
	result.domain_is_duplicate_free = IsDuplicateFreeOn(*result.source, projected_domain_bindings);
	return result;
}

static PairDependentDomain CreatePairDependentDomain(Binder &binder, const PairDependentJoinSide &side) {
	auto cte_ref = make_uniq<LogicalCTERef>(binder.GenerateTableIndex(), side.cte_index, side.types, side.names);
	auto cte_bindings = cte_ref->GetColumnBindings();

	PairDependentDomain result;
	vector<unique_ptr<Expression>> domain_expressions;
	for (auto position : side.domain_positions) {
		result.original_bindings.push_back(side.bindings[position]);
		result.types.push_back(side.types[position]);
		domain_expressions.push_back(make_uniq<BoundColumnRefExpression>(side.types[position], cte_bindings[position]));
	}
	if (side.domain_is_duplicate_free) {
		auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(domain_expressions));
		projection->children.push_back(std::move(cte_ref));
		result.plan = std::move(projection);
	} else {
		auto group_index = binder.GenerateTableIndex();
		auto aggregate_index = binder.GenerateTableIndex();
		vector<unique_ptr<Expression>> aggregates;
		auto distinct = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
		distinct->groups = std::move(domain_expressions);
		distinct->children.push_back(std::move(cte_ref));
		result.plan = std::move(distinct);
	}
	result.bindings = result.plan->GetColumnBindings();
	return result;
}

static PairDependentSideRef CreatePairDependentSideRef(Binder &binder, const PairDependentJoinSide &side) {
	auto cte_ref = make_uniq<LogicalCTERef>(binder.GenerateTableIndex(), side.cte_index, side.types, side.names);
	auto bindings = cte_ref->GetColumnBindings();
	PairDependentSideRef result;
	result.payload_bindings = bindings;
	for (auto position : side.domain_positions) {
		result.domain_bindings.push_back(bindings[position]);
	}
	result.plan = std::move(cte_ref);
	result.output = ColumnBindingLayout(result.payload_bindings, "pair-dependent FULL OUTER join side");
	return result;
}

static void AddNotDistinctConditions(vector<JoinCondition> &conditions, const vector<LogicalType> &types,
                                     const vector<ColumnBinding> &left_bindings,
                                     const vector<ColumnBinding> &right_bindings) {
	D_ASSERT(types.size() == left_bindings.size());
	D_ASSERT(types.size() == right_bindings.size());
	for (idx_t i = 0; i < types.size(); i++) {
		conditions.emplace_back(make_uniq<BoundColumnRefExpression>(types[i], left_bindings[i]),
		                        make_uniq<BoundColumnRefExpression>(types[i], right_bindings[i]),
		                        ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	}
}

static void AddMarkerCondition(vector<JoinCondition> &conditions, ColumnBinding marker_binding) {
	auto marker = make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, marker_binding);
	auto marker_true = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	conditions.emplace_back(BoundComparisonExpression::Create(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
	                                                          std::move(marker), std::move(marker_true)));
}

static void SetJoinProjectionMaps(LogicalOperator &join, const ColumnBindingLayout &left_output,
                                  const vector<ColumnBinding> &selected_left_bindings,
                                  const ColumnBindingLayout &right_output,
                                  const vector<ColumnBinding> &selected_right_bindings) {
	auto &logical_join = join.Cast<LogicalJoin>();
	logical_join.left_projection_map = left_output.CreateProjectionMap(selected_left_bindings);
	logical_join.right_projection_map = right_output.CreateProjectionMap(selected_right_bindings);
}

class PairDependentFullOuterJoinBuilder {
public:
	PairDependentFullOuterJoinBuilder(Binder &binder, unique_ptr<Expression> condition,
	                                  unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
	                                  const unordered_set<TableIndex> &left_bindings,
	                                  const unordered_set<TableIndex> &right_bindings)
	    : binder(binder), condition(std::move(condition)), left(std::move(left)), right(std::move(right)),
	      left_table_bindings(left_bindings), right_table_bindings(right_bindings) {
	}

	PairDependentFullOuterJoinResult Build();

private:
	Binder &binder;
	unique_ptr<Expression> condition;
	unique_ptr<LogicalOperator> left;
	unique_ptr<LogicalOperator> right;
	const unordered_set<TableIndex> &left_table_bindings;
	const unordered_set<TableIndex> &right_table_bindings;
};

PairDependentFullOuterJoinResult PairDependentFullOuterJoinBuilder::Build() {
	column_binding_set_t left_references;
	column_binding_set_t right_references;
	CollectPairDependentBindings(*condition, left_table_bindings, right_table_bindings, left_references,
	                             right_references);

	auto left_side = PreparePairDependentJoinSide(binder, std::move(left), left_references, "__duckdb_full_l_");
	auto right_side = PreparePairDependentJoinSide(binder, std::move(right), right_references, "__duckdb_full_r_");
	auto left_domain = CreatePairDependentDomain(binder, left_side);
	auto right_domain = CreatePairDependentDomain(binder, right_side);

	CorrelatedColumnBindingReplacer condition_replacer;
	condition_replacer.AddReplacements(left_domain.original_bindings, left_domain.bindings);
	condition_replacer.AddReplacements(right_domain.original_bindings, right_domain.bindings);
	condition_replacer.VisitExpression(&condition);

	auto match_root = LogicalCrossProduct::Create(std::move(left_domain.plan), std::move(right_domain.plan));

	vector<unique_ptr<Expression>> match_expressions;
	for (idx_t i = 0; i < left_domain.bindings.size(); i++) {
		match_expressions.push_back(make_uniq<BoundColumnRefExpression>(left_domain.types[i], left_domain.bindings[i]));
	}
	for (idx_t i = 0; i < right_domain.bindings.size(); i++) {
		match_expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(right_domain.types[i], right_domain.bindings[i]));
	}
	match_expressions.push_back(std::move(condition));
	unique_ptr<LogicalOperator> match_projection =
	    make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(match_expressions));
	match_projection->children.push_back(std::move(match_root));
	RecursiveDependentJoinPlanner::Plan(binder, match_projection);
	match_projection->ResolveOperatorTypes();
	auto match_bindings = match_projection->GetColumnBindings();
	auto left_domain_count = NumericCast<vector<ColumnBinding>::difference_type>(left_domain.bindings.size());
	auto left_match_bindings =
	    vector<ColumnBinding>(match_bindings.begin(), match_bindings.begin() + left_domain_count);
	auto right_match_bindings =
	    vector<ColumnBinding>(match_bindings.begin() + left_domain_count, match_bindings.end() - 1);
	auto marker_binding = match_bindings.back();

	// S LEFT JOIN match emits every matching S row for each R domain and emits an unmatched S row only once.
	auto right_ref = CreatePairDependentSideRef(binder, right_side);
	vector<JoinCondition> right_match_conditions;
	AddNotDistinctConditions(right_match_conditions, right_domain.types, right_ref.domain_bindings,
	                         right_match_bindings);
	AddMarkerCondition(right_match_conditions, marker_binding);
	auto right_with_match =
	    LogicalComparisonJoin::CreateJoin(JoinType::LEFT, JoinRefType::REGULAR, std::move(right_ref.plan),
	                                      std::move(match_projection), std::move(right_match_conditions));
	auto match_output = ColumnBindingLayout(match_bindings, "pair-dependent FULL OUTER join match");
	auto selected_match_bindings = left_match_bindings;
	selected_match_bindings.push_back(marker_binding);
	SetJoinProjectionMaps(*right_with_match, right_ref.output, right_ref.payload_bindings, match_output,
	                      selected_match_bindings);
	auto right_with_match_output =
	    ColumnBindingLayout(right_with_match->GetColumnBindings(), "pair-dependent FULL OUTER join match");

	// R FULL JOIN the matching domain restores matching pairs and the unmatched R rows.
	auto left_ref = CreatePairDependentSideRef(binder, left_side);
	vector<JoinCondition> final_conditions;
	AddNotDistinctConditions(final_conditions, left_domain.types, left_ref.domain_bindings, left_match_bindings);
	AddMarkerCondition(final_conditions, marker_binding);
	auto final_join = LogicalComparisonJoin::CreateJoin(JoinType::OUTER, JoinRefType::REGULAR, std::move(left_ref.plan),
	                                                    std::move(right_with_match), std::move(final_conditions));
	SetJoinProjectionMaps(*final_join, left_ref.output, left_ref.payload_bindings, right_with_match_output,
	                      right_ref.payload_bindings);

	vector<unique_ptr<Expression>> output_expressions;
	for (idx_t i = 0; i < left_ref.payload_bindings.size(); i++) {
		output_expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(left_side.types[i], left_ref.payload_bindings[i]));
	}
	for (idx_t i = 0; i < right_ref.payload_bindings.size(); i++) {
		output_expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(right_side.types[i], right_ref.payload_bindings[i]));
	}
	auto output = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(output_expressions));
	output->children.push_back(std::move(final_join));
	auto output_bindings = output->GetColumnBindings();

	PairDependentFullOuterJoinResult result;
	result.replacements.reserve(output_bindings.size());
	for (idx_t i = 0; i < left_side.bindings.size(); i++) {
		result.replacements.emplace_back(left_side.bindings[i], output_bindings[i]);
	}
	for (idx_t i = 0; i < right_side.bindings.size(); i++) {
		result.replacements.emplace_back(right_side.bindings[i], output_bindings[left_side.bindings.size() + i]);
	}

	unique_ptr<LogicalOperator> plan = std::move(output);
	plan =
	    make_uniq<LogicalMaterializedCTE>(Identifier("__duckdb_pair_right_" + to_string(right_side.cte_index.index)),
	                                      right_side.cte_index, right_side.types.size(), std::move(right_side.source),
	                                      std::move(plan), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	plan = make_uniq<LogicalMaterializedCTE>(Identifier("__duckdb_pair_left_" + to_string(left_side.cte_index.index)),
	                                         left_side.cte_index, left_side.types.size(), std::move(left_side.source),
	                                         std::move(plan), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	result.plan = std::move(plan);
	return result;
}

static bool HasPairDependentSubquery(Expression &expression, const unordered_set<TableIndex> &left_bindings,
                                     const unordered_set<TableIndex> &right_bindings) {
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY &&
	    JoinSide::GetCurrentJoinSide(expression, left_bindings, right_bindings) == JoinSide::BOTH) {
		return true;
	}
	bool result = false;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &child) {
		result = result || HasPairDependentSubquery(child, left_bindings, right_bindings);
	});
	return result;
}

static bool HasPairDependentSubquery(JoinCondition &condition, const unordered_set<TableIndex> &left_bindings,
                                     const unordered_set<TableIndex> &right_bindings) {
	if (condition.IsComparison()) {
		return HasPairDependentSubquery(condition.GetLHS(), left_bindings, right_bindings) ||
		       HasPairDependentSubquery(condition.GetRHS(), left_bindings, right_bindings);
	}
	return HasPairDependentSubquery(condition.GetJoinExpression(), left_bindings, right_bindings);
}

static bool HasPairDependentSubquery(LogicalOperator &op, const unordered_set<TableIndex> &left_bindings,
                                     const unordered_set<TableIndex> &right_bindings) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &condition : join.conditions) {
			if (HasPairDependentSubquery(condition, left_bindings, right_bindings)) {
				return true;
			}
		}
		return false;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		return HasPairDependentSubquery(*op.Cast<LogicalAnyJoin>().condition, left_bindings, right_bindings);
	default:
		return false;
	}
}

bool RecursiveDependentJoinPlanner::CanRewritePairDependentJoinCondition(LogicalOperator &op) {
	if (op.children.size() != 2 ||
	    (op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN && op.type != LogicalOperatorType::LOGICAL_ANY_JOIN)) {
		return false;
	}
	if (op.Cast<LogicalJoin>().join_type != JoinType::OUTER) {
		return false;
	}
	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*op.children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op.children[1], right_bindings);
	return HasPairDependentSubquery(op, left_bindings, right_bindings);
}

static unique_ptr<Expression> MoveJoinCondition(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return JoinCondition::CreateExpression(std::move(op.Cast<LogicalComparisonJoin>().conditions));
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		return std::move(op.Cast<LogicalAnyJoin>().condition);
	default:
		return nullptr;
	}
}

bool RecursiveDependentJoinPlanner::TryRewritePairDependentJoinCondition(Binder &binder,
                                                                         unique_ptr<LogicalOperator> &op,
                                                                         vector<ReplacementBinding> &replacements) {
	if (!op || !CanRewritePairDependentJoinCondition(*op)) {
		return false;
	}

	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);
	op->children[0]->ResolveOperatorTypes();
	op->children[1]->ResolveOperatorTypes();
	if (op->children[0]->GetColumnBindings().empty() || op->children[1]->GetColumnBindings().empty()) {
		return false;
	}

	auto condition = MoveJoinCondition(*op);
	auto left = std::move(op->children[0]);
	auto right = std::move(op->children[1]);

	PairDependentFullOuterJoinBuilder builder(binder, std::move(condition), std::move(left), std::move(right),
	                                          left_bindings, right_bindings);
	auto result = builder.Build();
	op = std::move(result.plan);
	replacements = std::move(result.replacements);
	return true;
}

} // namespace duckdb
