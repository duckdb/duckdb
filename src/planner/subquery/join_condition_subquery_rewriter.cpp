#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/subquery/column_binding_layout.hpp"
#include "duckdb/planner/subquery/recursive_dependent_join_planner.hpp"

namespace duckdb {

// Join-condition subqueries that reference both inputs cannot be planned against
// one child. Non-FULL joins are lowered to the existing dependent/lateral join
// representation. FULL OUTER joins need the paper's match-domain rewrite so
// unmatched rows from both inputs can be reconstructed after the predicate has
// been decorrelated by the normal RecursiveDependentJoinPlanner/FlattenDependentJoins path.

static void AddLateralCorrelation(CorrelatedColumns &correlated_columns, CorrelatedColumnInfo info) {
	for (auto &existing : correlated_columns) {
		if (existing == info) {
			return;
		}
	}
	correlated_columns.AddColumn(std::move(info));
}

static bool IsBindingIn(const ColumnBinding &binding, const unordered_set<TableIndex> &bindings) {
	return bindings.find(binding.table_index) != bindings.end();
}

static bool ReferenceEscapesSubqueryScope(idx_t reference_depth, idx_t scope_depth) {
	return scope_depth == 0 ? reference_depth > 0 : reference_depth > scope_depth;
}

class LateralChildDepthAdjuster : public LogicalOperatorVisitor {
public:
	explicit LateralChildDepthAdjuster(CorrelatedColumns &correlated_columns) : correlated_columns(correlated_columns) {
	}

	void Adjust(LogicalOperator &op) {
		CollectLocalBindings(op);
		VisitOperator(op);
	}

protected:
	void VisitOperator(LogicalOperator &op) override {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
			AdjustCorrelatedColumns(op.Cast<LogicalDependentJoin>().correlated_columns);
			break;
		case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			AdjustCorrelatedColumns(op.Cast<LogicalCTE>().correlated_columns);
			break;
		default:
			break;
		}
		LogicalOperatorVisitor::VisitOperator(op);
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.Depth() > 0 && !IsBindingIn(expr.Binding(), local_bindings)) {
			CorrelatedColumnInfo info(expr);
			expr.DepthMutable()++;
			info.depth++;
			AddLateralCorrelation(correlated_columns, std::move(info));
		}
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		AdjustCorrelatedColumns(expr.GetBinder()->correlated_columns);
		if (expr.SubqueryMutable().plan) {
			CollectLocalBindings(*expr.SubqueryMutable().plan);
			VisitOperator(*expr.SubqueryMutable().plan);
		}
		return nullptr;
	}

private:
	void CollectLocalBindings(LogicalOperator &op) {
		for (auto &binding : op.GetColumnBindings()) {
			local_bindings.insert(binding.table_index);
		}
		for (auto &child : op.children) {
			CollectLocalBindings(*child);
		}
	}

	void AdjustCorrelatedColumns(CorrelatedColumns &columns) {
		for (auto &column : columns) {
			if (column.depth == 0 || IsBindingIn(column.binding, local_bindings)) {
				continue;
			}
			auto info = column;
			column.depth++;
			info.depth++;
			AddLateralCorrelation(correlated_columns, std::move(info));
		}
	}

	CorrelatedColumns &correlated_columns;
	unordered_set<TableIndex> local_bindings;
};

class PendingJoinConditionLateralizer : public LogicalOperatorVisitor {
public:
	PendingJoinConditionLateralizer(const unordered_set<TableIndex> &left_bindings,
	                                const unordered_set<TableIndex> &right_bindings,
	                                CorrelatedColumns &correlated_columns)
	    : left_bindings(left_bindings), right_bindings(right_bindings), correlated_columns(correlated_columns) {
	}

protected:
	void VisitOperator(LogicalOperator &op) override {
		if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
			AdjustCorrelatedColumns(op.Cast<LogicalDependentJoin>().correlated_columns, subquery_plan_depth + 1);
			subquery_plan_depth++;
			LogicalOperatorVisitor::VisitOperator(op);
			subquery_plan_depth--;
			return;
		}
		if (op.type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE ||
		    op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			AdjustCorrelatedColumns(op.Cast<LogicalCTE>().correlated_columns, subquery_plan_depth);
		}
		LogicalOperatorVisitor::VisitOperator(op);
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (NeedsLateralRewrite(expr.Binding(), expr.Depth(), subquery_plan_depth)) {
			CorrelatedColumnInfo info(expr.Binding(), expr.GetReturnType(), expr.GetName(), expr.Depth() + 1);
			AddLateralCorrelation(correlated_columns, std::move(info));
			expr.DepthMutable()++;
		}
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		AdjustCorrelatedColumns(expr.GetBinder()->correlated_columns, subquery_plan_depth + 1);
		if (expr.SubqueryMutable().plan) {
			subquery_plan_depth++;
			VisitOperator(*expr.SubqueryMutable().plan);
			subquery_plan_depth--;
		}
		return nullptr;
	}

private:
	void AdjustCorrelatedColumns(CorrelatedColumns &columns, idx_t scope_depth) {
		for (auto &column : columns) {
			if (!NeedsLateralRewrite(column.binding, column.depth, scope_depth)) {
				continue;
			}
			auto info = column;
			column.depth++;
			info.depth++;
			AddLateralCorrelation(correlated_columns, std::move(info));
		}
	}

	bool NeedsLateralRewrite(const ColumnBinding &binding, idx_t reference_depth, idx_t scope_depth) const {
		if (IsBindingIn(binding, right_bindings)) {
			return false;
		}
		if (IsBindingIn(binding, left_bindings)) {
			return true;
		}
		return ReferenceEscapesSubqueryScope(reference_depth, scope_depth);
	}

	const unordered_set<TableIndex> &left_bindings;
	const unordered_set<TableIndex> &right_bindings;
	CorrelatedColumns &correlated_columns;
	idx_t subquery_plan_depth = 0;
};

static void AddPairDependentFilter(unique_ptr<LogicalOperator> &right, unique_ptr<Expression> condition) {
	auto filter = make_uniq<LogicalFilter>(std::move(condition));
	if (right->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE ||
	    right->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte = right->Cast<LogicalCTE>();
		filter->AddChild(std::move(cte.children[1]));
		cte.children[1] = std::move(filter);
		return;
	}
	filter->AddChild(std::move(right));
	right = std::move(filter);
}

unique_ptr<LogicalOperator> RecursiveDependentJoinPlanner::PlanPairDependentLateralJoin(
    Binder &binder, unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
    unique_ptr<Expression> condition, const unordered_set<TableIndex> &left_bindings,
    const unordered_set<TableIndex> &right_bindings, JoinType join_type) {
	CorrelatedColumns correlated_columns;
	LateralChildDepthAdjuster child_adjuster(correlated_columns);
	child_adjuster.Adjust(*right);
	PendingJoinConditionLateralizer condition_lateralizer(left_bindings, right_bindings, correlated_columns);
	condition_lateralizer.VisitExpression(&condition);
	if (correlated_columns.empty()) {
		throw InternalException("Pair-dependent join condition did not produce lateral correlations");
	}
	AddPairDependentFilter(right, std::move(condition));
	return binder.PlanLateralJoin(std::move(left), std::move(right), correlated_columns, join_type, nullptr);
}

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
	auto group_index = binder.GenerateTableIndex();
	auto aggregate_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> aggregates;
	auto distinct = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
	distinct->groups = std::move(domain_expressions);
	distinct->children.push_back(std::move(cte_ref));
	result.plan = std::move(distinct);
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
	logical_join.left_projection_map = left_output.HasSameLayout(selected_left_bindings)
	                                       ? vector<ProjectionIndex>()
	                                       : left_output.CreateProjectionMap(selected_left_bindings);
	logical_join.right_projection_map = right_output.HasSameLayout(selected_right_bindings)
	                                        ? vector<ProjectionIndex>()
	                                        : right_output.CreateProjectionMap(selected_right_bindings);
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
	auto planned_match = RecursiveDependentJoinPlanner::Plan(binder, std::move(match_projection));
	match_projection = std::move(planned_match.plan);
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

static bool SupportsPairDependentRewrite(JoinType join_type) {
	switch (join_type) {
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::RIGHT_SEMI:
	case JoinType::RIGHT_ANTI:
	case JoinType::OUTER:
		return true;
	default:
		return false;
	}
}

bool RecursiveDependentJoinPlanner::CanRewritePairDependentJoinCondition(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN || op.children.size() != 2) {
		return false;
	}
	auto &join = op.Cast<LogicalDependentJoin>();
	if (join.dependent_type != DependentJoinType::JOIN_CONDITION || !join.condition ||
	    !SupportsPairDependentRewrite(join.join_type)) {
		return false;
	}
	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*op.children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op.children[1], right_bindings);
	return HasPairDependentSubquery(*join.condition, left_bindings, right_bindings);
}

static unique_ptr<LogicalOperator> RestoreJoinOutput(Binder &binder, unique_ptr<LogicalOperator> plan,
                                                     const vector<ColumnBinding> &expected_bindings,
                                                     vector<ReplacementBinding> &replacements) {
	plan->ResolveOperatorTypes();
	auto output = ColumnBindingLayout(plan->GetColumnBindings(), "normalized pair-dependent join output");
	if (output.HasSameLayout(expected_bindings)) {
		return plan;
	}
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(expected_bindings.size());
	for (auto &binding : expected_bindings) {
		auto position = output.GetPosition(binding);
		expressions.push_back(make_uniq<BoundColumnRefExpression>(plan->types[position], binding));
	}
	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(expressions));
	projection->children.push_back(std::move(plan));
	auto projection_bindings = projection->GetColumnBindings();
	for (idx_t i = 0; i < expected_bindings.size(); i++) {
		replacements.emplace_back(expected_bindings[i], projection_bindings[i]);
	}
	return std::move(projection);
}

bool RecursiveDependentJoinPlanner::TryRewritePairDependentJoinCondition(Binder &binder,
                                                                         unique_ptr<LogicalOperator> &op,
                                                                         vector<ReplacementBinding> &replacements) {
	if (!op || !CanRewritePairDependentJoinCondition(*op)) {
		return false;
	}

	auto &join = op->Cast<LogicalDependentJoin>();
	auto join_type = join.join_type;
	auto expected_bindings = op->GetColumnBindings();
	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
	LogicalJoin::GetTableReferences(*op->children[0], left_bindings);
	LogicalJoin::GetTableReferences(*op->children[1], right_bindings);
	if (join_type == JoinType::OUTER) {
		op->children[0]->ResolveOperatorTypes();
		op->children[1]->ResolveOperatorTypes();
		if (op->children[0]->GetColumnBindings().empty() || op->children[1]->GetColumnBindings().empty()) {
			return false;
		}
	}

	auto condition = std::move(join.condition);
	auto left = std::move(op->children[0]);
	auto right = std::move(op->children[1]);

	switch (join_type) {
	case JoinType::LEFT:
	case JoinType::SEMI:
	case JoinType::ANTI:
		op = PlanPairDependentLateralJoin(binder, std::move(left), std::move(right), std::move(condition),
		                                  left_bindings, right_bindings, join_type);
		return true;
	case JoinType::RIGHT:
		op = PlanPairDependentLateralJoin(binder, std::move(right), std::move(left), std::move(condition),
		                                  right_bindings, left_bindings, JoinType::LEFT);
		op = RestoreJoinOutput(binder, std::move(op), expected_bindings, replacements);
		return true;
	case JoinType::RIGHT_SEMI:
	case JoinType::RIGHT_ANTI: {
		auto normalized_type = join_type == JoinType::RIGHT_SEMI ? JoinType::SEMI : JoinType::ANTI;
		op = PlanPairDependentLateralJoin(binder, std::move(right), std::move(left), std::move(condition),
		                                  right_bindings, left_bindings, normalized_type);
		return true;
	}
	case JoinType::OUTER: {
		PairDependentFullOuterJoinBuilder builder(binder, std::move(condition), std::move(left), std::move(right),
		                                          left_bindings, right_bindings);
		auto result = builder.Build();
		op = std::move(result.plan);
		replacements = std::move(result.replacements);
		return true;
	}
	default:
		return false;
	}
}

} // namespace duckdb
