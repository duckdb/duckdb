#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/logical_operator_deep_copy.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"

namespace duckdb {

static void AddLateralCorrelation(CorrelatedColumns &correlated_columns, CorrelatedColumnInfo info) {
	for (auto &existing : correlated_columns) {
		if (existing == info) {
			return;
		}
	}
	correlated_columns.AddColumn(std::move(info));
}

static void AddLateralCorrelation(CorrelatedColumns &correlated_columns, const BoundColumnRefExpression &colref) {
	AddLateralCorrelation(correlated_columns, CorrelatedColumnInfo(colref.Binding(), colref.GetReturnType(),
	                                                               colref.GetName(), colref.Depth() + 1));
}

static bool IsBindingIn(const ColumnBinding &binding, const unordered_set<TableIndex> &bindings) {
	return bindings.find(binding.table_index) != bindings.end();
}

static vector<Identifier> GenerateInternalColumnNames(idx_t column_count, const string &prefix) {
	vector<Identifier> result;
	result.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result.push_back(Identifier(prefix + to_string(i)));
	}
	return result;
}

static bool GetContiguousBindingIndex(const vector<ColumnBinding> &bindings, TableIndex &table_index) {
	if (bindings.empty()) {
		return false;
	}
	table_index = bindings[0].table_index;
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (bindings[i].table_index != table_index || bindings[i].column_index != ProjectionIndex(i)) {
			return false;
		}
	}
	return true;
}

static bool IsGeneratedColumn(LogicalGet &get, idx_t column_index) {
	auto table = get.GetTable();
	if (!table) {
		return false;
	}
	return table->GetColumn(LogicalIndex(column_index)).Generated();
}

static void ExpandLogicalGetOutput(LogicalGet &get) {
	for (idx_t column_index = 0; column_index < get.returned_types.size(); column_index++) {
		if (IsGeneratedColumn(get, column_index)) {
			continue;
		}
		if (!get.TryGetProjectionIndex(column_index).IsValid()) {
			get.AddColumnId(NumericCast<column_t>(column_index));
		}
	}
}

static void ExpandLogicalGetOutputs(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		ExpandLogicalGetOutput(op.Cast<LogicalGet>());
	}
	for (auto &child : op.children) {
		ExpandLogicalGetOutputs(*child);
	}
}

static vector<ReplacementBinding> CreateBindingReplacements(const vector<ColumnBinding> &old_bindings,
                                                            const vector<ColumnBinding> &new_bindings) {
	D_ASSERT(old_bindings.size() == new_bindings.size());
	vector<ReplacementBinding> result;
	result.reserve(old_bindings.size());
	for (idx_t i = 0; i < old_bindings.size(); i++) {
		if (old_bindings[i] != new_bindings[i]) {
			result.emplace_back(old_bindings[i], new_bindings[i]);
		}
	}
	return result;
}

static void ApplyBindingReplacements(ColumnBinding &binding, const vector<ReplacementBinding> &replacements) {
	for (auto &replacement : replacements) {
		if (binding == replacement.old_binding) {
			binding = replacement.new_binding;
			return;
		}
	}
}

class ReplaceConditionBindings : public LogicalOperatorVisitor {
public:
	explicit ReplaceConditionBindings(const vector<ReplacementBinding> &replacements) : replacements(replacements) {
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		auto old_binding = expr.Binding();
		ApplyBindingReplacements(expr.BindingMutable(), replacements);
		if (expr.Binding() != old_binding) {
			for (auto &replacement : replacements) {
				if (replacement.old_binding == old_binding && replacement.replace_type) {
					expr.SetReturnType(replacement.new_type);
					break;
				}
			}
		}
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		for (auto &corr : expr.GetBinder()->correlated_columns) {
			ApplyBindingReplacements(corr.binding, replacements);
		}
		VisitOperator(*expr.SubqueryMutable().plan);
		return nullptr;
	}

private:
	const vector<ReplacementBinding> &replacements;
};

class OuterReferenceDetector : public LogicalOperatorVisitor {
public:
	OuterReferenceDetector(const unordered_set<TableIndex> &left_bindings,
	                       const unordered_set<TableIndex> &right_bindings)
	    : left_bindings(left_bindings), right_bindings(right_bindings) {
	}

	bool HasOuterReference(unique_ptr<Expression> &condition) {
		VisitExpression(&condition);
		return has_outer_reference;
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.Depth() > 0 && !IsBindingIn(expr.Binding(), left_bindings) &&
		    !IsBindingIn(expr.Binding(), right_bindings)) {
			has_outer_reference = true;
		}
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		for (auto &corr : expr.GetBinder()->correlated_columns) {
			if (!IsBindingIn(corr.binding, left_bindings) && !IsBindingIn(corr.binding, right_bindings)) {
				has_outer_reference = true;
				break;
			}
		}
		VisitOperator(*expr.SubqueryMutable().plan);
		return nullptr;
	}

private:
	const unordered_set<TableIndex> &left_bindings;
	const unordered_set<TableIndex> &right_bindings;
	bool has_outer_reference = false;
};

class LateralizeJoinCondition : public LogicalOperatorVisitor {
public:
	explicit LateralizeJoinCondition(const unordered_set<TableIndex> &left_bindings,
	                                 const unordered_set<TableIndex> &right_bindings,
	                                 CorrelatedColumns &correlated_columns)
	    : left_bindings(left_bindings), right_bindings(right_bindings), correlated_columns(correlated_columns) {
	}

protected:
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (!IsBindingIn(expr.Binding(), right_bindings) &&
		    (IsBindingIn(expr.Binding(), left_bindings) || expr.Depth() > 0)) {
			AddLateralCorrelation(correlated_columns, expr);
			expr.DepthMutable()++;
		}
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		for (auto &corr : expr.GetBinder()->correlated_columns) {
			if (IsBindingIn(corr.binding, right_bindings) ||
			    (!IsBindingIn(corr.binding, left_bindings) && corr.depth <= 1)) {
				continue;
			}
			AddLateralCorrelation(correlated_columns, corr);
			corr.depth++;
		}
		// The join condition becomes a filter on the RHS of a left lateral join.
		// Existing references to anything outside the RHS therefore cross one more binder boundary.
		VisitOperator(*expr.SubqueryMutable().plan);
		return nullptr;
	}

private:
	const unordered_set<TableIndex> &left_bindings;
	const unordered_set<TableIndex> &right_bindings;
	CorrelatedColumns &correlated_columns;
};

static bool HasPairDependentSubquery(const Expression &expr, const unordered_set<TableIndex> &left_bindings,
                                     const unordered_set<TableIndex> &right_bindings) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		auto side = JoinSide::GetCurrentJoinSide(expr, left_bindings, right_bindings);
		if (side == JoinSide::BOTH) {
			return true;
		}
	}
	bool has_pair_dependent_subquery = false;
	ExpressionIterator::EnumerateChildren(expr, [&](const Expression &child) {
		if (HasPairDependentSubquery(child, left_bindings, right_bindings)) {
			has_pair_dependent_subquery = true;
		}
	});
	return has_pair_dependent_subquery;
}

static bool LateralizePairDependentCondition(unique_ptr<Expression> &condition,
                                             const unordered_set<TableIndex> &left_bindings,
                                             const unordered_set<TableIndex> &right_bindings,
                                             CorrelatedColumns &correlated_columns) {
	LateralizeJoinCondition lateralize(left_bindings, right_bindings, correlated_columns);
	lateralize.VisitExpression(&condition);
	if (correlated_columns.empty()) {
		return false;
	}
	return true;
}

static bool HasOuterReference(unique_ptr<Expression> &condition, const unordered_set<TableIndex> &left_bindings,
                              const unordered_set<TableIndex> &right_bindings) {
	OuterReferenceDetector detector(left_bindings, right_bindings);
	return detector.HasOuterReference(condition);
}

unique_ptr<LogicalOperator> Binder::PlanPairDependentLeftJoin(unique_ptr<LogicalOperator> left,
                                                              unique_ptr<LogicalOperator> right,
                                                              unique_ptr<Expression> condition,
                                                              const unordered_set<TableIndex> &left_bindings,
                                                              const unordered_set<TableIndex> &right_bindings) {
	CorrelatedColumns correlated_columns;
	if (!LateralizePairDependentCondition(condition, left_bindings, right_bindings, correlated_columns)) {
		return nullptr;
	}
	auto filter = make_uniq<LogicalFilter>(std::move(condition));
	filter->AddChild(std::move(right));
	right = std::move(filter);
	return PlanLateralJoin(std::move(left), std::move(right), correlated_columns, JoinType::LEFT, nullptr);
}

static unique_ptr<LogicalOperator> CreateCTERef(TableIndex table_index, TableIndex cte_index,
                                                const vector<LogicalType> &types, const vector<Identifier> &names) {
	return make_uniq<LogicalCTERef>(table_index, cte_index, types, names);
}

struct BoundColumnPayload {
	vector<LogicalType> types;
	vector<ColumnBinding> bindings;
};

struct FullJoinSide {
	TableIndex original_index;
	TableIndex cte_index;
	vector<ColumnBinding> bindings;
	vector<LogicalType> types;
	vector<Identifier> names;
};

struct FullJoinMatch {
	TableIndex cte_index;
	vector<LogicalType> types;
	vector<Identifier> names;
};

static BoundColumnPayload CreatePayload(vector<LogicalType> types, vector<ColumnBinding> bindings) {
	D_ASSERT(types.size() == bindings.size());
	return BoundColumnPayload {std::move(types), std::move(bindings)};
}

static BoundColumnPayload SlicePayload(const BoundColumnPayload &payload, idx_t offset, idx_t count) {
	D_ASSERT(offset + count <= payload.types.size());
	D_ASSERT(offset + count <= payload.bindings.size());
	BoundColumnPayload result;
	result.types.reserve(count);
	result.bindings.reserve(count);
	for (idx_t i = 0; i < count; i++) {
		result.types.push_back(payload.types[offset + i]);
		result.bindings.push_back(payload.bindings[offset + i]);
	}
	return result;
}

static void AddNotDistinctConditions(vector<JoinCondition> &conditions, const BoundColumnPayload &left_payload,
                                     const BoundColumnPayload &right_payload) {
	D_ASSERT(left_payload.types.size() == left_payload.bindings.size());
	D_ASSERT(right_payload.types.size() == right_payload.bindings.size());
	D_ASSERT(left_payload.types.size() == right_payload.types.size());
	for (idx_t i = 0; i < left_payload.types.size(); i++) {
		auto left = make_uniq<BoundColumnRefExpression>(left_payload.types[i], left_payload.bindings[i]);
		auto right = make_uniq<BoundColumnRefExpression>(right_payload.types[i], right_payload.bindings[i]);
		conditions.emplace_back(std::move(left), std::move(right), ExpressionType::COMPARE_NOT_DISTINCT_FROM);
	}
}

static bool PrepareFullJoinSide(unique_ptr<LogicalOperator> &side, FullJoinSide &info, const string &name_prefix) {
	ExpandLogicalGetOutputs(*side);
	side->ResolveOperatorTypes();

	info.bindings = side->GetColumnBindings();
	info.types = side->types;
	D_ASSERT(info.bindings.size() == info.types.size());
	if (!GetContiguousBindingIndex(info.bindings, info.original_index)) {
		return false;
	}
	info.names = GenerateInternalColumnNames(info.types.size(), name_prefix);
	return true;
}

static unique_ptr<LogicalOperator> CreateDistinctMatchProjection(Binder &binder, unique_ptr<LogicalOperator> match,
                                                                 idx_t payload_column_count,
                                                                 vector<LogicalType> &match_types) {
	match->ResolveOperatorTypes();
	auto match_bindings = match->GetColumnBindings();
	D_ASSERT(payload_column_count <= match_bindings.size());
	D_ASSERT(payload_column_count <= match->types.size());

	auto group_index = binder.GenerateTableIndex();
	auto aggregate_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> aggregates;
	auto distinct = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
	for (idx_t i = 0; i < payload_column_count; i++) {
		distinct->groups.push_back(make_uniq<BoundColumnRefExpression>(match->types[i], match_bindings[i]));
	}
	distinct->children.push_back(std::move(match));
	distinct->ResolveOperatorTypes();

	auto distinct_bindings = distinct->GetColumnBindings();
	vector<unique_ptr<Expression>> projections;
	projections.reserve(distinct_bindings.size() + 1);
	for (idx_t i = 0; i < distinct_bindings.size(); i++) {
		projections.push_back(make_uniq<BoundColumnRefExpression>(distinct->types[i], distinct_bindings[i]));
	}
	projections.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(projections));
	projection->children.push_back(std::move(distinct));
	projection->ResolveOperatorTypes();
	match_types = projection->types;
	return std::move(projection);
}

static unique_ptr<LogicalOperator> CreatePairDependentFullJoin(Binder &binder, const FullJoinSide &left,
                                                               const FullJoinSide &right, const FullJoinMatch &match) {
	auto final_left = CreateCTERef(left.original_index, left.cte_index, left.types, left.names);
	auto final_right = CreateCTERef(right.original_index, right.cte_index, right.types, right.names);
	auto left_match_index = binder.GenerateTableIndex();
	auto left_match = CreateCTERef(left_match_index, match.cte_index, match.types, match.names);

	auto final_left_payload = CreatePayload(left.types, final_left->GetColumnBindings());
	auto left_match_payload = CreatePayload(match.types, left_match->GetColumnBindings());

	vector<JoinCondition> left_match_conditions;
	AddNotDistinctConditions(left_match_conditions, final_left_payload,
	                         SlicePayload(left_match_payload, 0, left.types.size()));
	auto left_with_matches =
	    LogicalComparisonJoin::CreateJoin(JoinType::LEFT, JoinRefType::REGULAR, std::move(final_left),
	                                      std::move(left_match), std::move(left_match_conditions));

	left_with_matches->ResolveOperatorTypes();
	auto left_with_matches_payload = CreatePayload(left_with_matches->types, left_with_matches->GetColumnBindings());
	auto final_right_payload = CreatePayload(right.types, final_right->GetColumnBindings());
	auto marker_binding = left_with_matches_payload.bindings[left.types.size() + match.types.size() - 1];

	vector<JoinCondition> final_conditions;
	AddNotDistinctConditions(
	    final_conditions,
	    SlicePayload(left_with_matches_payload, left.types.size() + left.types.size(), right.types.size()),
	    final_right_payload);
	auto marker_ref = make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, marker_binding);
	auto marker_true = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	final_conditions.emplace_back(BoundComparisonExpression::Create(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
	                                                                std::move(marker_ref), std::move(marker_true)));

	auto final_join =
	    LogicalComparisonJoin::CreateJoin(JoinType::OUTER, JoinRefType::REGULAR, std::move(left_with_matches),
	                                      std::move(final_right), std::move(final_conditions));
	auto &logical_join = final_join->Cast<LogicalJoin>();
	logical_join.left_projection_map.reserve(left.types.size());
	for (idx_t i = 0; i < left.types.size(); i++) {
		logical_join.left_projection_map.emplace_back(i);
	}
	logical_join.right_projection_map.reserve(right.types.size());
	for (idx_t i = 0; i < right.types.size(); i++) {
		logical_join.right_projection_map.emplace_back(i);
	}
#ifdef DEBUG
	final_join->ResolveOperatorTypes();
	auto actual_bindings = final_join->GetColumnBindings();
	auto expected_bindings = left.bindings;
	expected_bindings.insert(expected_bindings.end(), right.bindings.begin(), right.bindings.end());
	D_ASSERT(actual_bindings == expected_bindings);
#endif
	return final_join;
}

static unique_ptr<LogicalOperator>
WrapPairDependentFullJoinCTEs(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
                              unique_ptr<LogicalOperator> match_source, unique_ptr<LogicalOperator> final_join,
                              TableIndex left_cte_index, TableIndex right_cte_index, TableIndex match_cte_index,
                              idx_t left_column_count, idx_t right_column_count, idx_t match_column_count) {
	auto match_cte_name = Identifier("__duckdb_full_match_" + to_string(match_cte_index.index));
	auto result =
	    make_uniq<LogicalMaterializedCTE>(match_cte_name, match_cte_index, match_column_count, std::move(match_source),
	                                      std::move(final_join), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	auto right_cte_name = Identifier("__duckdb_full_right_" + to_string(right_cte_index.index));
	result = make_uniq<LogicalMaterializedCTE>(right_cte_name, right_cte_index, right_column_count, std::move(right),
	                                           std::move(result), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	auto left_cte_name = Identifier("__duckdb_full_left_" + to_string(left_cte_index.index));
	result = make_uniq<LogicalMaterializedCTE>(left_cte_name, left_cte_index, left_column_count, std::move(left),
	                                           std::move(result), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	return result;
}

bool Binder::TryPlanPairDependentFullJoin(BoundJoinRef &ref, unique_ptr<LogicalOperator> &left,
                                          unique_ptr<LogicalOperator> &right, unique_ptr<LogicalOperator> &result) {
	left->ResolveOperatorTypes();
	right->ResolveOperatorTypes();

	TableIndex original_left_index;
	TableIndex original_right_index;
	if (!GetContiguousBindingIndex(left->GetColumnBindings(), original_left_index) ||
	    !GetContiguousBindingIndex(right->GetColumnBindings(), original_right_index)) {
		return false;
	}
	unordered_set<TableIndex> left_binding_set {original_left_index};
	unordered_set<TableIndex> right_binding_set {original_right_index};
	if (HasOuterReference(ref.condition, left_binding_set, right_binding_set)) {
		return false;
	}

	FullJoinSide left_side;
	FullJoinSide right_side;
	if (!PrepareFullJoinSide(left, left_side, "__duckdb_full_l_") ||
	    !PrepareFullJoinSide(right, right_side, "__duckdb_full_r_")) {
		return false;
	}
	D_ASSERT(left_side.original_index == original_left_index);
	D_ASSERT(right_side.original_index == original_right_index);

	left_side.cte_index = GenerateTableIndex();
	right_side.cte_index = GenerateTableIndex();
	FullJoinMatch match;
	match.cte_index = GenerateTableIndex();

	LogicalOperatorDeepCopy left_remapper(*this, nullptr);
	left_remapper.Remap(*left);
	LogicalOperatorDeepCopy right_remapper(*this, nullptr);
	right_remapper.Remap(*right);

	auto match_left_index = GenerateTableIndex();
	auto match_right_index = GenerateTableIndex();
	auto match_left = CreateCTERef(match_left_index, left_side.cte_index, left_side.types, left_side.names);
	auto match_right = CreateCTERef(match_right_index, right_side.cte_index, right_side.types, right_side.names);
	auto match_left_bindings = match_left->GetColumnBindings();
	auto match_right_bindings = match_right->GetColumnBindings();

	vector<ReplacementBinding> condition_replacements;
	auto left_replacements = CreateBindingReplacements(left_side.bindings, match_left_bindings);
	auto right_replacements = CreateBindingReplacements(right_side.bindings, match_right_bindings);
	condition_replacements.insert(condition_replacements.end(), left_replacements.begin(), left_replacements.end());
	condition_replacements.insert(condition_replacements.end(), right_replacements.begin(), right_replacements.end());
	ReplaceConditionBindings replace_condition(condition_replacements);
	replace_condition.VisitExpression(&ref.condition);

	auto match_join = LogicalCrossProduct::Create(std::move(match_left), std::move(match_right));
	auto match_filter = make_uniq<LogicalFilter>(std::move(ref.condition));
	for (auto &expression : match_filter->expressions) {
		PlanSubqueries(expression, match_join);
	}
	match_filter->AddChild(std::move(match_join));

	auto payload_column_count = left_side.types.size() + right_side.types.size();
	auto match_source =
	    CreateDistinctMatchProjection(*this, std::move(match_filter), payload_column_count, match.types);
	match.names = GenerateInternalColumnNames(match.types.size(), "__duckdb_full_match_");

	auto final_join = CreatePairDependentFullJoin(*this, left_side, right_side, match);
	result = WrapPairDependentFullJoinCTEs(
	    std::move(left), std::move(right), std::move(match_source), std::move(final_join), left_side.cte_index,
	    right_side.cte_index, match.cte_index, left_side.types.size(), right_side.types.size(), match.types.size());
	return true;
}

bool Binder::TryPlanPairDependentJoin(BoundJoinRef &ref, unique_ptr<LogicalOperator> &left,
                                      unique_ptr<LogicalOperator> &right, unique_ptr<LogicalOperator> &result) {
	if (!ref.condition || ref.lateral || ref.ref_type != JoinRefType::REGULAR) {
		return false;
	}
	unordered_set<TableIndex> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*left, left_bindings);
	LogicalJoin::GetTableReferences(*right, right_bindings);
	if (!HasPairDependentSubquery(*ref.condition, left_bindings, right_bindings)) {
		return false;
	}

	if (ref.type == JoinType::LEFT) {
		result = PlanPairDependentLeftJoin(std::move(left), std::move(right), std::move(ref.condition), left_bindings,
		                                   right_bindings);
		return result != nullptr;
	}
	if (ref.type == JoinType::OUTER) {
		return TryPlanPairDependentFullJoin(ref, left, right, result);
	}
	return false;
}

} // namespace duckdb
