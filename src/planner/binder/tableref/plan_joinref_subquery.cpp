#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/function/window/rows_functions.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
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
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_dependent_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/tableref/bound_joinref.hpp"

#include <algorithm>

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

static bool ReferenceEscapesSubqueryScope(idx_t reference_depth, idx_t scope_depth) {
	if (scope_depth == 0) {
		return reference_depth > 0;
	}
	return reference_depth > scope_depth;
}

static vector<Identifier> GenerateInternalColumnNames(idx_t column_count, const string &prefix) {
	vector<Identifier> result;
	result.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result.push_back(Identifier(prefix + to_string(i)));
	}
	return result;
}

static bool IsGeneratedColumn(LogicalGet &get, idx_t column_index) {
	auto table = get.GetTable();
	if (!table) {
		return false;
	}
	return table->GetColumn(LogicalIndex(column_index)).Generated();
}

static bool IsUserQueryableVirtualColumn(column_t column_index) {
	return column_index != COLUMN_IDENTIFIER_EMPTY && column_index != COLUMN_IDENTIFIER_ROW_NUMBER;
}

static void AddPayloadColumn(LogicalGet &get, column_t column_index) {
	if (!get.TryGetProjectionIndex(column_index).IsValid()) {
		get.AddColumnId(column_index);
	}
}

static void FinalizeLogicalGetPayload(LogicalGet &get) {
	for (idx_t column_index = 0; column_index < get.returned_types.size(); column_index++) {
		if (IsGeneratedColumn(get, column_index)) {
			continue;
		}
		AddPayloadColumn(get, NumericCast<column_t>(column_index));
	}

	vector<column_t> virtual_column_ids;
	virtual_column_ids.reserve(get.virtual_columns.size());
	for (auto &entry : get.virtual_columns) {
		if (IsUserQueryableVirtualColumn(entry.first)) {
			virtual_column_ids.push_back(entry.first);
		}
	}
	std::sort(virtual_column_ids.begin(), virtual_column_ids.end());
	for (auto virtual_column_id : virtual_column_ids) {
		AddPayloadColumn(get, virtual_column_id);
	}
}

static void FinalizeLogicalGetPayloads(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		FinalizeLogicalGetPayload(op.Cast<LogicalGet>());
	}
	for (auto &child : op.children) {
		FinalizeLogicalGetPayloads(*child);
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
	void VisitOperator(LogicalOperator &op) override {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
			ReplaceCorrelatedColumns(op.Cast<LogicalDependentJoin>().correlated_columns);
			break;
		}
		case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
			ReplaceCorrelatedColumns(op.Cast<LogicalCTE>().correlated_columns);
			break;
		}
		default:
			break;
		}
		LogicalOperatorVisitor::VisitOperator(op);
	}

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
		ReplaceCorrelatedColumns(expr.GetBinder()->correlated_columns);
		VisitOperator(*expr.SubqueryMutable().plan);
		return nullptr;
	}

private:
	void ReplaceCorrelatedColumns(CorrelatedColumns &columns) {
		for (auto &corr : columns) {
			ApplyBindingReplacements(corr.binding, replacements);
		}
	}

	const vector<ReplacementBinding> &replacements;
};

static void AddDepthIncreasedCorrelation(CorrelatedColumns &correlations, const CorrelatedColumnInfo &column) {
	auto copy = column;
	copy.depth++;
	AddLateralCorrelation(correlations, std::move(copy));
}

class ExternalExpressionDepthIncreaser : public LogicalOperatorVisitor {
public:
	explicit ExternalExpressionDepthIncreaser(CorrelatedColumns &increased_columns)
	    : increased_columns(increased_columns) {
	}

	void Increase(LogicalOperator &op) {
		CollectLocalBindings(op);
		VisitOperator(op);
	}

protected:
	void VisitOperator(LogicalOperator &op) override {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
			IncreaseCorrelatedColumns(op.Cast<LogicalDependentJoin>().correlated_columns);
			break;
		case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			IncreaseCorrelatedColumns(op.Cast<LogicalCTE>().correlated_columns);
			break;
		default:
			break;
		}
		LogicalOperatorVisitor::VisitOperator(op);
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.Depth() > 0 && !IsBindingIn(expr.Binding(), local_bindings)) {
			CorrelatedColumnInfo correlation(expr);
			expr.DepthMutable()++;
			AddDepthIncreasedCorrelation(increased_columns, correlation);
		}
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		CollectLocalBindings(*expr.SubqueryMutable().plan);
		IncreaseCorrelatedColumns(expr.GetBinder()->correlated_columns);
		VisitOperator(*expr.SubqueryMutable().plan);
		return nullptr;
	}

private:
	void CollectLocalBindings(LogicalOperator &op) {
		auto bindings = op.GetColumnBindings();
		for (auto &binding : bindings) {
			local_bindings.insert(binding.table_index);
		}
		for (auto &child : op.children) {
			CollectLocalBindings(*child);
		}
	}

	void IncreaseCorrelatedColumns(CorrelatedColumns &columns) {
		for (auto &column : columns) {
			if (column.depth > 0 && !IsBindingIn(column.binding, local_bindings)) {
				auto old_column = column;
				column.depth++;
				AddDepthIncreasedCorrelation(increased_columns, old_column);
			}
		}
	}

	unordered_set<TableIndex> local_bindings;
	CorrelatedColumns &increased_columns;
};

static void IncreaseExternalExpressionDepth(LogicalOperator &op, CorrelatedColumns &increased_columns) {
	ExternalExpressionDepthIncreaser depth_increaser(increased_columns);
	depth_increaser.Increase(op);
}

class LateralizeJoinCondition : public LogicalOperatorVisitor {
public:
	explicit LateralizeJoinCondition(const unordered_set<TableIndex> &left_bindings,
	                                 const unordered_set<TableIndex> &right_bindings,
	                                 CorrelatedColumns &correlated_columns)
	    : left_bindings(left_bindings), right_bindings(right_bindings), correlated_columns(correlated_columns) {
	}

protected:
	void VisitOperator(LogicalOperator &op) override {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
			// A planned nested subquery appears as a dependent join. References
			// local to that nested subquery should not be treated as escaping
			// the join condition that we are moving into the RHS filter.
			LateralizeCorrelatedColumns(op.Cast<LogicalDependentJoin>().correlated_columns, subquery_plan_depth + 1);
			subquery_plan_depth++;
			LogicalOperatorVisitor::VisitOperator(op);
			subquery_plan_depth--;
			return;
		}
		case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			LateralizeCorrelatedColumns(op.Cast<LogicalCTE>().correlated_columns, subquery_plan_depth);
			break;
		default:
			break;
		}
		LogicalOperatorVisitor::VisitOperator(op);
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (NeedsLateralCorrelation(expr.Binding(), expr.Depth(), subquery_plan_depth)) {
			AddLateralCorrelation(correlated_columns, expr);
		}
		if (NeedsDepthIncrement(expr.Binding(), expr.Depth(), subquery_plan_depth)) {
			expr.DepthMutable()++;
		}
		return nullptr;
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		for (auto &corr : expr.GetBinder()->correlated_columns) {
			if (NeedsLateralCorrelation(corr.binding, corr.depth, subquery_plan_depth + 1)) {
				AddLateralCorrelation(correlated_columns, corr);
			}
			if (NeedsDepthIncrement(corr.binding, corr.depth, subquery_plan_depth + 1)) {
				corr.depth++;
			}
		}
		// The join condition becomes a filter on the RHS of a left lateral join.
		// Existing references to anything outside the RHS therefore cross one more binder boundary.
		subquery_plan_depth++;
		VisitOperator(*expr.SubqueryMutable().plan);
		subquery_plan_depth--;
		return nullptr;
	}

private:
	void LateralizeCorrelatedColumns(CorrelatedColumns &columns, idx_t scope_depth) {
		for (auto &corr : columns) {
			if (NeedsLateralCorrelation(corr.binding, corr.depth, scope_depth)) {
				AddLateralCorrelation(correlated_columns, corr);
			}
			if (NeedsDepthIncrement(corr.binding, corr.depth, scope_depth)) {
				corr.depth++;
			}
		}
	}

	bool NeedsDepthIncrement(const ColumnBinding &binding, idx_t reference_depth, idx_t join_scope_depth) const {
		if (IsBindingIn(binding, right_bindings)) {
			return false;
		}
		if (IsBindingIn(binding, left_bindings)) {
			return true;
		}
		return ReferenceEscapesSubqueryScope(reference_depth, join_scope_depth);
	}

	bool NeedsLateralCorrelation(const ColumnBinding &binding, idx_t reference_depth, idx_t join_scope_depth) const {
		if (IsBindingIn(binding, right_bindings)) {
			return false;
		}
		if (IsBindingIn(binding, left_bindings)) {
			return true;
		}
		return ReferenceEscapesSubqueryScope(reference_depth, join_scope_depth);
	}

	const unordered_set<TableIndex> &left_bindings;
	const unordered_set<TableIndex> &right_bindings;
	CorrelatedColumns &correlated_columns;
	idx_t subquery_plan_depth = 0;
};

class PairDependentReferenceAnalyzer {
public:
	PairDependentReferenceAnalyzer(const unordered_set<TableIndex> &left_bindings,
	                               const unordered_set<TableIndex> &right_bindings)
	    : left_bindings(left_bindings), right_bindings(right_bindings) {
	}

	bool HasPairDependentSubquery(Expression &expr) const {
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
			auto side = GetSubquerySide(expr.Cast<BoundSubqueryExpression>());
			if (side == JoinSide::BOTH) {
				return true;
			}
		}
		bool result = false;
		ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
			if (!result && HasPairDependentSubquery(child)) {
				result = true;
			}
		});
		return result;
	}

	bool HasVolatile(Expression &expr) const {
		if (expr.IsVolatile()) {
			return true;
		}
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
			auto &subquery = expr.Cast<BoundSubqueryExpression>();
			if (subquery.Subquery().plan && HasVolatile(*subquery.SubqueryMutable().plan)) {
				return true;
			}
		}
		bool result = false;
		ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
			if (!result && HasVolatile(child)) {
				result = true;
			}
		});
		return result;
	}

private:
	JoinSide GetBindingSide(const ColumnBinding &binding) const {
		return JoinSide::GetCurrentJoinSide(binding.table_index, left_bindings, right_bindings);
	}

	JoinSide GetSubquerySide(BoundSubqueryExpression &subquery) const {
		JoinSide side = JoinSide::NONE;
		for (auto &child : subquery.GetChildren()) {
			auto child_side = GetExpressionSide(*child);
			side = JoinSide::CombineJoinSide(side, child_side);
		}
		for (auto &corr : subquery.GetBinder()->correlated_columns) {
			auto corr_side = GetBindingSide(corr.binding);
			side = JoinSide::CombineJoinSide(side, corr_side);
		}
		if (subquery.SubqueryMutable().plan) {
			auto plan_side = GetOperatorSide(*subquery.SubqueryMutable().plan);
			side = JoinSide::CombineJoinSide(side, plan_side);
		}
		return side;
	}

	JoinSide GetExpressionSide(Expression &expr) const {
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &colref = expr.Cast<BoundColumnRefExpression>();
			return GetBindingSide(colref.Binding());
		}
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
			return GetSubquerySide(expr.Cast<BoundSubqueryExpression>());
		}
		JoinSide side = JoinSide::NONE;
		ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
			auto child_side = GetExpressionSide(child);
			side = JoinSide::CombineJoinSide(side, child_side);
		});
		return side;
	}

	JoinSide GetOperatorSide(LogicalOperator &op) const {
		JoinSide side = JoinSide::NONE;
		AddCorrelatedColumnSides(side, GetOperatorCorrelatedColumns(op));
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
			if (!expr || !*expr) {
				return;
			}
			auto expr_side = GetExpressionSide(**expr);
			side = JoinSide::CombineJoinSide(side, expr_side);
		});
		for (auto &child : op.children) {
			auto child_side = GetOperatorSide(*child);
			side = JoinSide::CombineJoinSide(side, child_side);
		}
		return side;
	}

	void AddCorrelatedColumnSides(JoinSide &side, CorrelatedColumns *columns) const {
		if (!columns) {
			return;
		}
		for (auto &corr : *columns) {
			auto corr_side = GetBindingSide(corr.binding);
			side = JoinSide::CombineJoinSide(side, corr_side);
		}
	}

	CorrelatedColumns *GetOperatorCorrelatedColumns(LogicalOperator &op) const {
		switch (op.type) {
		case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
			return &op.Cast<LogicalDependentJoin>().correlated_columns;
		case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
		case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
			return &op.Cast<LogicalCTE>().correlated_columns;
		default:
			return nullptr;
		}
	}

	bool HasVolatile(LogicalOperator &op) const {
		bool result = false;
		LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expr) {
			if (!result && expr && *expr && HasVolatile(**expr)) {
				result = true;
			}
		});
		if (result) {
			return true;
		}
		for (auto &child : op.children) {
			if (HasVolatile(*child)) {
				return true;
			}
		}
		return false;
	}

	const unordered_set<TableIndex> &left_bindings;
	const unordered_set<TableIndex> &right_bindings;
};

static bool LateralizePairDependentCondition(unique_ptr<Expression> &condition,
                                             const unordered_set<TableIndex> &left_bindings,
                                             const unordered_set<TableIndex> &right_bindings,
                                             CorrelatedColumns &correlated_columns) {
	LateralizeJoinCondition lateralize(left_bindings, right_bindings, correlated_columns);
	lateralize.VisitExpression(&condition);
	return !correlated_columns.empty();
}

static unique_ptr<LogicalOperator> CreateFilter(unique_ptr<Expression> condition, unique_ptr<LogicalOperator> child) {
	auto filter = make_uniq<LogicalFilter>(std::move(condition));
	filter->AddChild(std::move(child));
	return std::move(filter);
}

static void AddPairDependentFilterToRHS(unique_ptr<LogicalOperator> &right, unique_ptr<Expression> condition) {
	if (right->type == LogicalOperatorType::LOGICAL_RECURSIVE_CTE ||
	    right->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte = right->Cast<LogicalCTE>();
		cte.children[1] = CreateFilter(std::move(condition), std::move(cte.children[1]));
		return;
	}
	right = CreateFilter(std::move(condition), std::move(right));
}

unique_ptr<LogicalOperator>
Binder::PlanPairDependentLateralJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
                                     unique_ptr<Expression> condition, const unordered_set<TableIndex> &left_bindings,
                                     const unordered_set<TableIndex> &right_bindings, JoinType join_type) {
	CorrelatedColumns correlated_columns;
	IncreaseExternalExpressionDepth(*right, correlated_columns);
	auto has_condition_correlations =
	    LateralizePairDependentCondition(condition, left_bindings, right_bindings, correlated_columns);
	if (!has_condition_correlations && correlated_columns.empty()) {
		return nullptr;
	}
	AddPairDependentFilterToRHS(right, std::move(condition));
	return PlanLateralJoin(std::move(left), std::move(right), correlated_columns, join_type, nullptr);
}

static unique_ptr<LogicalOperator> CreateCTERef(TableIndex table_index, TableIndex cte_index,
                                                const vector<LogicalType> &types, const vector<Identifier> &names) {
	return make_uniq<LogicalCTERef>(table_index, cte_index, types, names);
}

static unique_ptr<LogicalWindow> CreateRowNumberWindow(Binder &binder, unique_ptr<LogicalOperator> child,
                                                       TableIndex table_index) {
	auto window = make_uniq<LogicalWindow>(table_index);

	auto row_number = RowNumberFun::GetFunction().Bind(binder.context);
	row_number->WindowStartMutable() = WindowBoundary::UNBOUNDED_PRECEDING;
	row_number->WindowEndMutable() = WindowBoundary::CURRENT_ROW_ROWS;
	row_number->SetAlias("__duckdb_pair_rowid");

	window->expressions.push_back(std::move(row_number));
	window->AddChild(std::move(child));
	return window;
}

struct BoundColumnPayload {
	vector<LogicalType> types;
	vector<ColumnBinding> bindings;
};

struct PairDependentOutputLayout {
	PairDependentOutputLayout() = default;

	explicit PairDependentOutputLayout(vector<ColumnBinding> bindings_p) : bindings(std::move(bindings_p)) {
		for (idx_t i = 0; i < bindings.size(); i++) {
			positions[bindings[i]] = i;
		}
	}

	static PairDependentOutputLayout FromOperator(LogicalOperator &op) {
		return PairDependentOutputLayout(op.GetColumnBindings());
	}

	ProjectionIndex GetProjectionIndex(const ColumnBinding &binding) const {
		auto entry = positions.find(binding);
		if (entry == positions.end()) {
			throw InternalException("Could not find binding %s while reconstructing a pair-dependent join side",
			                        binding.ToString());
		}
		return ProjectionIndex(entry->second);
	}

	vector<ProjectionIndex> CreateProjectionMap(const vector<ColumnBinding> &selected_bindings) const {
		vector<ProjectionIndex> projection_map;
		projection_map.reserve(selected_bindings.size());
		for (auto &binding : selected_bindings) {
			projection_map.push_back(GetProjectionIndex(binding));
		}
		return projection_map;
	}

	vector<ColumnBinding> bindings;
	column_binding_map_t<idx_t> positions;
};

struct SideBindingLayout {
	TableIndex table_index;
	vector<idx_t> payload_indices;
	vector<ProjectionIndex> column_indices;
	idx_t column_count = 0;
	ProjectionIndex row_id_column_index;
	TableIndex cte_index;
	vector<LogicalType> cte_types;
	vector<Identifier> names;
	unique_ptr<LogicalOperator> cte_source;
};

struct PairDependentJoinSide {
	TableIndex cte_index;
	vector<ColumnBinding> bindings;
	vector<LogicalType> types;
	vector<LogicalType> cte_types;
	vector<Identifier> names;
	vector<SideBindingLayout> binding_layouts;
	bool has_row_id = false;
	idx_t row_id_offset = DConstants::INVALID_INDEX;
};

struct PairDependentJoinMatch {
	TableIndex cte_index;
	vector<LogicalType> types;
	vector<Identifier> names;
};

struct PairDependentSideRef {
	unique_ptr<LogicalOperator> plan;
	BoundColumnPayload payload;
	PairDependentOutputLayout output;
};

struct PairDependentSideLayoutRef {
	unique_ptr<LogicalOperator> plan;
	BoundColumnPayload payload;
	ColumnBinding row_id_binding;
	PairDependentOutputLayout output;
};

struct PairDependentMatchSetPlan {
	PairDependentJoinSide left_side;
	PairDependentJoinSide right_side;
	PairDependentJoinMatch match;
	unique_ptr<LogicalOperator> match_source;
};

static BoundColumnPayload CreatePayload(vector<LogicalType> types, vector<ColumnBinding> bindings) {
	D_ASSERT(types.size() == bindings.size());
	return BoundColumnPayload {std::move(types), std::move(bindings)};
}

static BoundColumnPayload CombinePayloads(const BoundColumnPayload &left, const BoundColumnPayload &right) {
	BoundColumnPayload result;
	result.types.reserve(left.types.size() + right.types.size());
	result.bindings.reserve(left.bindings.size() + right.bindings.size());
	result.types.insert(result.types.end(), left.types.begin(), left.types.end());
	result.types.insert(result.types.end(), right.types.begin(), right.types.end());
	result.bindings.insert(result.bindings.end(), left.bindings.begin(), left.bindings.end());
	result.bindings.insert(result.bindings.end(), right.bindings.begin(), right.bindings.end());
	return result;
}

static vector<ColumnBinding> CombineBindings(const vector<ColumnBinding> &left, const vector<ColumnBinding> &right) {
	vector<ColumnBinding> result;
	result.reserve(left.size() + right.size());
	result.insert(result.end(), left.begin(), left.end());
	result.insert(result.end(), right.begin(), right.end());
	return result;
}

static vector<ColumnBinding> SliceBindings(const vector<ColumnBinding> &bindings, idx_t offset, idx_t count) {
	D_ASSERT(offset + count <= bindings.size());
	vector<ColumnBinding> result;
	result.reserve(count);
	for (idx_t i = 0; i < count; i++) {
		result.push_back(bindings[offset + i]);
	}
	return result;
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

static SideBindingLayout &GetOrCreateBindingLayout(vector<SideBindingLayout> &layouts, TableIndex table_index) {
	for (auto &layout : layouts) {
		if (layout.table_index == table_index) {
			return layout;
		}
	}
	SideBindingLayout layout;
	layout.table_index = table_index;
	layouts.push_back(std::move(layout));
	return layouts.back();
}

static vector<SideBindingLayout> BuildSideBindingLayouts(const vector<ColumnBinding> &bindings) {
	vector<SideBindingLayout> layouts;
	for (idx_t payload_index = 0; payload_index < bindings.size(); payload_index++) {
		auto &binding = bindings[payload_index];
		auto &layout = GetOrCreateBindingLayout(layouts, binding.table_index);
		layout.payload_indices.push_back(payload_index);
		layout.column_indices.push_back(binding.column_index);
		layout.column_count = std::max(layout.column_count, binding.column_index.GetIndex() + 1);
	}
	for (auto &layout : layouts) {
		layout.row_id_column_index = ProjectionIndex(layout.column_count);
	}
	return layouts;
}

static void SetJoinProjectionMaps(LogicalOperator &join, const PairDependentOutputLayout &left_output,
                                  const vector<ColumnBinding> &selected_left_bindings,
                                  const PairDependentOutputLayout &right_output,
                                  const vector<ColumnBinding> &selected_right_bindings) {
	auto &logical_join = join.Cast<LogicalJoin>();
	logical_join.left_projection_map = left_output.CreateProjectionMap(selected_left_bindings);
	if (!selected_right_bindings.empty()) {
		logical_join.right_projection_map = right_output.CreateProjectionMap(selected_right_bindings);
	}
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

static bool PreparePairDependentJoinSide(Binder &binder, unique_ptr<LogicalOperator> &side, PairDependentJoinSide &info,
                                         const string &name_prefix) {
	FinalizeLogicalGetPayloads(*side);
	side->ResolveOperatorTypes();

	info.bindings = side->GetColumnBindings();
	info.types = side->types;
	D_ASSERT(info.bindings.size() == info.types.size());
	if (info.bindings.empty()) {
		return false;
	}
	info.binding_layouts = BuildSideBindingLayouts(info.bindings);
	info.has_row_id = info.binding_layouts.size() > 1;
	if (info.has_row_id) {
		// Multiple binding layouts are stitched back together after reading the side CTE.
		// The row number is part of that CTE source, so all layout refs share the same identity.
		info.row_id_offset = info.types.size();
		side = CreateRowNumberWindow(binder, std::move(side), binder.GenerateTableIndex());
		side->ResolveOperatorTypes();
		D_ASSERT(side->types.size() == info.types.size() + 1);
	}
	info.cte_types = side->types;
	info.names = GenerateInternalColumnNames(info.cte_types.size(), name_prefix);
	return true;
}

static unique_ptr<LogicalOperator> CreateDistinctMatchProjection(Binder &binder, unique_ptr<LogicalOperator> match,
                                                                 const BoundColumnPayload &payload,
                                                                 vector<LogicalType> &match_types) {
	match->ResolveOperatorTypes();
	auto match_bindings = match->GetColumnBindings();

	auto group_index = binder.GenerateTableIndex();
	auto aggregate_index = binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> aggregates;
	auto distinct = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
	for (idx_t i = 0; i < payload.bindings.size(); i++) {
		D_ASSERT(std::find(match_bindings.begin(), match_bindings.end(), payload.bindings[i]) != match_bindings.end());
		distinct->groups.push_back(make_uniq<BoundColumnRefExpression>(payload.types[i], payload.bindings[i]));
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

static bool CanUseDirectSideCTERef(const PairDependentJoinSide &side) {
	if (side.has_row_id || side.binding_layouts.size() != 1) {
		return false;
	}
	for (idx_t i = 0; i < side.bindings.size(); i++) {
		if (side.bindings[i] != ColumnBinding(side.binding_layouts[0].table_index, ProjectionIndex(i))) {
			return false;
		}
	}
	return true;
}

static unique_ptr<LogicalOperator> CreateSideBindingLayoutSource(Binder &binder, const PairDependentJoinSide &side,
                                                                 const SideBindingLayout &layout, bool include_row_id) {
	auto cte_ref = CreateCTERef(binder.GenerateTableIndex(), side.cte_index, side.cte_types, side.names);
	auto source_bindings = cte_ref->GetColumnBindings();
	auto expression_count = layout.column_count + (include_row_id ? 1 : 0);

	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(expression_count);
	for (idx_t i = 0; i < expression_count; i++) {
		expressions.push_back(make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL)));
	}
	for (idx_t layout_idx = 0; layout_idx < layout.payload_indices.size(); layout_idx++) {
		auto payload_index = layout.payload_indices[layout_idx];
		auto column_index = layout.column_indices[layout_idx];
		D_ASSERT(column_index.GetIndex() < expressions.size());
		expressions[column_index.GetIndex()] =
		    make_uniq<BoundColumnRefExpression>(side.types[payload_index], source_bindings[payload_index]);
	}
	if (include_row_id) {
		D_ASSERT(side.has_row_id);
		D_ASSERT(side.row_id_offset < side.cte_types.size());
		D_ASSERT(layout.row_id_column_index.GetIndex() < expressions.size());
		expressions[layout.row_id_column_index.GetIndex()] = make_uniq<BoundColumnRefExpression>(
		    side.cte_types[side.row_id_offset], source_bindings[side.row_id_offset]);
	}

	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(expressions));
	projection->children.push_back(std::move(cte_ref));
	projection->ResolveOperatorTypes();
	return std::move(projection);
}

static void CreateSideBindingLayoutCTEs(Binder &binder, PairDependentJoinSide &side, const string &name_prefix) {
	if (CanUseDirectSideCTERef(side)) {
		return;
	}
	for (idx_t layout_idx = 0; layout_idx < side.binding_layouts.size(); layout_idx++) {
		auto &layout = side.binding_layouts[layout_idx];
		layout.cte_index = binder.GenerateTableIndex();
		layout.cte_source = CreateSideBindingLayoutSource(binder, side, layout, side.has_row_id);
		layout.cte_types = layout.cte_source->types;
		layout.names = GenerateInternalColumnNames(layout.cte_types.size(), name_prefix);
	}
}

static PairDependentSideLayoutRef CreateSideBindingLayoutRef(const PairDependentJoinSide &side,
                                                             const SideBindingLayout &layout, bool include_row_id) {
	D_ASSERT(layout.cte_source);
	auto cte_ref = CreateCTERef(layout.table_index, layout.cte_index, layout.cte_types, layout.names);
	auto source_bindings = cte_ref->GetColumnBindings();

	PairDependentSideLayoutRef result;
	result.payload.types.reserve(layout.payload_indices.size());
	result.payload.bindings.reserve(layout.payload_indices.size());
	for (idx_t i = 0; i < layout.payload_indices.size(); i++) {
		result.payload.types.push_back(side.types[layout.payload_indices[i]]);
		auto column_index = layout.column_indices[i].GetIndex();
		D_ASSERT(column_index < source_bindings.size());
		result.payload.bindings.push_back(source_bindings[column_index]);
	}
	if (include_row_id) {
		auto row_id_index = layout.row_id_column_index.GetIndex();
		D_ASSERT(row_id_index < source_bindings.size());
		result.row_id_binding = source_bindings[row_id_index];
	}
	result.plan = std::move(cte_ref);
	result.output = PairDependentOutputLayout::FromOperator(*result.plan);
	return result;
}

static PairDependentSideRef CreatePairDependentSideRef(const PairDependentJoinSide &side) {
	if (CanUseDirectSideCTERef(side)) {
		auto plan = CreateCTERef(side.binding_layouts[0].table_index, side.cte_index, side.cte_types, side.names);
		auto output = PairDependentOutputLayout::FromOperator(*plan);
		return PairDependentSideRef {std::move(plan), CreatePayload(side.types, side.bindings), std::move(output)};
	}

	if (!side.has_row_id) {
		D_ASSERT(side.binding_layouts.size() == 1);
		auto layout_ref = CreateSideBindingLayoutRef(side, side.binding_layouts[0], false);
		return PairDependentSideRef {std::move(layout_ref.plan), std::move(layout_ref.payload),
		                             std::move(layout_ref.output)};
	}

	auto current_ref = CreateSideBindingLayoutRef(side, side.binding_layouts[0], true);
	auto current = std::move(current_ref.plan);
	auto current_payload = std::move(current_ref.payload);
	auto current_row_id_binding = current_ref.row_id_binding;
	auto current_output = std::move(current_ref.output);
	auto row_id_type = side.cte_types[side.row_id_offset];

	for (idx_t layout_idx = 1; layout_idx < side.binding_layouts.size(); layout_idx++) {
		auto next_ref = CreateSideBindingLayoutRef(side, side.binding_layouts[layout_idx], true);
		current->ResolveOperatorTypes();
		next_ref.plan->ResolveOperatorTypes();

		vector<JoinCondition> conditions;
		auto left_row_id = make_uniq<BoundColumnRefExpression>(row_id_type, current_row_id_binding);
		auto right_row_id = make_uniq<BoundColumnRefExpression>(row_id_type, next_ref.row_id_binding);
		conditions.emplace_back(std::move(left_row_id), std::move(right_row_id),
		                        ExpressionType::COMPARE_NOT_DISTINCT_FROM);

		// These joins only stitch together different projections of the same side CTE.
		// A match must exist for every row id, and keeping this as a LEFT join prevents
		// filter pullup from lifting the stitching predicate above the public side shape.
		auto joined = LogicalComparisonJoin::CreateJoin(JoinType::LEFT, JoinRefType::REGULAR, std::move(current),
		                                                std::move(next_ref.plan), std::move(conditions));
		auto left_outputs = current_payload.bindings;
		left_outputs.push_back(current_row_id_binding);
		SetJoinProjectionMaps(*joined, current_output, left_outputs, next_ref.output, next_ref.payload.bindings);
		joined->ResolveOperatorTypes();
		current_output = PairDependentOutputLayout::FromOperator(*joined);
		current = std::move(joined);
		current_payload.types.insert(current_payload.types.end(), next_ref.payload.types.begin(),
		                             next_ref.payload.types.end());
		current_payload.bindings.insert(current_payload.bindings.end(), next_ref.payload.bindings.begin(),
		                                next_ref.payload.bindings.end());
	}

	return PairDependentSideRef {std::move(current), CreatePayload(side.types, side.bindings),
	                             std::move(current_output)};
}

static unique_ptr<LogicalOperator> CreatePairDependentFullJoin(Binder &binder, const PairDependentJoinSide &left,
                                                               const PairDependentJoinSide &right,
                                                               const PairDependentJoinMatch &match) {
	auto final_left = CreatePairDependentSideRef(left);
	auto final_right = CreatePairDependentSideRef(right);
	auto left_match_index = binder.GenerateTableIndex();
	auto left_match = CreateCTERef(left_match_index, match.cte_index, match.types, match.names);

	auto left_match_payload = CreatePayload(match.types, left_match->GetColumnBindings());
	auto left_match_output = PairDependentOutputLayout::FromOperator(*left_match);

	vector<JoinCondition> left_match_conditions;
	AddNotDistinctConditions(left_match_conditions, final_left.payload,
	                         SlicePayload(left_match_payload, 0, left.types.size()));
	auto left_with_matches =
	    LogicalComparisonJoin::CreateJoin(JoinType::LEFT, JoinRefType::REGULAR, std::move(final_left.plan),
	                                      std::move(left_match), std::move(left_match_conditions));
	SetJoinProjectionMaps(*left_with_matches, final_left.output, left.bindings, left_match_output,
	                      left_match_payload.bindings);
	auto left_with_matches_output =
	    PairDependentOutputLayout(CombineBindings(left.bindings, left_match_payload.bindings));

	auto marker_binding = left_match_payload.bindings[match.types.size() - 1];

	vector<JoinCondition> final_conditions;
	AddNotDistinctConditions(final_conditions, SlicePayload(left_match_payload, left.types.size(), right.types.size()),
	                         final_right.payload);
	auto marker_ref = make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN, marker_binding);
	auto marker_true = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	final_conditions.emplace_back(BoundComparisonExpression::Create(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
	                                                                std::move(marker_ref), std::move(marker_true)));

	auto final_join =
	    LogicalComparisonJoin::CreateJoin(JoinType::OUTER, JoinRefType::REGULAR, std::move(left_with_matches),
	                                      std::move(final_right.plan), std::move(final_conditions));
	auto expected_bindings = left.bindings;
	expected_bindings.insert(expected_bindings.end(), right.bindings.begin(), right.bindings.end());
	SetJoinProjectionMaps(*final_join, left_with_matches_output, left.bindings, final_right.output, right.bindings);
	final_join->ResolveOperatorTypes();
	auto actual_bindings = final_join->GetColumnBindings();
	D_ASSERT(actual_bindings == expected_bindings);
	return final_join;
}

static unique_ptr<LogicalOperator> WrapSideBindingLayoutCTEs(PairDependentJoinSide &side,
                                                             unique_ptr<LogicalOperator> result) {
	for (idx_t layout_idx = side.binding_layouts.size(); layout_idx > 0; layout_idx--) {
		auto &layout = side.binding_layouts[layout_idx - 1];
		if (!layout.cte_source) {
			continue;
		}
		auto cte_name = Identifier("__duckdb_pair_layout_" + to_string(layout.cte_index.index));
		result = make_uniq<LogicalMaterializedCTE>(cte_name, layout.cte_index, layout.cte_types.size(),
		                                           std::move(layout.cte_source), std::move(result),
		                                           CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	}
	return result;
}

static unique_ptr<LogicalOperator> WrapPairDependentJoinCTEs(unique_ptr<LogicalOperator> left,
                                                             unique_ptr<LogicalOperator> right,
                                                             PairDependentMatchSetPlan &match_set,
                                                             unique_ptr<LogicalOperator> final_join) {
	auto match_cte_index = match_set.match.cte_index;
	auto match_cte_name = Identifier("__duckdb_pair_match_" + to_string(match_cte_index.index));
	unique_ptr<LogicalOperator> result = make_uniq<LogicalMaterializedCTE>(
	    match_cte_name, match_cte_index, match_set.match.types.size(), std::move(match_set.match_source),
	    std::move(final_join), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	result = WrapSideBindingLayoutCTEs(match_set.right_side, std::move(result));
	result = WrapSideBindingLayoutCTEs(match_set.left_side, std::move(result));
	auto right_cte_index = match_set.right_side.cte_index;
	auto right_cte_name = Identifier("__duckdb_pair_right_" + to_string(right_cte_index.index));
	result =
	    make_uniq<LogicalMaterializedCTE>(right_cte_name, right_cte_index, match_set.right_side.cte_types.size(),
	                                      std::move(right), std::move(result), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	auto left_cte_index = match_set.left_side.cte_index;
	auto left_cte_name = Identifier("__duckdb_pair_left_" + to_string(left_cte_index.index));
	result =
	    make_uniq<LogicalMaterializedCTE>(left_cte_name, left_cte_index, match_set.left_side.cte_types.size(),
	                                      std::move(left), std::move(result), CTEMaterialize::CTE_MATERIALIZE_DEFAULT);
	return result;
}

static bool CreatePairDependentMatchSet(Binder &binder, BoundJoinRef &ref, unique_ptr<LogicalOperator> &left,
                                        unique_ptr<LogicalOperator> &right, PairDependentMatchSetPlan &match_set,
                                        const string &left_prefix, const string &right_prefix,
                                        const string &match_prefix, bool include_right_payload) {
	left->ResolveOperatorTypes();
	right->ResolveOperatorTypes();

	unordered_set<TableIndex> left_binding_set;
	unordered_set<TableIndex> right_binding_set;
	LogicalJoin::GetTableReferences(*left, left_binding_set);
	LogicalJoin::GetTableReferences(*right, right_binding_set);
	PairDependentReferenceAnalyzer references(left_binding_set, right_binding_set);
	if (references.HasVolatile(*ref.condition)) {
		return false;
	}
	if (!PreparePairDependentJoinSide(binder, left, match_set.left_side, left_prefix) ||
	    !PreparePairDependentJoinSide(binder, right, match_set.right_side, right_prefix)) {
		return false;
	}

	match_set.left_side.cte_index = binder.GenerateTableIndex();
	match_set.right_side.cte_index = binder.GenerateTableIndex();
	match_set.match.cte_index = binder.GenerateTableIndex();
	CreateSideBindingLayoutCTEs(binder, match_set.left_side, left_prefix);
	CreateSideBindingLayoutCTEs(binder, match_set.right_side, right_prefix);

	LogicalOperatorDeepCopy left_remapper(binder, nullptr);
	left_remapper.RemapTableIndexesInPlace(*left);
	LogicalOperatorDeepCopy right_remapper(binder, nullptr);
	right_remapper.RemapTableIndexesInPlace(*right);

	auto match_left = CreateCTERef(binder.GenerateTableIndex(), match_set.left_side.cte_index,
	                               match_set.left_side.cte_types, match_set.left_side.names);
	auto match_right = CreateCTERef(binder.GenerateTableIndex(), match_set.right_side.cte_index,
	                                match_set.right_side.cte_types, match_set.right_side.names);
	auto match_left_bindings = match_left->GetColumnBindings();
	auto match_right_bindings = match_right->GetColumnBindings();
	auto match_left_payload = CreatePayload(match_set.left_side.types,
	                                        SliceBindings(match_left_bindings, 0, match_set.left_side.types.size()));
	auto match_right_payload = CreatePayload(match_set.right_side.types,
	                                         SliceBindings(match_right_bindings, 0, match_set.right_side.types.size()));
	auto match_payload = CombinePayloads(match_left_payload, match_right_payload);
	auto &projected_payload = include_right_payload ? match_payload : match_left_payload;

	vector<ReplacementBinding> condition_replacements;
	auto left_replacements = CreateBindingReplacements(match_set.left_side.bindings, match_left_payload.bindings);
	auto right_replacements = CreateBindingReplacements(match_set.right_side.bindings, match_right_payload.bindings);
	condition_replacements.insert(condition_replacements.end(), left_replacements.begin(), left_replacements.end());
	condition_replacements.insert(condition_replacements.end(), right_replacements.begin(), right_replacements.end());
	ReplaceConditionBindings replace_condition(condition_replacements);
	replace_condition.VisitExpression(&ref.condition);

	auto match_join = LogicalCrossProduct::Create(std::move(match_left), std::move(match_right));
	auto match_filter = make_uniq<LogicalFilter>(std::move(ref.condition));
	match_filter->AddChild(std::move(match_join));

	match_set.match_source =
	    CreateDistinctMatchProjection(binder, std::move(match_filter), projected_payload, match_set.match.types);
	match_set.match.names = GenerateInternalColumnNames(match_set.match.types.size(), match_prefix);
	return true;
}

static void WrapPairDependentMatchSet(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
                                      PairDependentMatchSetPlan &match_set, unique_ptr<LogicalOperator> final_join,
                                      unique_ptr<LogicalOperator> &result) {
	result = WrapPairDependentJoinCTEs(std::move(left), std::move(right), match_set, std::move(final_join));
}

class PairDependentJoinPlanner {
public:
	PairDependentJoinPlanner(Binder &binder, BoundJoinRef &ref, unique_ptr<LogicalOperator> &left,
	                         unique_ptr<LogicalOperator> &right)
	    : binder(binder), ref(ref), left(left), right(right) {
		LogicalJoin::GetTableReferences(*left, left_bindings);
		LogicalJoin::GetTableReferences(*right, right_bindings);
	}

	bool TryPlan(unique_ptr<LogicalOperator> &result) {
		if (!CanPlan()) {
			return false;
		}
		switch (ref.type) {
		case JoinType::LEFT:
			return TryPlanLeft(result);
		case JoinType::RIGHT:
			return TryPlanRight(result);
		case JoinType::SEMI:
		case JoinType::ANTI:
			return TryPlanExistence(result);
		case JoinType::OUTER:
			return TryPlanFull(result);
		default:
			return false;
		}
	}

private:
	bool CanPlan() {
		if (!ref.condition || ref.lateral || ref.ref_type != JoinRefType::REGULAR) {
			return false;
		}
		PairDependentReferenceAnalyzer references(left_bindings, right_bindings);
		return references.HasPairDependentSubquery(*ref.condition);
	}

	bool TryPlanLeft(unique_ptr<LogicalOperator> &result) {
		result = binder.PlanPairDependentLateralJoin(std::move(left), std::move(right), std::move(ref.condition),
		                                             left_bindings, right_bindings, JoinType::LEFT);
		return result != nullptr;
	}

	bool TryPlanRight(unique_ptr<LogicalOperator> &result) {
		result = binder.PlanPairDependentLateralJoin(std::move(right), std::move(left), std::move(ref.condition),
		                                             right_bindings, left_bindings, JoinType::LEFT);
		return result != nullptr;
	}

	bool TryPlanExistence(unique_ptr<LogicalOperator> &result) {
		result = binder.PlanPairDependentLateralJoin(std::move(left), std::move(right), std::move(ref.condition),
		                                             left_bindings, right_bindings, ref.type);
		return result != nullptr;
	}

	bool TryPlanFull(unique_ptr<LogicalOperator> &result) {
		PairDependentMatchSetPlan match_set;
		if (!CreatePairDependentMatchSet(binder, ref, left, right, match_set, "__duckdb_full_l_", "__duckdb_full_r_",
		                                 "__duckdb_full_match_", true)) {
			return false;
		}

		auto final_join =
		    CreatePairDependentFullJoin(binder, match_set.left_side, match_set.right_side, match_set.match);
		WrapPairDependentMatchSet(std::move(left), std::move(right), match_set, std::move(final_join), result);
		return true;
	}

	Binder &binder;
	BoundJoinRef &ref;
	unique_ptr<LogicalOperator> &left;
	unique_ptr<LogicalOperator> &right;
	unordered_set<TableIndex> left_bindings;
	unordered_set<TableIndex> right_bindings;
};

bool Binder::TryPlanPairDependentJoin(BoundJoinRef &ref, unique_ptr<LogicalOperator> &left,
                                      unique_ptr<LogicalOperator> &right, unique_ptr<LogicalOperator> &result) {
	PairDependentJoinPlanner planner(*this, ref, left, right);
	return planner.TryPlan(result);
}

} // namespace duckdb
