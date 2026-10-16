#include "duckdb/optimizer/constraint_propagation/helpers.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

bool CollectEquiKeyPairs(const FactStore &store, const LogicalComparisonJoin &join,
                         vector<std::pair<idx_t, idx_t>> &pairs, idx_t *matched_conditions) {
	if (matched_conditions) {
		*matched_conditions = 0;
	}
	pairs.clear();
	const auto *b0 = store.FindBindings(join.children[0].get());
	const auto *b1 = store.FindBindings(join.children[1].get());
	if (!b0 || !b1) {
		return false;
	}
	for (auto &cond : join.conditions) {
		if (!cond.IsComparison()) {
			continue;
		}
		bool is_equality = cond.GetComparisonType() == ExpressionType::COMPARE_EQUAL;
		bool is_null_safe = cond.GetComparisonType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM;
		if (!is_equality && !is_null_safe) {
			continue;
		}
		auto &left_expr = cond.LeftReference();
		auto &right_expr = cond.RightReference();
		if (left_expr->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF ||
		    right_expr->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto lb = left_expr->Cast<BoundColumnRefExpression>().Binding();
		auto rb = right_expr->Cast<BoundColumnRefExpression>().Binding();
		auto lp = PositionIn(*b0, lb);
		auto rp = PositionIn(*b1, rb);
		if (!lp.IsValid() || !rp.IsValid()) {
			continue;
		}
		if (is_null_safe) {
			const auto &f0 = store.Get(join.children[0].get());
			const auto &f1 = store.Get(join.children[1].get());
			if (!f0.NotNull().Test(lp.GetIndex()) && !f1.NotNull().Test(rp.GetIndex())) {
				continue;
			}
		}
		pairs.emplace_back(lp.GetIndex(), rp.GetIndex());
		if (matched_conditions) {
			(*matched_conditions)++;
		}
	}
	return true;
}

bool CollectEquiKeys(const FactStore &store, const LogicalComparisonJoin &join, ColumnMask &key0, ColumnMask &key1,
                     idx_t *matched_conditions) {
	vector<std::pair<idx_t, idx_t>> pairs;
	if (!CollectEquiKeyPairs(store, join, pairs, matched_conditions)) {
		key0 = key1 = ColumnMask::Empty();
		return false;
	}
	const auto *b0 = store.FindBindings(join.children[0].get());
	const auto *b1 = store.FindBindings(join.children[1].get());
	key0 = ColumnMask(b0->size());
	key1 = ColumnMask(b1->size());
	for (auto &kv : pairs) {
		key0.Set(kv.first);
		key1.Set(kv.second);
	}
	return true;
}

void FlattenConjuncts(const Expression &expr, vector<const Expression *> &out) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION &&
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conj.GetChildren()) {
			FlattenConjuncts(*child, out);
		}
		return;
	}
	out.push_back(&expr);
}

static bool SupportsSetSemantics(const LogicalType &t) {
	switch (t.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::VARCHAR:
		return true;
	default:
		return false;
	}
}

static bool SupportsRangeSemantics(const LogicalType &t) {
	switch (t.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return true;
	default:
		return false;
	}
}

static const BoundColumnRefExpression *TryGetColref(const Expression &e) {
	if (e.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return nullptr;
	}
	return &e.Cast<BoundColumnRefExpression>();
}

static const BoundConstantExpression *TryGetConstant(const Expression &e) {
	if (e.GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		return nullptr;
	}
	return &e.Cast<BoundConstantExpression>();
}

//! Fill the constraint header
static bool PrepareConstraint(ExtractedConstraint &out, const BoundColumnRefExpression &col,
                              const BoundConstantExpression &constant) {
	if (constant.GetValue().IsNull()) {
		return false;
	}
	if (constant.GetValue().type() != col.GetReturnType()) {
		return false;
	}
	out.column = &col;
	out.allowed.type = col.GetReturnType();
	out.allowed.null_possible = false;
	return true;
}

//! `col IS NOT NULL`
static bool TryExtractIsNotNull(const Expression &conjunct, ExtractedConstraint &out) {
	if (conjunct.GetExpressionClass() != ExpressionClass::BOUND_OPERATOR) {
		return false;
	}

	auto &op = conjunct.Cast<BoundOperatorExpression>();
	auto *col = TryGetColref(*op.GetChildren()[0]);
	if (!col) {
		return false;
	}
	out.column = col;
	out.allowed.type = col->GetReturnType();
	out.allowed.null_possible = false;
	return true;
}

//! `col OP const` or `const OP col` for OP in {=, <, <=, >, >=}.
static bool TryExtractComparison(const Expression &conjunct, ExtractedConstraint &out) {
	if (conjunct.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &cmp = conjunct.Cast<BoundFunctionExpression>();
	if (!BoundComparisonExpression::IsComparison(cmp)) {
		return false;
	}

	const Expression &left_expr = BoundComparisonExpression::Left(cmp);
	const Expression &right_expr = BoundComparisonExpression::Right(cmp);

	// Exactly one colref and one constant; either orientation.
	const BoundColumnRefExpression *col = nullptr;
	const BoundConstantExpression *constant = nullptr;
	bool col_on_left = false;
	if (auto *lc = TryGetColref(left_expr)) {
		if (auto *rc = TryGetConstant(right_expr)) {
			col = lc;
			constant = rc;
			col_on_left = true;
		}
	}
	if (!col) {
		if (auto *lc = TryGetConstant(left_expr)) {
			if (auto *rc = TryGetColref(right_expr)) {
				col = rc;
				constant = lc;
			}
		}
	}
	if (!col) {
		return false;
	}

	if (!PrepareConstraint(out, *col, *constant)) {
		return false;
	}

	const ExpressionType op = conjunct.GetExpressionType();
	if (op == ExpressionType::COMPARE_EQUAL) {
		if (!SupportsSetSemantics(col->GetReturnType())) {
			return false;
		}
		out.allowed.is_set = true;
		out.allowed.values.push_back(constant->GetValue());
		return true;
	}

	if (!SupportsRangeSemantics(col->GetReturnType())) {
		return false;
	}

	// `col > c` and `c < col` both bound col from below
	bool is_greater = op == ExpressionType::COMPARE_GREATERTHAN || op == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	if (!col_on_left) {
		is_greater = !is_greater;
	}
	bool is_or_equal =
	    op == ExpressionType::COMPARE_GREATERTHANOREQUALTO || op == ExpressionType::COMPARE_LESSTHANOREQUALTO;

	if (is_greater) {
		out.allowed.has_lo = true;
		out.allowed.lo = constant->GetValue();
		out.allowed.lo_inclusive = is_or_equal;
	} else {
		out.allowed.has_hi = true;
		out.allowed.hi = constant->GetValue();
		out.allowed.hi_inclusive = is_or_equal;
	}
	return true;
}

bool TryExtractConstraint(const Expression &conjunct, ExtractedConstraint &out) {
	switch (conjunct.GetExpressionType()) {
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		return TryExtractIsNotNull(conjunct, out);
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return TryExtractComparison(conjunct, out);
	default:
		return false; // TODO: add IN lists and OR-of-equalities.
	}
}

bool TryExtractTableFilterDomain(const TableFilter &filter, const BoundColumnRefExpression &column_ref,
                                 ValueDomain &out) {
	auto expr = filter.ToExpression(column_ref);
	if (!expr) {
		return false;
	}
	ExtractedConstraint constraint;
	if (!TryExtractConstraint(*expr, constraint)) {
		return false;
	}

	D_ASSERT(constraint.column->Binding() == column_ref.Binding());
	out = std::move(constraint.allowed);
	return true;
}

} // namespace duckdb
