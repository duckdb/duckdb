#include "duckdb/planner/expression/list.hpp"
#include "duckdb/optimizer/rule/comparison_simplification.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/optimizer/expression_rewriter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

static bool DateTimestampComparisonIsInvertible(BoundFunctionExpression &expr, BoundFunctionExpression &cast_expression,
                                                const Value &constant_value, Value &cast_constant, bool column_ref_left,
                                                unique_ptr<Expression> &replacement) {
	if (Timestamp::GetTime(constant_value.GetValue<timestamp_t>()) == dtime_t(0)) {
		return true; // it's midnight: no replacement needed
	}
	auto op = expr.GetExpressionType();

	// Non-midnight TIMESTAMPs cannot equal DATE values.
	switch (op) {
	case ExpressionType::COMPARE_EQUAL:
		// d =  T   -> false, preserving NULL
		replacement = ExpressionRewriter::ConstantOrNull(std::move(BoundCastExpression::ChildMutable(cast_expression)),
		                                                 Value::BOOLEAN(false));
		return true;
	case ExpressionType::COMPARE_NOTEQUAL:
		// d != T   -> true, preserving NULL
		replacement = ExpressionRewriter::ConstantOrNull(std::move(BoundCastExpression::ChildMutable(cast_expression)),
		                                                 Value::BOOLEAN(true));
		return true;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		// d IS DISTINCT FROM T     -> true
		replacement = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
		return true;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		// d IS NOT DISTINCT FROM T -> false
		replacement = make_uniq<BoundConstantExpression>(Value::BOOLEAN(false));
		return true;
	default:
		break;
	}

	// The examples describe the column-left form; column-right comparisons invert the day bump.
	bool add_one_day;
	switch (op) {
	case ExpressionType::COMPARE_LESSTHAN:
		// d <  T   -> d <  DATE '2024-06-16'  -- needs +1
		add_one_day = column_ref_left;
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// d >= T   -> d >= DATE '2024-06-16'  -- needs +1
		add_one_day = column_ref_left;
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// d <= T   -> d <= DATE '2024-06-15'  -- no +1
		add_one_day = !column_ref_left;
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		// d >  T   -> d >  DATE '2024-06-15'  -- no +1
		add_one_day = !column_ref_left;
		break;
	default:
		return false;
	}
	if (add_one_day) {
		cast_constant = Value::DATE(date_t(cast_constant.GetValue<date_t>().days + 1));
	}
	return true;
}

static bool ConstantCastIsInvertible(BoundFunctionExpression &expr, BoundFunctionExpression &cast_expression,
                                     const Value &constant_value, Value &cast_constant, const LogicalType &target_type,
                                     bool column_ref_left, unique_ptr<Expression> &replacement) {
	if (cast_constant.IsNull() || BoundCastExpression::CastIsInvertible(cast_expression.GetReturnType(), target_type)) {
		return true;
	}
	if (target_type.id() != LogicalTypeId::DATE || cast_expression.GetReturnType().id() != LogicalTypeId::TIMESTAMP) {
		return false;
	}
	return DateTimestampComparisonIsInvertible(expr, cast_expression, constant_value, cast_constant, column_ref_left,
	                                           replacement);
}

static unique_ptr<Expression> CreateNullCheckExpression(ExpressionType expression_type, unique_ptr<Expression> child) {
	D_ASSERT(expression_type == ExpressionType::OPERATOR_IS_NULL ||
	         expression_type == ExpressionType::OPERATOR_IS_NOT_NULL);
	auto result = make_uniq<BoundOperatorExpression>(expression_type, LogicalType::BOOLEAN);
	result->GetChildrenMutable().push_back(std::move(child));
	return std::move(result);
}

ComparisonSimplificationRule::ComparisonSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a ComparisonExpression that has a ConstantExpression as a check
	auto op = make_uniq<ComparisonExpressionMatcher>();
	op->matchers.push_back(make_uniq<FoldableConstantMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	root = std::move(op);
}

//! If the given expression is a row constructor (possibly wrapped in casts), return it.
//! effective_type is set to the type the comparison operates on (the cast target, if a cast is present)
static optional_ptr<BoundFunctionExpression> UnwrapRowConstructor(Expression &expr, LogicalType &effective_type) {
	auto *current = &expr;
	auto has_cast = false;
	while (current->GetExpressionClass() == ExpressionClass::BOUND_FUNCTION && BoundCastExpression::IsCast(*current)) {
		auto &cast = current->Cast<BoundFunctionExpression>();
		if (BoundCastExpression::IsTryCast(cast)) {
			// a failing try_cast yields NULL instead of an error - we cannot replicate that in the decomposed filters
			return nullptr;
		}
		if (!has_cast) {
			effective_type = cast.GetReturnType();
			has_cast = true;
		}
		current = BoundCastExpression::ChildMutable(cast).get();
	}
	if (current->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	auto &function = current->Cast<BoundFunctionExpression>();
	if (function.Function().GetName() != "row") {
		return nullptr;
	}
	if (!has_cast) {
		effective_type = function.GetReturnType();
	}
	return &function;
}

//! Extract the children of one side of a row constructor comparison.
//! A side is either a row constructor function (possibly wrapped in casts) or a folded TUPLE/STRUCT
//! constant (the binder folds row(a, b) with constant arguments into a single constant Value).
//! Returns false if the side is not a row constructor in either form.
static bool ExtractRowComparisonSide(Expression &expr, LogicalType &type, vector<unique_ptr<Expression>> &children,
                                     bool &is_constant) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		auto &constant = expr.Cast<BoundConstantExpression>();
		type = constant.GetValue().type();
		if (type.id() != LogicalTypeId::TUPLE && type.id() != LogicalTypeId::STRUCT) {
			return false;
		}
		auto &child_values = StructValue::GetChildren(constant.GetValue());
		children.reserve(child_values.size());
		for (auto &child_value : child_values) {
			children.push_back(make_uniq<BoundConstantExpression>(child_value));
		}
		is_constant = true;
		return true;
	}
	auto row = UnwrapRowConstructor(expr, type);
	if (!row) {
		return false;
	}
	// only tuple/struct comparisons compare their children element-wise; anything else (e.g. casts to
	// VARCHAR or VARIANT) compares the entire value as a whole
	if (type.id() != LogicalTypeId::TUPLE && type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	// copy the children: Apply may still reject the rewrite, and must leave the plan untouched when it does
	for (auto &child : row->GetChildrenMutable()) {
		children.push_back(child->Copy());
	}
	is_constant = false;
	return true;
}

RowComparisonSimplificationRule::RowComparisonSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	auto comparison = make_uniq<ComparisonExpressionMatcher>();
	comparison->expr_type = make_uniq<SpecificExpressionTypeMatcher>(ExpressionType::COMPARE_EQUAL);
	// at least one side is a row constructor - the other side is either a row constructor
	// or a folded TUPLE/STRUCT constant (the binder folds row(a, b) with constant arguments into a single constant)
	comparison->matchers.push_back(make_uniq<ExpressionMatcher>(ExpressionClass::BOUND_FUNCTION));
	comparison->matchers.push_back(make_uniq<ExpressionMatcher>());
	comparison->policy = SetMatcher::Policy::SOME;
	root = std::move(comparison);
}

unique_ptr<Expression> RowComparisonSimplificationRule::Apply(LogicalOperator &op,
                                                              vector<reference<Expression>> &bindings,
                                                              bool &changes_made, bool is_root) {
	if (!is_root || op.type != LogicalOperatorType::LOGICAL_FILTER) {
		return nullptr;
	}
	auto &left = bindings[1].get();
	auto &right = bindings[2].get();
	LogicalType left_type;
	vector<unique_ptr<Expression>> left_children;
	bool left_is_constant;
	if (!ExtractRowComparisonSide(left, left_type, left_children, left_is_constant)) {
		return nullptr;
	}
	LogicalType right_type;
	vector<unique_ptr<Expression>> right_children;
	bool right_is_constant;
	if (!ExtractRowComparisonSide(right, right_type, right_children, right_is_constant)) {
		return nullptr;
	}
	// both sides are aligned to a common type at bind time
	if (left_type != right_type || left_children.empty() || left_children.size() != right_children.size()) {
		return nullptr;
	}
	// the children of a row constructor side must be plain columns
	for (idx_t side = 0; side < 2; side++) {
		auto &side_children = side == 0 ? left_children : right_children;
		auto side_is_constant = side == 0 ? left_is_constant : right_is_constant;
		if (side_is_constant) {
			continue;
		}
		for (auto &child : side_children) {
			if (child->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF || child->GetReturnType().IsNested()) {
				return nullptr;
			}
		}
	}
	// Row comparisons are decomposed with IS NOT DISTINCT FROM, matching the row comparison semantics:
	// a NULL field does not make the row comparison NULL - row equality compares per-field distinctness.
	// For a row-vs-constant comparison with a NULL-free constant the comparison can be decomposed with `=`:
	// `a = 42` and `a IS NOT DISTINCT FROM 42` reject the same rows, and `=` is absorbed by the filter
	// combiner, enabling constant filter pushdown. A constant containing NULLs is decomposed with
	// IS NOT DISTINCT FROM instead - (a, b) = (NULL, 2) matches rows with a IS NULL, which `=` cannot
	// express - and will benefit from pushdown once the combiner supports such comparisons.
	ExpressionType comparison_type = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	if (left_is_constant || right_is_constant) {
		comparison_type = ExpressionType::COMPARE_EQUAL;
		auto &constant_children = left_is_constant ? left_children : right_children;
		for (auto &child : constant_children) {
			if (child->Cast<BoundConstantExpression>().GetValue().IsNull()) {
				comparison_type = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
				break;
			}
		}
	}
	auto &child_types = StructType::GetChildTypes(left_type);
	if (child_types.size() != left_children.size()) {
		return nullptr;
	}
	auto result = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (idx_t child_idx = 0; child_idx < left_children.size(); child_idx++) {
		// the unwrapped children carry their original types - compare them in the aligned field type
		auto left_child = BoundCastExpression::AddCastToType(GetContext(), std::move(left_children[child_idx]),
		                                                     child_types[child_idx].second);
		auto right_child = BoundCastExpression::AddCastToType(GetContext(), std::move(right_children[child_idx]),
		                                                      child_types[child_idx].second);
		result->GetChildrenMutable().push_back(
		    BoundComparisonExpression::Create(comparison_type, std::move(left_child), std::move(right_child)));
	}
	return std::move(result);
}

unique_ptr<Expression> ComparisonSimplificationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                           bool &changes_made, bool is_root) {
	auto &expr = bindings[0].get().Cast<BoundFunctionExpression>();
	auto &constant_expr = bindings[1].get();
	auto &left = BoundComparisonExpression::LeftMutable(expr);
	auto &right = BoundComparisonExpression::RightMutable(expr);
	bool column_ref_left = !RefersToSameObject(*left, constant_expr);
	auto &column_ref_expr = column_ref_left ? *left : *right;
	// the constant_expr is a scalar expression that we have to fold
	// use an ExpressionExecutor to execute the expression
	D_ASSERT(constant_expr.IsFoldable());
	Value constant_value;
	if (!ExpressionExecutor::TryEvaluateScalar(GetContext(), constant_expr, constant_value)) {
		return nullptr;
	}
	if (constant_value.IsNull() && column_ref_expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		if (expr.GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			return CreateNullCheckExpression(ExpressionType::OPERATOR_IS_NULL,
			                                 column_ref_left ? std::move(left) : std::move(right));
		}
		if (expr.GetExpressionType() == ExpressionType::COMPARE_DISTINCT_FROM) {
			return CreateNullCheckExpression(ExpressionType::OPERATOR_IS_NOT_NULL,
			                                 column_ref_left ? std::move(left) : std::move(right));
		}
	}
	if (constant_value.IsNull() && !(expr.GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM ||
	                                 expr.GetExpressionType() == ExpressionType::COMPARE_DISTINCT_FROM)) {
		// comparison with constant NULL, return NULL
		return make_uniq<BoundConstantExpression>(Value(LogicalType::BOOLEAN));
	}
	if (BoundComparisonExpression::IsComparison(column_ref_expr) && !constant_value.IsNull() &&
	    constant_value.type().id() == LogicalTypeId::BOOLEAN && BooleanValue::Get(constant_value)) {
		if (expr.GetExpressionType() == ExpressionType::COMPARE_EQUAL ||
		    (expr.GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM && is_root &&
		     op.type == LogicalOperatorType::LOGICAL_FILTER)) {
			return column_ref_left ? std::move(left) : std::move(right);
		}
	}
	if (BoundCastExpression::IsCast(column_ref_expr)) {
		//! Here we check if we can apply the expression on the constant side
		//! We can do this if the cast itself is invertible and casting the constant is
		//! invertible in practice.
		auto &cast_expression = column_ref_expr.Cast<BoundFunctionExpression>();
		auto target_type = BoundCastExpression::SourceType(cast_expression);
		if (!BoundCastExpression::CastIsInvertible(target_type, cast_expression.GetReturnType())) {
			return nullptr;
		}

		// Can we cast the constant at all?
		string error_message;
		auto new_constant = constant_value.TryCastAs(rewriter.context, target_type, &error_message, true);
		if (!new_constant) {
			return nullptr;
		}
		auto &cast_constant = *new_constant;

		// Is the constant cast invertible?
		unique_ptr<Expression> replacement;
		if (!ConstantCastIsInvertible(expr, cast_expression, constant_value, cast_constant, target_type,
		                              column_ref_left, replacement)) {
			return nullptr;
		}
		if (replacement) {
			return replacement;
		}

		//! We can cast, now we change our column_ref_expression from an operator cast to a column reference
		auto child_expression = std::move(BoundCastExpression::ChildMutable(cast_expression));
		auto new_constant_expr = make_uniq<BoundConstantExpression>(cast_constant);
		if (column_ref_left) {
			left = std::move(child_expression);
			right = std::move(new_constant_expr);
		} else {
			left = std::move(new_constant_expr);
			right = std::move(child_expression);
		}
		changes_made = true;
	}
	return nullptr;
}

} // namespace duckdb
