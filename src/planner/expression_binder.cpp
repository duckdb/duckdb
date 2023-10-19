#include "duckdb/planner/expression_binder.hpp"

#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

ExpressionBinder::ExpressionBinder(Binder &binder, ClientContext &context, bool replace_binder)
    : binder(binder), context(context) {
	InitializeStackCheck();
	if (replace_binder) {
		stored_binder = &binder.GetActiveBinder();
		binder.SetActiveBinder(*this);
	} else {
		binder.PushExpressionBinder(*this);
	}
}

ExpressionBinder::~ExpressionBinder() {
	if (binder.HasActiveBinder()) {
		if (stored_binder) {
			binder.SetActiveBinder(*stored_binder);
		} else {
			binder.PopExpressionBinder();
		}
	}
}

void ExpressionBinder::InitializeStackCheck() {
	if (binder.HasActiveBinder()) {
		stack_depth = binder.GetActiveBinder().stack_depth;
	} else {
		stack_depth = 0;
	}
}

StackChecker<ExpressionBinder> ExpressionBinder::StackCheck(const ParsedExpression &expr, idx_t extra_stack) {
	D_ASSERT(stack_depth != DConstants::INVALID_INDEX);
	if (stack_depth + extra_stack >= MAXIMUM_STACK_DEPTH) {
		throw BinderException("Maximum recursion depth exceeded (Maximum: %llu) while binding \"%s\"",
		                      MAXIMUM_STACK_DEPTH, expr.ToString());
	}
	return StackChecker<ExpressionBinder>(*this, extra_stack);
}

BindResult ExpressionBinder::BindExpression(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression) {
	auto stack_checker = StackCheck(*expr);

	auto &expr_ref = *expr;
	switch (expr_ref.expression_class) {
	case ExpressionClass::BETWEEN:
		return BindExpression(expr_ref.Cast<BetweenExpression>(), depth);
	case ExpressionClass::CASE:
		return BindExpression(expr_ref.Cast<CaseExpression>(), depth);
	case ExpressionClass::CAST:
		return BindExpression(expr_ref.Cast<CastExpression>(), depth);
	case ExpressionClass::COLLATE:
		return BindExpression(expr_ref.Cast<CollateExpression>(), depth);
	case ExpressionClass::COLUMN_REF:
		return BindExpression(expr_ref.Cast<ColumnRefExpression>(), depth);
	case ExpressionClass::COMPARISON:
		return BindExpression(expr_ref.Cast<ComparisonExpression>(), depth);
	case ExpressionClass::CONJUNCTION:
		return BindExpression(expr_ref.Cast<ConjunctionExpression>(), depth);
	case ExpressionClass::CONSTANT:
		return BindExpression(expr_ref.Cast<ConstantExpression>(), depth);
	case ExpressionClass::FUNCTION: {
		auto &function = expr_ref.Cast<FunctionExpression>();
		if (function.function_name == "unnest" || function.function_name == "unlist") {
			// special case, not in catalog
			return BindUnnest(function, depth, root_expression);
		}
		// binding function expression has extra parameter needed for macro's
		return BindExpression(function, depth, expr);
	}
	case ExpressionClass::LAMBDA:
		return BindExpression(expr_ref.Cast<LambdaExpression>(), depth, false, LogicalTypeId::INVALID);
	case ExpressionClass::OPERATOR:
		return BindExpression(expr_ref.Cast<OperatorExpression>(), depth);
	case ExpressionClass::SUBQUERY:
		return BindExpression(expr_ref.Cast<SubqueryExpression>(), depth);
	case ExpressionClass::PARAMETER:
		return BindExpression(expr_ref.Cast<ParameterExpression>(), depth);
	case ExpressionClass::POSITIONAL_REFERENCE: {
		return BindPositionalReference(expr, depth, root_expression);
	}
	case ExpressionClass::STAR:
		return BindResult(binder.FormatError(expr_ref, "STAR expression is not supported here"));
	default:
		throw NotImplementedException("Unimplemented expression class");
	}
}

bool ExpressionBinder::BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr) {
	// try to bind in one of the outer queries, if the binding error occurred in a subquery
	auto &active_binders = binder.GetActiveBinders();
	// make a copy of the set of binders, so we can restore it later
	auto binders = active_binders;

	// we already failed with the current binder
	active_binders.pop_back();
	idx_t depth = 1;
	bool success = false;

	while (!active_binders.empty()) {
		auto &next_binder = active_binders.back().get();
		ExpressionBinder::QualifyColumnNames(next_binder.binder, expr);
		auto bind_result = next_binder.Bind(expr, depth);
		if (bind_result.empty()) {
			success = true;
			break;
		}
		depth++;
		active_binders.pop_back();
	}
	active_binders = binders;
	return success;
}

void ExpressionBinder::BindChild(unique_ptr<ParsedExpression> &expr, idx_t depth, string &error) {
	if (expr) {
		string bind_error = Bind(expr, depth);
		if (error.empty()) {
			error = bind_error;
		}
	}
}

void ExpressionBinder::ExtractCorrelatedExpressions(Binder &binder, Expression &expr) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
		if (bound_colref.depth > 0) {
			binder.AddCorrelatedColumn(CorrelatedColumnInfo(bound_colref));
		}
	}
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](Expression &child) { ExtractCorrelatedExpressions(binder, child); });
}

bool ExpressionBinder::ContainsType(const LogicalType &type, LogicalTypeId target) {
	if (type.id() == target) {
		return true;
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		auto child_count = StructType::GetChildCount(type);
		for (idx_t i = 0; i < child_count; i++) {
			if (ContainsType(StructType::GetChildType(type, i), target)) {
				return true;
			}
		}
		return false;
	}
	case LogicalTypeId::UNION: {
		auto member_count = UnionType::GetMemberCount(type);
		for (idx_t i = 0; i < member_count; i++) {
			if (ContainsType(UnionType::GetMemberType(type, i), target)) {
				return true;
			}
		}
		return false;
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return ContainsType(ListType::GetChildType(type), target);
	default:
		return false;
	}
}

LogicalType ExpressionBinder::ExchangeType(const LogicalType &type, LogicalTypeId target, LogicalType new_type) {
	if (type.id() == target) {
		return new_type;
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		// we make a copy of the child types of the struct here
		auto child_types = StructType::GetChildTypes(type);
		for (auto &child_type : child_types) {
			child_type.second = ExchangeType(child_type.second, target, new_type);
		}
		return LogicalType::STRUCT(child_types);
	}
	case LogicalTypeId::UNION: {
		auto member_types = UnionType::CopyMemberTypes(type);
		for (auto &member_type : member_types) {
			member_type.second = ExchangeType(member_type.second, target, new_type);
		}
		return LogicalType::UNION(std::move(member_types));
	}
	case LogicalTypeId::LIST:
		return LogicalType::LIST(ExchangeType(ListType::GetChildType(type), target, new_type));
	case LogicalTypeId::MAP:
		return LogicalType::MAP(ExchangeType(ListType::GetChildType(type), target, new_type));
	default:
		return type;
	}
}

bool ExpressionBinder::ContainsNullType(const LogicalType &type) {
	return ContainsType(type, LogicalTypeId::SQLNULL);
}

LogicalType ExpressionBinder::ExchangeNullType(const LogicalType &type) {
	return ExchangeType(type, LogicalTypeId::SQLNULL, LogicalType::INTEGER);
}

unique_ptr<Expression> ExpressionBinder::Bind(unique_ptr<ParsedExpression> &expr, optional_ptr<LogicalType> result_type,
                                              bool root_expression) {
	// bind the main expression
	auto error_msg = Bind(expr, 0, root_expression);
	if (!error_msg.empty()) {
		// failed to bind: try to bind correlated columns in the expression (if any)
		bool success = BindCorrelatedColumns(expr);
		if (!success) {
			throw BinderException(error_msg);
		}
		auto &bound_expr = expr->Cast<BoundExpression>();
		ExtractCorrelatedExpressions(binder, *bound_expr.expr);
	}
	auto &bound_expr = expr->Cast<BoundExpression>();
	unique_ptr<Expression> result = std::move(bound_expr.expr);
	if (target_type.id() != LogicalTypeId::INVALID) {
		// the binder has a specific target type: add a cast to that type
		result = BoundCastExpression::AddCastToType(context, std::move(result), target_type);
	} else {
		if (!binder.can_contain_nulls) {
			// SQL NULL type is only used internally in the binder
			// cast to INTEGER if we encounter it outside of the binder
			if (ContainsNullType(result->return_type)) {
				auto exchanged_type = ExchangeNullType(result->return_type);
				result = BoundCastExpression::AddCastToType(context, std::move(result), exchanged_type);
			}
		}
		if (result->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}
	if (result_type) {
		*result_type = result->return_type;
	}
	return result;
}

string ExpressionBinder::Bind(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression) {
	// bind the node, but only if it has not been bound yet
	auto &expression = *expr;
	auto alias = expression.alias;
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION) {
		// already bound, don't bind it again
		return string();
	}
	// bind the expression
	BindResult result = BindExpression(expr, depth, root_expression);
	if (result.HasError()) {
		return result.error;
	}
	// successfully bound: replace the node with a BoundExpression
	expr = make_uniq<BoundExpression>(std::move(result.expression));
	auto &be = expr->Cast<BoundExpression>();
	be.alias = alias;
	if (!alias.empty()) {
		be.expr->alias = alias;
	}
	return string();
}

} // namespace duckdb
