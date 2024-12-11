#include "duckdb/planner/expression_binder.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/main/client_config.hpp"

namespace duckdb {

void ExpressionBinder::SetCatalogLookupCallback(catalog_entry_callback_t callback) {
	binder.SetCatalogLookupCallback(std::move(callback));
}

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
	static constexpr idx_t INITIAL_DEPTH = 5;
	if (binder.HasActiveBinder()) {
		stack_depth = binder.GetActiveBinder().stack_depth + INITIAL_DEPTH;
	} else {
		stack_depth = INITIAL_DEPTH;
	}
}

StackChecker<ExpressionBinder> ExpressionBinder::StackCheck(const ParsedExpression &expr, idx_t extra_stack) {
	D_ASSERT(stack_depth != DConstants::INVALID_INDEX);
	auto &options = ClientConfig::GetConfig(context);
	if (stack_depth + extra_stack >= options.max_expression_depth) {
		throw BinderException("Max expression depth limit of %lld exceeded. Use \"SET max_expression_depth TO x\" to "
		                      "increase the maximum expression depth.",
		                      options.max_expression_depth);
	}
	return StackChecker<ExpressionBinder>(*this, extra_stack);
}

BindResult ExpressionBinder::BindExpression(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression) {
	auto stack_checker = StackCheck(*expr);

	auto &expr_ref = *expr;
	switch (expr_ref.GetExpressionClass()) {
	case ExpressionClass::BETWEEN:
		return BindExpression(expr_ref.Cast<BetweenExpression>(), depth);
	case ExpressionClass::CASE:
		return BindExpression(expr_ref.Cast<CaseExpression>(), depth);
	case ExpressionClass::CAST:
		return BindExpression(expr_ref.Cast<CastExpression>(), depth);
	case ExpressionClass::COLLATE:
		return BindExpression(expr_ref.Cast<CollateExpression>(), depth);
	case ExpressionClass::COLUMN_REF:
		return BindExpression(expr_ref.Cast<ColumnRefExpression>(), depth, root_expression);
	case ExpressionClass::LAMBDA_REF:
		return BindExpression(expr_ref.Cast<LambdaRefExpression>(), depth);
	case ExpressionClass::COMPARISON:
		return BindExpression(expr_ref.Cast<ComparisonExpression>(), depth);
	case ExpressionClass::CONJUNCTION:
		return BindExpression(expr_ref.Cast<ConjunctionExpression>(), depth);
	case ExpressionClass::CONSTANT:
		return BindExpression(expr_ref.Cast<ConstantExpression>(), depth);
	case ExpressionClass::FUNCTION: {
		auto &function = expr_ref.Cast<FunctionExpression>();
		if (IsUnnestFunction(function.function_name)) {
			// special case, not in catalog
			return BindUnnest(function, depth, root_expression);
		}
		// binding a function expression requires an extra parameter for macros
		return BindExpression(function, depth, expr);
	}
	case ExpressionClass::LAMBDA:
		return BindExpression(expr_ref.Cast<LambdaExpression>(), depth, LogicalTypeId::INVALID, nullptr);
	case ExpressionClass::OPERATOR:
		return BindExpression(expr_ref.Cast<OperatorExpression>(), depth);
	case ExpressionClass::SUBQUERY:
		return BindExpression(expr_ref.Cast<SubqueryExpression>(), depth);
	case ExpressionClass::PARAMETER:
		return BindExpression(expr_ref.Cast<ParameterExpression>(), depth);
	case ExpressionClass::POSITIONAL_REFERENCE:
		return BindPositionalReference(expr, depth, root_expression);
	case ExpressionClass::STAR:
		return BindResult(BinderException::Unsupported(expr_ref, "STAR expression is not supported here"));
	default:
		throw NotImplementedException("Unimplemented expression class");
	}
}

static bool CombineMissingColumns(ErrorData &current, ErrorData new_error) {
	auto &current_info = current.ExtraInfo();
	auto &new_info = new_error.ExtraInfo();
	auto current_entry = current_info.find("error_subtype");
	auto new_entry = new_info.find("error_subtype");
	if (current_entry == current_info.end() || new_entry == new_info.end()) {
		// no subtype info in either expression
		return false;
	}
	if (current_entry->second != "COLUMN_NOT_FOUND" || new_entry->second != "COLUMN_NOT_FOUND") {
		// either info is not a `COLUMN_NOT_FOUND`
		return false;
	}
	current_entry = current_info.find("name");
	new_entry = new_info.find("name");
	if (current_entry == current_info.end() || new_entry == new_info.end()) {
		// no candidate info in either column
		return false;
	}
	if (current_entry->second != new_entry->second) {
		// error does not concern the same name/column
		return false;
	}
	auto column_name = current_entry->second;
	current_entry = current_info.find("candidates");
	new_entry = new_info.find("candidates");
	if (current_entry == current_info.end()) {
		// no current candidates - use new candidates
		current = std::move(new_error);
		return true;
	}
	if (new_entry == new_info.end()) {
		// no new candidates - use current candidates
		return true;
	}
	// both errors have candidates - combine the candidates
	auto current_candidates = StringUtil::Split(current_entry->second, ",");
	auto new_candidates = StringUtil::Split(new_entry->second, ",");
	current_candidates.insert(current_candidates.end(), new_candidates.begin(), new_candidates.end());

	// run the similarity ranking on both sets of candidates
	unordered_set<string> candidates;
	vector<pair<string, double>> scores;
	for (auto &candidate : current_candidates) {
		// split by "." since the candidates might be in the form "table.column"
		auto column_splits = StringUtil::Split(candidate, ".");
		if (column_splits.empty()) {
			continue;
		}
		auto &candidate_column = column_splits.back();
		auto entry = candidates.find(candidate);
		if (entry != candidates.end()) {
			// already found
			continue;
		}
		auto score = StringUtil::SimilarityRating(candidate_column, column_name);
		candidates.insert(candidate);
		scores.emplace_back(make_pair(std::move(candidate), score));
	}
	// get a new top-n
	auto top_candidates = StringUtil::TopNStrings(scores);
	// get query location
	QueryErrorContext context;
	current_entry = current_info.find("position");
	new_entry = new_info.find("position");
	uint64_t position;
	if (current_entry != current_info.end() &&
	    TryCast::Operation<string_t, uint64_t>(current_entry->second, position)) {
		context = QueryErrorContext(position);
	} else if (new_entry != new_info.end() && TryCast::Operation<string_t, uint64_t>(new_entry->second, position)) {
		context = QueryErrorContext(position);
	}
	// generate a new (combined) error
	current = BinderException::ColumnNotFound(column_name, top_candidates, context);
	return true;
}

static void CombineErrors(ErrorData &current, ErrorData new_error) {
	// try to combine missing column exceptions in order to pick the most relevant one
	if (CombineMissingColumns(current, new_error)) {
		// keep the old info
		return;
	}

	// override the error with the new one
	current = std::move(new_error);
}

BindResult ExpressionBinder::BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr, ErrorData error_message) {
	// try to bind in one of the outer queries, if the binding error occurred in a subquery
	auto &active_binders = binder.GetActiveBinders();
	// make a copy of the set of binders, so we can restore it later
	auto binders = active_binders;
	auto bind_error = std::move(error_message);
	// we already failed with the current binder
	active_binders.pop_back();
	idx_t depth = 1;
	while (!active_binders.empty()) {
		auto &next_binder = active_binders.back().get();
		ExpressionBinder::QualifyColumnNames(next_binder.binder, expr);
		auto next_error = next_binder.Bind(expr, depth);
		if (!next_error.HasError()) {
			bind_error = std::move(next_error);
			break;
		}
		CombineErrors(bind_error, std::move(next_error));
		depth++;
		active_binders.pop_back();
	}
	active_binders = binders;
	return BindResult(bind_error);
}

void ExpressionBinder::BindChild(unique_ptr<ParsedExpression> &expr, idx_t depth, ErrorData &error) {
	if (expr) {
		ErrorData bind_error = Bind(expr, depth);
		if (!error.HasError()) {
			error = std::move(bind_error);
		}
	}
}

void ExpressionBinder::ExtractCorrelatedExpressions(Binder &binder, Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
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
	case LogicalTypeId::ARRAY:
		return ContainsType(ArrayType::GetChildType(type), target);
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
	case LogicalTypeId::ARRAY:
		return LogicalType::ARRAY(ExchangeType(ArrayType::GetChildType(type), target, new_type),
		                          ArrayType::GetSize(type));
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
	if (error_msg.HasError()) {
		// Try binding the correlated column. If binding the correlated column
		// has error messages, those should be propagated up. So for the test case
		// having subquery failed to bind:14 the real error message should be something like
		// aggregate with constant input must be bound to a root node.
		auto result = BindCorrelatedColumns(expr, error_msg);
		if (result.HasError()) {
			CombineErrors(error_msg, std::move(result.error));
			error_msg.Throw();
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

ErrorData ExpressionBinder::Bind(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression) {
	// bind the node, but only if it has not been bound yet
	auto query_location = expr->GetQueryLocation();
	auto &expression = *expr;
	auto alias = expression.GetAlias();
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION) {
		// already bound, don't bind it again
		return ErrorData();
	}
	// bind the expression
	BindResult result = BindExpression(expr, depth, root_expression);
	if (result.HasError()) {
		return std::move(result.error);
	}
	// successfully bound: replace the node with a BoundExpression
	result.expression->SetQueryLocation(query_location);
	expr = make_uniq<BoundExpression>(std::move(result.expression));
	auto &be = expr->Cast<BoundExpression>();
	be.SetAlias(alias);
	if (!alias.empty()) {
		be.expr->SetAlias(alias);
	}
	return ErrorData();
}

BindResult ExpressionBinder::BindUnsupportedExpression(ParsedExpression &expr, idx_t depth, const string &message) {
	// we always prefer to throw an error if it occurs in a child expression
	// since that error might be more descriptive
	// bind all children
	ErrorData result;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<ParsedExpression> &child) { BindChild(child, depth, result); });
	if (result.HasError()) {
		return BindResult(std::move(result));
	}
	return BindResult(BinderException::Unsupported(expr, message));
}

bool ExpressionBinder::IsUnnestFunction(const string &function_name) {
	return function_name == "unnest" || function_name == "unlist";
}

bool ExpressionBinder::TryBindAlias(ColumnRefExpression &colref, bool root_expression, BindResult &result) {
	return false;
}

} // namespace duckdb
