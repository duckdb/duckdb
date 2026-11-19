#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/function_language.hpp"
#include "duckdb/function/language_macro_function.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/planner/expression/bound_subquery_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/table_binding.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"

namespace duckdb {

void ExpressionBinder::ReplaceMacroParametersInLambda(FunctionExpression &function,
                                                      vector<identifier_set_t> &lambda_params) {
	for (auto &child : function.GetArgumentsMutable()) {
		if (child.GetExpression().GetExpressionClass() != ExpressionClass::LAMBDA) {
			ReplaceMacroParameters(child.GetExpressionMutable(), lambda_params);
			continue;
		}

		// Special-handling for LHS lambda parameters.
		// We do not replace them, and we add them to the lambda_params vector.
		auto &lambda_expr = child.GetExpressionMutable()->Cast<LambdaExpression>();
		string error_message;
		auto column_ref_expressions = lambda_expr.ExtractColumnRefExpressions(error_message);

		if (!error_message.empty()) {
			// Possibly a JSON function, replace both LHS and RHS.
			ReplaceMacroParameters(lambda_expr.LeftMutable(), lambda_params);
			ReplaceMacroParameters(lambda_expr.RightMutable(), lambda_params);
			continue;
		}

		// Push the lambda parameter names of this level.
		lambda_params.emplace_back();
		for (const auto &column_ref_expr : column_ref_expressions) {
			const auto &column_ref = column_ref_expr.get().Cast<ColumnRefExpression>();
			lambda_params.back().emplace(column_ref.GetName());
		}

		// Only replace in the RHS of the expression.
		ReplaceMacroParameters(lambda_expr.RightMutable(), lambda_params);

		lambda_params.pop_back();
	}
}

void ExpressionBinder::ReplaceMacroParameters(unique_ptr<ParsedExpression> &expr,
                                              vector<identifier_set_t> &lambda_params) {
	switch (expr->GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		// If the expression is a column reference, we replace it with its argument.
		auto &col_ref = expr->Cast<ColumnRefExpression>();
		if (LambdaExpression::IsLambdaParameter(lambda_params, col_ref.GetColumnName())) {
			return;
		}

		bool bind_macro_parameter = false;
		if (col_ref.IsQualified()) {
			// the table qualifier is the component directly before the column name
			auto &names = col_ref.ColumnNames();
			if (names[names.size() - 2].StartsWith(DummyBinding::DUMMY_NAME)) {
				bind_macro_parameter = true;
			}
		} else {
			bind_macro_parameter = macro_binding->HasMatchingBinding(col_ref.GetColumnName());
		}

		if (bind_macro_parameter) {
			D_ASSERT(macro_binding->HasMatchingBinding(col_ref.GetColumnName()));
			expr = macro_binding->ParamToArg(col_ref);
		}
		return;
	}
	case ExpressionClass::FUNCTION: {
		// Special-handling for lambdas, which are inside function expressions.
		auto &function = expr->Cast<FunctionExpression>();
		if (function.IsLambdaFunction()) {
			return ReplaceMacroParametersInLambda(function, lambda_params);
		}
		break;
	}
	case ExpressionClass::SUBQUERY: {
		auto &sq = (expr->Cast<SubqueryExpression>()).Subquery();
		ParsedExpressionIterator::EnumerateQueryNodeChildren(
		    *sq->node, [&](unique_ptr<ParsedExpression> &child) { ReplaceMacroParameters(child, lambda_params); });
		break;
	}
	default:
		break;
	}

	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ReplaceMacroParameters(child, lambda_params); });
}

// Find aggregate expression children
void ExpressionBinder::FindAggregateExprs(unique_ptr<ParsedExpression> &expr,
                                          vector<reference<unique_ptr<ParsedExpression>>> &exprs) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &fn_expr = expr->Cast<FunctionExpression>();

		// Look up the function in the catalog, check to see if it is actually an aggregate function
		EntryLookupInfo fn_entry(CatalogType::AGGREGATE_FUNCTION_ENTRY, fn_expr.GetQualifiedName());
		auto entry = GetCatalogEntry(fn_entry, OnEntryNotFound::RETURN_NULL);

		if (entry && entry->type == CatalogType::AGGREGATE_FUNCTION_ENTRY) {
			exprs.push_back(expr);
			return;
		}
	}

	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child_expr) { FindAggregateExprs(child_expr, exprs); });
}

void ExpressionBinder::UnfoldWindowMacroExpression(unique_ptr<ParsedExpression> &expr, ScalarMacroFunction &macro_def) {
	auto macro_copy = macro_def.expression->Copy();
	vector<reference<unique_ptr<ParsedExpression>>> aggregate_exprs;
	FindAggregateExprs(macro_copy, aggregate_exprs);

	// Only allowed if the macro body has a single aggregate expression
	if (aggregate_exprs.size() != 1) {
		throw BinderException("Window function macro bodies must contain exactly one aggregate function");
	}

	// The window spec is pushed down to the aggregate function target within the macro body
	unique_ptr<ParsedExpression> &agg_expr_ref = aggregate_exprs[0];
	auto &agg_fn_expr = agg_expr_ref->Cast<FunctionExpression>();

	// Transfer the macro function attributes
	auto &window_expr = expr->Cast<WindowExpression>();
	window_expr.SetQualifiedName(agg_fn_expr.GetQualifiedName());
	window_expr.GetArgumentsMutable().clear();
	for (auto &arg : agg_fn_expr.GetArgumentsMutable()) {
		window_expr.GetArgumentsMutable().push_back(std::move(arg));
	}
	if (!window_expr.Distinct()) {
		window_expr.DistinctMutable() = agg_fn_expr.Distinct();
	}
	if (window_expr.Filter() && agg_fn_expr.Filter()) {
		// Two FILTER clauses: combine
		window_expr.FilterMutable() =
		    make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(window_expr.FilterMutable()),
		                                     std::move(agg_fn_expr.FilterMutable()));
	} else if (agg_fn_expr.Filter()) {
		//	One FILTER from the MACRO
		window_expr.FilterMutable() = std::move(agg_fn_expr.FilterMutable());
	}
	// Transfer argument ORDER BYs
	if (agg_fn_expr.OrderBy()) {
		auto clone = agg_fn_expr.OrderBy()->Copy();
		window_expr.ArgOrdersMutable() = std::move(clone->Cast<OrderModifier>().orders);
	}

	// Replace the aggregate expression with the new window expression
	agg_expr_ref = std::move(expr);
	expr = std::move(macro_copy);
}

MacroFunction &ExpressionBinder::UnfoldMacroExpression(FunctionExpression &function,
                                                       ScalarMacroCatalogEntry &macro_func,
                                                       unique_ptr<ParsedExpression> &expr, idx_t depth) {
	// validate the arguments and separate positional and default arguments
	vector<unique_ptr<ParsedExpression>> positional_arguments;
	InsertionOrderPreservingMap<unique_ptr<ParsedExpression>, Identifier, identifier_map_t<idx_t>> named_arguments;
	binder.lambda_bindings = lambda_bindings;
	auto bind_result = MacroFunction::BindMacroFunction(binder, macro_func.macros, macro_func.name, function,
	                                                    positional_arguments, named_arguments, depth, this);
	if (!bind_result.error.empty()) {
		throw BinderException(*expr, bind_result.error);
	}
	auto &macro_def = macro_func.macros[bind_result.function_idx.GetIndex()]->Cast<ScalarMacroFunction>();

	auto new_macro_binding =
	    MacroFunction::CreateDummyBinding(macro_def, macro_func.name, positional_arguments, named_arguments);

	// replace current expression with stored macro expression
	// special case: If this is a window function, then we need to return a window expression
	if (expr->GetExpressionClass() == ExpressionClass::WINDOW) {
		UnfoldWindowMacroExpression(expr, macro_def);
	} else {
		expr = macro_def.expression->Copy();
	}

	SubstituteMacroParameters(expr, *new_macro_binding);
	return macro_def;
}

void ExpressionBinder::SubstituteMacroParameters(unique_ptr<ParsedExpression> &expr, DummyBinding &binding) {
	macro_binding = &binding;
	// qualify only the macro parameters with a new empty binder that only knows the macro binding
	auto dummy_binder = Binder::CreateBinder(context);
	dummy_binder->macro_binding = &binding;
	ExpressionBinder::QualifyColumnNames(*dummy_binder, expr);

	// now replace the parameters
	vector<identifier_set_t> lambda_params;
	ReplaceMacroParameters(expr, lambda_params);
}

//! Whether a bound argument reads a column of the calling query
static bool ReferencesColumns(const Expression &expr) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		return true;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY &&
	    expr.Cast<BoundSubqueryExpression>().IsCorrelated()) {
		return true;
	}
	bool result = false;
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { result = result || ReferencesColumns(child); });
	return result;
}

//! Point every node of a generated replacement at the call, generated offsets are not offsets into the query. Returns
//! whether the replacement contains a prepared statement parameter
static bool SetCallLocation(unique_ptr<ParsedExpression> &expr, const QueryLocation &location) {
	expr->SetQueryLocation(location);
	bool has_parameter = expr->GetExpressionClass() == ExpressionClass::PARAMETER;
	auto visit = [&](unique_ptr<ParsedExpression> &child) {
		has_parameter = SetCallLocation(child, location) || has_parameter;
	};
	if (expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &node = *expr->Cast<SubqueryExpression>().Subquery()->node;
		// the query node walk covers the node's modifiers and CTEs
		ParsedExpressionIterator::EnumerateQueryNodeChildren(node, visit,
		                                                     [&](TableRef &ref) { ref.query_location = location; });
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, visit);
	return has_parameter;
}

//! Append the language's note to an error raised while binding its replacement
static ErrorData AppendErrorNote(const ErrorData &error, const string &note) {
	switch (error.Type()) {
	case ExceptionType::BINDER:
	case ExceptionType::CONVERSION:
	case ExceptionType::CATALOG:
		break;
	default:
		return error;
	}
	ErrorData result(error.Type(), error.RawMessage() + "\n" + note);
	for (auto &entry : error.ExtraInfo()) {
		result.SetExtraInfo(entry.first, entry.second);
	}
	return result;
}

//! The probe state of one argument of a language member call
struct LanguageArgumentState {
	bool probed = false;
	bool folded = false;
	LanguageCallArgument argument;
	unique_ptr<Expression> bound;
};

void ExpressionBinder::ProbeLanguageArgument(ParsedExpression &parsed, const LogicalType &parameter_type, idx_t depth,
                                             LanguageArgumentState &state) {
	D_ASSERT(!state.probed);
	state.probed = true;
	auto &argument = state.argument;
	// the caller's expression, without the parameter cast selection added
	const bool is_typed = parameter_type.id() != LogicalTypeId::UNKNOWN;
	auto &original = is_typed && parsed.GetExpressionClass() == ExpressionClass::CAST
	                     ? parsed.Cast<CastExpression>().Child()
	                     : parsed;
	argument.sql = original.ToString();
	argument.has_subquery = parsed.HasSubquery();
	argument.has_window = parsed.IsWindow();
	auto aggregate_copy = parsed.Copy();
	vector<reference<unique_ptr<ParsedExpression>>> aggregates;
	FindAggregateExprs(aggregate_copy, aggregates);
	argument.has_aggregate = !aggregates.empty();
	argument.type = parameter_type;
	if (argument.has_aggregate || argument.has_window) {
		// binding them in the caller's binder would register them even when the replacement drops the argument
		return;
	}
	// bind a copy on its own, like macro overload selection, continuing this binder's expression depth
	ExpressionBinder probe_binder(binder, context);
	probe_binder.ContinueStackDepth(*this);
	probe_binder.lambda_bindings = binder.lambda_bindings;
	auto saved_correlated_columns = binder.correlated_columns;
	auto copy = parsed.Copy();
	try {
		auto bind_result = probe_binder.BindExpression(copy, depth);
		if (bind_result.HasError()) {
			argument.bind_error = bind_result.error.RawMessage();
		} else {
			state.bound = std::move(bind_result.expression);
		}
	} catch (...) {
		// a thrown error has no rollback contract: the bind may have changed binder state this probe cannot undo,
		// for example by binding a CTE the argument references. Only a soft BindResult error is recoverable
		binder.correlated_columns = std::move(saved_correlated_columns);
		throw;
	}
	binder.correlated_columns = std::move(saved_correlated_columns);
	if (!state.bound) {
		return;
	}
	if (state.bound->GetReturnType().id() == LogicalTypeId::UNKNOWN) {
		// an unresolved prepared statement parameter: re-bind once its type is known
		throw ParameterNotResolvedException();
	}
	argument.type = state.bound->GetReturnType();
	argument.references_columns = ReferencesColumns(*state.bound);
	argument.is_volatile = state.bound->IsVolatile();
}

//! Fold a probed argument, once it is read
static void FoldLanguageArgument(ClientContext &context, LanguageArgumentState &state) {
	if (state.folded) {
		return;
	}
	state.folded = true;
	if (!state.bound || !state.bound->IsFoldable()) {
		return;
	}
	Value value;
	if (ExpressionExecutor::TryEvaluateScalar(context, *state.bound, value)) {
		state.argument.is_constant = true;
		state.argument.value = std::move(value);
	}
}

//! The selected overload, when it takes every argument whose type is still unknown as ANY
static bool AcceptsUnknownArguments(const FunctionBindResult &selection, const vector<LogicalType> &call_types) {
	if (!selection) {
		return false;
	}
	auto &binding = selection.GetBinding();
	for (idx_t i = 0; i < call_types.size(); i++) {
		if (call_types[i].id() != LogicalTypeId::UNKNOWN) {
			continue;
		}
		auto &parameter_type = binding.parameter_types[binding.argument_to_parameters[i]];
		if (parameter_type.id() != LogicalTypeId::ANY) {
			return false;
		}
	}
	return true;
}

MacroFunction &ExpressionBinder::UnfoldLanguageMacro(FunctionExpression &function, ScalarMacroCatalogEntry &macro_func,
                                                     unique_ptr<ParsedExpression> &expr, idx_t depth,
                                                     string &error_note) {
	// validate the arguments, separate positional and named arguments, and select the member to call. Selection moves
	// the argument expressions out of the call, so it runs on a copy: everything up to the substitution can throw,
	// and the original must stay rebindable for callers that retry binding this tree
	auto function_copy_owner = function.Copy();
	auto &function_copy = function_copy_owner->Cast<FunctionExpression>();
	identifier_set_t named_by_call;
	for (auto &argument : function_copy.GetArguments()) {
		if (argument.HasName()) {
			named_by_call.insert(argument.GetName());
		}
	}
	vector<unique_ptr<ParsedExpression>> positional_arguments;
	InsertionOrderPreservingMap<unique_ptr<ParsedExpression>, Identifier, identifier_map_t<idx_t>> named_arguments;
	binder.lambda_bindings = lambda_bindings;
	auto macro_result = MacroFunction::BindMacroFunction(binder, macro_func.macros, macro_func.name, function_copy,
	                                                     positional_arguments, named_arguments, depth, this);
	if (!macro_result.error.empty()) {
		throw BinderException(*expr, macro_result.error);
	}
	const auto member_index = macro_result.function_idx.GetIndex();
	auto &member = *macro_func.macros[member_index];
	auto &language_member = member.Cast<LanguageMacroFunction>();
	if (expr->GetExpressionClass() == ExpressionClass::WINDOW) {
		throw BinderException(*expr, "Function %s is a %s function and cannot be used as a window function",
		                      macro_func.name, language_member.language);
	}

	// flatten the arguments into parameter declaration order, defaults were already filled in
	const auto parameter_count = language_member.parameters.size();
	auto arguments = std::move(positional_arguments);
	vector<Identifier> parameter_names;
	vector<bool> is_default(parameter_count, false);
	for (idx_t i = 0; i < parameter_count; i++) {
		auto &param_name = language_member.parameters[i]->Cast<ColumnRefExpression>().GetColumnName();
		parameter_names.push_back(param_name);
		if (i < arguments.size()) {
			continue;
		}
		auto named_it = named_arguments.find(param_name);
		D_ASSERT(named_it != named_arguments.end());
		arguments.push_back(std::move(named_it->second));
		is_default[i] = named_by_call.find(param_name) == named_by_call.end();
	}
	auto parameter_types = language_member.types;
	parameter_types.resize(parameter_count, LogicalType::UNKNOWN);

	auto language_entry = TryGetLanguageFunction(context, language_member.language);
	if (!language_entry) {
		throw MissingExtensionException(
		    "Function %s is written in language \"%s\", but no function implementing language \"%s\" is loaded. "
		    "Load the extension that registers the scalar function \"%s\"",
		    macro_func.name, language_member.language, language_member.language, language_member.language);
	}

	// select the overload of the language function as if the call were L('<source>', arguments...). An untyped argument
	// is bound for its type only when an overload's parameter for it is not ANY
	vector<LanguageArgumentState> argument_states(parameter_count);
	for (idx_t i = 0; i < parameter_count; i++) {
		argument_states[i].argument.is_default = is_default[i];
	}
	vector<LogicalType> call_types {LogicalType::VARCHAR};
	for (idx_t i = 0; i < parameter_count; i++) {
		auto is_parameter = arguments[i]->GetExpressionClass() == ExpressionClass::PARAMETER;
		if (parameter_types[i].id() == LogicalTypeId::UNKNOWN && is_parameter) {
			// a bare prepared parameter: an unresolved one defers the call to EXECUTE before the callback runs
			ProbeLanguageArgument(*arguments[i], parameter_types[i], depth, argument_states[i]);
		}
		call_types.push_back(argument_states[i].probed ? argument_states[i].argument.type : parameter_types[i]);
	}
	optional<FunctionBindResult> selection;
	try {
		selection = FunctionBinder::SelectFunction(context, language_entry->functions, call_types);
	} catch (ParameterNotResolvedException &) {
		// tied on the unknown arguments: bind them for their types below
	}
	if (!selection.has_value() || !AcceptsUnknownArguments(*selection, call_types)) {
		for (idx_t i = 0; i < parameter_count; i++) {
			if (call_types[i + 1].id() == LogicalTypeId::UNKNOWN && !argument_states[i].probed) {
				ProbeLanguageArgument(*arguments[i], parameter_types[i], depth, argument_states[i]);
				call_types[i + 1] = argument_states[i].argument.type;
			}
		}
		try {
			selection = FunctionBinder::SelectFunction(context, language_entry->functions, call_types);
		} catch (ParameterNotResolvedException &) {
			// prepared parameters were bound above: what stays unknown has no type before the replacement binds it
			idx_t unknown_idx = 0;
			while (unknown_idx + 2 < call_types.size() && call_types[unknown_idx + 1].id() != LogicalTypeId::UNKNOWN) {
				unknown_idx++;
			}
			throw BinderException(*expr,
			                      "Function %s: cannot select an overload of language \"%s\": argument %llu has no "
			                      "type before it is bound (an aggregate, a window, a lambda, or an argument that does "
			                      "not bind on its own). Declare the parameter's type to select an overload",
			                      macro_func.name, language_member.language, unknown_idx + 1);
		}
	}
	if (!*selection) {
		ErrorData error = selection->GetError();
		error.Throw(StringUtil::Format("Function %s: ", macro_func.name));
	}
	auto language_function = selection->GetBinding().GetFunction<ScalarFunction>();
	auto read_argument = [&](idx_t argument_idx) -> const LanguageCallArgument & {
		auto &state = argument_states[argument_idx];
		if (!state.probed) {
			ProbeLanguageArgument(*arguments[argument_idx], parameter_types[argument_idx], depth, state);
		}
		FoldLanguageArgument(context, state);
		return state.argument;
	};

	// ask the language for the replacement
	auto function_name = macro_func.ParentSchema().GetQualifiedName(macro_func.name);
	auto search_path = binder.EntryRetriever().GetSearchPath().Get();
	auto function_info = language_function->GetFunctionInfo();
	ScalarFunctionCallReplaceInput input(
	    context, function_info.get(), function_name, member_index, language_member.language, language_member.source,
	    parameter_names, parameter_types, member.return_type, parameter_count, read_argument, search_path);
	if (language_function->HasCallReplaceCallback()) {
		language_function->GetCallReplaceCallback()(input);
	}
	unique_ptr<ParsedExpression> replacement;
	if (input.HasReplacement()) {
		replacement = input.TakeReplacement();
	} else {
		// the direct form: a call of the language function never invokes its callback, so this cannot recurse
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(ConstantExpression::String(language_member.source));
		for (auto &name : parameter_names) {
			children.push_back(make_uniq<ColumnRefExpression>(name));
		}
		replacement = make_uniq<FunctionExpression>(
		    QualifiedName(Identifier::SystemCatalog(), Identifier::DefaultSchema(), language_entry->name),
		    std::move(children));
	}
	if (SetCallLocation(replacement, expr->GetQueryLocation())) {
		// like a macro body: only the caller's own arguments may carry its parameters
		throw BinderException(
		    *expr, "The replacement for %s returned by language \"%s\" contains a prepared statement parameter",
		    macro_func.name, language_member.language);
	}

	// substitute the arguments like a macro body, and bind the result in the caller's scope
	DummyBinding binding(parameter_types, input.GetReplacementParameterNames(), macro_func.name.GetIdentifierName());
	binding.arguments = &arguments;
	SubstituteMacroParameters(replacement, binding);
	expr = std::move(replacement);
	error_note = input.GetErrorNote();
	return member;
}

BindResult ExpressionBinder::BindLanguageMacro(FunctionExpression &function, ScalarMacroCatalogEntry &macro_func,
                                               idx_t depth, unique_ptr<ParsedExpression> &expr,
                                               optional_ptr<MacroFunction> &member) {
	string error_note;
	member = UnfoldLanguageMacro(function, macro_func, expr, depth, error_note);
	if (error_note.empty()) {
		return BindExpression(expr, depth);
	}
	return BindLanguageReplacement(expr, depth, error_note);
}

BindResult ExpressionBinder::BindLanguageReplacement(unique_ptr<ParsedExpression> &expr, idx_t depth,
                                                     const string &error_note) {
	BindResult result;
	try {
		result = BindExpression(expr, depth);
	} catch (std::exception &ex) {
		AppendErrorNote(ErrorData(ex), error_note).Throw();
	}
	if (result.HasError()) {
		result.error = AppendErrorNote(result.error, error_note);
	}
	return result;
}

BindResult ExpressionBinder::BindMacro(FunctionExpression &function, ScalarMacroCatalogEntry &macro_func, idx_t depth,
                                       unique_ptr<ParsedExpression> &expr) {
	auto stack_checker = StackCheck(*expr, 3);
	// expanding nested macros can take a long time, allow the query to be interrupted
	context.FastInterruptCheck();

	// a function entry holds either language members or plain macro members, never both: plain
	// entries keep the original selection, which consumes the call's arguments and replaces the
	// expression wholesale, so regular macros bind without copying the call
	const bool is_language_entry =
	    !macro_func.macros.empty() && macro_func.macros[0]->type == MacroType::LANGUAGE_MACRO;

	optional_ptr<MacroFunction> member;
	BindResult result;
	if (is_language_entry) {
		result = BindLanguageMacro(function, macro_func, depth, expr, member);
	} else {
		// unfold the macro expression and bind the result
		member = UnfoldMacroExpression(function, macro_func, expr, depth);
		result = BindExpression(expr, depth);
	}
	if (result.HasError()) {
		return result;
	}
	// enforce a declared RETURNS type by casting the member's result to it
	if (member->return_type.id() != LogicalTypeId::UNKNOWN) {
		result.expression =
		    BoundCastExpression::AddCastToType(context, std::move(result.expression), member->return_type);
	}
	return result;
}

} // namespace duckdb
