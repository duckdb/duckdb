#include "duckdb/planner/expression_binder/match_recognize_binder.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// What a value read out of the match is worth
//===--------------------------------------------------------------------===//
//! The field a value travels in while it is carried to the row that reports it
static constexpr const char *MATCH_RECOGNIZE_VALUE_FIELD = "v";

static unique_ptr<ParsedExpression> CreateStructExtract(unique_ptr<ParsedExpression> value, const string &child_name) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(value));
	children.push_back(make_uniq<ConstantExpression>(child_name));
	return make_uniq<FunctionExpression>("struct_extract", std::move(children));
}

static unique_ptr<ParsedExpression> CreateStructExtract(const string &column_name, const string &child_name) {
	return CreateStructExtract(make_uniq<ColumnRefExpression>(Identifier(column_name)), child_name);
}

unique_ptr<ParsedExpression> MatchRecognizeStateField(const string &state, const string &field) {
	return CreateStructExtract(state, field);
}

//! read. The row the match is reported on is not one of them.
static unique_ptr<ParsedExpression> OnlyWhenMatched(const string &state, unique_ptr<ParsedExpression> value) {
	auto in_match = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, CreateStructExtract(state, "is_empty"));
	auto result = make_uniq<CaseExpression>();
	CaseCheck check;
	check.when_expr = std::move(in_match);
	check.then_expr = std::move(value);
	result->CaseChecksMutable().push_back(std::move(check));
	result->ElseMutable() = make_uniq<ConstantExpression>(Value());
	return std::move(result);
}

//! struct_pack(v := <value>) is never NULL, so a NULL the value itself holds stays apart from the NULL
//! that masks a row the variable did not match - which is the NULL that MatchScopedValue walks back over.
//! An empty match covers no rows, so the row that carries it has no value to report and is masked too.
static unique_ptr<ParsedExpression> PackValue(const string &state, unique_ptr<ParsedExpression> value) {
	value->SetAlias(Identifier(MATCH_RECOGNIZE_VALUE_FIELD));
	vector<unique_ptr<ParsedExpression>> fields;
	fields.push_back(std::move(value));
	auto packed = make_uniq<FunctionExpression>("struct_pack", std::move(fields));

	auto in_match = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, CreateStructExtract(state, "is_empty"));
	auto result = make_uniq<CaseExpression>();
	CaseCheck check;
	check.when_expr = std::move(in_match);
	check.then_expr = std::move(packed);
	result->CaseChecksMutable().push_back(std::move(check));
	result->ElseMutable() = make_uniq<ConstantExpression>(Value());
	return std::move(result);
}

//! CASE WHEN <classifier> IN (symbols) THEN <column> END - NULL on every row none of them matched.
//! A plain pattern variable stands for itself; a SUBSET name stands for all of its members.
static unique_ptr<ParsedExpression> ClassifierMatches(const string &state, const vector<string> &symbols) {
	D_ASSERT(!symbols.empty());
	vector<unique_ptr<ParsedExpression>> in_children;
	in_children.push_back(CreateStructExtract(state, "classifier"));
	for (auto &symbol : symbols) {
		in_children.push_back(make_uniq<ConstantExpression>(Value(symbol)));
	}
	return make_uniq<OperatorExpression>(ExpressionType::COMPARE_IN, std::move(in_children));
}

static unique_ptr<ParsedExpression> ClassifiedValue(const string &state, const vector<string> &symbols,
                                                    unique_ptr<ParsedExpression> value) {
	auto matches_symbol = ClassifierMatches(state, symbols);
	auto result = make_uniq<CaseExpression>();
	CaseCheck check;
	check.when_expr = std::move(matches_symbol);
	check.then_expr = std::move(value);
	result->CaseChecksMutable().push_back(std::move(check));
	result->ElseMutable() = make_uniq<ConstantExpression>(Value());
	return std::move(result);
}

//! An ordering as the sorter will actually apply it, with the session's defaults filled in. A
//! reversed window has to spell both out, because "the opposite of the default" is not something the
//! enums can say.
static void ResolveOrder(ClientContext &context, OrderType &type, OrderByNullType &null_order) {
	auto &config = DBConfig::GetConfig(context);
	type = config.ResolveOrder(context, type);
	null_order = config.ResolveNullOrder(context, type, null_order);
}

//! A reference to <symbol>.<column> resolves to that column on the last row the variable matched.
//! ONE ROW PER MATCH reports a finished match, so it sees the whole match (FINAL semantics); ALL ROWS
//! PER MATCH reports progress, so it only sees the match up to the current row (RUNNING semantics).
static void ScopeToMatch(ClientContext &context, const string &state, WindowExpression &window,
                         const MatchRecognizeConfig &config, bool running, bool reversed = false) {
	// walked backwards, the rows that came before the current one come after it
	window.WindowStartMutable() =
	    running && reversed ? WindowBoundary::CURRENT_ROW_ROWS : WindowBoundary::UNBOUNDED_PRECEDING;
	window.WindowEndMutable() =
	    running && !reversed ? WindowBoundary::CURRENT_ROW_ROWS : WindowBoundary::UNBOUNDED_FOLLOWING;

	// matches are numbered within a partition, so both are needed to identify one
	for (auto &expr : config.partition_expressions) {
		window.PartitionsMutable().push_back(expr->Copy());
	}
	window.PartitionsMutable().push_back(CreateStructExtract(state, "match_number"));
	// Every window says which way round it walks, rather than leaning on the order its input happens
	// to arrive in. Windows that leave it unsaid are free to be grouped with one that says the
	// opposite, and then they walk that way too.
	for (auto &order : config.order_by_expressions) {
		auto type = order.type;
		auto null_order = order.null_order;
		if (reversed) {
			ResolveOrder(context, type, null_order);
			type = type == OrderType::DESCENDING ? OrderType::ASCENDING : OrderType::DESCENDING;
			null_order =
			    null_order == OrderByNullType::NULLS_FIRST ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
		}
		window.OrderByMutable().emplace_back(type, null_order, order.expression->Copy());
	}
	// A stable descending sort is not the exact reverse of a stable ascending one, so tied rows would
	// land in a different order in the two directions and FIRST(x, n) and LAST(x, n) would disagree
	// about which of them is which. The row's place in the partition is unique, so ordering on it last
	// leaves nothing tied.
	window.OrderByMutable().emplace_back(reversed ? OrderType::DESCENDING : OrderType::ASCENDING,
	                                     OrderByNullType::NULLS_LAST, CreateStructExtract(state, "row_index"));
}

//! Takes a value packed by PackValue and reports it from the first or last row of the match that carries
//! one. IGNORE NULLS is what makes it that row rather than the first or last row of the match.
static unique_ptr<ParsedExpression> MatchScopedValue(ClientContext &context, const string &state,
                                                     const MatchRecognizeConfig &config,
                                                     unique_ptr<ParsedExpression> packed, bool running,
                                                     bool first = false, idx_t offset = 0) {
	// nth_value counts from the frame's first row, so counting back from the match's last one is the
	// same window walked the other way round
	const auto reversed = !first && offset > 0;
	auto window =
	    make_uniq<WindowExpression>("", "", offset > 0 ? "nth_value" : (first ? "first_value" : "last_value"));
	window->GetArgumentsMutable().emplace_back(std::move(packed));
	if (offset > 0) {
		window->GetArgumentsMutable().emplace_back(
		    make_uniq<ConstantExpression>(Value::BIGINT(NumericCast<int64_t>(offset + 1))));
	}
	window->HasIgnoreNullsMutable() = true;
	window->IgnoreNullsMutable() = true;
	ScopeToMatch(context, state, *window, config, running, reversed);
	return CreateStructExtract(std::move(window), MATCH_RECOGNIZE_VALUE_FIELD);
}

//! Whether a call is spelled the way only an aggregate can be
static bool HasAggregateModifiers(const FunctionExpression &function) {
	return function.Distinct() || function.Filter() || (function.OrderBy() && !function.OrderBy()->orders.empty());
}

//! What a DEFINE condition needs from the plan below the matcher: one column per value the matcher
//! cannot compute itself, and the descriptors telling it what to do with them.
//! FIRST() and LAST() count from the end of the match they read from, by a constant the two clauses
//! spell the same way. A NULL constant casts to a NULL offset rather than failing, so it is turned
//! away here rather than read as a number further down.
idx_t MatchRecognizeNavigationOffset(const string &function_name, const ParsedExpression &offset_expr) {
	if (offset_expr.GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		throw BinderException("The offset of %s() must be a constant", function_name);
	}
	auto offset_value = offset_expr.Cast<ConstantExpression>().GetValue();
	if (offset_value.IsNull()) {
		throw BinderException("The offset of %s() must be a non-negative integer, not NULL", function_name);
	}
	if (!offset_value.DefaultTryCastAs(LogicalType::UBIGINT) || offset_value.IsNull()) {
		throw BinderException("The offset of %s() must be a non-negative integer", function_name);
	}
	const auto offset = offset_value.GetValue<uint64_t>();
	// MEASURES counts the offset from one, as the window function it lowers to takes it, and a match
	// no rows can reach is still an offset the two clauses have to agree on rather than wrap around
	if (offset >= NumericCast<uint64_t>(NumericLimits<int64_t>::Maximum())) {
		throw BinderException("The offset of %s() is larger than any match can have rows", function_name);
	}
	return NumericCast<idx_t>(offset);
}

//! Everything a reference names behind its first name is navigation into the column, so dropping a
//! qualifier drops that one name and keeps the rest: X.c.f is field f of column c, not column f.
unique_ptr<ParsedExpression> MatchRecognizeWithoutQualifier(const ColumnRefExpression &colref) {
	auto copy = colref.Copy();
	auto &copied = copy->Cast<ColumnRefExpression>();
	auto names = copied.ColumnNames();
	D_ASSERT(names.size() > 1);
	names.erase(names.begin());
	copied.ColumnNamesMutable() = std::move(names);
	return copy;
}

//===--------------------------------------------------------------------===//
// DEFINE
//===--------------------------------------------------------------------===//
unique_ptr<Expression> MatchRecognizeConditionInputs::Project(unique_ptr<Expression> value, const string &base) {
	return ProjectAs(std::move(value), generated.Reserve(base));
}

unique_ptr<Expression> MatchRecognizeConditionInputs::ProjectAs(unique_ptr<Expression> value, const string &name) {
	// A projection computes its columns from what its child produces, so what is added here can read
	// the input and the windows below it and nothing else: not a column of this projection, which
	// only exists for whoever reads the projection, and not a field only the matcher supplies, which
	// only exists while a match is being assembled. The binder decides both of those where the
	// expression is bound; this is the boundary that holds it to the decision.
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(*value, [&](const BoundColumnRefExpression &column) {
		if (column.Binding().table_index == projection_index) {
			throw InternalException("MATCH_RECOGNIZE projected \"%s\" from another column of the same projection",
			                        name);
		}
	});
	ExpressionIterator::VisitExpression<BoundReferenceExpression>(*value, [&](const BoundReferenceExpression &) {
		throw InternalException("MATCH_RECOGNIZE projected \"%s\", which reads a field only the matcher supplies",
		                        name);
	});
	auto type = value->GetReturnType();
	const auto index = select_list.size();
	value->SetAlias(Identifier(name));
	select_list.push_back(std::move(value));
	names.emplace_back(name);
	types.push_back(type);
	hidden.push_back(name);
	return make_uniq<BoundColumnRefExpression>(Identifier(name), type,
	                                           ColumnBinding(projection_index, ProjectionIndex(index)));
}

MatchRecognizeDefineBinder::MatchRecognizeDefineBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
                                                       MatchRecognizeConditionInputs &inputs_p,
                                                       const WindowExpression &window_template_p,
                                                       const case_insensitive_set_t &symbols_p,
                                                       const unique_ptr<Expression> &match_number_p)
    : SelectBinder(binder, context, node), inputs(inputs_p), window_template(window_template_p), symbols(symbols_p),
      match_number(match_number_p) {
}

BindResult MatchRecognizeDefineBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
                                                      bool root_expression) {
	auto &expr = *expr_ptr;
	if (expr.GetExpressionType() == ExpressionType::FUNCTION) {
		auto &function = expr.Cast<FunctionExpression>();
		auto function_name = StringUtil::Upper(function.FunctionName().GetIdentifierName());
		if (function_name == "CLASSIFIER" && function.GetArguments().empty()) {
			if (!outside.empty()) {
				OutsideMatch("CLASSIFIER()");
			}
			// the row being tested is the one this DEFINE decides on, so it classifies as this symbol
			expr_ptr = make_uniq<ConstantExpression>(Value(define_name));
			return SelectBinder::BindExpression(expr_ptr, depth, root_expression);
		}
		if (function_name == "MATCH_NUMBER" && function.GetArguments().empty()) {
			if (!outside.empty()) {
				// the matcher writes the number per attempt, so it is not a value the plan below the
				// matcher - or the frame it walks - has anything to compute
				OutsideMatch("MATCH_NUMBER()");
			}
			return BindResult(match_number->Copy());
		}
		if (function_name == "PREV" || function_name == "NEXT") {
			if (frame) {
				throw BinderException("%s() reads a neighbour in the order the matcher walks, so it cannot be part "
				                      "of %s",
				                      function_name, outside);
			}
			return BindNeighbour(function, function_name, expr_ptr, depth);
		}
		if (function_name == "FIRST" || function_name == "LAST") {
			return BindNavigation(function, function_name, depth);
		}
	}
	if (expr.GetExpressionClass() == ExpressionClass::WINDOW) {
		// A window walks the ordered partition rather than the match, so it is computed below the
		// matcher like the navigation the clause writes as one. What binding it returns is the window
		// operator's own output, which is a child of the projection the conditions are evaluated over
		// - so it is left as it is, and the projection column for it is appended once, by the
		// remapping every other reference into the input goes through.
		const auto saved = outside;
		outside = "a window function, which is computed over the whole partition before there is a match";
		auto bound = SelectBinder::BindExpression(expr_ptr, depth, root_expression);
		outside = saved;
		return bound;
	}
	if (expr.GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto &names = colref.ColumnNames();
		const auto qualifier = names.size() >= 2 ? names[0].GetIdentifierName() : string();
		if (!qualifier.empty() && StringUtil::CIEquals(qualifier, define_name)) {
			// the variable being defined is the row being tested, so it is the row itself
			expr_ptr = MatchRecognizeWithoutQualifier(colref);
			return SelectBinder::BindExpression(expr_ptr, depth, root_expression);
		}
		if (!qualifier.empty() && symbols.count(qualifier)) {
			// naming another variable means its value on the last row matched to it so far, which
			// is what LAST() means
			return BindNavigated(MatchRecognizeWithoutQualifier(colref), MatchRecognizeDefineColumn(qualifier), true, 0,
			                     depth);
		}
	}
	return SelectBinder::BindExpression(expr_ptr, depth, root_expression);
}

BindResult MatchRecognizeDefineBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function,
                                                     idx_t depth) {
	return BindResult(BinderException(expr, UnsupportedAggregateMessage()));
}

string MatchRecognizeDefineBinder::UnsupportedAggregateMessage() {
	return "A MATCH_RECOGNIZE condition decides one row at a time, so it cannot be an aggregate";
}

void MatchRecognizeDefineBinder::OutsideMatch(const string &what) const {
	D_ASSERT(!outside.empty());
	throw BinderException("%s only means something while the matcher is assembling a match, so it cannot be part of %s",
	                      what, outside);
}

BindResult MatchRecognizeDefineBinder::BindNeighbour(FunctionExpression &function, const string &function_name,
                                                     unique_ptr<ParsedExpression> &expr_ptr, idx_t depth) {
	auto &arguments = function.GetArgumentsMutable();
	if (arguments.empty() || arguments.size() > 2) {
		throw BinderException("%s() takes an expression and an optional offset", function_name);
	}
	auto neighbour = window_template.Copy();
	auto &window = neighbour->Cast<WindowExpression>();
	window.SetFunctionName(function_name == "PREV" ? "lag" : "lead");
	window.GetArgumentsMutable() = std::move(arguments);
	expr_ptr = std::move(neighbour);
	// it is a window like any other from here on, so it is bound like one
	return BindExpression(expr_ptr, depth, false);
}

BindResult MatchRecognizeDefineBinder::BindNavigation(FunctionExpression &function, const string &function_name,
                                                      idx_t depth) {
	auto &arguments = function.GetArgumentsMutable();
	if (arguments.empty() || arguments.size() > 2) {
		throw BinderException("%s() takes an expression and an optional offset", function_name);
	}
	idx_t offset = 0;
	if (arguments.size() == 2) {
		offset = MatchRecognizeNavigationOffset(function_name, arguments[1].GetExpression());
	}
	auto inner = std::move(arguments[0].GetExpressionMutable());
	string symbol;
	if (inner->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = inner->Cast<ColumnRefExpression>();
		auto &names = colref.ColumnNames();
		if (names.size() >= 2 && symbols.count(names[0].GetIdentifierName())) {
			symbol = MatchRecognizeDefineColumn(names[0].GetIdentifierName());
			inner = MatchRecognizeWithoutQualifier(colref);
		}
	}
	return BindNavigated(std::move(inner), symbol, function_name == "LAST", offset, depth);
}

BindResult MatchRecognizeDefineBinder::BindNavigated(unique_ptr<ParsedExpression> inner, string symbol, bool last,
                                                     idx_t offset, idx_t depth) {
	if (navigated) {
		throw BinderException("Nested row pattern navigation is not supported");
	}
	if (!outside.empty()) {
		OutsideMatch("Reading a row of the match");
	}
	navigated = true;
	const auto saved = outside;
	outside = "the expression a navigation reads, which is computed for every row of the input";
	auto bound = BindExpression(inner, depth, false);
	outside = saved;
	navigated = false;
	if (bound.HasError()) {
		return bound;
	}
	auto column = inputs.Project(std::move(bound.expression), "__mr_nav");
	inputs.navigations.push_back(
	    MatchRecognizeNavigation {last, std::move(symbol), inputs.select_list.size() - 1, offset});
	return BindResult(std::move(column));
}

//===--------------------------------------------------------------------===//
// MEASURES
//===--------------------------------------------------------------------===//
//! An aggregate in MEASURES aggregates the rows of the match that its arguments name a variable for.
//! The variable belongs to this clause's namespace rather than the input's, so it is resolved here
//! and the reference is left as the column it names.
static void ScopeToVariable(unique_ptr<ParsedExpression> &expr, const case_insensitive_map_t<vector<string>> &symbols,
                            case_insensitive_set_t &scope) {
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = expr->Cast<ColumnRefExpression>();
		auto &names = colref.ColumnNames();
		if (names.size() >= 2 && symbols.find(names[0].GetIdentifierName()) != symbols.end()) {
			scope.insert(names[0].GetIdentifierName());
			expr = MatchRecognizeWithoutQualifier(colref);
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ScopeToVariable(child, symbols, scope); });
}

MatchRecognizeMeasureBinder::MatchRecognizeMeasureBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
                                                         string state_p, const MatchRecognizeConfig &config_p,
                                                         const case_insensitive_map_t<vector<string>> &symbols_p,
                                                         bool all_rows)
    : SelectBinder(binder, context, node), state(std::move(state_p)), config(config_p), symbols(symbols_p),
      one_row(!all_rows), running(all_rows) {
}

BindResult MatchRecognizeMeasureBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
                                                       bool root_expression) {
	auto &expr = *expr_ptr;
	if (expr.GetExpressionType() == ExpressionType::FUNCTION) {
		auto &function = expr.Cast<FunctionExpression>();
		auto function_name = StringUtil::Upper(function.FunctionName().GetIdentifierName());
		// RUNNING and FINAL choose how much of the match the measure below them sees. ONE ROW PER
		// MATCH reports a finished match, so its current row is the last one: the two are the same
		// thing there and the keywords make no difference.
		const auto is_running = function.FunctionName() == MATCH_RECOGNIZE_RUNNING_MARKER;
		if (is_running || function.FunctionName() == MATCH_RECOGNIZE_FINAL_MARKER) {
			expr_ptr = std::move(function.GetArgumentsMutable()[0].GetExpressionMutable());
			const auto saved = running;
			running = one_row ? false : is_running;
			auto result = BindExpression(expr_ptr, depth, root_expression);
			running = saved;
			return result;
		}
		if (function_name == "CLASSIFIER" && function.GetArguments().empty()) {
			expr_ptr = StateField("classifier");
			return BindGenerated(expr_ptr, depth, root_expression);
		}
		if (function_name == "MATCH_NUMBER" && function.GetArguments().empty()) {
			expr_ptr = StateField("match_number");
			return BindGenerated(expr_ptr, depth, root_expression);
		}
		// logical navigation over the rows of the match. LAST(X.c) is what an unadorned X.c already
		// means, so both share the masking; only the end they read from differs.
		if ((function_name == "FIRST" || function_name == "LAST") && !function.GetArguments().empty() &&
		    function.GetArguments().size() <= 2) {
			return BindNavigation(function, function_name, expr_ptr, depth, root_expression);
		}
		// DISTINCT, FILTER and an argument ORDER BY only mean anything to an aggregate, so a call
		// carrying one is one - including a macro standing for one, which the window unfolds. An
		// aggregate written without them reaches the hook below instead, once the ordinary binder
		// has resolved what it is.
		if (HasAggregateModifiers(function)) {
			return BindOverMatch(function, depth);
		}
	}
	if (expr.GetExpressionType() == ExpressionType::COLUMN_REF && !scoped) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		auto &names = colref.ColumnNames();
		auto entry = names.size() >= 2 ? symbols.find(names[0].GetIdentifierName()) : symbols.end();
		if (entry != symbols.end()) {
			// a pattern variable scopes the column to the rows it matched
			expr_ptr = MatchScopedValue(
			    context, state, config,
			    ClassifiedValue(state, entry->second, PackValue(state, MatchRecognizeWithoutQualifier(colref))),
			    running);
			return BindGenerated(expr_ptr, depth, root_expression);
		}
		if (ClaimsAlias(colref)) {
			// A measure may name an earlier measure, the way a select list may name an earlier column,
			// and a column of the input takes precedence over one. Which of the two this is only shows
			// once it has been bound - and if it is the column, binding it again below costs nothing
			// because a column reference leaves nothing behind.
			auto probe = expr_ptr->Copy();
			auto bound = SelectBinder::BindExpression(probe, depth, root_expression);
			if (bound.HasError() || bound.expression->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return bound;
			}
		}
		// an empty match covers no rows, so a column of the input has no row here to be read from
		expr_ptr = OnlyWhenMatched(state, std::move(expr_ptr));
		return BindGenerated(expr_ptr, depth, root_expression);
	}
	return SelectBinder::BindExpression(expr_ptr, depth, root_expression);
}

BindResult MatchRecognizeMeasureBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function,
                                                      idx_t depth) {
	return BindOverMatch(expr, depth);
}

BindResult MatchRecognizeMeasureBinder::BindOverMatch(FunctionExpression &expr, idx_t depth) {
	// naming a variable restricts the aggregate to the rows it matched
	case_insensitive_set_t scope;
	for (auto &argument : expr.GetArgumentsMutable()) {
		ScopeToVariable(argument.GetExpressionMutable(), symbols, scope);
	}
	if (expr.OrderByMutable()) {
		for (auto &order : expr.OrderByMutable()->orders) {
			ScopeToVariable(order.expression, symbols, scope);
		}
	}
	// the filter decides which of the match's rows the aggregate sees, so it reads the match the
	// same way the arguments do
	if (expr.FilterMutable()) {
		ScopeToVariable(expr.FilterMutable(), symbols, scope);
	}
	if (scope.size() > 1) {
		throw BinderException("An aggregate in MEASURES reads the rows of one pattern variable, so \"%s\" "
		                      "cannot also read those of \"%s\"",
		                      *scope.begin(), *std::next(scope.begin()));
	}

	auto &qualified = expr.GetQualifiedName();
	auto window =
	    make_uniq<WindowExpression>(qualified.Catalog().GetIdentifierName(), qualified.Schema().GetIdentifierName(),
	                                qualified.Name().GetIdentifierName());
	window->GetArgumentsMutable() = std::move(expr.GetArgumentsMutable());
	window->DistinctMutable() = expr.Distinct();
	// the ordering of an ordered aggregate's input is its own, and the order the match was found
	// in is no substitute for it
	if (expr.OrderByMutable()) {
		window->ArgOrdersMutable() = std::move(expr.OrderByMutable()->orders);
	}
	// an empty match covers no rows, so the row carrying it must not reach the aggregate. Dropping
	// the rows a variable did not match is not the same as passing them as NULL: an aggregate that
	// keeps NULLs would see them.
	unique_ptr<ParsedExpression> in_match =
	    make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, StateField("is_empty"));
	if (!scope.empty()) {
		auto classified = symbols.find(*scope.begin());
		D_ASSERT(classified != symbols.end());
		in_match = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(in_match),
		                                            ClassifierMatches(state, classified->second));
	}
	if (expr.FilterMutable()) {
		window->FilterMutable() = make_uniq<ConjunctionExpression>(
		    ExpressionType::CONJUNCTION_AND, std::move(expr.FilterMutable()), std::move(in_match));
	} else {
		window->FilterMutable() = std::move(in_match);
	}
	ScopeToMatch(context, state, *window, config, running);

	// the aggregate reads the rows of the match through the frame above, so what it reads is not
	// also masked one value at a time
	const auto saved = scoped;
	scoped = true;
	auto result = BindWindowExpression(*window, depth);
	scoped = saved;
	return result;
}

unique_ptr<ParsedExpression> MatchRecognizeMeasureBinder::StateField(const string &field) {
	return CreateStructExtract(make_uniq<ColumnRefExpression>(Identifier(state)), Identifier(field));
}

BindResult MatchRecognizeMeasureBinder::BindGenerated(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
                                                      bool root_expression) {
	const auto saved = scoped;
	scoped = true;
	auto result = BindExpression(expr_ptr, depth, root_expression);
	scoped = saved;
	return result;
}

BindResult MatchRecognizeMeasureBinder::BindNavigation(FunctionExpression &function, const string &function_name,
                                                       unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
                                                       bool root_expression) {
	idx_t offset = 0;
	if (function.GetArguments().size() == 2) {
		offset = MatchRecognizeNavigationOffset(function_name, function.GetArguments()[1].GetExpression());
	}
	auto inner = std::move(function.GetArgumentsMutable()[0].GetExpressionMutable());
	vector<string> symbol;
	if (inner->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = inner->Cast<ColumnRefExpression>();
		auto &names = colref.ColumnNames();
		auto entry = names.size() >= 2 ? symbols.find(names[0].GetIdentifierName()) : symbols.end();
		if (entry != symbols.end()) {
			symbol = entry->second;
			inner = MatchRecognizeWithoutQualifier(colref);
		}
	}
	// what is navigated is an expression of the clause's own, so it is read the way one is
	auto packed = PackValue(state, std::move(inner));
	auto masked = symbol.empty() ? std::move(packed) : ClassifiedValue(state, symbol, std::move(packed));
	expr_ptr = MatchScopedValue(context, state, config, std::move(masked), running, function_name == "FIRST", offset);
	return BindGenerated(expr_ptr, depth, root_expression);
}

} // namespace duckdb
