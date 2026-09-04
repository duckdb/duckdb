
#include "duckdb/function/match_recognize.hpp"

#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/pattern_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/match_recognize_ref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

//! main's SelectNode and SelectStatement only default construct, so the members are filled in here
static unique_ptr<SelectNode> MakeSelectNode(unique_ptr<TableRef> from_table) {
	auto node = make_uniq<SelectNode>();
	node->from_table = std::move(from_table);
	return node;
}

static unique_ptr<SelectStatement> MakeSelectStatement(unique_ptr<QueryNode> node) {
	auto statement = make_uniq<SelectStatement>();
	statement->node = std::move(node);
	return statement;
}

BindResult ExpressionBinder::BindPatternExpression(unique_ptr<ParsedExpression> &expr, idx_t depth) {
	switch (expr->GetExpressionType()) {
	case ExpressionType::ALTERNATION: {
		auto &alternation = expr->Cast<AlternationExpression>();
		auto bound_left = BindExpression(alternation.child_left, depth);
		if (bound_left.HasError()) {
			return BindResult(bound_left.error);
		}
		auto bound_right = BindExpression(alternation.child_right, depth);
		if (bound_right.HasError()) {
			return BindResult(bound_right.error);
		}
		return BindResult(make_uniq_base<Expression, BoundAlternationExpression>(std::move(bound_left.expression),
		                                                                         std::move(bound_right.expression)));
	}
	case ExpressionType::CONCATENATION: {
		auto &concatenation = expr->Cast<ConcatenationExpression>();
		vector<unique_ptr<Expression>> bound_children;
		for (auto &child : concatenation.children) {
			auto child_bind_result = BindExpression(child, depth);
			if (child_bind_result.HasError()) {
				return BindResult(child_bind_result.error);
			}
			bound_children.push_back(std::move(child_bind_result.expression));
		}
		return BindResult(make_uniq_base<Expression, BoundConcatenationExpression>(std::move(bound_children)));
	}
	case ExpressionType::QUANTIFIER: {
		auto &quantifier = expr->Cast<QuantifiedExpression>();
		auto bound_child = BindExpression(quantifier.child, depth);
		if (bound_child.HasError()) {
			return BindResult(bound_child.error);
		}
		return BindResult(make_uniq_base<Expression, BoundQuantifierExpression>(
		    std::move(bound_child.expression), quantifier.min_count, quantifier.max_count, quantifier.excluded));
	}
	default:
		throw NotImplementedException("Unimplemented pattern expression %s",
		                              ExpressionTypeToString(expr->GetExpressionType()));
	}
}

//! Inside a DEFINE, naming another pattern variable means its value on the last row matched to it
//! so far. That is what LAST() means, so the reference becomes one and is resolved by the same
//! machinery. A reference already inside a navigation function is left alone.
static void NavigateOtherSymbols(unique_ptr<ParsedExpression> &expr, const string &define_name,
                                 const case_insensitive_set_t &symbols) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto name = StringUtil::Upper(expr->Cast<FunctionExpression>().FunctionName().GetIdentifierName());
		if (name == "FIRST" || name == "LAST") {
			return;
		}
	}
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = expr->Cast<ColumnRefExpression>();
		auto &names = colref.ColumnNames();
		if (names.size() == 2 && !StringUtil::CIEquals(names[0].GetIdentifierName(), define_name) &&
		    symbols.find(names[0].GetIdentifierName()) != symbols.end()) {
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(std::move(expr));
			expr = make_uniq<FunctionExpression>("LAST", std::move(children));
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { NavigateOtherSymbols(child, define_name, symbols); });
}

static void CheckAndZapQualifiers(ParsedExpression &root_expr, const string &define_name) {
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(root_expr, [&](ColumnRefExpression &colref) {
		if (colref.IsQualified() && colref.ColumnNames()[0] != define_name) {
			throw BinderException("\"%s\" in the definition of %s is not a pattern variable or a column of the input",
			                      colref.ColumnNames()[0].GetIdentifierName(), define_name);
		}
		colref.ColumnNamesMutable() = {colref.GetColumnName()};
	});
}

//! CLASSIFIER() reads as the symbol being defined only because the row being tested is the one the
//! condition decides on. Under navigation it names another row, whose symbol is state the matcher holds
//! while it assembles the match and not anything the plan below it can produce.
static void CheckNavigatedClassifier(const ParsedExpression &expr, bool navigated) {
	if (expr.GetExpressionType() == ExpressionType::FUNCTION) {
		auto &function = expr.Cast<FunctionExpression>();
		auto function_name = StringUtil::Upper(function.FunctionName().GetIdentifierName());
		if (function_name == "CLASSIFIER" && function.GetArguments().empty()) {
			if (navigated) {
				throw NotImplementedException("CLASSIFIER() cannot be navigated in a DEFINE condition");
			}
			return;
		}
		navigated = navigated || function_name == "PREV" || function_name == "NEXT" || function_name == "FIRST" ||
		            function_name == "LAST";
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { CheckNavigatedClassifier(child, navigated); });
}

static void ReplaceFunctions(unique_ptr<ParsedExpression> &expr, const WindowExpression &pattern_window,
                             const string &define_name) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &function = expr->Cast<FunctionExpression>();
		auto function_name = StringUtil::Upper(function.FunctionName().GetIdentifierName());

		string window_function;
		if (function_name == "PREV") {
			window_function = "lag";
		} else if (function_name == "NEXT") {
			window_function = "lead";

		} else if (function_name == "CLASSIFIER" && function.GetArguments().empty()) {
			// the row being tested is the one this DEFINE is deciding on, so it classifies as this symbol
			expr = make_uniq<ConstantExpression>(Value(define_name));
			return;
		}

		if (!window_function.empty()) {
			auto new_expr =
			    pattern_window.Copy(); // we copy here because we need to keep all the partitioning and stuff
			auto &new_window = new_expr->Cast<WindowExpression>();
			new_window.SetFunctionName(window_function);
			new_window.GetArgumentsMutable() = std::move(function.GetArgumentsMutable());
			expr = std::move(new_expr);
		}
		// we do nothing if it's something else
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ReplaceFunctions(child, pattern_window, define_name); });
}

//! DEFINE navigation turns into window functions, which cannot be nested inside the pattern window.
//! Materialise them in the subquery below it and reference the result instead.
static void HoistWindows(unique_ptr<ParsedExpression> &expr, SelectNode &subquery, idx_t &counter) {
	if (expr->GetExpressionClass() == ExpressionClass::WINDOW) {
		auto alias = "__mr_win_" + to_string(counter++);
		expr->SetAlias(Identifier(alias));
		auto colref = make_uniq<ColumnRefExpression>(Identifier(alias));
		subquery.select_list.push_back(std::move(expr));
		expr = std::move(colref);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { HoistWindows(child, subquery, counter); });
}

//! Pattern symbols live in the same namespace as the input columns, so they are qualified with an
//! internal prefix to keep a DEFINE from resolving to a base table column of the same name.
static string DefineColumnName(const string &symbol) {
	return MATCH_RECOGNIZE_DEFINE_PREFIX + symbol;
}

//! A FIRST()/LAST() call in a DEFINE condition. These navigate the rows of the match being assembled,
//! so the matcher resolves them per row rather than the plan computing them up front.
struct MatchRecognizeNavigation {
	bool last;
	//! The pattern variable navigated, empty for the match as a whole
	string symbol;
	//! The subquery column holding the navigated expression
	string column;
	idx_t offset;
};

static bool ContainsNavigation(const ParsedExpression &expr) {
	bool found = false;
	ParsedExpressionIterator::VisitExpression<FunctionExpression>(expr, [&](const FunctionExpression &function) {
		auto name = StringUtil::Upper(function.FunctionName().GetIdentifierName());
		found = found || name == "FIRST" || name == "LAST";
	});
	return found;
}

//! Replace FIRST()/LAST() with a column the matcher fills in, and record what it has to navigate
static void ExtractNavigation(unique_ptr<ParsedExpression> &expr, SelectNode &subquery,
                              const case_insensitive_set_t &symbols, vector<MatchRecognizeNavigation> &navigations) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &function = expr->Cast<FunctionExpression>();
		auto name = StringUtil::Upper(function.FunctionName().GetIdentifierName());
		if (name == "FIRST" || name == "LAST") {
			auto &args = function.GetArgumentsMutable();
			if (args.empty() || args.size() > 2) {
				throw BinderException("%s() takes an expression and an optional offset", name);
			}
			idx_t offset = 0;
			if (args.size() == 2) {
				auto &offset_expr = *args[1].GetExpressionMutable();
				if (offset_expr.GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
					throw BinderException("The offset of %s() must be a constant", name);
				}
				auto offset_value = offset_expr.Cast<ConstantExpression>().GetValue();
				if (!offset_value.DefaultTryCastAs(LogicalType::UBIGINT)) {
					throw BinderException("The offset of %s() must be a non-negative integer", name);
				}
				offset = NumericCast<idx_t>(offset_value.GetValue<uint64_t>());
			}
			auto inner = std::move(args[0].GetExpressionMutable());
			if (ContainsNavigation(*inner)) {
				throw BinderException("Nested row pattern navigation is not supported");
			}

			string symbol;
			if (inner->GetExpressionType() == ExpressionType::COLUMN_REF) {
				auto &colref = inner->Cast<ColumnRefExpression>();
				auto &names = colref.ColumnNames();
				if (names.size() == 2 && symbols.find(names[0].GetIdentifierName()) != symbols.end()) {
					symbol = DefineColumnName(names[0].GetIdentifierName());
					inner = make_uniq<ColumnRefExpression>(colref.GetColumnName());
				}
			}

			auto column = "__mr_nav_" + to_string(navigations.size());
			inner->SetAlias(Identifier(column));
			subquery.select_list.push_back(std::move(inner));
			navigations.push_back(MatchRecognizeNavigation {name == "LAST", symbol, column, offset});
			expr = make_uniq<ColumnRefExpression>(Identifier(column));
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ExtractNavigation(child, subquery, symbols, navigations); });
}

//! Pattern leaves only have to carry the symbol they name; there is no column behind them
//! Whether any part of the pattern sits inside a {- -}
static bool HasExclusion(const ParsedExpression &expr) {
	if (expr.GetExpressionType() == ExpressionType::QUANTIFIER && expr.Cast<QuantifiedExpression>().excluded) {
		return true;
	}
	bool found = false;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { found = found || HasExclusion(child); });
	return found;
}

static void PatternSymbolsToConstants(unique_ptr<ParsedExpression> &expr) {
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = expr->Cast<ColumnRefExpression>();
		expr = make_uniq<ConstantExpression>(Value(colref.GetColumnName().GetIdentifierName()));
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { PatternSymbolsToConstants(child); });
}

//! MATCH_NUMBER() becomes a column the matcher rewrites for every match it attempts
static void ReplaceMatchNumber(unique_ptr<ParsedExpression> &expr) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &function = expr->Cast<FunctionExpression>();
		if (StringUtil::Upper(function.FunctionName().GetIdentifierName()) == "MATCH_NUMBER" &&
		    function.GetArguments().empty()) {
			expr = make_uniq<ColumnRefExpression>(Identifier(MATCH_RECOGNIZE_MATCH_NUMBER_COLUMN));
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ReplaceMatchNumber(child); });
}

static unique_ptr<ParsedExpression> CreateStructExtract(unique_ptr<ParsedExpression> value, const string &child_name) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(value));
	children.push_back(make_uniq<ConstantExpression>(child_name));
	return make_uniq<FunctionExpression>("struct_extract", std::move(children));
}

static unique_ptr<ParsedExpression> CreateStructExtract(const string &column_name, const string &child_name) {
	return CreateStructExtract(make_uniq<ColumnRefExpression>(Identifier(column_name)), child_name);
}

//! The field a value travels in while it is carried to the row that reports it
constexpr const char *MATCH_RECOGNIZE_VALUE_FIELD = "v";

//! struct_pack(v := <value>) is never NULL, so a NULL the value itself holds stays apart from the NULL
//! that masks a row the variable did not match - which is the NULL that MatchScopedValue walks back over
static unique_ptr<ParsedExpression> PackValue(unique_ptr<ParsedExpression> value) {
	value->SetAlias(Identifier(MATCH_RECOGNIZE_VALUE_FIELD));
	vector<unique_ptr<ParsedExpression>> fields;
	fields.push_back(std::move(value));
	return make_uniq<FunctionExpression>("struct_pack", std::move(fields));
}

//! CASE WHEN <classifier> IN (symbols) THEN <column> END - NULL on every row none of them matched.
//! A plain pattern variable stands for itself; a SUBSET name stands for all of its members.
static unique_ptr<ParsedExpression> ClassifiedValue(const vector<string> &symbols, unique_ptr<ParsedExpression> value) {
	D_ASSERT(!symbols.empty());
	vector<unique_ptr<ParsedExpression>> in_children;
	in_children.push_back(CreateStructExtract("__pattern_window", "classifier"));
	for (auto &symbol : symbols) {
		in_children.push_back(make_uniq<ConstantExpression>(Value(symbol)));
	}
	auto matches_symbol = make_uniq<OperatorExpression>(ExpressionType::COMPARE_IN, std::move(in_children));
	auto result = make_uniq<CaseExpression>();
	CaseCheck check;
	check.when_expr = std::move(matches_symbol);
	check.then_expr = std::move(value);
	result->CaseChecksMutable().push_back(std::move(check));
	result->ElseMutable() = make_uniq<ConstantExpression>(Value());
	return std::move(result);
}

//! A reference to <symbol>.<column> resolves to that column on the last row the variable matched.
//! ONE ROW PER MATCH reports a finished match, so it sees the whole match (FINAL semantics); ALL ROWS
//! PER MATCH reports progress, so it only sees the match up to the current row (RUNNING semantics).
static void ScopeToMatch(WindowExpression &window, const MatchRecognizeConfig &config, bool running) {
	window.WindowStartMutable() = WindowBoundary::UNBOUNDED_PRECEDING;
	window.WindowEndMutable() = running ? WindowBoundary::CURRENT_ROW_ROWS : WindowBoundary::UNBOUNDED_FOLLOWING;

	// matches are numbered within a partition, so both are needed to identify one
	for (auto &expr : config.partition_expressions) {
		window.PartitionsMutable().push_back(expr->Copy());
	}
	window.PartitionsMutable().push_back(CreateStructExtract("__pattern_window", "match_number"));
	for (auto &order : config.order_by_expressions) {
		window.OrderByMutable().emplace_back(order.type, order.null_order, order.expression->Copy());
	}
}

//! Takes a value packed by PackValue and reports it from the first or last row of the match that carries
//! one. IGNORE NULLS is what makes it that row rather than the first or last row of the match.
static unique_ptr<ParsedExpression> MatchScopedValue(const MatchRecognizeConfig &config,
                                                     unique_ptr<ParsedExpression> packed, bool running,
                                                     bool first = false) {
	auto window = make_uniq<WindowExpression>("", "", first ? "first_value" : "last_value");
	window->GetArgumentsMutable().emplace_back(std::move(packed));
	window->HasIgnoreNullsMutable() = true;
	window->IgnoreNullsMutable() = true;
	ScopeToMatch(*window, config, running);
	return CreateStructExtract(std::move(window), MATCH_RECOGNIZE_VALUE_FIELD);
}

//! Rewrite a MEASURES expression into something evaluable next to the pattern window
static void RewriteMeasure(Binder &binder, unique_ptr<ParsedExpression> &expr, const MatchRecognizeConfig &config,
                           const case_insensitive_map_t<vector<string>> &symbols, bool running, bool one_row,
                           bool inside_aggregate = false) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &function = expr->Cast<FunctionExpression>();
		auto function_name = StringUtil::Upper(function.FunctionName().GetIdentifierName());
		// RUNNING and FINAL choose how much of the match the measure below them sees
		const auto is_running = function.FunctionName() == MATCH_RECOGNIZE_RUNNING_MARKER;
		if (is_running || function.FunctionName() == MATCH_RECOGNIZE_FINAL_MARKER) {
			// ONE ROW PER MATCH reports a finished match, so its current row is the last one: the two
			// are the same thing there and the keywords make no difference
			expr = std::move(function.GetArgumentsMutable()[0].GetExpressionMutable());
			RewriteMeasure(binder, expr, config, symbols, one_row ? false : is_running, one_row, inside_aggregate);
			return;
		}
		if (function_name == "CLASSIFIER" && function.GetArguments().empty()) {
			expr = CreateStructExtract("__pattern_window", "classifier");
			return;
		}
		if (function_name == "MATCH_NUMBER" && function.GetArguments().empty()) {
			expr = CreateStructExtract("__pattern_window", "match_number");
			return;
		}
		// logical navigation over the rows of the match. LAST(X.c) is what an unadorned X.c already
		// means, so both share the masking; only the end they read from differs.
		if ((function_name == "FIRST" || function_name == "LAST") && function.GetArguments().size() == 1) {
			auto inner = std::move(function.GetArgumentsMutable()[0].GetExpressionMutable());
			vector<string> symbol;
			if (inner->GetExpressionType() == ExpressionType::COLUMN_REF) {
				auto &colref = inner->Cast<ColumnRefExpression>();
				auto &names = colref.ColumnNames();
				auto entry = names.size() == 2 ? symbols.find(names[0].GetIdentifierName()) : symbols.end();
				if (entry != symbols.end()) {
					symbol = entry->second;
					inner = make_uniq<ColumnRefExpression>(colref.GetColumnName());
				}
			}
			RewriteMeasure(binder, inner, config, symbols, running, one_row, inside_aggregate);
			auto packed = PackValue(std::move(inner));
			auto masked = symbol.empty() ? std::move(packed) : ClassifiedValue(symbol, std::move(packed));
			expr = MatchScopedValue(config, std::move(masked), running, function_name == "FIRST");
			return;
		}
		// an aggregate in MEASURES aggregates the rows of the match
		EntryLookupInfo lookup(CatalogType::AGGREGATE_FUNCTION_ENTRY, QualifiedName(function.FunctionName()));
		auto entry = binder.GetCatalogEntry(function.GetQualifiedName().Catalog(), function.GetQualifiedName().Schema(),
		                                    lookup, OnEntryNotFound::RETURN_NULL);
		if (entry && entry->type == CatalogType::AGGREGATE_FUNCTION_ENTRY) {
			// the aggregate already spans the match, so inside it a variable only masks its rows
			for (auto &argument : function.GetArgumentsMutable()) {
				RewriteMeasure(binder, argument.GetExpressionMutable(), config, symbols, running, one_row, true);
			}
			auto &qualified = function.GetQualifiedName();
			auto window = make_uniq<WindowExpression>(qualified.Catalog().GetIdentifierName(),
			                                          qualified.Schema().GetIdentifierName(),
			                                          qualified.Name().GetIdentifierName());
			window->GetArgumentsMutable() = std::move(function.GetArgumentsMutable());
			window->DistinctMutable() = function.Distinct();
			// an empty match covers no rows, so the row carrying it must not reach the aggregate
			auto in_match = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT,
			                                              CreateStructExtract("__pattern_window", "is_empty"));
			if (function.FilterMutable()) {
				window->FilterMutable() = make_uniq<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(function.FilterMutable()), std::move(in_match));
			} else {
				window->FilterMutable() = std::move(in_match);
			}
			ScopeToMatch(*window, config, running);
			expr = std::move(window);
			return;
		}
	}
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = expr->Cast<ColumnRefExpression>();
		auto &names = colref.ColumnNames();
		auto entry = names.size() == 2 ? symbols.find(names[0].GetIdentifierName()) : symbols.end();
		if (entry != symbols.end()) {
			// a known pattern variable scopes the column to the rows it matched
			auto column = make_uniq<ColumnRefExpression>(colref.GetColumnName());
			if (inside_aggregate) {
				// the aggregate spans the match and skips the rows the variable did not match, which is
				// exactly what the masking NULL means there
				expr = ClassifiedValue(entry->second, std::move(column));
				return;
			}
			expr = MatchScopedValue(config, ClassifiedValue(entry->second, PackValue(std::move(column))), running);
			return;
		}
		if (colref.IsQualified()) {
			// the input is reached through a subquery here, so a table qualifier no longer resolves
			colref.ColumnNamesMutable() = {colref.GetColumnName()};
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		RewriteMeasure(binder, child, config, symbols, running, one_row, inside_aggregate);
	});
}

BoundStatement Binder::Bind(MatchRecognizeRef &ref) {
	// MATCH_RECOGNIZE is planned as a stack of select nodes:
	//   1. the input, plus one boolean column per DEFINE
	//   2. the pattern window, which classifies every row of every match; rows outside a match are
	//      dropped here
	//   3. the MEASURES, computed across the match a row belongs to
	//   4. for ONE ROW PER MATCH, a filter down to the row each match starts on

	auto select_node = MakeSelectNode(std::move(ref.input));
	select_node->select_list.push_back(make_uniq<StarExpression>());

	// Pattern Matching Window: placeholder window expression
	auto pattern_window = make_uniq<WindowExpression>("", "", "match_recognize");

	pattern_window->WindowStartMutable() = WindowBoundary::UNBOUNDED_PRECEDING;
	pattern_window->WindowEndMutable() = WindowBoundary::UNBOUNDED_FOLLOWING;

	// copy partitions to bind them twice in different places
	vector<unique_ptr<ParsedExpression>> partitions;
	for (auto &expr : ref.config->partition_expressions) {
		partitions.push_back(expr->Copy());
	}
	pattern_window->PartitionsMutable() = std::move(partitions);
	pattern_window->OrderByMutable() = std::move(ref.config->order_by_expressions);

	// {- -} only decides which of a match's rows reach the output, so it needs rows in the output to
	// act on. ONE ROW PER MATCH reports the match rather than its rows, leaving it nothing to do.
	const bool has_exclusion = HasExclusion(*ref.config->pattern);
	if (has_exclusion && ref.config->rows_per_match != MatchRecognizeRows::MATCH_RECOGNIZE_ROWS_ALL) {
		throw BinderException("Pattern exclusion syntax {- -} requires ALL ROWS PER MATCH");
	}

	// a union variable only stands for a set of rows after the match is assembled, so it is confined
	// to MEASURES: the matcher works one symbol at a time and cannot yet navigate or skip to a union
	case_insensitive_set_t subset_names;
	for (auto &subset : ref.config->subsets) {
		subset_names.insert(subset.name);
	}
	if (!subset_names.empty()) {
		if (subset_names.count(ref.config->after_match_variable)) {
			throw NotImplementedException("AFTER MATCH SKIP TO a SUBSET variable is not supported yet");
		}
		for (auto &expr : ref.config->defines_expression_list) {
			ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(
			    *expr, [&](const ColumnRefExpression &colref) {
				    if (colref.IsQualified() && subset_names.count(colref.ColumnNames()[0].GetIdentifierName())) {
					    throw NotImplementedException("A SUBSET variable cannot be referenced in DEFINE yet");
				    }
			    });
		}
	}

	// another select node
	// all the inputs for the defines go in their own select node

	auto define_select_node = MakeSelectNode(std::move(select_node->from_table));

	vector<unique_ptr<WindowExpression>> child_windows;
	define_select_node->select_list.push_back(make_uniq<StarExpression>());

	// we use this window function as a template for order, partition, and boundaries
	D_ASSERT(pattern_window->GetArguments().empty()); // for now
	auto window_template = pattern_window->Copy();

	// case_insensitive_set_t define_names;

	vector<string> hidden_columns;
	case_insensitive_set_t pattern_symbols;
	vector<string> define_symbols;
	vector<unique_ptr<ParsedExpression>> define_conditions;
	idx_t nav_counter = 0;

	// MATCH_NUMBER() reads this column; the matcher rewrites it per match
	auto match_number_column = make_uniq<ConstantExpression>(Value::UBIGINT(0));
	match_number_column->SetAlias(MATCH_RECOGNIZE_MATCH_NUMBER_COLUMN);
	define_select_node->select_list.push_back(std::move(match_number_column));
	hidden_columns.emplace_back(MATCH_RECOGNIZE_MATCH_NUMBER_COLUMN);

	vector<MatchRecognizeNavigation> navigations;
	case_insensitive_set_t declared_symbols;
	for (auto &expr : ref.config->defines_expression_list) {
		declared_symbols.insert(expr->GetAlias().GetIdentifierName());
	}

	for (auto &expr : ref.config->defines_expression_list) {
		auto define_name = expr->GetAlias().GetIdentifierName();
		D_ASSERT(!define_name.empty());
		if (pattern_symbols.find(define_name) != pattern_symbols.end()) {
			// a symbol stands for one condition, so a second one for the same symbol has nowhere to go
			throw BinderException("MATCH_RECOGNIZE defines pattern variable \"%s\" more than once", define_name);
		}

		CheckNavigatedClassifier(*expr, false);
		// a reference to another variable is navigation over that variable's rows, so it has to
		// become one before the navigation is pulled out
		NavigateOtherSymbols(expr, define_name, declared_symbols);
		// logical navigation is resolved by the matcher, so it leaves before qualifiers are checked
		ExtractNavigation(expr, *define_select_node, declared_symbols, navigations);
		CheckAndZapQualifiers(*expr, define_name);
		ReplaceFunctions(expr, window_template->Cast<WindowExpression>(), define_name);
		HoistWindows(expr, *define_select_node, nav_counter);
		ReplaceMatchNumber(expr);

		pattern_symbols.insert(define_name);
		define_symbols.push_back(DefineColumnName(define_name));
		define_conditions.push_back(std::move(expr));
	}

	// rewrite the pattern symbols to the internal names the matcher reports
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(
	    *ref.config->pattern, [&](ColumnRefExpression &colref) {
		    D_ASSERT(colref.ColumnNames().size() == 1);
		    colref.ColumnNamesMutable() = {Identifier(DefineColumnName(colref.GetColumnName().GetIdentifierName()))};
	    });

	// a symbol used by the pattern but never defined matches every row
	ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(
	    *ref.config->pattern, [&](const ColumnRefExpression &colref) {
		    auto column_name = colref.ColumnNames()[0].GetIdentifierName();
		    for (auto &existing : define_symbols) {
			    if (StringUtil::CIEquals(existing, column_name)) {
				    return;
			    }
		    }
		    define_symbols.push_back(column_name);
		    if (ref.config->define_auto) {
			    // the variable stands for the column of the same name being true, or non zero, or
			    // whatever else that column's type calls true. The symbol carries the internal
			    // prefix by now, so the column it names is what is left after it.
			    auto source = column_name.substr(strlen(MATCH_RECOGNIZE_DEFINE_PREFIX));
			    define_conditions.push_back(make_uniq<CastExpression>(
			        LogicalType::BOOLEAN, make_uniq<ColumnRefExpression>(Identifier(source))));
		    } else {
			    define_conditions.push_back(make_uniq<ConstantExpression>(Value::BOOLEAN(true)));
		    }
		    pattern_symbols.insert(MatchRecognizeSymbolName(column_name));
	    });

	// a measure may name a pattern variable or a SUBSET of them; both resolve to a set of symbols
	case_insensitive_map_t<vector<string>> measure_symbols;
	for (auto &symbol : pattern_symbols) {
		measure_symbols[symbol] = {symbol};
	}
	for (auto &subset : ref.config->subsets) {
		if (measure_symbols.find(subset.name) != measure_symbols.end()) {
			throw BinderException("SUBSET name \"%s\" is already a pattern variable", subset.name);
		}
		vector<string> members;
		for (auto &member : subset.members) {
			auto entry = measure_symbols.find(member);
			if (entry == measure_symbols.end() || entry->second.size() != 1) {
				throw BinderException("SUBSET \"%s\" refers to unknown pattern variable \"%s\"", subset.name, member);
			}
			members.push_back(entry->second[0]);
		}
		measure_symbols[subset.name] = std::move(members);
	}

	// the matcher only needs the symbol a leaf names, and there is no longer a column to bind it to
	PatternSymbolsToConstants(ref.config->pattern);

	// the columns the conditions read have to reach the matcher, so they are passed as arguments
	vector<unique_ptr<ParsedExpression>> condition_columns;
	case_insensitive_set_t seen_columns;
	seen_columns.insert(MATCH_RECOGNIZE_MATCH_NUMBER_COLUMN);
	for (auto &condition : define_conditions) {
		ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(
		    *condition, [&](const ColumnRefExpression &colref) {
			    auto column_name = colref.GetColumnName().GetIdentifierName();
			    if (seen_columns.insert(column_name).second) {
				    condition_columns.push_back(make_uniq<ColumnRefExpression>(Identifier(column_name)));
			    }
		    });
	}

	// Argument layout: the columns the conditions read are packed into one struct so that they are
	// materialised for the matcher, and the conditions into another so that they are bound but never
	// evaluated - the bind callback unpacks them into the function data.
	auto &arguments = pattern_window->GetArgumentsMutable();

	vector<unique_ptr<ParsedExpression>> column_fields;
	case_insensitive_map_t<idx_t> column_field_index;
	column_field_index[MATCH_RECOGNIZE_MATCH_NUMBER_COLUMN] = 0;
	column_fields.push_back(make_uniq<ColumnRefExpression>(Identifier(MATCH_RECOGNIZE_MATCH_NUMBER_COLUMN)));
	for (auto &column : condition_columns) {
		column_field_index[column->Cast<ColumnRefExpression>().GetColumnName().GetIdentifierName()] =
		    column_fields.size();
		column_fields.push_back(std::move(column));
	}
	arguments.emplace_back(make_uniq<FunctionExpression>("struct_pack", std::move(column_fields)));

	vector<unique_ptr<ParsedExpression>> condition_fields;
	for (idx_t i = 0; i < define_conditions.size(); i++) {
		define_conditions[i]->SetAlias(Identifier("c" + to_string(i)));
		condition_fields.push_back(std::move(define_conditions[i]));
	}
	arguments.emplace_back(make_uniq<FunctionExpression>("struct_pack", std::move(condition_fields)));

	arguments.emplace_back(std::move(ref.config->pattern));

	vector<Value> symbol_values;
	for (auto &symbol : define_symbols) {
		symbol_values.emplace_back(symbol);
	}
	arguments.emplace_back(make_uniq<ConstantExpression>(Value::LIST(LogicalType::VARCHAR, std::move(symbol_values))));

	auto skip_variable = Value(LogicalType::VARCHAR);
	if (!ref.config->after_match_variable.empty()) {
		skip_variable = Value(DefineColumnName(ref.config->after_match_variable));
	}
	arguments.emplace_back(make_uniq<ConstantExpression>(std::move(skip_variable)));
	arguments.emplace_back(
	    make_uniq<ConstantExpression>(Value::UTINYINT(static_cast<uint8_t>(ref.config->after_match))));

	child_list_t<LogicalType> navigation_type {{"last", LogicalType::BOOLEAN},
	                                           {"symbol", LogicalType::VARCHAR},
	                                           {"field", LogicalType::UBIGINT},
	                                           {"offset", LogicalType::UBIGINT}};
	vector<Value> navigation_values;
	for (auto &navigation : navigations) {
		auto entry = column_field_index.find(navigation.column);
		D_ASSERT(entry != column_field_index.end());
		navigation_values.push_back(Value::STRUCT(LogicalType::STRUCT(navigation_type),
		                                          {Value::BOOLEAN(navigation.last), Value(navigation.symbol),
		                                           Value::UBIGINT(entry->second), Value::UBIGINT(navigation.offset)}));
	}
	arguments.emplace_back(
	    make_uniq<ConstantExpression>(Value::LIST(LogicalType::STRUCT(navigation_type), std::move(navigation_values))));

	for (auto &navigation : navigations) {
		hidden_columns.push_back(navigation.column);
	}

	for (idx_t nav_idx = 0; nav_idx < nav_counter; nav_idx++) {
		hidden_columns.push_back("__mr_win_" + to_string(nav_idx));
	}

	auto define_select = MakeSelectStatement(std::move(define_select_node));
	select_node->from_table = make_uniq<SubqueryRef>(std::move(define_select));
	pattern_window->SetAlias("__pattern_window_spans");
	select_node->select_list.push_back(std::move(pattern_window));

	// The window reports every match a row takes part in, so overlapping matches each get their own
	// row here. Unnesting also drops the rows that matched nothing, since their list is empty.
	auto spans_select = MakeSelectStatement(std::move(select_node));
	auto unnest_node = MakeSelectNode(make_uniq<SubqueryRef>(std::move(spans_select)));
	auto spans_star = make_uniq<StarExpression>();
	spans_star->ExcludeListMutable().insert(QualifiedColumnName(Identifier("__pattern_window_spans")));
	unnest_node->select_list.push_back(std::move(spans_star));

	vector<unique_ptr<ParsedExpression>> spans_argument;
	spans_argument.push_back(make_uniq<ColumnRefExpression>(Identifier("__pattern_window_spans")));
	auto unnest_spans = make_uniq<FunctionExpression>("unnest", std::move(spans_argument));

	unnest_spans->SetAlias("__pattern_window");
	unnest_node->select_list.push_back(std::move(unnest_spans));
	select_node = std::move(unnest_node);

	// MEASURES are projected on top of the pattern window, where the match a row belongs to is known
	const auto all_rows = ref.config->rows_per_match == MatchRecognizeRows::MATCH_RECOGNIZE_ROWS_ALL;
	auto pattern_select = MakeSelectStatement(std::move(select_node));
	auto measures_node = MakeSelectNode(make_uniq<SubqueryRef>(std::move(pattern_select)));

	// the DEFINE columns are an implementation detail, so they do not reach the output
	auto star = make_uniq<StarExpression>();
	for (auto &entry : hidden_columns) {
		star->ExcludeListMutable().insert(QualifiedColumnName(Identifier(entry)));
	}
	measures_node->select_list.push_back(std::move(star));

	vector<Identifier> measure_aliases;
	for (auto &expr : ref.config->measures_expression_list) {
		D_ASSERT(!expr->GetAlias().empty());
		measure_aliases.push_back(expr->GetAlias());
		// rewriting can replace the expression wholesale, which would drop the MEASURES alias
		auto alias = expr->GetAlias();
		RewriteMeasure(*this, expr, *ref.config, measure_symbols, all_rows, !all_rows);
		expr->SetAlias(std::move(alias));
		measures_node->select_list.push_back(std::move(expr));
	}

	select_node = std::move(measures_node);

	// ONE ROW PER MATCH reports one row per match, and reports the match rather than any of its rows:
	// the output is the partitioning followed by the measures. Filtering has to happen above the
	// measures rather than beside them, because they are computed across the whole match.
	// an excluded row still belongs to the match, so it is dropped above the measures rather than
	// before them: the aggregates over the match have to have seen it
	if (all_rows && has_exclusion) {
		auto measures_select = MakeSelectStatement(std::move(select_node));
		auto filter_node = MakeSelectNode(make_uniq<SubqueryRef>(std::move(measures_select)));
		filter_node->select_list.push_back(make_uniq<StarExpression>());
		filter_node->where_clause = make_uniq<OperatorExpression>(
		    ExpressionType::OPERATOR_NOT, CreateStructExtract("__pattern_window", "is_excluded"));
		select_node = std::move(filter_node);
	}

	if (!all_rows) {
		// ONE ROW PER MATCH reports the match rather than its rows, so the only columns it can report
		// are the ones identifying the match: what it was partitioned by, and what was measured
		if (ref.config->partition_expressions.empty() && measure_aliases.empty()) {
			throw BinderException(
			    "MATCH_RECOGNIZE with ONE ROW PER MATCH has nothing to return: it reports the match rather "
			    "than its rows, so without MEASURES or PARTITION BY there are no columns. Add a MEASURES "
			    "clause, or use ALL ROWS PER MATCH to report the matched rows themselves.");
		}
		auto measures_select = MakeSelectStatement(std::move(select_node));
		auto filter_node = MakeSelectNode(make_uniq<SubqueryRef>(std::move(measures_select)));
		for (auto &expr : ref.config->partition_expressions) {
			filter_node->select_list.push_back(expr->Copy());
		}
		for (auto &alias : measure_aliases) {
			filter_node->select_list.push_back(make_uniq<ColumnRefExpression>(alias));
		}
		// the last row is the one reported: a bare column or CLASSIFIER() in MEASURES reads it
		// directly, which is the FINAL semantics the standard gives them
		filter_node->where_clause = CreateStructExtract("__pattern_window", "is_match_end");
		select_node = std::move(filter_node);
	}

	auto child_binder = Binder::CreateBinder(context, this);
	auto result = child_binder->Bind(*select_node);
	bind_context.AddGenericBinding(result.plan->GetRootIndex(),
	                               !ref.alias.empty() ? ref.alias : "__match_recognize_table", result.names,
	                               result.types);
	return result;
}

} // namespace duckdb
