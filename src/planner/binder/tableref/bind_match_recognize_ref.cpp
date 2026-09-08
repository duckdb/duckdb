
#include "duckdb/function/match_recognize.hpp"

#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/main/settings.hpp"

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
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
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
		    std::move(bound_child.expression), quantifier.min_count, quantifier.max_count, quantifier.excluded,
		    quantifier.reluctant));
	}
	case ExpressionType::ANCHOR:
		return BindResult(make_uniq_base<Expression, BoundAnchorExpression>(expr->Cast<AnchorExpression>().at_end));
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
		if (names.size() >= 2 && !StringUtil::CIEquals(names[0].GetIdentifierName(), define_name) &&
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

//! The names a qualified reference could be naming a table by. Anything else in front of a column is
//! navigation into that column rather than a table it belongs to.
static void CollectInputTableNames(const TableRef &ref, case_insensitive_set_t &names) {
	if (!ref.alias.empty()) {
		names.insert(ref.alias.GetIdentifierName());
	}
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		names.insert(ref.Cast<BaseTableRef>().Table().GetIdentifierName());
		break;
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		CollectInputTableNames(*join.left, names);
		CollectInputTableNames(*join.right, names);
		break;
	}
	default:
		break;
	}
}

//! Everything a reference names behind its first name is navigation into the column, so dropping a
//! qualifier drops that one name and keeps the rest: X.c.f is field f of column c, not column f.
static void DropQualifier(ColumnRefExpression &colref) {
	auto names = colref.ColumnNames();
	D_ASSERT(names.size() > 1);
	names.erase(names.begin());
	colref.ColumnNamesMutable() = std::move(names);
}

static unique_ptr<ParsedExpression> WithoutQualifier(const ColumnRefExpression &colref) {
	auto copy = colref.Copy();
	DropQualifier(copy->Cast<ColumnRefExpression>());
	return copy;
}

//! A condition is evaluated over a subquery of the input, so a pattern variable in front of a column
//! has to go. A table of the input is gone by then too, but it has been hoisted rather than dropped.
static void ZapDefineQualifier(ParsedExpression &root_expr, const string &define_name,
                               const case_insensitive_set_t &symbols) {
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(root_expr, [&](ColumnRefExpression &colref) {
		if (!colref.IsQualified()) {
			return;
		}
		const auto qualifier = colref.ColumnNames()[0].GetIdentifierName();
		if (!StringUtil::CIEquals(qualifier, define_name) && !symbols.count(qualifier)) {
			return;
		}
		DropQualifier(colref);
	});
}

//! A reference qualified with one of the input's own tables only resolves where the input still is
//! one. Every clause here is evaluated above a subquery of it, so the reference is computed down
//! there and read back under a name of its own. That is also what keeps two tables' columns of the
//! same name apart, where dropping the qualifier would collapse them onto one.
struct HoistedInputRefs {
	explicit HoistedInputRefs(string prefix_p) : prefix(std::move(prefix_p)) {
	}

	//! The subquery column standing for this reference, allocated on first use
	const string &Hoist(const ColumnRefExpression &colref) {
		string key;
		for (auto &name : colref.ColumnNames()) {
			key += name.GetIdentifierName();
			key += ".";
		}
		auto entry = columns.find(key);
		if (entry != columns.end()) {
			return entry->second;
		}
		auto hoisted = colref.Copy();
		auto column = prefix + to_string(columns.size());
		hoisted->SetAlias(Identifier(column));
		select_list.push_back(std::move(hoisted));
		return columns.emplace(std::move(key), std::move(column)).first->second;
	}

	string prefix;
	case_insensitive_map_t<string> columns;
	//! The expressions to add to the subquery the input is reached through
	vector<unique_ptr<ParsedExpression>> select_list;
};

static void HoistInputQualifiers(unique_ptr<ParsedExpression> &expr, const case_insensitive_set_t &input_tables,
                                 HoistedInputRefs &refs) {
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = expr->Cast<ColumnRefExpression>();
		if (colref.IsQualified() && input_tables.count(colref.ColumnNames()[0].GetIdentifierName())) {
			auto alias = expr->GetAlias();
			expr = make_uniq<ColumnRefExpression>(Identifier(refs.Hoist(colref)));
			expr->SetAlias(std::move(alias));
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { HoistInputQualifiers(child, input_tables, refs); });
}

//! FIRST() and LAST() count from the end of the match they read from, by a constant the two clauses
//! spell the same way. A NULL constant casts to a NULL offset rather than failing, so it is turned
//! away here rather than read as a number further down.
static idx_t BindNavigationOffset(const string &function_name, const ParsedExpression &offset_expr) {
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
	return NumericCast<idx_t>(offset_value.GetValue<uint64_t>());
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
static void HoistWindows(unique_ptr<ParsedExpression> &expr, SelectNode &subquery, const string &prefix,
                         idx_t &counter) {
	if (expr->GetExpressionClass() == ExpressionClass::WINDOW) {
		auto alias = prefix + to_string(counter++);
		expr->SetAlias(Identifier(alias));
		auto colref = make_uniq<ColumnRefExpression>(Identifier(alias));
		subquery.select_list.push_back(std::move(expr));
		expr = std::move(colref);
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { HoistWindows(child, subquery, prefix, counter); });
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
	if (expr.GetExpressionType() == ExpressionType::FUNCTION) {
		auto name = StringUtil::Upper(expr.Cast<FunctionExpression>().FunctionName().GetIdentifierName());
		if (name == "FIRST" || name == "LAST") {
			return true;
		}
	}
	bool found = false;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { found = found || ContainsNavigation(child); });
	return found;
}

//! Replace FIRST()/LAST() with a column the matcher fills in, and record what it has to navigate
static void ExtractNavigation(unique_ptr<ParsedExpression> &expr, SelectNode &subquery,
                              const case_insensitive_set_t &symbols, const string &prefix,
                              vector<MatchRecognizeNavigation> &navigations) {
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
				offset = BindNavigationOffset(name, args[1].GetExpression());
			}
			auto inner = std::move(args[0].GetExpressionMutable());
			if (ContainsNavigation(*inner)) {
				throw BinderException("Nested row pattern navigation is not supported");
			}

			string symbol;
			if (inner->GetExpressionType() == ExpressionType::COLUMN_REF) {
				auto &colref = inner->Cast<ColumnRefExpression>();
				auto &names = colref.ColumnNames();
				if (names.size() >= 2 && symbols.find(names[0].GetIdentifierName()) != symbols.end()) {
					symbol = DefineColumnName(names[0].GetIdentifierName());
					inner = WithoutQualifier(colref);
				}
			}

			auto column = prefix + to_string(navigations.size());
			inner->SetAlias(Identifier(column));
			subquery.select_list.push_back(std::move(inner));
			navigations.push_back(MatchRecognizeNavigation {name == "LAST", symbol, column, offset});
			expr = make_uniq<ColumnRefExpression>(Identifier(column));
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		ExtractNavigation(child, subquery, symbols, prefix, navigations);
	});
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
static void ReplaceMatchNumber(unique_ptr<ParsedExpression> &expr, const string &column) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &function = expr->Cast<FunctionExpression>();
		if (StringUtil::Upper(function.FunctionName().GetIdentifierName()) == "MATCH_NUMBER" &&
		    function.GetArguments().empty()) {
			expr = make_uniq<ColumnRefExpression>(Identifier(column));
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ReplaceMatchNumber(child, column); });
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

//! An empty match covers no rows, so nothing an expression reads from a row of the match is there to be
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
	if (type == OrderType::ORDER_DEFAULT) {
		type = Settings::Get<DefaultOrderSetting>(context);
	}
	if (null_order != OrderByNullType::ORDER_DEFAULT) {
		return;
	}
	const auto ascending = type == OrderType::ASCENDING;
	switch (Settings::Get<DefaultNullOrderSetting>(context)) {
	case DefaultOrderByNullType::NULLS_FIRST:
		null_order = OrderByNullType::NULLS_FIRST;
		break;
	case DefaultOrderByNullType::NULLS_LAST_ON_ASC_FIRST_ON_DESC:
		null_order = ascending ? OrderByNullType::NULLS_LAST : OrderByNullType::NULLS_FIRST;
		break;
	case DefaultOrderByNullType::NULLS_FIRST_ON_ASC_LAST_ON_DESC:
		null_order = ascending ? OrderByNullType::NULLS_FIRST : OrderByNullType::NULLS_LAST;
		break;
	default:
		null_order = OrderByNullType::NULLS_LAST;
		break;
	}
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

//! PREV and NEXT navigate the ordered partition rather than the rows of the match, so they do not
//! depend on the match at all. A measure's are computed per input row below the pattern window and
//! read back from above it, which is the route a DEFINE condition's already take.
static void HoistMeasureNavigation(unique_ptr<ParsedExpression> &expr, const WindowExpression &pattern_window,
                                   const case_insensitive_map_t<vector<string>> &symbols, SelectNode &subquery,
                                   const string &prefix, idx_t &counter) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &function = expr->Cast<FunctionExpression>();
		auto function_name = StringUtil::Upper(function.FunctionName().GetIdentifierName());
		if (function_name == "PREV" || function_name == "NEXT") {
			auto &arguments = function.GetArgumentsMutable();
			if (arguments.empty() || arguments.size() > 2) {
				throw BinderException("%s() takes an expression and an optional offset", function_name);
			}
			for (auto &argument : arguments) {
				HoistMeasureNavigation(argument.GetExpressionMutable(), pattern_window, symbols, subquery, prefix,
				                       counter);
			}
			auto &inner = *arguments[0].GetExpressionMutable();
			if (inner.GetExpressionType() == ExpressionType::COLUMN_REF) {
				auto &names = inner.Cast<ColumnRefExpression>().ColumnNames();
				if (names.size() >= 2 && symbols.find(names[0].GetIdentifierName()) != symbols.end()) {
					throw NotImplementedException("%s() navigates the ordered partition rather than the match, so "
					                              "naming a pattern variable inside it is not supported",
					                              function_name);
				}
			}
			auto navigation = pattern_window.Copy();
			auto &window = navigation->Cast<WindowExpression>();
			window.SetFunctionName(function_name == "PREV" ? "lag" : "lead");
			window.GetArgumentsMutable() = std::move(arguments);
			auto column = prefix + to_string(counter++);
			window.SetAlias(Identifier(column));
			subquery.select_list.push_back(std::move(navigation));
			// the navigation may be the whole measure, whose alias names the output column
			auto alias = expr->GetAlias();
			expr = make_uniq<ColumnRefExpression>(Identifier(column));
			expr->SetAlias(std::move(alias));
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		HoistMeasureNavigation(child, pattern_window, symbols, subquery, prefix, counter);
	});
}

static optional_ptr<CatalogEntry> LookupFunction(Binder &binder, const FunctionExpression &function, CatalogType type) {
	EntryLookupInfo lookup(type, QualifiedName(function.FunctionName()));
	auto &qualified = function.GetQualifiedName();
	auto entry = binder.GetCatalogEntry(qualified.Catalog(), qualified.Schema(), lookup, OnEntryNotFound::RETURN_NULL);
	return entry && entry->type == type ? entry : nullptr;
}

//! Counted the way the window binder finds the aggregate it pushes a frame down to: an aggregate is
//! one of them and what it aggregates is not looked into any further.
static idx_t CountAggregates(Binder &binder, const ParsedExpression &expr) {
	if (expr.GetExpressionType() == ExpressionType::FUNCTION &&
	    LookupFunction(binder, expr.Cast<FunctionExpression>(), CatalogType::AGGREGATE_FUNCTION_ENTRY)) {
		return 1;
	}
	idx_t count = 0;
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { count += CountAggregates(binder, child); });
	return count;
}

//! A macro standing for an aggregate aggregates the rows of the match like the aggregate it stands
//! for. The window binder already pushes a window's frame down into a macro body that is one
//! aggregate, so all that is needed here is to recognise which macros those are and let them take
//! the same route - rather than expanding them a second way of our own.
static bool IsAggregateMacro(Binder &binder, const FunctionExpression &function) {
	auto entry = LookupFunction(binder, function, CatalogType::MACRO_ENTRY);
	if (!entry) {
		return false;
	}
	bool aggregate = false;
	for (auto &macro : entry->Cast<ScalarMacroCatalogEntry>().macros) {
		if (macro->type != MacroType::SCALAR_MACRO) {
			return false;
		}
		const auto aggregates = CountAggregates(binder, *macro->Cast<ScalarMacroFunction>().expression);
		if (aggregates > 1) {
			// there is no single aggregate for the match's frame to be pushed down to, which is what
			// the window binder says about the same macro used over any other frame
			throw BinderException("Window function macro bodies must contain exactly one aggregate function");
		}
		// none of them and it is an ordinary scalar macro, which binds where it stands
		aggregate = aggregate || aggregates == 1;
	}
	return aggregate;
}

//! Rewrite a MEASURES expression into something evaluable next to the pattern window
//! An aggregate in MEASURES aggregates the rows of the match that its arguments name a variable for,
//! so rewriting them collects which variable that is.
static void RewriteMeasure(Binder &binder, const string &state, unique_ptr<ParsedExpression> &expr,
                           const MatchRecognizeConfig &config, const case_insensitive_map_t<vector<string>> &symbols,
                           bool running, bool one_row, optional_ptr<case_insensitive_set_t> aggregate_scope = nullptr) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &function = expr->Cast<FunctionExpression>();
		auto function_name = StringUtil::Upper(function.FunctionName().GetIdentifierName());
		// RUNNING and FINAL choose how much of the match the measure below them sees
		const auto is_running = function.FunctionName() == MATCH_RECOGNIZE_RUNNING_MARKER;
		if (is_running || function.FunctionName() == MATCH_RECOGNIZE_FINAL_MARKER) {
			// ONE ROW PER MATCH reports a finished match, so its current row is the last one: the two
			// are the same thing there and the keywords make no difference
			expr = std::move(function.GetArgumentsMutable()[0].GetExpressionMutable());
			RewriteMeasure(binder, state, expr, config, symbols, one_row ? false : is_running, one_row,
			               aggregate_scope);
			return;
		}
		if (function_name == "CLASSIFIER" && function.GetArguments().empty()) {
			expr = CreateStructExtract(state, "classifier");
			return;
		}
		if (function_name == "MATCH_NUMBER" && function.GetArguments().empty()) {
			expr = CreateStructExtract(state, "match_number");
			return;
		}
		// logical navigation over the rows of the match. LAST(X.c) is what an unadorned X.c already
		// means, so both share the masking; only the end they read from differs.
		if ((function_name == "FIRST" || function_name == "LAST") && !function.GetArguments().empty() &&
		    function.GetArguments().size() <= 2) {
			idx_t offset = 0;
			if (function.GetArguments().size() == 2) {
				offset = BindNavigationOffset(function_name, function.GetArguments()[1].GetExpression());
			}
			auto inner = std::move(function.GetArgumentsMutable()[0].GetExpressionMutable());
			vector<string> symbol;
			if (inner->GetExpressionType() == ExpressionType::COLUMN_REF) {
				auto &colref = inner->Cast<ColumnRefExpression>();
				auto &names = colref.ColumnNames();
				auto entry = names.size() >= 2 ? symbols.find(names[0].GetIdentifierName()) : symbols.end();
				if (entry != symbols.end()) {
					symbol = entry->second;
					inner = WithoutQualifier(colref);
				}
			}
			RewriteMeasure(binder, state, inner, config, symbols, running, one_row, aggregate_scope);
			auto packed = PackValue(state, std::move(inner));
			auto masked = symbol.empty() ? std::move(packed) : ClassifiedValue(state, symbol, std::move(packed));
			expr = MatchScopedValue(binder.context, state, config, std::move(masked), running, function_name == "FIRST",
			                        offset);
			return;
		}
		// an aggregate in MEASURES aggregates the rows of the match, whether it is written directly or
		// reached through a macro standing for one
		if (LookupFunction(binder, function, CatalogType::AGGREGATE_FUNCTION_ENTRY) ||
		    IsAggregateMacro(binder, function)) {
			case_insensitive_set_t scope;
			for (auto &argument : function.GetArgumentsMutable()) {
				RewriteMeasure(binder, state, argument.GetExpressionMutable(), config, symbols, running, one_row,
				               &scope);
			}
			auto &qualified = function.GetQualifiedName();
			auto window = make_uniq<WindowExpression>(qualified.Catalog().GetIdentifierName(),
			                                          qualified.Schema().GetIdentifierName(),
			                                          qualified.Name().GetIdentifierName());
			window->GetArgumentsMutable() = std::move(function.GetArgumentsMutable());
			window->DistinctMutable() = function.Distinct();
			// the ordering of an ordered aggregate's input is its own, and the order the match was
			// found in is no substitute for it
			if (function.OrderByMutable()) {
				for (auto &order : function.OrderByMutable()->orders) {
					RewriteMeasure(binder, state, order.expression, config, symbols, running, one_row, &scope);
					window->ArgOrdersMutable().emplace_back(order.type, order.null_order, std::move(order.expression));
				}
			}
			// the filter decides which of the match's rows the aggregate sees, so it reads the match the
			// same way the arguments do: CLASSIFIER(), MATCH_NUMBER() and a pattern variable all mean
			// there what they mean anywhere else in MEASURES
			if (function.FilterMutable()) {
				RewriteMeasure(binder, state, function.FilterMutable(), config, symbols, running, one_row, &scope);
			}
			if (scope.size() > 1) {
				throw BinderException("An aggregate in MEASURES reads the rows of one pattern variable, so \"%s\" "
				                      "cannot also read those of \"%s\"",
				                      *scope.begin(), *std::next(scope.begin()));
			}
			// an empty match covers no rows, so the row carrying it must not reach the aggregate
			unique_ptr<ParsedExpression> in_match =
			    make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, CreateStructExtract(state, "is_empty"));
			if (!scope.empty()) {
				// naming a variable restricts the aggregate to the rows it matched. Dropping those rows
				// is not the same as passing them as NULL: an aggregate that keeps NULLs would see them.
				auto classified = symbols.find(*scope.begin());
				D_ASSERT(classified != symbols.end());
				in_match = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(in_match),
				                                            ClassifierMatches(state, classified->second));
			}
			if (function.FilterMutable()) {
				window->FilterMutable() = make_uniq<ConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(function.FilterMutable()), std::move(in_match));
			} else {
				window->FilterMutable() = std::move(in_match);
			}
			ScopeToMatch(binder.context, state, *window, config, running);
			expr = std::move(window);
			return;
		}
	}
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = expr->Cast<ColumnRefExpression>();
		auto &names = colref.ColumnNames();
		auto entry = names.size() >= 2 ? symbols.find(names[0].GetIdentifierName()) : symbols.end();
		if (entry != symbols.end()) {
			// a known pattern variable scopes the column to the rows it matched
			auto column = WithoutQualifier(colref);
			if (aggregate_scope) {
				// the enclosing aggregate is the one that drops the rows the variable did not match
				aggregate_scope->insert(names[0].GetIdentifierName());
				expr = std::move(column);
				return;
			}
			expr =
			    MatchScopedValue(binder.context, state, config,
			                     ClassifiedValue(state, entry->second, PackValue(state, std::move(column))), running);
			return;
		}
		// an empty match covers no rows, so a column of the input has no row here to be read from
		if (!aggregate_scope) {
			expr = OnlyWhenMatched(state, std::move(expr));
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		RewriteMeasure(binder, state, child, config, symbols, running, one_row, aggregate_scope);
	});
}

BoundStatement Binder::Bind(MatchRecognizeRef &ref) {
	// MATCH_RECOGNIZE is planned as a stack of select nodes:
	//   1. the input, plus one boolean column per DEFINE
	//   2. the pattern window, which classifies every row of every match; rows outside a match are
	//      dropped here
	//   3. the MEASURES, computed across the match a row belongs to
	//   4. for ONE ROW PER MATCH, a filter down to the row each match starts on

	// The matcher's state travels between the select nodes below in a column of its own. Naming it
	// after this clause keeps it apart from the input's columns and from the state of another
	// MATCH_RECOGNIZE this one is stacked on.
	// Every column the clause generates is named after that index, so nothing it adds at one level can
	// be caught by a name added at another. It is not proof against an input column deliberately
	// spelled the same way, which needs the input's own names to rule out.
	const string scope_id = to_string(GenerateTableIndex().index);
	const string state_column = "__pattern_window_" + scope_id;
	const string spans_column = state_column + "_spans";
	const string window_prefix = "__mr_win_" + scope_id + "_";
	const string navigation_prefix = "__mr_nav_" + scope_id + "_";
	const string measure_prefix = "__mr_measure_" + scope_id + "_";
	const string match_number_column = MATCH_RECOGNIZE_MATCH_NUMBER_COLUMN + ("_" + scope_id);
	HoistedInputRefs input_refs("__mr_ref_" + scope_id + "_");

	// a condition may name any pattern variable, so the whole namespace has to be known before the
	// first expression is touched - a variable the PATTERN only mentions is one too
	case_insensitive_set_t declared_symbols;
	for (auto &expr : ref.config->defines_expression_list) {
		declared_symbols.insert(expr->GetAlias().GetIdentifierName());
	}
	ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(
	    *ref.config->pattern, [&](const ColumnRefExpression &colref) {
		    declared_symbols.insert(colref.GetColumnName().GetIdentifierName());
	    });

	// A pattern variable and a table of the input can be spelled the same way, and the variable is
	// what the clause means by it, so the input's tables are only the names left over.
	case_insensitive_set_t input_tables;
	CollectInputTableNames(*ref.input, input_tables);
	for (auto &symbol : declared_symbols) {
		input_tables.erase(symbol);
	}
	for (auto &subset : ref.config->subsets) {
		input_tables.erase(subset.name);
	}

	// ONE ROW PER MATCH reports what the match was partitioned by, under the name the partitioning
	// spelled it. Hoisting is about where the expression is computed and not about what it is called,
	// so the name is taken before it happens.
	vector<Identifier> partition_names;
	for (auto &expr : ref.config->partition_expressions) {
		partition_names.push_back(expr->GetName());
	}

	// every clause is evaluated above a subquery of the input, so a reference into one of its tables
	// is computed there and read back by name
	for (auto &expr : ref.config->partition_expressions) {
		HoistInputQualifiers(expr, input_tables, input_refs);
	}
	for (auto &order : ref.config->order_by_expressions) {
		HoistInputQualifiers(order.expression, input_tables, input_refs);
	}
	for (auto &expr : ref.config->defines_expression_list) {
		HoistInputQualifiers(expr, input_tables, input_refs);
	}
	for (auto &expr : ref.config->measures_expression_list) {
		HoistInputQualifiers(expr, input_tables, input_refs);
	}

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
	// the measures need the ordering too, so the pattern window takes a copy rather than the original
	for (auto &order : ref.config->order_by_expressions) {
		pattern_window->OrderByMutable().emplace_back(order.type, order.null_order, order.expression->Copy());
	}

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
	auto match_number_value = make_uniq<ConstantExpression>(Value::UBIGINT(0));
	match_number_value->SetAlias(Identifier(match_number_column));
	define_select_node->select_list.push_back(std::move(match_number_value));
	hidden_columns.emplace_back(match_number_column);

	// the references hoisted out of the clauses are computed here, where the input still is one
	for (auto &expr : input_refs.select_list) {
		define_select_node->select_list.push_back(std::move(expr));
	}

	vector<MatchRecognizeNavigation> navigations;

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
		ExtractNavigation(expr, *define_select_node, declared_symbols, navigation_prefix, navigations);
		ZapDefineQualifier(*expr, define_name, declared_symbols);
		ReplaceFunctions(expr, window_template->Cast<WindowExpression>(), define_name);
		HoistWindows(expr, *define_select_node, window_prefix, nav_counter);
		ReplaceMatchNumber(expr, match_number_column);

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
	seen_columns.insert(match_number_column);
	for (auto &condition : define_conditions) {
		ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(
		    *condition, [&](const ColumnRefExpression &colref) {
			    // what is left qualified here navigates into a column, so the column is the first name
			    auto column_name = colref.ColumnNames()[0].GetIdentifierName();
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
	column_field_index[match_number_column] = 0;
	column_fields.push_back(make_uniq<ColumnRefExpression>(Identifier(match_number_column)));
	for (auto &column : condition_columns) {
		column_field_index[column->Cast<ColumnRefExpression>().GetColumnName().GetIdentifierName()] =
		    column_fields.size();
		column_fields.push_back(std::move(column));
	}
	arguments.emplace_back(make_uniq<FunctionExpression>("struct_pack", std::move(column_fields)));

	vector<unique_ptr<ParsedExpression>> condition_fields;
	for (idx_t i = 0; i < define_conditions.size(); i++) {
		// a condition decides whether a row is the variable, so the matcher reads it as a boolean and
		// the plan has to produce one
		auto condition = make_uniq<CastExpression>(LogicalType::BOOLEAN, std::move(define_conditions[i]));
		condition->SetAlias(Identifier("c" + to_string(i)));
		condition_fields.push_back(std::move(condition));
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

	for (auto &expr : ref.config->measures_expression_list) {
		HoistMeasureNavigation(expr, window_template->Cast<WindowExpression>(), measure_symbols, *define_select_node,
		                       window_prefix, nav_counter);
	}

	for (idx_t nav_idx = 0; nav_idx < nav_counter; nav_idx++) {
		hidden_columns.push_back(window_prefix + to_string(nav_idx));
	}

	auto define_select = MakeSelectStatement(std::move(define_select_node));
	select_node->from_table = make_uniq<SubqueryRef>(std::move(define_select));
	pattern_window->SetAlias(Identifier(spans_column));
	select_node->select_list.push_back(std::move(pattern_window));

	// The window reports every match a row takes part in, so overlapping matches each get their own
	// row here. Unnesting also drops the rows that matched nothing, since their list is empty.
	auto spans_select = MakeSelectStatement(std::move(select_node));
	auto unnest_node = MakeSelectNode(make_uniq<SubqueryRef>(std::move(spans_select)));
	auto spans_star = make_uniq<StarExpression>();
	spans_star->ExcludeListMutable().insert(QualifiedColumnName(Identifier(spans_column)));
	unnest_node->select_list.push_back(std::move(spans_star));

	vector<unique_ptr<ParsedExpression>> spans_argument;
	spans_argument.push_back(make_uniq<ColumnRefExpression>(Identifier(spans_column)));
	auto unnest_spans = make_uniq<FunctionExpression>("unnest", std::move(spans_argument));

	unnest_spans->SetAlias(Identifier(state_column));
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

	// A measure is named twice: by the name the user gave it, which is what the output calls it, and
	// by one of its own, which is what the projections below the output refer to it by. Keeping the
	// two apart is what lets a measure be called after a column of the input without the reference
	// finding that column instead.
	vector<Identifier> measure_aliases;
	vector<string> measure_columns;
	for (auto &expr : ref.config->measures_expression_list) {
		D_ASSERT(!expr->GetAlias().empty());
		measure_aliases.push_back(expr->GetAlias());
		measure_columns.push_back(measure_prefix + to_string(measure_columns.size()));
		// rewriting can replace the expression wholesale, which would drop the alias with it
		RewriteMeasure(*this, state_column, expr, *ref.config, measure_symbols, all_rows, !all_rows);
		expr->SetAlias(Identifier(measure_columns.back()));
		measures_node->select_list.push_back(std::move(expr));
	}

	select_node = std::move(measures_node);

	// ONE ROW PER MATCH reports one row per match, and reports the match rather than any of its rows:
	// the output is the partitioning followed by the measures. Filtering has to happen above the
	// measures rather than beside them, because they are computed across the whole match.
	// an excluded row still belongs to the match, so it is dropped above the measures rather than
	// before them: the aggregates over the match have to have seen it
	if (all_rows) {
		auto measures_select = MakeSelectStatement(std::move(select_node));
		auto filter_node = MakeSelectNode(make_uniq<SubqueryRef>(std::move(measures_select)));
		// the matcher's state has served the measures and the filter below, and it is no business of
		// whoever reads the result
		auto output_star = make_uniq<StarExpression>();
		output_star->ExcludeListMutable().insert(QualifiedColumnName(Identifier(state_column)));
		// the hoisted references and the measures under their internal names have served the
		// projections below, and the output reports the measures under the names the user gave them
		for (auto &entry : input_refs.columns) {
			output_star->ExcludeListMutable().insert(QualifiedColumnName(Identifier(entry.second)));
		}
		for (auto &column : measure_columns) {
			output_star->ExcludeListMutable().insert(QualifiedColumnName(Identifier(column)));
		}
		filter_node->select_list.push_back(std::move(output_star));
		for (idx_t i = 0; i < measure_columns.size(); i++) {
			auto measure = make_uniq<ColumnRefExpression>(Identifier(measure_columns[i]));
			measure->SetAlias(measure_aliases[i]);
			filter_node->select_list.push_back(std::move(measure));
		}
		if (has_exclusion) {
			filter_node->where_clause = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT,
			                                                          CreateStructExtract(state_column, "is_excluded"));
		}
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
		for (idx_t i = 0; i < ref.config->partition_expressions.size(); i++) {
			auto partition = ref.config->partition_expressions[i]->Copy();
			partition->SetAlias(partition_names[i]);
			filter_node->select_list.push_back(std::move(partition));
		}
		for (idx_t i = 0; i < measure_columns.size(); i++) {
			auto measure = make_uniq<ColumnRefExpression>(Identifier(measure_columns[i]));
			measure->SetAlias(measure_aliases[i]);
			filter_node->select_list.push_back(std::move(measure));
		}
		// the last row is the one reported: a bare column or CLASSIFIER() in MEASURES reads it
		// directly, which is the FINAL semantics the standard gives them
		filter_node->where_clause = CreateStructExtract(state_column, "is_match_end");
		select_node = std::move(filter_node);
	}

	auto child_binder = Binder::CreateBinder(context, this);
	auto result = child_binder->Bind(*select_node);
	const auto alias = !ref.alias.empty() ? ref.alias : Identifier("__match_recognize_table");
	auto names = BindContext::AliasColumnNames(alias, result.names, ref.column_name_alias);
	bind_context.AddGenericBinding(result.plan->GetRootIndex(), alias, names, result.types);
	return result;
}

} // namespace duckdb
