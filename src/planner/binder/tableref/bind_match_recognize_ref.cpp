
#include "duckdb/function/match_recognize.hpp"

#include "duckdb/function/window/match_recognize_functions.hpp"
#include "duckdb/function/window/window_match_recognize.hpp"

#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/function/scalar_macro_function.hpp"
#include "duckdb/main/config.hpp"
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
#include "duckdb/parser/tableref/bound_ref_wrapper.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/match_recognize_ref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_binder/select_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

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

//! Build the tree the matcher walks out of the one the parser produced. The pattern is not an
//! expression: it is never evaluated, only compiled, so it is built directly rather than bound.
static unique_ptr<MatchRecognizePattern> BuildPattern(const ParsedExpression &expr,
                                                      const case_insensitive_map_t<idx_t> &symbol_index) {
	switch (expr.GetExpressionType()) {
	case ExpressionType::COLUMN_REF: {
		// the matcher compares symbols on every candidate row, so a leaf carries an index into the
		// symbols rather than the name itself
		auto result = make_uniq<MatchRecognizePattern>(MatchRecognizePatternType::SYMBOL);
		auto entry = symbol_index.find(expr.Cast<ColumnRefExpression>().GetColumnName().GetIdentifierName());
		if (entry == symbol_index.end()) {
			throw InternalException("MATCH_RECOGNIZE pattern symbol %s has no condition", expr.ToString());
		}
		result->symbol = entry->second;
		return result;
	}
	case ExpressionType::ANCHOR: {
		auto result = make_uniq<MatchRecognizePattern>(MatchRecognizePatternType::ANCHOR);
		result->at_end = expr.Cast<AnchorExpression>().at_end;
		return result;
	}
	case ExpressionType::ALTERNATION: {
		auto &alternation = expr.Cast<AlternationExpression>();
		auto result = make_uniq<MatchRecognizePattern>(MatchRecognizePatternType::ALTERNATION);
		result->children.push_back(BuildPattern(*alternation.child_left, symbol_index));
		result->children.push_back(BuildPattern(*alternation.child_right, symbol_index));
		return result;
	}
	case ExpressionType::CONCATENATION: {
		auto result = make_uniq<MatchRecognizePattern>(MatchRecognizePatternType::CONCATENATION);
		for (auto &child : expr.Cast<ConcatenationExpression>().children) {
			result->children.push_back(BuildPattern(*child, symbol_index));
		}
		return result;
	}
	case ExpressionType::QUANTIFIER: {
		auto &quantifier = expr.Cast<QuantifiedExpression>();
		auto result = make_uniq<MatchRecognizePattern>(MatchRecognizePatternType::QUANTIFIER);
		result->min_count = quantifier.min_count;
		result->max_count = quantifier.max_count;
		result->excluded = quantifier.excluded;
		result->reluctant = quantifier.reluctant;
		result->children.push_back(BuildPattern(*quantifier.child, symbol_index));
		return result;
	}
	default:
		throw NotImplementedException("Unimplemented pattern expression %s",
		                              ExpressionTypeToString(expr.GetExpressionType()));
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

//! Names for the columns the clause generates. The input is bound before any of them is handed out,
//! so a generated name is one the input does not already have rather than one it is hoped not to
//! have - and no two of them are the same either.
struct GeneratedNames {
	explicit GeneratedNames(case_insensitive_set_t taken_p) : taken(std::move(taken_p)) {
	}

	string Reserve(const string &base) {
		auto name = base;
		for (idx_t suffix = 0; taken.count(name); suffix++) {
			name = base + "_" + to_string(suffix);
		}
		taken.insert(name);
		return name;
	}

	case_insensitive_set_t taken;
};

//! A reference that names one of the input's own tables, or navigates into one of its columns, only
//! resolves where the input still is one. Every clause here is evaluated above a subquery of it, so
//! the reference is computed down there, verbatim, and read back under a name of its own.
//!
//! Computing it rather than rewriting it is what makes the difference: two tables' columns of the
//! same name stay apart, a schema qualification or an unaliased table function is still the name it
//! was, and a struct field behind a qualifier is not mistaken for a column.
struct HoistedInputRefs {
	explicit HoistedInputRefs(GeneratedNames &names_p) : names(names_p) {
	}

	//! The subquery column standing for this reference, allocated on first use
	const string &Hoist(const ColumnRefExpression &colref) {
		// the components are length prefixed, so a dot inside a quoted identifier cannot read as the
		// boundary between two of them
		string key;
		for (auto &name : colref.ColumnNames()) {
			const auto identifier = name.GetIdentifierName();
			key += to_string(identifier.size());
			key += ":";
			key += identifier;
		}
		auto entry = columns.find(key);
		if (entry != columns.end()) {
			return entry->second;
		}
		auto hoisted = colref.Copy();
		auto column = names.Reserve("__mr_ref_" + to_string(columns.size()));
		hoisted->SetAlias(Identifier(column));
		select_list.push_back(std::move(hoisted));
		return columns.emplace(std::move(key), std::move(column)).first->second;
	}

	GeneratedNames &names;
	//! Keyed by what the reference spells, so that one written twice is computed once
	case_insensitive_map_t<string> columns;
	//! The expressions to add to the subquery the input is reached through
	vector<unique_ptr<ParsedExpression>> select_list;
};

//! A reference whose first name is a pattern variable belongs to this clause and is resolved here.
//! Every other reference is the input's, so it is computed against the input - which is also what
//! makes a reference the input finds ambiguous say so, rather than quietly taking the first of them.
static void HoistInputReferences(unique_ptr<ParsedExpression> &expr, const case_insensitive_set_t &symbols,
                                 HoistedInputRefs &refs) {
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = expr->Cast<ColumnRefExpression>();
		if (!symbols.count(colref.ColumnNames()[0].GetIdentifierName())) {
			auto alias = expr->GetAlias();
			expr = make_uniq<ColumnRefExpression>(Identifier(refs.Hoist(colref)));
			expr->SetAlias(std::move(alias));
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { HoistInputReferences(child, symbols, refs); });
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
	const auto offset = offset_value.GetValue<uint64_t>();
	// MEASURES counts the offset from one, as the window function it lowers to takes it, and a match
	// no rows can reach is still an offset the two clauses have to agree on rather than wrap around
	if (offset >= NumericCast<uint64_t>(NumericLimits<int64_t>::Maximum())) {
		throw BinderException("The offset of %s() is larger than any match can have rows", function_name);
	}
	return NumericCast<idx_t>(offset);
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
	//! Where the navigated expression sits in the projection below the matcher
	idx_t column;
	idx_t offset;
};

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

//! PREV and NEXT navigate the ordered partition rather than the rows of the match, so they do not
//! depend on the match at all. A measure's are computed per input row below the pattern window and
//! read back from above it, which is the route a DEFINE condition's already take.
static void HoistMeasureNavigation(unique_ptr<ParsedExpression> &expr, const WindowExpression &pattern_window,
                                   const case_insensitive_map_t<vector<string>> &symbols,
                                   vector<unique_ptr<ParsedExpression>> &hoisted, GeneratedNames &names) {
	if (expr->GetExpressionType() == ExpressionType::FUNCTION) {
		auto &function = expr->Cast<FunctionExpression>();
		auto function_name = StringUtil::Upper(function.FunctionName().GetIdentifierName());
		if (function_name == "PREV" || function_name == "NEXT") {
			auto &arguments = function.GetArgumentsMutable();
			if (arguments.empty() || arguments.size() > 2) {
				throw BinderException("%s() takes an expression and an optional offset", function_name);
			}
			for (auto &argument : arguments) {
				HoistMeasureNavigation(argument.GetExpressionMutable(), pattern_window, symbols, hoisted, names);
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
			auto column = names.Reserve("__mr_win");
			window.SetAlias(Identifier(column));
			hoisted.push_back(std::move(navigation));
			// the navigation may be the whole measure, whose alias names the output column
			auto alias = expr->GetAlias();
			expr = make_uniq<ColumnRefExpression>(Identifier(column));
			expr->SetAlias(std::move(alias));
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		HoistMeasureNavigation(child, pattern_window, symbols, hoisted, names);
	});
}

//! Whether a call is spelled the way only an aggregate can be
static bool HasAggregateModifiers(const FunctionExpression &function) {
	return function.Distinct() || function.Filter() || (function.OrderBy() && !function.OrderBy()->orders.empty());
}

//! What a DEFINE condition needs from the plan below the matcher: one column per value the matcher
//! cannot compute itself, and the descriptors telling it what to do with them.
struct MatchRecognizeConditionInputs {
	//! The projection the matcher reads from, built as the conditions are bound
	TableIndex projection_index;
	vector<unique_ptr<Expression>> &select_list;
	vector<Identifier> &names;
	vector<LogicalType> &types;
	//! Names the output does not report, because they only exist for the matcher
	vector<string> &hidden;
	GeneratedNames &generated;
	vector<MatchRecognizeNavigation> &navigations;

	//! Compute this below the matcher and read it back as a column of its own
	unique_ptr<Expression> Project(unique_ptr<Expression> value, const string &base) {
		return ProjectAs(std::move(value), generated.Reserve(base));
	}

	unique_ptr<Expression> ProjectAs(unique_ptr<Expression> value, const string &name) {
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
};

//! Binds a DEFINE condition. The matcher settles a condition one candidate row at a time, so
//! everything a condition reads that is not the row being tested - a navigation over the match, a
//! neighbour in the ordered partition, the number of the match - becomes a column of the projection
//! below it, and the condition reads that column.
//!
//! Deciding this while binding rather than before it is what lets a navigation reached through a
//! macro be seen at all: by the time the hooks below are reached, the ordinary binder has expanded it.
class MatchRecognizeDefineBinder : public SelectBinder {
public:
	MatchRecognizeDefineBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
	                           MatchRecognizeConditionInputs &inputs, const WindowExpression &window_template_p,
	                           const case_insensitive_set_t &symbols_p, const unique_ptr<Expression> &match_number_p)
	    : SelectBinder(binder, context, node), inputs(inputs), window_template(window_template_p), symbols(symbols_p),
	      match_number(match_number_p) {
	}

	//! The variable whose condition is being bound
	void BeginDefine(const string &name) {
		define_name = name;
	}

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override {
		auto &expr = *expr_ptr;
		if (expr.GetExpressionType() == ExpressionType::FUNCTION) {
			auto &function = expr.Cast<FunctionExpression>();
			auto function_name = StringUtil::Upper(function.FunctionName().GetIdentifierName());
			if (function_name == "CLASSIFIER" && function.GetArguments().empty()) {
				if (navigated) {
					// under navigation it names another row, whose symbol is state the matcher holds
					// while it assembles the match and not anything the plan below it can produce
					throw NotImplementedException("CLASSIFIER() cannot be navigated in a DEFINE condition");
				}
				// the row being tested is the one this DEFINE decides on, so it classifies as this symbol
				expr_ptr = make_uniq<ConstantExpression>(Value(define_name));
				return SelectBinder::BindExpression(expr_ptr, depth, root_expression);
			}
			if (function_name == "MATCH_NUMBER" && function.GetArguments().empty()) {
				return BindResult(match_number->Copy());
			}
			if (function_name == "PREV" || function_name == "NEXT") {
				return BindNeighbour(function, function_name, expr_ptr, depth);
			}
			if (function_name == "FIRST" || function_name == "LAST") {
				return BindNavigation(function, function_name, depth);
			}
		}
		if (expr.GetExpressionClass() == ExpressionClass::WINDOW) {
			// a window walks the ordered partition rather than the match, so it is computed below the
			// matcher like the navigation the clause writes as one
			auto bound = SelectBinder::BindExpression(expr_ptr, depth, root_expression);
			if (bound.HasError()) {
				return bound;
			}
			return BindResult(inputs.Project(std::move(bound.expression), "__mr_win"));
		}
		if (expr.GetExpressionType() == ExpressionType::COLUMN_REF) {
			auto &colref = expr.Cast<ColumnRefExpression>();
			auto &names = colref.ColumnNames();
			const auto qualifier = names.size() >= 2 ? names[0].GetIdentifierName() : string();
			if (!qualifier.empty() && StringUtil::CIEquals(qualifier, define_name)) {
				// the variable being defined is the row being tested, so it is the row itself
				expr_ptr = WithoutQualifier(colref);
				return SelectBinder::BindExpression(expr_ptr, depth, root_expression);
			}
			if (!qualifier.empty() && symbols.count(qualifier)) {
				// naming another variable means its value on the last row matched to it so far, which
				// is what LAST() means
				return BindNavigated(WithoutQualifier(colref), DefineColumnName(qualifier), true, 0, depth);
			}
		}
		return SelectBinder::BindExpression(expr_ptr, depth, root_expression);
	}

	BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function, idx_t depth) override {
		return BindResult(BinderException(expr, UnsupportedAggregateMessage()));
	}

	string UnsupportedAggregateMessage() override {
		return "A MATCH_RECOGNIZE condition decides one row at a time, so it cannot be an aggregate";
	}

private:
	//! PREV()/NEXT() walk the ordered partition rather than the match, so they do not depend on the
	//! match at all and are computed once, below the matcher
	BindResult BindNeighbour(FunctionExpression &function, const string &function_name,
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
		const auto saved = navigated;
		navigated = true;
		auto bound = SelectBinder::BindExpression(expr_ptr, depth, false);
		navigated = saved;
		if (bound.HasError()) {
			return bound;
		}
		return BindResult(inputs.Project(std::move(bound.expression), "__mr_win"));
	}

	//! FIRST()/LAST() navigate the rows of the match being assembled, so the matcher resolves them per
	//! row: what the plan supplies is the expression navigated, and the matcher reads it off the row
	//! it navigated to
	BindResult BindNavigation(FunctionExpression &function, const string &function_name, idx_t depth) {
		auto &arguments = function.GetArgumentsMutable();
		if (arguments.empty() || arguments.size() > 2) {
			throw BinderException("%s() takes an expression and an optional offset", function_name);
		}
		idx_t offset = 0;
		if (arguments.size() == 2) {
			offset = BindNavigationOffset(function_name, arguments[1].GetExpression());
		}
		auto inner = std::move(arguments[0].GetExpressionMutable());
		string symbol;
		if (inner->GetExpressionType() == ExpressionType::COLUMN_REF) {
			auto &colref = inner->Cast<ColumnRefExpression>();
			auto &names = colref.ColumnNames();
			if (names.size() >= 2 && symbols.count(names[0].GetIdentifierName())) {
				symbol = DefineColumnName(names[0].GetIdentifierName());
				inner = WithoutQualifier(colref);
			}
		}
		return BindNavigated(std::move(inner), symbol, function_name == "LAST", offset, depth);
	}

	BindResult BindNavigated(unique_ptr<ParsedExpression> inner, string symbol, bool last, idx_t offset, idx_t depth) {
		if (navigated) {
			throw BinderException("Nested row pattern navigation is not supported");
		}
		navigated = true;
		auto bound = BindExpression(inner, depth, false);
		navigated = false;
		if (bound.HasError()) {
			return bound;
		}
		auto column = inputs.Project(std::move(bound.expression), "__mr_nav");
		inputs.navigations.push_back(
		    MatchRecognizeNavigation {last, std::move(symbol), inputs.select_list.size() - 1, offset});
		return BindResult(std::move(column));
	}

	MatchRecognizeConditionInputs &inputs;
	const WindowExpression &window_template;
	//! Every pattern variable the clause declares
	const case_insensitive_set_t &symbols;
	//! The column the matcher rewrites with the number of the match it is assembling
	const unique_ptr<Expression> &match_number;
	//! The variable whose condition is being bound
	string define_name;
	//! Whether what is being bound is read off a row other than the one being tested
	bool navigated = false;
};

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
			expr = WithoutQualifier(colref);
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { ScopeToVariable(child, symbols, scope); });
}

//! Binds the MEASURES clause. Everything MATCH_RECOGNIZE adds to an expression is decided here -
//! which rows of the match a value is read from, what a pattern variable in front of a column means,
//! and how much of the match RUNNING and FINAL let it see - and everything else is ordinary binding.
//!
//! An aggregate arrives at the hook below only once the ordinary binder has expanded the macros and
//! chosen the overload, so what the frame is applied to is the aggregate that is really being called.
class MatchRecognizeMeasureBinder : public SelectBinder {
public:
	MatchRecognizeMeasureBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, string state_p,
	                            const MatchRecognizeConfig &config_p,
	                            const case_insensitive_map_t<vector<string>> &symbols_p, bool all_rows)
	    : SelectBinder(binder, context, node), state(std::move(state_p)), config(config_p), symbols(symbols_p),
	      one_row(!all_rows), running(all_rows) {
	}

protected:
	BindResult BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) override {
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
				    ClassifiedValue(state, entry->second, PackValue(state, WithoutQualifier(colref))), running);
				return BindGenerated(expr_ptr, depth, root_expression);
			}
			// an empty match covers no rows, so a column of the input has no row here to be read from
			expr_ptr = OnlyWhenMatched(state, std::move(expr_ptr));
			return BindGenerated(expr_ptr, depth, root_expression);
		}
		return SelectBinder::BindExpression(expr_ptr, depth, root_expression);
	}

	BindResult BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function, idx_t depth) override {
		return BindOverMatch(expr, depth);
	}

private:
	//! An aggregate in MEASURES aggregates the rows of the match, which is the window below
	BindResult BindOverMatch(FunctionExpression &expr, idx_t depth) {
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

	//! One field of the matcher's state, as it stands where the measures are projected
	unique_ptr<ParsedExpression> StateField(const string &field) {
		return CreateStructExtract(make_uniq<ColumnRefExpression>(Identifier(state)), Identifier(field));
	}

	//! Bind an expression this binder built. Its own parts are not the clause's to interpret again,
	//! and the user's expression inside it has already been through here.
	BindResult BindGenerated(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
		const auto saved = scoped;
		scoped = true;
		auto result = BindExpression(expr_ptr, depth, root_expression);
		scoped = saved;
		return result;
	}

	BindResult BindNavigation(FunctionExpression &function, const string &function_name,
	                          unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
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
		// what is navigated is an expression of the clause's own, so it is read the way one is
		auto packed = PackValue(state, std::move(inner));
		auto masked = symbol.empty() ? std::move(packed) : ClassifiedValue(state, symbol, std::move(packed));
		expr_ptr =
		    MatchScopedValue(context, state, config, std::move(masked), running, function_name == "FIRST", offset);
		return BindGenerated(expr_ptr, depth, root_expression);
	}

	//! The column the matcher's state travels in
	string state;
	const MatchRecognizeConfig &config;
	//! Every pattern variable and SUBSET, mapped to the symbols it stands for
	const case_insensitive_map_t<vector<string>> &symbols;
	//! ONE ROW PER MATCH reports a finished match, so RUNNING and FINAL are the same thing there
	bool one_row;
	//! Whether the expression being bound sees the match up to the current row, or all of it
	bool running;
	//! Whether a value here is already read through something that scopes it to the match
	bool scoped = false;
};

//! The conditions and the window's frame are bound against the input, and evaluated over the
//! projection built on top of it. A reference into the input therefore becomes a reference into that
//! projection - to the column already computing it, or to one appended for it.
static void RemapToProjection(unique_ptr<Expression> &expr, MatchRecognizeConditionInputs &inputs,
                              column_binding_map_t<idx_t> &input_columns) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		// the matcher evaluates a condition per candidate row, which a subquery cannot be reduced to
		throw BinderException("A DEFINE condition may not contain a subquery");
	}
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		if (colref.Binding().table_index == inputs.projection_index) {
			return;
		}
		auto entry = input_columns.find(colref.Binding());
		if (entry == input_columns.end()) {
			auto column = inputs.ProjectAs(expr->Copy(), inputs.generated.Reserve("__mr_read"));
			input_columns[colref.Binding()] = inputs.select_list.size() - 1;
			expr = std::move(column);
			return;
		}
		expr =
		    make_uniq<BoundColumnRefExpression>(colref.GetAlias(), colref.GetReturnType(),
		                                        ColumnBinding(inputs.projection_index, ProjectionIndex(entry->second)));
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { RemapToProjection(child, inputs, input_columns); });
}

//! Record a column the matcher is handed per row, or report where it already is
static idx_t AddMatcherInput(const unique_ptr<Expression> &column, vector<unique_ptr<Expression>> &children,
                             expression_map_t<idx_t> &child_index) {
	auto entry = child_index.find(*column);
	if (entry != child_index.end()) {
		return entry->second;
	}
	const auto index = children.size();
	children.push_back(column->Copy());
	child_index[*children.back()] = index;
	return index;
}

//! Point a condition's column references at the columns the matcher is handed
static void RebindToMatcherInputs(unique_ptr<Expression> &expr, vector<unique_ptr<Expression>> &children,
                                  expression_map_t<idx_t> &child_index) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		// the matcher evaluates a condition per candidate row, which a subquery cannot be reduced to
		throw BinderException("A DEFINE condition may not contain a subquery");
	}
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		const auto index = AddMatcherInput(expr, children, child_index);
		expr = make_uniq<BoundReferenceExpression>(expr->GetReturnType(), index);
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<Expression> &child) { RebindToMatcherInputs(child, children, child_index); });
}

BoundStatement Binder::Bind(MatchRecognizeRef &ref) {
	// MATCH_RECOGNIZE is planned as a stack of select nodes:
	//   1. the input, plus one boolean column per DEFINE
	//   2. the pattern window, which classifies every row of every match; rows outside a match are
	//      dropped here
	//   3. the MEASURES, computed across the match a row belongs to
	//   4. for ONE ROW PER MATCH, a filter down to the row each match starts on

	// The input is bound once, here, while its tables, aliases and qualified columns are still what
	// the query wrote. Everything below is planned on top of that binding, so the names it produces
	// are known before this clause generates any of its own.
	auto input_binder = Binder::CreateBinder(context, this);
	auto bound_input = input_binder->Bind(*ref.input);
	// what the input's columns are called is what the bind context says, not what a table ref bind
	// happens to report back
	case_insensitive_set_t input_names;
	for (auto &binding : input_binder->bind_context.GetBindingsList()) {
		for (auto &name : binding->GetColumnNames()) {
			input_names.insert(name.GetIdentifierName());
		}
	}
	auto input_ref = make_uniq<BoundRefWrapper>(std::move(bound_input), std::move(input_binder));

	// The matcher's state travels between the select nodes below in a column of its own. Every column
	// the clause generates is named apart from the input's own columns and from each other, which is
	// also what keeps two stacked MATCH_RECOGNIZE clauses from naming the same thing.
	GeneratedNames names(std::move(input_names));
	const string state_column = names.Reserve("__pattern_window");
	const string spans_column = names.Reserve(state_column + "_spans");
	const string match_number_column = names.Reserve(MATCH_RECOGNIZE_MATCH_NUMBER_COLUMN);
	HoistedInputRefs input_refs(names);

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
	// a SUBSET name stands for pattern variables too, so it is the clause's namespace as well
	case_insensitive_set_t qualifying_symbols = declared_symbols;
	for (auto &subset : ref.config->subsets) {
		qualifying_symbols.insert(subset.name);
	}

	// ONE ROW PER MATCH reports what the match was partitioned by, under the name the partitioning
	// spelled it. Hoisting is about where the expression is computed and not about what it is called,
	// so the name is taken before it happens.
	vector<Identifier> partition_names;
	for (auto &expr : ref.config->partition_expressions) {
		partition_names.push_back(expr->GetName());
	}

	// Every clause here is evaluated above a subquery of the input, so a reference that means
	// anything to the input rather than to this clause is computed against the input itself and read
	// back by name.
	for (auto &expr : ref.config->partition_expressions) {
		HoistInputReferences(expr, qualifying_symbols, input_refs);
	}
	for (auto &order : ref.config->order_by_expressions) {
		HoistInputReferences(order.expression, qualifying_symbols, input_refs);
	}
	for (auto &expr : ref.config->defines_expression_list) {
		HoistInputReferences(expr, qualifying_symbols, input_refs);
	}
	for (auto &expr : ref.config->measures_expression_list) {
		HoistInputReferences(expr, qualifying_symbols, input_refs);
	}

	// The hoisted references sit in a projection of their own, directly over the input. Everything
	// above reads them by name, including the navigation windows a DEFINE turns into, which could not
	// order by a column computed beside them.
	unique_ptr<TableRef> input_table = std::move(input_ref);
	if (!input_refs.select_list.empty()) {
		auto refs_node = MakeSelectNode(std::move(input_table));
		refs_node->select_list.push_back(make_uniq<StarExpression>());
		for (auto &expr : input_refs.select_list) {
			refs_node->select_list.push_back(std::move(expr));
		}
		input_table = make_uniq<SubqueryRef>(MakeSelectStatement(std::move(refs_node)));
	}

	// PREV() and NEXT() navigate the ordered partition rather than the match, so they become ordinary
	// window functions over the partitioning and ordering the matcher walks. This is what they are
	// spelled over.
	auto window_template = make_uniq<WindowExpression>("", "", "");
	window_template->WindowStartMutable() = WindowBoundary::UNBOUNDED_PRECEDING;
	window_template->WindowEndMutable() = WindowBoundary::UNBOUNDED_FOLLOWING;
	for (auto &expr : ref.config->partition_expressions) {
		window_template->PartitionsMutable().push_back(expr->Copy());
	}
	for (auto &order : ref.config->order_by_expressions) {
		window_template->OrderByMutable().emplace_back(order.type, order.null_order, order.expression->Copy());
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
	if (!ref.config->after_match_variable.empty() && !declared_symbols.count(ref.config->after_match_variable) &&
	    !subset_names.count(ref.config->after_match_variable)) {
		// resuming at a variable the pattern never mentions has nowhere to resume from, and saying so
		// here is more use than a match that never finds a row matched to it
		throw BinderException("AFTER MATCH SKIP TO \"%s\", which is not a pattern variable of this MATCH_RECOGNIZE",
		                      ref.config->after_match_variable);
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

	// Everything the matcher reads that is not the row being tested is computed in a projection of
	// its own below the pattern window, and the conditions are bound as that projection is built.
	auto define_binder = Binder::CreateBinder(context, this);

	BoundSelectNode define_node;
	define_node.from_table = define_binder->Bind(*input_table);
	define_node.projection_index = GenerateTableIndex();
	define_node.group_index = GenerateTableIndex();
	define_node.group_projection_index = GenerateTableIndex();
	define_node.aggregate_index = GenerateTableIndex();
	define_node.groupings_index = GenerateTableIndex();
	define_node.window_index = GenerateTableIndex();
	define_node.prune_index = GenerateTableIndex();

	// the input's own columns pass through, because the output still reports them
	column_binding_map_t<idx_t> input_columns;
	for (auto &binding : define_binder->bind_context.GetBindingsList()) {
		auto &column_names = binding->GetColumnNames();
		auto &column_types = binding->GetColumnTypes();
		for (idx_t i = 0; i < column_names.size(); i++) {
			const ColumnBinding source(binding->GetIndex(), ProjectionIndex(i));
			input_columns[source] = define_node.select_list.size();
			define_node.select_list.push_back(
			    make_uniq<BoundColumnRefExpression>(column_names[i], column_types[i], source));
			define_node.names.push_back(column_names[i]);
			define_node.types.push_back(column_types[i]);
		}
	}

	vector<string> hidden_columns;
	vector<MatchRecognizeNavigation> navigations;
	MatchRecognizeConditionInputs inputs {define_node.projection_index,
	                                      define_node.select_list,
	                                      define_node.names,
	                                      define_node.types,
	                                      hidden_columns,
	                                      names,
	                                      navigations};

	// MATCH_NUMBER() reads this column; the matcher rewrites it for every match it attempts
	auto match_number_ref =
	    inputs.ProjectAs(make_uniq<BoundConstantExpression>(Value::UBIGINT(0)), match_number_column);

	case_insensitive_set_t pattern_symbols;
	vector<string> define_symbols;
	vector<unique_ptr<Expression>> define_conditions;
	MatchRecognizeDefineBinder condition_binder(*define_binder, context, define_node, inputs, *window_template,
	                                            declared_symbols, match_number_ref);
	for (auto &expr : ref.config->defines_expression_list) {
		auto define_name = expr->GetAlias().GetIdentifierName();
		D_ASSERT(!define_name.empty());
		if (pattern_symbols.find(define_name) != pattern_symbols.end()) {
			// a symbol stands for one condition, so a second one for the same symbol has nowhere to go
			throw BinderException("MATCH_RECOGNIZE defines pattern variable \"%s\" more than once", define_name);
		}
		condition_binder.BeginDefine(define_name);
		// a condition decides whether a row is the variable, so the matcher reads it as a boolean and
		// the plan has to produce one
		unique_ptr<ParsedExpression> condition = make_uniq<CastExpression>(LogicalType::BOOLEAN, std::move(expr));
		define_conditions.push_back(condition_binder.Bind(condition));

		pattern_symbols.insert(define_name);
		define_symbols.push_back(DefineColumnName(define_name));
	}

	// rewrite the pattern symbols to the internal names the matcher reports
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(
	    *ref.config->pattern, [&](ColumnRefExpression &colref) {
		    D_ASSERT(colref.ColumnNames().size() == 1);
		    colref.ColumnNamesMutable() = {Identifier(DefineColumnName(colref.GetColumnName().GetIdentifierName()))};
	    });

	// a symbol used by the pattern but never defined matches every row
	vector<string> undefined_symbols;
	ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(
	    *ref.config->pattern, [&](const ColumnRefExpression &colref) {
		    auto column_name = colref.ColumnNames()[0].GetIdentifierName();
		    for (auto &existing : define_symbols) {
			    if (StringUtil::CIEquals(existing, column_name)) {
				    return;
			    }
		    }
		    for (auto &existing : undefined_symbols) {
			    if (StringUtil::CIEquals(existing, column_name)) {
				    return;
			    }
		    }
		    undefined_symbols.push_back(column_name);
	    });
	for (auto &column_name : undefined_symbols) {
		define_symbols.push_back(column_name);
		const auto symbol = MatchRecognizeSymbolName(column_name);
		unique_ptr<ParsedExpression> condition;
		if (ref.config->define_auto) {
			// the variable stands for the column of the same name being true, or non zero, or whatever
			// else that column's type calls true
			condition =
			    make_uniq<CastExpression>(LogicalType::BOOLEAN, make_uniq<ColumnRefExpression>(Identifier(symbol)));
		} else {
			condition =
			    make_uniq<CastExpression>(LogicalType::BOOLEAN, make_uniq<ConstantExpression>(Value::BOOLEAN(true)));
		}
		condition_binder.BeginDefine(symbol);
		define_conditions.push_back(condition_binder.Bind(condition));
		pattern_symbols.insert(symbol);
	}

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

	// PREV() and NEXT() in MEASURES walk the ordered partition rather than the match, and the ordering
	// is the matcher's input rather than its output, so they are computed down here too and the
	// measure reads the column they became
	vector<unique_ptr<ParsedExpression>> measure_navigation;
	for (auto &expr : ref.config->measures_expression_list) {
		HoistMeasureNavigation(expr, *window_template, measure_symbols, measure_navigation, names);
	}
	if (!measure_navigation.empty()) {
		SelectBinder navigation_binder(*define_binder, context, define_node);
		for (auto &expr : measure_navigation) {
			auto column = expr->GetAlias().GetIdentifierName();
			inputs.ProjectAs(navigation_binder.Bind(expr), column);
		}
	}

	auto match_data = make_uniq<MatchRecognizeFunctionData>();

	// the frame the matcher walks is evaluated over the same projection the conditions are
	vector<unique_ptr<Expression>> bound_partitions;
	for (auto &expr : ref.config->partition_expressions) {
		auto partition = expr->Copy();
		bound_partitions.push_back(condition_binder.Bind(partition));
	}
	auto &order_config = DBConfig::GetConfig(context);
	vector<BoundOrderByNode> bound_orders;
	for (auto &order : ref.config->order_by_expressions) {
		auto expr = order.expression->Copy();
		// the window walks the partition in the order the sorter will actually apply, so an ordering
		// the query left unsaid is filled in here rather than reaching the sorter unresolved
		const auto type = order_config.ResolveOrder(context, order.type);
		const auto null_order = order_config.ResolveNullOrder(context, type, order.null_order);
		bound_orders.emplace_back(type, null_order, condition_binder.Bind(expr));
	}

	// everything bound against the input now reads the projection that computes it
	for (auto &condition : define_conditions) {
		RemapToProjection(condition, inputs, input_columns);
	}
	for (auto &partition : bound_partitions) {
		RemapToProjection(partition, inputs, input_columns);
	}
	for (auto &order : bound_orders) {
		RemapToProjection(order.expression, inputs, input_columns);
	}

	// What the matcher reads per row is what the window hands it. The first of them is the number of
	// the match being assembled, which the matcher rewrites per attempt rather than reading from the
	// plan; the rest are the columns the conditions read.
	vector<unique_ptr<Expression>> children;
	expression_map_t<idx_t> child_index;
	children.push_back(match_number_ref->Copy());
	child_index[*children.back()] = MATCH_RECOGNIZE_MATCH_NUMBER_FIELD;

	// a navigated column is one the matcher fills in per row, so where it sits among the ones it is
	// handed is what the navigation descriptor names
	unordered_set<idx_t> navigation_fields;
	vector<idx_t> navigation_input;
	for (auto &navigation : navigations) {
		auto &projected = define_node.select_list[navigation.column];
		unique_ptr<Expression> column = make_uniq<BoundColumnRefExpression>(
		    projected->GetAlias(), projected->GetReturnType(),
		    ColumnBinding(define_node.projection_index, ProjectionIndex(navigation.column)));
		navigation_input.push_back(AddMatcherInput(column, children, child_index));
		navigation_fields.insert(navigation_input.back());
	}

	for (auto &condition : define_conditions) {
		RebindToMatcherInputs(condition, children, child_index);
		// Both kinds depend on the match being assembled, so both are settled per candidate row.
		// Re-deciding them for a whole partition after every match would be quadratic.
		bool reads_match_number = false;
		bool reads_navigation = false;
		ExpressionIterator::VisitExpression<BoundReferenceExpression>(
		    *condition, [&](const BoundReferenceExpression &bound_ref) {
			    reads_match_number = reads_match_number || bound_ref.Index() == MATCH_RECOGNIZE_MATCH_NUMBER_FIELD;
			    reads_navigation = reads_navigation || navigation_fields.count(bound_ref.Index()) > 0;
		    });
		match_data->row_scoped.push_back(reads_navigation || reads_match_number);
		match_data->depends_on_match_number = match_data->depends_on_match_number || reads_match_number;
		match_data->conditions.push_back(std::move(condition));
	}

	// the projection is complete once the matcher knows what it reads, so it can be planned
	define_node.column_count = define_node.select_list.size();
	BoundStatement bound_define;
	bound_define.types = define_node.types;
	bound_define.names = define_node.names;
	bound_define.plan = CreatePlan(define_node);

	match_data->symbols = define_symbols;
	// the matcher compares symbols on every candidate row, so the pattern's leaves carry an index into
	// the symbols rather than the name itself
	case_insensitive_map_t<idx_t> symbol_index;
	for (idx_t i = 0; i < match_data->symbols.size(); i++) {
		symbol_index[match_data->symbols[i]] = i;
	}
	match_data->pattern = BuildPattern(*ref.config->pattern, symbol_index);

	for (idx_t i = 0; i < navigations.size(); i++) {
		match_data->navigations.push_back(MatchRecognizeFunctionData::Navigation {
		    navigations[i].last, navigations[i].symbol, navigation_input[i], navigations[i].offset});
	}

	match_data->after_match = ref.config->after_match;
	if (!ref.config->after_match_variable.empty()) {
		match_data->after_match_variable = DefineColumnName(ref.config->after_match_variable);
	}

	auto bound_window = make_uniq<BoundWindowExpression>(
	    WindowMatchRecognizeExecutor::ResultType(), nullptr,
	    make_uniq<BoundWindowFunction>(MatchRecognizeFun::GetFunction()), std::move(match_data));
	bound_window->GetChildrenMutable() = std::move(children);
	bound_window->WindowStartMutable() = WindowBoundary::UNBOUNDED_PRECEDING;
	bound_window->WindowEndMutable() = WindowBoundary::UNBOUNDED_FOLLOWING;
	bound_window->PartitionsMutable() = std::move(bound_partitions);
	bound_window->OrderByMutable() = std::move(bound_orders);

	// The window reports every match a row takes part in, so overlapping matches each get their own
	// row when they are unnested. Unnesting also drops the rows that matched nothing, since their
	// list is empty.
	const auto window_index = GenerateTableIndex();
	auto logical_window = make_uniq<LogicalWindow>(window_index);
	logical_window->expressions.push_back(std::move(bound_window));
	logical_window->AddChild(std::move(bound_define.plan));

	BoundStatement bound_window_result;
	bound_window_result.types = bound_define.types;
	bound_window_result.types.push_back(WindowMatchRecognizeExecutor::ResultType());
	bound_window_result.names = bound_define.names;
	bound_window_result.names.push_back(Identifier(spans_column));
	bound_window_result.plan = std::move(logical_window);

	auto spans_binder = Binder::CreateBinder(context, this);
	spans_binder->bind_context.AddGenericBinding(define_node.GetRootIndex(), Identifier(names.Reserve("__mr_rows")),
	                                             bound_define.names, bound_define.types);
	spans_binder->bind_context.AddGenericBinding(window_index, Identifier(names.Reserve("__mr_spans")),
	                                             {Identifier(spans_column)},
	                                             {WindowMatchRecognizeExecutor::ResultType()});

	auto unnest_node =
	    MakeSelectNode(make_uniq<BoundRefWrapper>(std::move(bound_window_result), std::move(spans_binder)));
	auto spans_star = make_uniq<StarExpression>();
	spans_star->ExcludeListMutable().insert(QualifiedColumnName(Identifier(spans_column)));
	unnest_node->select_list.push_back(std::move(spans_star));

	vector<unique_ptr<ParsedExpression>> spans_argument;
	spans_argument.push_back(make_uniq<ColumnRefExpression>(Identifier(spans_column)));
	auto unnest_spans = make_uniq<FunctionExpression>("unnest", std::move(spans_argument));

	unnest_spans->SetAlias(Identifier(state_column));
	unnest_node->select_list.push_back(std::move(unnest_spans));
	auto spans_node = std::move(unnest_node);

	// MEASURES are projected on top of the pattern window, where the match a row belongs to is known.
	// They are bound by a binder of their own, so that what MATCH_RECOGNIZE adds to an expression is
	// decided at the same point as what SQL already means by it.
	const auto all_rows = ref.config->rows_per_match == MatchRecognizeRows::MATCH_RECOGNIZE_ROWS_ALL;
	auto measures_binder = Binder::CreateBinder(context, this);
	auto spans_ref = make_uniq<SubqueryRef>(MakeSelectStatement(std::move(spans_node)));
	auto bound_spans = measures_binder->Bind(*spans_ref);

	BoundSelectNode measures;
	measures.from_table = std::move(bound_spans);
	measures.projection_index = GenerateTableIndex();
	measures.group_index = GenerateTableIndex();
	measures.group_projection_index = GenerateTableIndex();
	measures.aggregate_index = GenerateTableIndex();
	measures.groupings_index = GenerateTableIndex();
	measures.window_index = GenerateTableIndex();
	measures.prune_index = GenerateTableIndex();

	// the DEFINE columns are an implementation detail, so they do not reach the output
	case_insensitive_set_t hidden;
	for (auto &entry : hidden_columns) {
		hidden.insert(entry);
	}
	for (auto &binding : measures_binder->bind_context.GetBindingsList()) {
		auto &column_names = binding->GetColumnNames();
		auto &column_types = binding->GetColumnTypes();
		for (idx_t i = 0; i < column_names.size(); i++) {
			if (hidden.count(column_names[i].GetIdentifierName())) {
				continue;
			}
			measures.select_list.push_back(make_uniq<BoundColumnRefExpression>(
			    column_names[i], column_types[i], ColumnBinding(binding->GetIndex(), ProjectionIndex(i))));
			measures.names.push_back(column_names[i]);
			measures.types.push_back(column_types[i]);
		}
	}

	// A measure is named twice: by the name the user gave it, which is what the output calls it, and
	// by one of its own, which is what the projections below the output refer to it by. Keeping the
	// two apart is what lets a measure be called after a column of the input without the reference
	// finding that column instead.
	vector<Identifier> measure_aliases;
	vector<string> measure_columns;
	{
		MatchRecognizeMeasureBinder measure_expression_binder(*measures_binder, context, measures, state_column,
		                                                      *ref.config, measure_symbols, all_rows);
		for (auto &expr : ref.config->measures_expression_list) {
			D_ASSERT(!expr->GetAlias().empty());
			measure_aliases.push_back(expr->GetAlias());
			measure_columns.push_back(names.Reserve("__mr_measure_" + to_string(measure_columns.size())));
			auto bound = measure_expression_binder.Bind(expr);
			bound->SetAlias(Identifier(measure_columns.back()));
			measures.names.emplace_back(measure_columns.back());
			measures.types.push_back(bound->GetReturnType());
			measures.select_list.push_back(std::move(bound));
		}
	}
	measures.column_count = measures.select_list.size();

	BoundStatement bound_measures;
	bound_measures.types = measures.types;
	bound_measures.names = measures.names;
	bound_measures.plan = CreatePlan(measures);

	// what the measures report is all the level above them sees; the columns they were computed from
	// stay behind with the binder that bound them
	auto output_binder = Binder::CreateBinder(context, this);
	output_binder->bind_context.AddGenericBinding(measures.GetRootIndex(), Identifier(names.Reserve("__mr_measures")),
	                                              bound_measures.names, bound_measures.types);

	auto select_node = MakeSelectNode(make_uniq<BoundRefWrapper>(std::move(bound_measures), std::move(output_binder)));
	select_node->select_list.push_back(make_uniq<StarExpression>());

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
	auto output_names = BindContext::AliasColumnNames(alias, result.names, ref.column_name_alias);
	bind_context.AddGenericBinding(result.plan->GetRootIndex(), alias, output_names, result.types);
	return result;
}

} // namespace duckdb
