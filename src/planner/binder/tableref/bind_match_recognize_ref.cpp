
#include "duckdb/function/match_recognize.hpp"

#include "duckdb/function/window/match_recognize_functions.hpp"
#include "duckdb/function/window/window_match_recognize.hpp"

#include "duckdb/main/config.hpp"

#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/pattern_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/bound_ref_wrapper.hpp"
#include "duckdb/parser/tableref/match_recognize_ref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/expression_binder/match_recognize_binder.hpp"
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
	// a reference to something outside this clause is the surrounding query's to resolve, so it is
	// carried out to it rather than left with the binder that found it
	MoveCorrelatedExpressions(*input_binder);
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
	case_insensitive_set_t pattern_variables;
	ParsedExpressionIterator::VisitExpression<ColumnRefExpression>(
	    *ref.config->pattern, [&](const ColumnRefExpression &colref) {
		    pattern_variables.insert(colref.GetColumnName().GetIdentifierName());
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
		if (!pattern_variables.count(define_name)) {
			// a condition only decides rows for the variable the pattern matches with it, so one for a
			// variable the pattern never mentions decides nothing at all - which is rarely what was meant
			throw BinderException("MATCH_RECOGNIZE defines \"%s\", which its PATTERN does not use", define_name);
		}
		condition_binder.BeginDefine(define_name);
		// a condition decides whether a row is the variable, so the matcher reads it as a boolean and
		// the plan has to produce one
		unique_ptr<ParsedExpression> condition = make_uniq<CastExpression>(LogicalType::BOOLEAN, std::move(expr));
		define_conditions.push_back(condition_binder.Bind(condition));

		pattern_symbols.insert(define_name);
		define_symbols.push_back(MatchRecognizeDefineColumn(define_name));
	}

	// rewrite the pattern symbols to the internal names the matcher reports
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(
	    *ref.config->pattern, [&](ColumnRefExpression &colref) {
		    D_ASSERT(colref.ColumnNames().size() == 1);
		    colref.ColumnNamesMutable() = {
		        Identifier(MatchRecognizeDefineColumn(colref.GetColumnName().GetIdentifierName()))};
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

	MoveCorrelatedExpressions(*define_binder);

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
		match_data->after_match_variable = MatchRecognizeDefineColumn(ref.config->after_match_variable);
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
	MoveCorrelatedExpressions(*measures_binder);
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
			filter_node->where_clause = make_uniq<OperatorExpression>(
			    ExpressionType::OPERATOR_NOT, MatchRecognizeStateField(state_column, "is_excluded"));
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
		filter_node->where_clause = MatchRecognizeStateField(state_column, "is_match_end");
		select_node = std::move(filter_node);
	}

	auto child_binder = Binder::CreateBinder(context, this);
	auto result = child_binder->Bind(*select_node);
	MoveCorrelatedExpressions(*child_binder);
	const auto alias = !ref.alias.empty() ? ref.alias : Identifier("__match_recognize_table");
	auto output_names = BindContext::AliasColumnNames(alias, result.names, ref.column_name_alias);
	bind_context.AddGenericBinding(result.plan->GetRootIndex(), alias, output_names, result.types);
	return result;
}

} // namespace duckdb
