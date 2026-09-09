
#include "duckdb/function/match_recognize.hpp"

#include "duckdb/function/window/match_recognize_functions.hpp"
#include "duckdb/function/window/window_match_recognize.hpp"

#include "duckdb/main/config.hpp"

#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/pattern_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
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

//! Build the tree the matcher walks. The pattern is never evaluated, only compiled, so it is built
//! directly rather than bound.
static unique_ptr<MatchRecognizePattern> BuildPattern(const ParsedExpression &expr,
                                                      const case_insensitive_map_t<idx_t> &symbol_index) {
	switch (expr.GetExpressionType()) {
	case ExpressionType::COLUMN_REF: {
		// a leaf carries an index into the symbols rather than the name
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

//! A reference into the input only resolves where the input still is one, and every clause here is
//! evaluated above a subquery of it - so it is computed down there verbatim and read back under a name
//! of its own. Computing rather than rewriting is what keeps two tables' columns of the same name
//! apart, and a struct field behind a qualifier from reading as a column.
struct HoistedInputRefs {
	explicit HoistedInputRefs(GeneratedNames &names_p) : names(names_p) {
	}

	//! The subquery column standing for this reference, allocated on first use
	const string &Hoist(const ColumnRefExpression &colref) {
		// length prefixed, so a dot inside a quoted identifier cannot read as a component boundary
		string key;
		for (auto &name : colref.ColumnNames()) {
			const auto &identifier = name.GetIdentifierName();
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

//! A reference whose first name is a pattern variable, or one a lambda binds, is not the input's.
//! Everything else is computed against the input, which is also what makes an ambiguous reference say
//! so rather than quietly taking the first of them.
static void HoistInputReferences(unique_ptr<ParsedExpression> &expr, const case_insensitive_set_t &symbols,
                                 const case_insensitive_set_t &aliases, HoistedInputRefs &refs,
                                 vector<identifier_set_t> &lambda_parameters) {
	if (expr->GetExpressionClass() == ExpressionClass::LAMBDA) {
		identifier_set_t parameters;
		if (MatchRecognizeLambdaParameters(*expr, parameters)) {
			auto &lambda = expr->Cast<LambdaExpression>();
			lambda_parameters.push_back(std::move(parameters));
			HoistInputReferences(lambda.RightMutable(), symbols, aliases, refs, lambda_parameters);
			lambda_parameters.pop_back();
			return;
		}
	}
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		auto &colref = expr->Cast<ColumnRefExpression>();
		auto &names = colref.ColumnNames();
		if (LambdaExpression::IsLambdaParameter(lambda_parameters, names[0])) {
			return;
		}
		// alias.<name> names one of this clause's own measures rather than a column of the input
		const auto is_alias = names.size() == 2 && StringUtil::CIEquals("alias", names[0].GetIdentifierName()) &&
		                      aliases.count(names[1].GetIdentifierName()) > 0;
		if (!is_alias && !symbols.count(names[0].GetIdentifierName())) {
			auto alias = expr->GetAlias();
			expr = make_uniq<ColumnRefExpression>(Identifier(refs.Hoist(colref)));
			expr->SetAlias(std::move(alias));
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<ParsedExpression> &child) {
		HoistInputReferences(child, symbols, aliases, refs, lambda_parameters);
	});
}

static void HoistInputReferences(unique_ptr<ParsedExpression> &expr, const case_insensitive_set_t &symbols,
                                 const case_insensitive_set_t &aliases, HoistedInputRefs &refs) {
	vector<identifier_set_t> lambda_parameters;
	HoistInputReferences(expr, symbols, aliases, refs, lambda_parameters);
}

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

//! PREV and NEXT walk the ordered partition rather than the match, so a measure's are computed per
//! input row below the pattern window and read back from above it
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

//! The conditions and the frame are bound against the input but evaluated over the projection above
//! it, so a reference into the input becomes one into that projection
static void RemapToProjection(unique_ptr<Expression> &expr, MatchRecognizeConditionInputs &inputs,
                              column_binding_map_t<idx_t> &input_columns) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		// the matcher evaluates a condition per candidate row, which a subquery cannot be reduced to
		throw BinderException("A DEFINE condition may not contain a subquery");
	}
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &colref = expr->Cast<BoundColumnRefExpression>();
		if (colref.Binding().table_index == inputs.projection_index ||
		    colref.Binding().table_index == inputs.match_number_index) {
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

//! The frame the matcher walks is the input's to compute. The binder rejects the spellings that would
//! read a matcher field; this is the boundary that catches one it does not know about.
static void RejectMatcherFields(const Expression &expr, TableIndex match_number_index, const char *clause) {
	ExpressionIterator::VisitExpression<BoundColumnRefExpression>(expr, [&](const BoundColumnRefExpression &colref) {
		if (colref.Binding().table_index == match_number_index) {
			throw InternalException("MATCH_RECOGNIZE built a %s that reads a field only the matcher supplies", clause);
		}
	});
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
                                  expression_map_t<idx_t> &child_index, TableIndex match_number_index) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
		// the matcher evaluates a condition per candidate row, which a subquery cannot be reduced to
		throw BinderException("A DEFINE condition may not contain a subquery");
	}
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		if (expr->Cast<BoundColumnRefExpression>().Binding().table_index == match_number_index) {
			// the matcher supplies this one itself, so the plan hands it no column for it
			return;
		}
		const auto index = AddMatcherInput(expr, children, child_index);
		expr = make_uniq<BoundReferenceExpression>(expr->GetReturnType(), index);
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		RebindToMatcherInputs(child, children, child_index, match_number_index);
	});
}

BoundStatement Binder::Bind(MatchRecognizeRef &ref) {
	// MATCH_RECOGNIZE is planned as a stack of select nodes:
	//   1. the input, plus one boolean column per DEFINE
	//   2. the pattern window, which classifies every row of every match; rows outside a match are
	//      dropped here
	//   3. the MEASURES, computed across the match a row belongs to
	//   4. for ONE ROW PER MATCH, a filter down to the row each match starts on

	// the input is bound once, here, while its tables and qualified columns are still what the query
	// wrote, and before this clause generates any name of its own
	auto input_binder = Binder::CreateBinder(context, this);
	auto bound_input = input_binder->Bind(*ref.input);
	// what the columns are called is what the bind context says, not what the table ref bind reports
	case_insensitive_set_t input_names;
	for (auto &binding : input_binder->bind_context.GetBindingsList()) {
		for (auto &name : binding->GetColumnNames()) {
			input_names.insert(name.GetIdentifierName());
		}
	}
	// a correlated reference is the surrounding query's to resolve, so it is carried out to it
	MoveCorrelatedExpressions(*input_binder);
	auto input_ref = make_uniq<BoundRefWrapper>(std::move(bound_input), std::move(input_binder));

	// The matcher's state travels between the select nodes below in a column of its own. Every generated
	// column is named apart from the input's and from each other, which is also what keeps two stacked
	// clauses apart.
	GeneratedNames names(std::move(input_names));
	const string state_column = names.Reserve("__pattern_window");
	const string spans_column = names.Reserve(state_column + "_spans");
	HoistedInputRefs input_refs(names);

	// a condition may name any pattern variable, including one the PATTERN only mentions, so the whole
	// namespace has to be known before the first expression is touched
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

	// ONE ROW PER MATCH reports the partitioning under the name it was spelled with, which is taken
	// before hoisting rewrites where it is computed
	vector<Identifier> partition_names;
	for (auto &expr : ref.config->partition_expressions) {
		partition_names.push_back(expr->GetName());
	}

	const case_insensitive_set_t no_aliases;

	// a reference that means anything to the input is computed there and read back by name
	for (auto &expr : ref.config->partition_expressions) {
		HoistInputReferences(expr, qualifying_symbols, no_aliases, input_refs);
	}
	for (auto &order : ref.config->order_by_expressions) {
		HoistInputReferences(order.expression, qualifying_symbols, no_aliases, input_refs);
	}
	for (auto &expr : ref.config->defines_expression_list) {
		HoistInputReferences(expr, qualifying_symbols, no_aliases, input_refs);
	}
	// a measure may name one written before it, which is not the input's to compute - though a column
	// of the input by the same name still wins, where the measures are bound
	case_insensitive_set_t measure_alias_names;
	for (auto &expr : ref.config->measures_expression_list) {
		measure_alias_names.insert(expr->GetAlias().GetIdentifierName());
	}
	auto measure_names = qualifying_symbols;
	for (auto &alias : measure_alias_names) {
		measure_names.insert(alias);
	}
	for (auto &expr : ref.config->measures_expression_list) {
		HoistInputReferences(expr, measure_names, measure_alias_names, input_refs);
	}

	// The hoisted references sit in a projection of their own, so the navigation windows a DEFINE turns
	// into can order by them. It is there even when nothing was hoisted, because it is also what makes
	// the input's columns addressable by position: a base table hands out a column binding only once
	// something has asked for that column.
	auto refs_node = MakeSelectNode(std::move(input_ref));
	refs_node->select_list.push_back(make_uniq<StarExpression>());
	for (auto &expr : input_refs.select_list) {
		refs_node->select_list.push_back(std::move(expr));
	}
	unique_ptr<TableRef> input_table = make_uniq<SubqueryRef>(MakeSelectStatement(std::move(refs_node)));

	// PREV() and NEXT() become ordinary windows over the partitioning and ordering the matcher walks
	auto window_template = make_uniq<WindowExpression>("", "", "");
	window_template->WindowStartMutable() = WindowBoundary::UNBOUNDED_PRECEDING;
	window_template->WindowEndMutable() = WindowBoundary::UNBOUNDED_FOLLOWING;
	for (auto &expr : ref.config->partition_expressions) {
		window_template->PartitionsMutable().push_back(expr->Copy());
	}
	for (auto &order : ref.config->order_by_expressions) {
		window_template->OrderByMutable().emplace_back(order.type, order.null_order, order.expression->Copy());
	}

	// {- -} decides which of a match's rows reach the output, so it needs rows in the output
	const bool has_exclusion = HasExclusion(*ref.config->pattern);
	if (has_exclusion && ref.config->rows_per_match != MatchRecognizeRows::MATCH_RECOGNIZE_ROWS_ALL) {
		throw BinderException("Pattern exclusion syntax {- -} requires ALL ROWS PER MATCH");
	}

	// a union stands for a set of rows only once the match is assembled, and the matcher works one
	// symbol at a time
	case_insensitive_set_t subset_names;
	for (auto &subset : ref.config->subsets) {
		subset_names.insert(subset.name);
	}
	if (!ref.config->after_match_variable.empty() && !declared_symbols.count(ref.config->after_match_variable) &&
	    !subset_names.count(ref.config->after_match_variable)) {
		// resuming at a variable the pattern never mentions has nowhere to resume from
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

	// what the matcher reads that is not the row being tested is computed in a projection of its own,
	// built as the conditions are bound
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
	// MATCH_NUMBER() is the one thing a condition reads that the plan does not supply. Until the field
	// it lands in is known it is a column of a table no operator produces - a column rather than a
	// reference, so that it is captured out of a lambda body the way any other value is.
	const auto match_number_index = GenerateTableIndex();
	unique_ptr<Expression> match_number_ref = make_uniq<BoundColumnRefExpression>(
	    Identifier("match_number"), LogicalType::UBIGINT, ColumnBinding(match_number_index, ProjectionIndex(0)));

	MatchRecognizeConditionInputs inputs {define_node.projection_index,
	                                      match_number_index,
	                                      define_node.select_list,
	                                      define_node.names,
	                                      define_node.types,
	                                      hidden_columns,
	                                      names,
	                                      navigations};

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
			// a condition for a variable the pattern never mentions decides nothing at all
			throw BinderException("MATCH_RECOGNIZE defines \"%s\", which its PATTERN does not use", define_name);
		}
		condition_binder.BeginDefine(define_name);
		// the matcher reads a condition as a boolean, so the plan has to produce one
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
			// the variable stands for the column of the same name reading as true
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

	// PREV() and NEXT() in MEASURES walk the matcher's input, so they are computed down here too
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
		condition_binder.BeginFrame(MatchRecognizeScope::PARTITION_BY);
		bound_partitions.push_back(condition_binder.Bind(partition));
		RejectMatcherFields(*bound_partitions.back(), match_number_index, "PARTITION BY");
	}
	auto &order_config = DBConfig::GetConfig(context);
	vector<BoundOrderByNode> bound_orders;
	for (auto &order : ref.config->order_by_expressions) {
		auto expr = order.expression->Copy();
		// an ordering the query left unsaid is resolved here rather than reaching the sorter unresolved
		const auto type = order_config.ResolveOrder(context, order.type);
		const auto null_order = order_config.ResolveNullOrder(context, type, order.null_order);
		condition_binder.BeginFrame(MatchRecognizeScope::ORDER_BY);
		bound_orders.emplace_back(type, null_order, condition_binder.Bind(expr));
		RejectMatcherFields(*bound_orders.back().expression, match_number_index, "ORDER BY");
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

	// What the matcher reads per row is the columns the window hands it, in the order the conditions
	// address them.
	vector<unique_ptr<Expression>> children;
	expression_map_t<idx_t> child_index;

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
		RebindToMatcherInputs(condition, children, child_index, match_number_index);
	}
	// the matcher's own field comes after the ones the plan supplies
	match_data->match_number_field = children.size();
	for (auto &condition : define_conditions) {
		// both depend on the match being assembled, so both are settled per candidate row rather than
		// per partition after every match, which would be quadratic
		bool reads_match_number = false;
		bool reads_navigation = false;
		ExpressionIterator::VisitExpressionMutable<BoundColumnRefExpression>(
		    condition, [&](BoundColumnRefExpression &colref, unique_ptr<Expression> &child) {
			    if (colref.Binding().table_index != match_number_index) {
				    return;
			    }
			    child = make_uniq<BoundReferenceExpression>(colref.GetAlias(), colref.GetReturnType(),
			                                                match_data->match_number_field);
			    reads_match_number = true;
		    });
		ExpressionIterator::VisitExpression<BoundReferenceExpression>(
		    *condition, [&](const BoundReferenceExpression &bound_ref) {
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

	// the window reports every match a row takes part in, so unnesting gives overlapping matches a row
	// each and drops the rows that matched nothing
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

	// MEASURES are projected on top of the pattern window, where the match a row belongs to is known,
	// by a binder of their own
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

	// A measure is named twice: by the name the user gave it, which the output reports, and by one of
	// its own, which the projections below refer to it by - so a measure named after a column of the
	// input does not make that column unreachable. The alias map is what lets a measure name an
	// earlier one.
	for (idx_t i = 0; i < ref.config->measures_expression_list.size(); i++) {
		auto &expr = ref.config->measures_expression_list[i];
		measures.bind_state.alias_map[expr->GetAlias()] = i;
		measures.bind_state.original_expressions.push_back(expr->Copy());
	}

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
			// one more measure is now there to be named by the ones after it
			measures.bound_column_count++;
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

	// the level above sees the measures and not the columns they were computed from
	auto output_binder = Binder::CreateBinder(context, this);
	output_binder->bind_context.AddGenericBinding(measures.GetRootIndex(), Identifier(names.Reserve("__mr_measures")),
	                                              bound_measures.names, bound_measures.types);

	auto select_node = MakeSelectNode(make_uniq<BoundRefWrapper>(std::move(bound_measures), std::move(output_binder)));
	select_node->select_list.push_back(make_uniq<StarExpression>());

	// an excluded row still belongs to the match, so it is dropped above the measures rather than
	// before them: the aggregates over the match have to have seen it
	if (all_rows) {
		auto measures_select = MakeSelectStatement(std::move(select_node));
		auto filter_node = MakeSelectNode(make_uniq<SubqueryRef>(std::move(measures_select)));
		// the matcher's state has served the measures and the filter, and stops here
		auto output_star = make_uniq<StarExpression>();
		output_star->ExcludeListMutable().insert(QualifiedColumnName(Identifier(state_column)));
		// the output reports the measures under the names the user gave them
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
		// the last row is the one reported, which is the FINAL semantics a bare column has here
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
