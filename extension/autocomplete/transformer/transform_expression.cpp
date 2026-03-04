#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/optimizer/rule/date_trunc_simplification.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/default_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/emptytableref.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"

namespace duckdb {

// BaseExpression <- SingleExpression Indirection*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBaseExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto indirection_opt = list_pr.Child<OptionalParseResult>(1);
	if (!indirection_opt.HasResult()) {
		return expr;
	}

	auto indirection_repeat = indirection_opt.optional_result->Cast<RepeatParseResult>();
	for (auto child : indirection_repeat.children) {
		auto indirection_expr = transformer.Transform<unique_ptr<ParsedExpression>>(child);
		if (indirection_expr->GetExpressionClass() == ExpressionClass::CAST) {
			auto cast_expr = unique_ptr_cast<ParsedExpression, CastExpression>(std::move(indirection_expr));
			cast_expr->child = std::move(expr);
			expr = std::move(cast_expr);
		} else if (indirection_expr->GetExpressionClass() == ExpressionClass::OPERATOR) {
			auto operator_expr = unique_ptr_cast<ParsedExpression, OperatorExpression>(std::move(indirection_expr));
			operator_expr->children.insert(operator_expr->children.begin(), std::move(expr));
			expr = std::move(operator_expr);
		} else if (indirection_expr->GetExpressionClass() == ExpressionClass::FUNCTION) {
			auto function_expr = unique_ptr_cast<ParsedExpression, FunctionExpression>(std::move(indirection_expr));
			function_expr->children.insert(function_expr->children.begin(), std::move(expr));
			expr = std::move(function_expr);
		} else if (indirection_expr->GetExpressionClass() == ExpressionClass::CONSTANT) {
			vector<unique_ptr<ParsedExpression>> struct_children;
			struct_children.push_back(std::move(expr));
			struct_children.push_back(std::move(indirection_expr));
			auto struct_expr =
			    make_uniq<OperatorExpression>(ExpressionType::STRUCT_EXTRACT, std::move(struct_children));
			expr = std::move(struct_expr);
		} else {
			throw NotImplementedException("Unhandled case for Base Expression with indirection");
		}
	}
	return expr;
}

unique_ptr<ColumnRefExpression>
PEGTransformerFactory::TransformNestedColumnName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> column_names;
	auto opt_identifiers = list_pr.Child<OptionalParseResult>(0);
	if (opt_identifiers.HasResult()) {
		auto repeat_identifiers = opt_identifiers.optional_result->Cast<RepeatParseResult>();
		for (auto &child : repeat_identifiers.children) {
			auto repeat_list = child->Cast<ListParseResult>();
			column_names.push_back(repeat_list.Child<IdentifierParseResult>(0).identifier);
		}
	}
	column_names.push_back(list_pr.Child<IdentifierParseResult>(1).identifier);
	return make_uniq<ColumnRefExpression>(std::move(column_names));
}

// ColumnReference <- CatalogReservedSchemaTableColumnName / SchemaReservedTableColumnName / TableReservedColumnName /
// NestedColumnName
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformColumnReference(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ColumnRefExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ColumnRefExpression>
PEGTransformerFactory::TransformCatalogReservedSchemaTableColumnName(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> column_names;
	column_names.push_back(transformer.Transform<string>(list_pr.Child<ListParseResult>(0)));
	column_names.push_back(transformer.Transform<string>(list_pr.Child<ListParseResult>(1)));
	column_names.push_back(transformer.Transform<string>(list_pr.Child<ListParseResult>(2)));
	column_names.push_back(list_pr.Child<IdentifierParseResult>(3).identifier);
	return make_uniq<ColumnRefExpression>(std::move(column_names));
}

unique_ptr<ColumnRefExpression>
PEGTransformerFactory::TransformSchemaReservedTableColumnName(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> column_names;
	column_names.push_back(transformer.Transform<string>(list_pr.Child<ListParseResult>(0)));
	column_names.push_back(transformer.Transform<string>(list_pr.Child<ListParseResult>(1)));
	column_names.push_back(list_pr.Child<IdentifierParseResult>(2).identifier);
	return make_uniq<ColumnRefExpression>(std::move(column_names));
}

string PEGTransformerFactory::TransformReservedTableQualification(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

static bool IsExcludableWindowFunction(ExpressionType type) {
	switch (type) {
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
	case ExpressionType::WINDOW_NTH_VALUE:
	case ExpressionType::WINDOW_AGGREGATE:
		return true;
	case ExpressionType::WINDOW_RANK_DENSE:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_NTILE:
	case ExpressionType::WINDOW_CUME_DIST:
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
	case ExpressionType::WINDOW_FILL:
		return false;
	default:
		throw InternalException("Unknown excludable window type %s", ExpressionTypeToString(type).c_str());
	}
}

static bool IsOrderableWindowFunction(ExpressionType type) {
	switch (type) {
	case ExpressionType::WINDOW_FIRST_VALUE:
	case ExpressionType::WINDOW_LAST_VALUE:
	case ExpressionType::WINDOW_NTH_VALUE:
	case ExpressionType::WINDOW_RANK:
	case ExpressionType::WINDOW_PERCENT_RANK:
	case ExpressionType::WINDOW_ROW_NUMBER:
	case ExpressionType::WINDOW_NTILE:
	case ExpressionType::WINDOW_CUME_DIST:
	case ExpressionType::WINDOW_LEAD:
	case ExpressionType::WINDOW_LAG:
	case ExpressionType::WINDOW_FILL:
	case ExpressionType::WINDOW_AGGREGATE:
		return true;
	case ExpressionType::WINDOW_RANK_DENSE:
		return false;
	default:
		throw InternalException("Unknown orderable window type %s", ExpressionTypeToString(type).c_str());
	}
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformFunctionExpression(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto qualified_function = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(0));
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1))->Cast<ListParseResult>();
	bool distinct = false;
	transformer.TransformOptional<bool>(extract_parens, 0, distinct);

	auto function_arg_opt = extract_parens.Child<OptionalParseResult>(1);
	vector<unique_ptr<ParsedExpression>> function_children;
	if (function_arg_opt.HasResult()) {
		auto function_argument_list = ExtractParseResultsFromList(function_arg_opt.optional_result);
		for (auto function_argument : function_argument_list) {
			function_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(function_argument));
		}
	}
	auto order_modifier = make_uniq<OrderModifier>();
	transformer.TransformOptional<vector<OrderByNode>>(extract_parens, 2, order_modifier->orders);

	unique_ptr<ParsedExpression> filter_expr;
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 3, filter_expr);
	bool ignore_nulls = false;
	bool has_ignore_nulls_result = extract_parens.Child<OptionalParseResult>(3).HasResult();
	transformer.TransformOptional<bool>(extract_parens, 3, ignore_nulls);

	auto export_opt = list_pr.Child<OptionalParseResult>(4);
	if (function_children.size() == 1 && ExpressionIsEmptyStar(*function_children[0]) && !distinct &&
	    order_modifier->orders.empty()) {
		// COUNT(*) gets converted into COUNT()
		function_children.clear();
	}
	auto lowercase_name = StringUtil::Lower(qualified_function.name);

	auto over_opt = list_pr.Child<OptionalParseResult>(5);
	if (over_opt.HasResult()) {
		if (transformer.in_window_definition) {
			throw ParserException("window functions are not allowed in window definitions");
		}
		const auto win_fun_type = WindowExpression::WindowToExpressionType(lowercase_name);
		if (win_fun_type == ExpressionType::INVALID) {
			throw InternalException("Unknown/unsupported window function");
		}

		if (win_fun_type != ExpressionType::WINDOW_AGGREGATE && distinct) {
			throw ParserException("DISTINCT is not implemented for non-aggregate window functions!");
		}

		if (!order_modifier->orders.empty() && !IsOrderableWindowFunction(win_fun_type)) {
			throw ParserException("ORDER BY is not supported for the window function \"%s\"", lowercase_name.c_str());
		}

		if (win_fun_type != ExpressionType::WINDOW_AGGREGATE && filter_expr) {
			throw ParserException("FILTER is not implemented for non-aggregate window functions!");
		}
		if (export_opt.HasResult()) {
			throw ParserException("EXPORT_STATE is not supported for window functions!");
		}

		if (win_fun_type == ExpressionType::WINDOW_AGGREGATE && has_ignore_nulls_result) {
			throw ParserException("RESPECT/IGNORE NULLS is not supported for windowed aggregates");
		}
		transformer.in_window_definition = true;
		auto expr = transformer.Transform<unique_ptr<WindowExpression>>(over_opt.optional_result);
		expr->catalog = qualified_function.catalog;
		expr->schema = qualified_function.schema;
		expr->function_name = lowercase_name;
		expr->type = win_fun_type;
		if (expr->type == ExpressionType::WINDOW_AGGREGATE) {
			expr->children = std::move(function_children);
		} else {
			if (!function_children.empty()) {
				expr->children.push_back(std::move(function_children[0]));
			}
			if (expr->type == ExpressionType::WINDOW_LEAD || expr->type == ExpressionType::WINDOW_LAG) {
				if (function_children.size() > 1) {
					expr->offset_expr = std::move(function_children[1]);
				}
				if (function_children.size() > 2) {
					expr->default_expr = std::move(function_children[2]);
				}
				if (function_children.size() > 3) {
					throw ParserException("Incorrect number of parameters for function %s", qualified_function.name);
				}
			} else if (expr->type == ExpressionType::WINDOW_NTH_VALUE) {
				if (function_children.size() > 1) {
					expr->children.push_back(std::move(function_children[1]));
				}
				if (function_children.size() > 2) {
					throw ParserException("Incorrect number of parameters for function %s", qualified_function.name);
				}
			} else {
				if (function_children.size() > 1) {
					throw ParserException("Incorrect number of parameters for function %s", qualified_function.name);
				}
			}
		}
		expr->ignore_nulls = ignore_nulls;
		expr->filter_expr = std::move(filter_expr);
		expr->arg_orders = std::move(order_modifier->orders);
		expr->distinct = distinct;

		if (expr->exclude_clause != WindowExcludeMode::NO_OTHER && !expr->arg_orders.empty() &&
		    !IsExcludableWindowFunction(expr->type)) {
			throw ParserException("EXCLUDE is not supported for the window function \"%s\"",
			                      expr->function_name.c_str());
		}
		transformer.in_window_definition = false;
		return std::move(expr);
	}
	if (lowercase_name == "count" && function_children.empty()) {
		lowercase_name = "count_star";
	}

	if (lowercase_name == "if") {
		if (function_children.size() != 3) {
			throw ParserException("Wrong number of arguments to IF.");
		}
		auto expr = make_uniq<CaseExpression>();
		CaseCheck check;
		check.when_expr = std::move(function_children[0]);
		check.then_expr = std::move(function_children[1]);
		expr->case_checks.push_back(std::move(check));
		expr->else_expr = std::move(function_children[2]);
		return std::move(expr);
	} else if (lowercase_name == "unpack") {
		if (function_children.size() != 1) {
			throw ParserException("Wrong number of arguments to the UNPACK operator");
		}
		auto expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK);
		expr->children = std::move(function_children);
		return std::move(expr);
	} else if (lowercase_name == "try") {
		if (function_children.size() != 1) {
			throw ParserException("Wrong number of arguments provided to TRY expression");
		}
		auto try_expression = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_TRY);
		try_expression->children = std::move(function_children);
		return std::move(try_expression);
	} else if (lowercase_name == "construct_array") {
		auto construct_array = make_uniq<OperatorExpression>(ExpressionType::ARRAY_CONSTRUCTOR);
		construct_array->children = std::move(function_children);
		return std::move(construct_array);
	} else if (lowercase_name == "position") {
		if (function_children.size() != 2) {
			throw ParserException("Wrong number of arguments to __internal_position_operator.");
		}
		// swap arguments for POSITION(x IN y)
		std::swap(function_children[0], function_children[1]);
		lowercase_name = "position";
	} else if (lowercase_name == "ifnull") {
		if (function_children.size() != 2) {
			throw ParserException("Wrong number of arguments to IFNULL.");
		}

		//  Two-argument COALESCE
		auto coalesce_op = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
		coalesce_op->children.push_back(std::move(function_children[0]));
		coalesce_op->children.push_back(std::move(function_children[1]));
		return std::move(coalesce_op);
	} else if (lowercase_name == "date") {
		if (function_children.size() != 1) {
			throw ParserException("Wrong number of arguments provided to DATE function");
		}
		return std::move(make_uniq<CastExpression>(LogicalType::DATE, std::move(function_children[0])));
	} else if (lowercase_name == "list" && order_modifier->orders.size() == 1) {
		// list(expr ORDER BY expr <sense> <nulls>) => list_sort(list(expr), <sense>, <nulls>)
		if (function_children.size() != 1) {
			throw ParserException("Wrong number of arguments to LIST.");
		}
		auto arg_expr = function_children[0].get();
		auto &order_by = order_modifier->orders[0];
		if (arg_expr->Equals(*order_by.expression)) {
			auto sense = make_uniq<ConstantExpression>(EnumUtil::ToChars(order_by.type));
			auto nulls = make_uniq<ConstantExpression>(EnumUtil::ToChars(order_by.null_order));
			auto unordered = make_uniq<FunctionExpression>(
			    qualified_function.catalog, qualified_function.schema, lowercase_name, std::move(function_children),
			    std::move(filter_expr), std::move(order_modifier), distinct, false, export_opt.HasResult());
			lowercase_name = "list_sort";
			order_modifier = make_uniq<OrderModifier>(); // NOLINT
			filter_expr.reset();                         // NOLINT
			function_children.clear();                   // NOLINT
			distinct = false;
			function_children.emplace_back(std::move(unordered));
			function_children.emplace_back(std::move(sense));
			function_children.emplace_back(std::move(nulls));
		}
	}
	auto within_group_opt = list_pr.Child<OptionalParseResult>(2);
	if (within_group_opt.HasResult()) {
		auto order_by_clause = transformer.Transform<vector<OrderByNode>>(within_group_opt.optional_result);
		if (distinct) {
			throw ParserException("DISTINCT is not allowed in combination with WITHIN GROUP");
		}
		if (!order_modifier->orders.empty()) {
			throw ParserException("Cannot use multiple ORDER BY statements with WITHIN GROUP");
		}
		if (!order_modifier) {
			throw InternalException("ORDER modifier for WITHIN GROUP is not initialized");
		}
		order_modifier->orders = std::move(order_by_clause);
		if (order_modifier->orders.size() != 1) {
			throw ParserException("Cannot use multiple ORDER BY clauses with WITHIN GROUP");
		}
		if (lowercase_name == "percentile_cont") {
			if (function_children.size() != 1) {
				throw ParserException("Wrong number of arguments for PERCENTILE_CONT");
			}
			lowercase_name = "quantile_cont";
		} else if (lowercase_name == "percentile_disc") {
			if (function_children.size() != 1) {
				throw ParserException("Wrong number of arguments for PERCENTILE_DISC");
			}
			lowercase_name = "quantile_disc";
		} else if (lowercase_name == "mode") {
			if (!function_children.empty()) {
				throw ParserException("Wrong number of arguments for MODE");
			}
			lowercase_name = "mode";
		} else {
			throw ParserException("Unknown ordered aggregate \"%s\".", qualified_function.name);
		}
	}
	auto result = make_uniq<FunctionExpression>(qualified_function.catalog, qualified_function.schema, lowercase_name,
	                                            std::move(function_children), std::move(filter_expr),
	                                            std::move(order_modifier), distinct, false, export_opt.HasResult());

	return std::move(result);
}

vector<OrderByNode> PEGTransformerFactory::TransformWithinGroupClause(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.GetChild(2));
	return transformer.Transform<vector<OrderByNode>>(extract_parens);
}

QualifiedName PEGTransformerFactory::TransformFunctionIdentifier(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		QualifiedName result;
		result.catalog = INVALID_CATALOG;
		result.schema = INVALID_SCHEMA;
		result.name = choice_pr.result->Cast<IdentifierParseResult>().identifier;
		return result;
	}
	return transformer.Transform<QualifiedName>(list_pr.Child<ChoiceParseResult>(0).result);
}

QualifiedName PEGTransformerFactory::TransformSchemaReservedFunctionName(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	result.name = list_pr.Child<IdentifierParseResult>(1).identifier;
	return result;
}

QualifiedName
PEGTransformerFactory::TransformCatalogReservedSchemaFunctionName(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	QualifiedName result;
	auto opt_schema = list_pr.Child<OptionalParseResult>(1);
	if (opt_schema.HasResult()) {
		result.catalog = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
		result.schema = transformer.Transform<string>(opt_schema.optional_result);
	} else {
		result.schema = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	}
	result.name = list_pr.Child<IdentifierParseResult>(2).identifier;
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformListExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformArrayBoundedListExpression(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool is_array = list_pr.Child<OptionalParseResult>(0).HasResult();
	auto list_expr = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(1));
	if (!is_array) {
		return make_uniq<FunctionExpression>(INVALID_CATALOG, "main", "list_value", std::move(list_expr));
	}
	return make_uniq<OperatorExpression>(ExpressionType::ARRAY_CONSTRUCTOR, std::move(list_expr));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformFilterClause(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto inner_list = extract_parens->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(inner_list.Child<ListParseResult>(1));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformParenthesisExpression(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	// ParenthesisExpression <- Parens(List(Expression))
	vector<unique_ptr<ParsedExpression>> children;

	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expressions = ExtractParseResultsFromList(ExtractResultFromParens(list_pr.Child<ListParseResult>(0)));

	for (auto &expression : expressions) {
		children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expression));
	}
	if (children.size() == 1) {
		return std::move(children[0]);
	}
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "row", std::move(children));
}

void PEGTransformerFactory::RemoveOrderQualificationRecursive(unique_ptr<ParsedExpression> &root_expr) {
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(
	    *root_expr, [&](ColumnRefExpression &col_ref) {
		    auto &col_names = col_ref.column_names;
		    if (col_names.size() > 1) {
			    col_names = vector<string> {col_names.back()};
		    }
	    });
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformArrayParensSelect(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto subquery_expr = make_uniq<SubqueryExpression>();
	subquery_expr->subquery = transformer.Transform<unique_ptr<SelectStatement>>(extract_parens);
	// ARRAY expression
	// wrap subquery into
	// "SELECT CASE WHEN ARRAY_AGG(col) IS NULL THEN [] ELSE ARRAY_AGG(col) END FROM (...) tbl"
	auto select_node = make_uniq<SelectNode>();
	unique_ptr<ParsedExpression> array_agg_child;
	optional_ptr<SelectNode> sub_select;
	if (subquery_expr->subquery->node->type == QueryNodeType::SELECT_NODE) {
		// easy case - subquery is a SELECT
		sub_select = subquery_expr->subquery->node->Cast<SelectNode>();
		if (sub_select->select_list.size() != 1) {
			throw BinderException(*subquery_expr, "Subquery returns %zu columns - expected 1",
			                      sub_select->select_list.size());
		}
		array_agg_child = make_uniq<PositionalReferenceExpression>(1ULL);
	} else {
		// subquery is not a SELECT but a UNION or CTE
		// we can still support this but it is more challenging since we can't push columns for the ORDER BY
		auto columns_star = make_uniq<StarExpression>();
		columns_star->columns = true;
		array_agg_child = std::move(columns_star);
	}

	// ARRAY_AGG(COLUMNS(*))
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(array_agg_child));
	auto aggr = make_uniq<FunctionExpression>("array_agg", std::move(children));
	// push ORDER BY modifiers into the array_agg
	for (auto &modifier : subquery_expr->subquery->node->modifiers) {
		if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
			aggr->order_bys = unique_ptr_cast<ResultModifier, OrderModifier>(modifier->Copy());
			break;
		}
	}
	// transform constants (e.g. ORDER BY 1) into positional references (ORDER BY #1)
	idx_t array_idx = 0;
	if (aggr->order_bys) {
		for (auto &order : aggr->order_bys->orders) {
			if (order.expression->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
				auto &constant_expr = order.expression->Cast<ConstantExpression>();
				Value bigint_value;
				string error;
				if (constant_expr.value.DefaultTryCastAs(LogicalType::BIGINT, bigint_value, &error)) {
					int64_t order_index = BigIntValue::Get(bigint_value);
					idx_t positional_index = order_index < 0 ? NumericLimits<idx_t>::Maximum() : idx_t(order_index);
					order.expression = make_uniq<PositionalReferenceExpression>(positional_index);
				}
			} else if (sub_select) {
				// if we have a SELECT we can push the ORDER BY clause into the SELECT list and reference it
				auto alias = "__array_internal_idx_" + to_string(++array_idx);
				order.expression->alias = alias;
				sub_select->select_list.push_back(std::move(order.expression));
				order.expression = make_uniq<ColumnRefExpression>(alias);
			} else {
				// otherwise we remove order qualifications
				RemoveOrderQualificationRecursive(order.expression);
			}
		}
	}
	// ARRAY_AGG(COLUMNS(*)) IS NULL
	auto agg_is_null = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, aggr->Copy());
	// empty list
	vector<unique_ptr<ParsedExpression>> list_children;
	auto empty_list = make_uniq<FunctionExpression>("list_value", std::move(list_children));
	// CASE
	auto case_expr = make_uniq<CaseExpression>();
	CaseCheck check;
	check.when_expr = std::move(agg_is_null);
	check.then_expr = std::move(empty_list);
	case_expr->case_checks.push_back(std::move(check));
	case_expr->else_expr = std::move(aggr);

	select_node->select_list.push_back(std::move(case_expr));

	// FROM (...) tbl
	auto child_subquery = make_uniq<SubqueryRef>(std::move(subquery_expr->subquery));
	select_node->from_table = std::move(child_subquery);

	auto new_subquery = make_uniq<SelectStatement>();
	new_subquery->node = std::move(select_node);
	subquery_expr->subquery = std::move(new_subquery);

	subquery_expr->subquery_type = SubqueryType::SCALAR;
	return std::move(subquery_expr);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformStructExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto func_name = "struct_pack";
	vector<unique_ptr<ParsedExpression>> struct_children;
	auto struct_children_list = ExtractParseResultsFromList(list_pr.GetChild(1));
	for (auto struct_child : struct_children_list) {
		struct_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(struct_child));
	}

	return make_uniq<FunctionExpression>(INVALID_CATALOG, "main", func_name, std::move(struct_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformStructField(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto alias = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	expr->SetAlias(alias);
	return expr;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformBoundedListExpression(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto has_expr = list_pr.Child<OptionalParseResult>(1);
	vector<unique_ptr<ParsedExpression>> list_children;
	if (has_expr.HasResult()) {
		auto expr_list = ExtractParseResultsFromList(has_expr.optional_result);
		for (auto &expr : expr_list) {
			list_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
		}
	}
	return list_children;
}

// Expression <- LambdaArrowExpression
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExpression(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformLambdaArrowExpression(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto lambda_opt = list_pr.Child<OptionalParseResult>(1);
	if (!lambda_opt.HasResult()) {
		return expr;
	}
	auto inner_lambda_list = lambda_opt.optional_result->Cast<RepeatParseResult>();
	for (auto lambda_expr : inner_lambda_list.children) {
		auto &inner_list_pr = lambda_expr->Cast<ListParseResult>();
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		expr = make_uniq<LambdaExpression>(std::move(expr), std::move(right_expr));
	}
	return expr;
}

// LogicalOrExpression <- LogicalAndExpression ('OR' LogicalAndExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformLogicalOrExpression(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto or_expr_opt = list_pr.Child<OptionalParseResult>(1);
	if (!or_expr_opt.HasResult()) {
		return expr;
	}
	auto or_expr_repeat = or_expr_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &or_expr : or_expr_repeat.children) {
		auto &inner_list_pr = or_expr->Cast<ListParseResult>();
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(expr), std::move(right_expr));
	}
	return expr;
}

// LogicalAndExpression <- LogicalNotExpression ('AND' LogicalNotExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformLogicalAndExpression(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto and_expr_opt = list_pr.Child<OptionalParseResult>(1);
	if (!and_expr_opt.HasResult()) {
		return expr;
	}
	auto and_expr_repeat = and_expr_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &and_expr : and_expr_repeat.children) {
		auto &inner_list_pr = and_expr->Cast<ListParseResult>();
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		expr =
		    make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(expr), std::move(right_expr));
	}
	return expr;
}

// LogicalNotExpression <- 'NOT'* IsExpression
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformLogicalNotExpression(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	auto not_expr_opt = list_pr.Child<OptionalParseResult>(0);
	if (!not_expr_opt.HasResult()) {
		return expr;
	}
	auto not_expr_repeat = not_expr_opt.optional_result->Cast<RepeatParseResult>();
	size_t n = not_expr_repeat.children.size();
	for (size_t i = 0; i < n; i++) {
		vector<unique_ptr<ParsedExpression>> inner_list_children;
		inner_list_children.push_back(std::move(expr));
		expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(inner_list_children));
	}
	return expr;
}

// IsExpression <- IsDistinctFromExpression IsTest*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsExpression(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto is_test_opt = list_pr.Child<OptionalParseResult>(1);
	if (!is_test_opt.HasResult()) {
		return expr;
	}
	auto is_test_expr_repeat = is_test_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &is_test_expr : is_test_expr_repeat.children) {
		auto is_expr = transformer.Transform<unique_ptr<ParsedExpression>>(is_test_expr);
		if (is_expr->GetExpressionClass() == ExpressionClass::COMPARISON) {
			auto compare_expr = unique_ptr_cast<ParsedExpression, ComparisonExpression>(std::move(is_expr));
			compare_expr->left = make_uniq<CastExpression>(LogicalType::BOOLEAN, std::move(expr));
			expr = std::move(compare_expr);
		} else if (is_expr->GetExpressionClass() == ExpressionClass::OPERATOR) {
			auto operator_expr = unique_ptr_cast<ParsedExpression, OperatorExpression>(std::move(is_expr));
			operator_expr->children.insert(operator_expr->children.begin(), std::move(expr));
			expr = std::move(operator_expr);
		} else {
			throw InternalException("Unexpected expression encountered in IsExpression: %s",
			                        ExpressionClassToString(is_expr->GetExpressionClass()));
		}
	}
	return expr;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsTest(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsLiteral(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto not_expr = list_pr.Child<OptionalParseResult>(1);
	auto inner_list_pr = list_pr.Child<ListParseResult>(2);
	auto literal_value = transformer.TransformEnum<Value>(inner_list_pr.Child<ChoiceParseResult>(0).result);
	if (literal_value.IsNull()) {
		auto expr_type = not_expr.HasResult() ? ExpressionType::OPERATOR_IS_NOT_NULL : ExpressionType::OPERATOR_IS_NULL;
		return make_uniq<OperatorExpression>(expr_type, nullptr);
	}
	auto expr_type =
	    not_expr.HasResult() ? ExpressionType::COMPARE_DISTINCT_FROM : ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	return make_uniq<ComparisonExpression>(expr_type, nullptr, make_uniq<ConstantExpression>(literal_value));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNotNull(PEGTransformer &transformer,
                                                                     optional_ptr<ParseResult> parse_result) {
	return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, nullptr);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsNull(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, nullptr);
}

// IsDistinctFromExpression <- ComparisonExpression (IsDistinctFromOp ComparisonExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformIsDistinctFromExpression(PEGTransformer &transformer,
                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto is_test_opt = list_pr.Child<OptionalParseResult>(1);
	if (!is_test_opt.HasResult()) {
		return expr;
	}
	auto is_distinct_repeat = is_test_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &is_distinct : is_distinct_repeat.children) {
		auto &distinct_list = is_distinct->Cast<ListParseResult>();
		auto distinct_type = transformer.Transform<ExpressionType>(distinct_list.Child<ListParseResult>(0));
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(distinct_list.Child<ListParseResult>(1));
		auto distinct_operator = make_uniq<ComparisonExpression>(distinct_type, std::move(expr), std::move(right_expr));
		expr = std::move(distinct_operator);
	}
	return expr;
}

// ComparisonExpression <- BetweenInLikeExpression (ComparisonOperator BetweenInLikeExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformComparisonExpression(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto comparison_opt = list_pr.Child<OptionalParseResult>(1);
	if (!comparison_opt.HasResult()) {
		return expr;
	}
	auto comparison_repeat = comparison_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &comparison_expr : comparison_repeat.children) {
		auto &inner_list_pr = comparison_expr->Cast<ListParseResult>();
		auto comparison_operator = transformer.Transform<ExpressionType>(inner_list_pr.Child<ListParseResult>(0));
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		expr = make_uniq<ComparisonExpression>(comparison_operator, std::move(expr), std::move(right_expr));
	}
	return expr;
}

ExpressionType PEGTransformerFactory::TransformComparisonOperator(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<ExpressionType>(list_pr.Child<ChoiceParseResult>(0).result);
}

bool TryNegateLikeFunction(string &function_name) {
	if (function_name == "~~") {
		function_name = "!~~";
		return true;
	} else if (function_name == "~~*") {
		function_name = "!~~*";
		return true;
	} else if (function_name == "~~~") {
		return false;
	} else if (function_name == "regexp_full_match") {
		return false;
	}
	return false;
}

// BetweenInLikeExpression <- OtherOperatorExpression BetweenInLikeOp?
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformBetweenInLikeExpression(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto between_in_like_opt = list_pr.Child<OptionalParseResult>(1);
	if (!between_in_like_opt.HasResult()) {
		return expr;
	}
	auto between_in_like_expr =
	    transformer.Transform<unique_ptr<ParsedExpression>>(between_in_like_opt.optional_result);
	auto &op_list = between_in_like_opt.optional_result->Cast<ListParseResult>();
	bool has_not = op_list.Child<OptionalParseResult>(0).HasResult();
	if (between_in_like_expr->GetExpressionClass() == ExpressionClass::BETWEEN) {
		auto between_expr = unique_ptr_cast<ParsedExpression, BetweenExpression>(std::move(between_in_like_expr));
		between_expr->input = std::move(expr);
		if (has_not) {
			expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(between_expr));
		} else {
			expr = std::move(between_expr);
		}
	} else if (between_in_like_expr->GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto func_expr = unique_ptr_cast<ParsedExpression, FunctionExpression>(std::move(between_in_like_expr));
		if (func_expr->function_name == "contains") {
			func_expr->children.push_back(std::move(expr));
		} else {
			func_expr->children.insert(func_expr->children.begin(), std::move(expr));
		}
		if (has_not) {
			if (!TryNegateLikeFunction(func_expr->function_name)) {
				// If it wasn't a special "Like" function, wrap it in a standard NOT operator
				expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(func_expr));
			} else {
				expr = std::move(func_expr);
			}
		} else if (func_expr->function_name == "!~") {
			func_expr->function_name = "regexp_full_match";
			func_expr->is_operator = false;
			expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(func_expr));
		} else {
			expr = std::move(func_expr);
		}
	} else if (between_in_like_expr->GetExpressionClass() == ExpressionClass::OPERATOR) {
		auto &operator_expr = between_in_like_expr->Cast<OperatorExpression>();
		operator_expr.children.insert(operator_expr.children.begin(), std::move(expr));
		if (has_not) {
			expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(between_in_like_expr));
		} else {
			expr = std::move(between_in_like_expr);
		}
	} else if (between_in_like_expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery_expr = between_in_like_expr->Cast<SubqueryExpression>();
		subquery_expr.child = std::move(expr);
		if (has_not) {
			expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(between_in_like_expr));
		} else {
			expr = std::move(between_in_like_expr);
		}
	}
	return expr;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBetweenInLikeOp(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto not_expr = list_pr.Child<OptionalParseResult>(0);
	auto inner_list = list_pr.Child<ListParseResult>(1);
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list.Child<ChoiceParseResult>(0).result);
	return expr;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformInClause(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformInExpression(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	if (StringUtil::CIEquals(choice_pr->name, "InExpressionList") ||
	    StringUtil::CIEquals(choice_pr->name, "InSelectStatement")) {
		return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr);
	}
	auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr);
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(right_expr));
	return make_uniq<FunctionExpression>("contains", std::move(children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformInExpressionList(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto expr_list_pr = ExtractParseResultsFromList(extract_parens);
	vector<unique_ptr<ParsedExpression>> in_children;
	for (auto &expr : expr_list_pr) {
		in_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	if (in_children.size() == 1 && in_children[0]->GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery_expr = in_children[0]->Cast<SubqueryExpression>();
		auto result = make_uniq<SubqueryExpression>();
		result->subquery_type = SubqueryType::ANY;
		result->comparison_type = ExpressionType::COMPARE_EQUAL;
		result->subquery = std::move(subquery_expr.subquery);
		return std::move(result);
	}
	auto result = make_uniq<OperatorExpression>(ExpressionType::COMPARE_IN, std::move(in_children));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformInSelectStatement(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto result = make_uniq<SubqueryExpression>();
	result->subquery_type = SubqueryType::ANY;
	result->comparison_type = ExpressionType::COMPARE_EQUAL;
	result->subquery = transformer.Transform<unique_ptr<SelectStatement>>(extract_parens);
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBetweenClause(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto lower = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	auto higher = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(3));
	auto result = make_uniq<BetweenExpression>(nullptr, std::move(lower), std::move(higher));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLikeClause(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	string like_variation = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	vector<unique_ptr<ParsedExpression>> like_children;
	like_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1)));
	auto escape_opt = list_pr.Child<OptionalParseResult>(2);
	if (escape_opt.HasResult()) {
		if (like_variation == "~~") {
			like_variation = "like_escape";
		} else if (like_variation == "~~*") {
			like_variation = "ilike_escape";
		}
		like_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(escape_opt.optional_result));
	}
	auto result = make_uniq<FunctionExpression>(like_variation, std::move(like_children));
	if (like_variation != "regexp_full_match") {
		result->is_operator = true;
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformEscapeClause(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.GetChild(1));
}

string PEGTransformerFactory::TransformLikeVariations(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<string>(list_pr.Child<ChoiceParseResult>(0).result);
}

// OtherOperatorExpression <- BitwiseExpression (OtherOperator BitwiseExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformOtherOperatorExpression(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto other_operator_opt = list_pr.Child<OptionalParseResult>(1);
	if (!other_operator_opt.HasResult()) {
		return expr;
	}
	auto other_operator_repeat = other_operator_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &other_operator_expr : other_operator_repeat.children) {
		auto &inner_list_pr = other_operator_expr->Cast<ListParseResult>();
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		auto other_operator_pr = inner_list_pr.Child<ListParseResult>(0);
		auto other_operator_choice = other_operator_pr.Child<ChoiceParseResult>(0).result;
		if (StringUtil::CIEquals(other_operator_choice->name, "AnyAllOperator")) {
			auto any_all = transformer.Transform<pair<ExpressionType, bool>>(other_operator_choice);
			auto expression_type = any_all.first;
			auto is_any = any_all.second;
			auto subquery_expr = make_uniq<SubqueryExpression>();
			if (right_expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
				subquery_expr->subquery_type = SubqueryType::ANY;
				subquery_expr->comparison_type = expression_type;
				if (right_expr->GetExpressionClass() != ExpressionClass::SUBQUERY) {
					throw NotImplementedException("ANY/ALL expected a subquery");
				}
				auto &right_expr_subquery = right_expr->Cast<SubqueryExpression>();
				subquery_expr->subquery = std::move(right_expr_subquery.subquery);
				subquery_expr->child = std::move(expr);
				if (!is_any) {
					// ALL sublink is equivalent to NOT(ANY) with inverted comparison
					// e.g. [= ALL()] is equivalent to [NOT(<> ANY())]
					// first invert the comparison type
					subquery_expr->comparison_type = NegateComparisonExpression(subquery_expr->comparison_type);
					return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(subquery_expr));
				}
				expr = std::move(subquery_expr);
			} else {
				// left=ANY(right)
				// we turn this into left=ANY((SELECT UNNEST(right)))
				auto select_statement = make_uniq<SelectStatement>();
				auto select_node = make_uniq<SelectNode>();
				vector<unique_ptr<ParsedExpression>> children;
				children.push_back(std::move(right_expr));

				select_node->select_list.push_back(make_uniq<FunctionExpression>("UNNEST", std::move(children)));
				select_node->from_table = make_uniq<EmptyTableRef>();
				select_statement->node = std::move(select_node);
				subquery_expr->subquery = std::move(select_statement);
				subquery_expr->subquery_type = SubqueryType::ANY;
				subquery_expr->child = std::move(expr);
				subquery_expr->comparison_type = expression_type;
				if (subquery_expr->comparison_type == ExpressionType::INVALID) {
					throw ParserException("Unsupported comparison \"%s\" for ANY/ALL subquery",
					                      ExpressionTypeToString(expression_type));
				}
				if (!is_any) {
					// ALL sublink is equivalent to NOT(ANY) with inverted comparison
					// e.g. [= ALL()] is equivalent to [NOT(<> ANY())]
					// first invert the comparison type
					subquery_expr->comparison_type = NegateComparisonExpression(subquery_expr->comparison_type);
					return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(subquery_expr));
				}
				return std::move(subquery_expr);
			}
		} else {
			auto other_operator = transformer.Transform<string>(other_operator_pr);
			vector<unique_ptr<ParsedExpression>> children_function;
			children_function.push_back(std::move(expr));
			children_function.push_back(std::move(right_expr));
			auto func_expr = make_uniq<FunctionExpression>(std::move(other_operator), std::move(children_function));
			func_expr->is_operator = true;
			expr = std::move(func_expr);
		}
	}
	return expr;
}

string PEGTransformerFactory::TransformOtherOperator(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<string>(list_pr.Child<ChoiceParseResult>(0).result);
}

string PEGTransformerFactory::TransformJsonOperator(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<KeywordParseResult>(0).keyword;
}

string PEGTransformerFactory::TransformInetOperator(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
}

string PEGTransformerFactory::TransformStringOperator(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
}

string PEGTransformerFactory::TransformListOperator(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
}

pair<ExpressionType, bool> PEGTransformerFactory::TransformAnyAllOperator(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto comparison_type = transformer.Transform<ExpressionType>(list_pr.Child<ListParseResult>(0));
	auto subquery_type = transformer.Transform<bool>(list_pr.Child<ListParseResult>(1));
	return make_pair(comparison_type, subquery_type);
}

bool PEGTransformerFactory::TransformAnyOrAll(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<bool>(list_pr.Child<ChoiceParseResult>(0).result);
}

// BitwiseExpression <- AdditiveExpression (BitOperator AdditiveExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBitwiseExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto bit_operator_opt = list_pr.Child<OptionalParseResult>(1);
	if (!bit_operator_opt.HasResult()) {
		return expr;
	}
	auto bit_repeat = bit_operator_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &bit_expr : bit_repeat.children) {
		auto &inner_list_pr = bit_expr->Cast<ListParseResult>();
		auto bit = transformer.Transform<string>(inner_list_pr.Child<ListParseResult>(0));
		vector<unique_ptr<ParsedExpression>> bit_children;
		bit_children.push_back(std::move(expr));
		bit_children.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1)));
		auto func_expr = make_uniq<FunctionExpression>(std::move(bit), std::move(bit_children));
		func_expr->is_operator = true;
		expr = std::move(func_expr);
	}
	return expr;
}

string PEGTransformerFactory::TransformBitOperator(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
}

// AdditiveExpression <- MultiplicativeExpression (Term MultiplicativeExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformAdditiveExpression(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto term_opt = list_pr.Child<OptionalParseResult>(1);
	if (!term_opt.HasResult()) {
		return expr;
	}
	auto term_repeat = term_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &term_expr : term_repeat.children) {
		auto &inner_list_pr = term_expr->Cast<ListParseResult>();
		auto term = transformer.Transform<string>(inner_list_pr.Child<ListParseResult>(0));
		vector<unique_ptr<ParsedExpression>> term_children;
		term_children.push_back(std::move(expr));
		term_children.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1)));
		auto func_expr = make_uniq<FunctionExpression>(std::move(term), std::move(term_children));
		func_expr->is_operator = true;
		expr = std::move(func_expr);
	}
	return expr;
}

string PEGTransformerFactory::TransformTerm(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
}

// MultiplicativeExpression <- ExponentiationExpression (Factor ExponentiationExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformMultiplicativeExpression(PEGTransformer &transformer,
                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto factor_opt = list_pr.Child<OptionalParseResult>(1);
	if (!factor_opt.HasResult()) {
		return expr;
	}
	auto factor_repeat = factor_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &factor_expr : factor_repeat.children) {
		auto &inner_list_pr = factor_expr->Cast<ListParseResult>();
		auto factor = transformer.Transform<string>(inner_list_pr.Child<ListParseResult>(0));
		if (factor == "/" && transformer.options.integer_division) {
			factor = "//";
		}
		vector<unique_ptr<ParsedExpression>> factor_children;
		factor_children.push_back(std::move(expr));
		factor_children.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1)));
		auto func_expr = make_uniq<FunctionExpression>(std::move(factor), std::move(factor_children));
		func_expr->is_operator = true;
		expr = std::move(func_expr);
	}
	return expr;
}

string PEGTransformerFactory::TransformFactor(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
}

// ExponentiationExpression <- CollateExpression (ExponentOperator CollateExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformExponentiationExpression(PEGTransformer &transformer,
                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto exponent_opt = list_pr.Child<OptionalParseResult>(1);
	if (!exponent_opt.HasResult()) {
		return expr;
	}
	auto exponent_repeat = exponent_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &exponent_expr : exponent_repeat.children) {
		auto &inner_list_pr = exponent_expr->Cast<ListParseResult>();
		auto exponent = transformer.Transform<string>(inner_list_pr.Child<ListParseResult>(0));
		vector<unique_ptr<ParsedExpression>> exponent_children;
		exponent_children.push_back(std::move(expr));
		exponent_children.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1)));
		auto func_expr = make_uniq<FunctionExpression>(std::move(exponent), std::move(exponent_children));
		func_expr->is_operator = true;
		expr = std::move(func_expr);
	}
	return expr;
}

string PEGTransformerFactory::TransformExponentOperator(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
}

// CollateExpression <- AtTimeZoneExpression (CollateOperator AtTimeZoneExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCollateExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto collate_opt = list_pr.Child<OptionalParseResult>(1);
	if (!collate_opt.HasResult()) {
		return expr;
	}
	auto collate_expr_repeat = collate_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &collate_expr_pr : collate_expr_repeat.children) {
		auto &inner_list_pr = collate_expr_pr->Cast<ListParseResult>();
		vector<unique_ptr<ParsedExpression>> collate_children;
		auto collate_string_expr =
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		string collate_string;
		if (collate_string_expr->GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto &const_expr = collate_string_expr->Cast<ConstantExpression>();
			collate_string = const_expr.value.GetValue<string>();
		} else if (collate_string_expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			auto &col_ref = collate_string_expr->Cast<ColumnRefExpression>();
			collate_string = StringUtil::Join(col_ref.column_names, ".");
		} else {
			throw NotImplementedException("Unexpected expression encountered for collate, %s",
			                              EnumUtil::ToString(collate_string_expr->GetExpressionClass()));
		}
		auto collate_expr = make_uniq<CollateExpression>(collate_string, std::move(expr));
		expr = std::move(collate_expr);
	}
	return expr;
}

// AtTimeZoneExpression <- PrefixExpression (AtTimeZoneOperator PrefixExpression)*
unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformAtTimeZoneExpression(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto at_time_zone_opt = list_pr.Child<OptionalParseResult>(1);
	if (!at_time_zone_opt.HasResult()) {
		return expr;
	}
	auto at_time_zone_repeat = at_time_zone_opt.optional_result->Cast<RepeatParseResult>();
	for (auto &time_zone_expr : at_time_zone_repeat.children) {
		auto &inner_list_pr = time_zone_expr->Cast<ListParseResult>();
		vector<unique_ptr<ParsedExpression>> time_zone_children;
		time_zone_children.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1)));
		time_zone_children.push_back(std::move(expr));
		auto func_expr =
		    make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "timezone", std::move(time_zone_children));
		expr = std::move(func_expr);
	}
	return expr;
}

bool IsNumberLiteral(optional_ptr<ParseResult> pr) {
	if (!pr) {
		return false;
	}
	if (pr->name == "BaseExpression") {
		auto &list = pr->Cast<ListParseResult>();
		if (list.GetChild(1)->Cast<OptionalParseResult>().HasResult()) {
			return false;
		}
		return IsNumberLiteral(list.GetChild(0));
	}
	if (pr->name == "SingleExpression") {
		auto &list = pr->Cast<ListParseResult>();
		return IsNumberLiteral(list.GetChild(0)->Cast<ChoiceParseResult>().result);
	}
	if (pr->name == "LiteralExpression") {
		auto &list = pr->Cast<ListParseResult>();
		return IsNumberLiteral(list.GetChild(0)->Cast<ChoiceParseResult>().result);
	}
	return pr->name == "NumberLiteral";
}

string GetRawText(optional_ptr<ParseResult> pr) {
	if (pr->name == "NumberLiteral") {
		return pr->Cast<NumberParseResult>().number;
	}
	if (pr->name == "BaseExpression") {
		return GetRawText(pr->Cast<ListParseResult>().GetChild(0));
	}
	if (pr->name == "SingleExpression" || pr->name == "LiteralExpression") {
		auto &list = pr->Cast<ListParseResult>();
		return GetRawText(list.GetChild(0)->Cast<ChoiceParseResult>().result);
	}
	return "";
}

// PrefixExpression <- PrefixOperator* BaseExpression
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformPrefixExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto prefix_opt = list_pr.Child<OptionalParseResult>(0);
	auto base_expr_pr = list_pr.Child<ListParseResult>(1);

	if (!prefix_opt.HasResult()) {
		return transformer.Transform<unique_ptr<ParsedExpression>>(base_expr_pr);
	}

	auto &prefix_repeat = prefix_opt.optional_result->Cast<RepeatParseResult>();

	// --- SPECIAL CASE: Handle -<Number> atomically to prevent overflow/precision loss ---
	// We only do this if there is exactly one prefix and it is a minus.
	if (prefix_repeat.children.size() == 1) {
		auto prefix = transformer.Transform<string>(prefix_repeat.children[0]);
		if (prefix == "-" && IsNumberLiteral(base_expr_pr)) {
			string raw_number = GetRawText(base_expr_pr);
			string full_text = "-" + raw_number;
			return ConvertNumberToValue(full_text);
		}
	}

	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(base_expr_pr);

	// Apply prefixes in order (from right to left, as they were parsed)
	for (auto &prefix_expr : prefix_repeat.children) {
		auto prefix = transformer.Transform<string>(prefix_expr);

		if (prefix == "-" && expr->type == ExpressionType::VALUE_CONSTANT) {
			auto &const_expr = expr->Cast<ConstantExpression>();
			if (TryNegateValue(const_expr.value)) {
				continue;
			}
		}

		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(expr));
		auto func_expr = make_uniq<FunctionExpression>(prefix, std::move(children));
		func_expr->is_operator = true;
		expr = std::move(func_expr);
	}
	return expr;
}
string PEGTransformerFactory::TransformPrefixOperator(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.TransformEnum<string>(choice_pr.result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformParameter(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	// Parameter <- AnonymousParameter / NumberedParameter / ColLabelParameter
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformAnonymousParameter(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	// AnonymousParameter <- '?'
	auto expr = make_uniq<ParameterExpression>();

	// Auto-increment the parameter count
	idx_t known_param_index = transformer.ParamCount() + 1;
	string identifier = StringUtil::Format("%d", known_param_index);

	// Register it
	transformer.SetParam(identifier, known_param_index, PreparedParamType::AUTO_INCREMENT);
	transformer.SetParamCount(MaxValue<idx_t>(transformer.ParamCount(), known_param_index));

	expr->identifier = identifier;
	return std::move(expr);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNumberedParameter(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	// NumberedParameter <- '$' NumberLiteral
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto number = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.GetChild(1));

	auto &const_expr = number->Cast<ConstantExpression>();
	int32_t param_number = const_expr.value.GetValue<int32_t>();

	if (param_number <= 0) {
		throw ParserException("Parameter numbers must be greater than 0");
	}

	auto expr = make_uniq<ParameterExpression>();
	string identifier = const_expr.value.ToString();
	idx_t known_param_index = DConstants::INVALID_INDEX;

	transformer.GetParam(identifier, known_param_index, PreparedParamType::POSITIONAL);

	if (known_param_index == DConstants::INVALID_INDEX) {
		known_param_index = NumericCast<idx_t>(param_number);
		transformer.SetParam(identifier, known_param_index, PreparedParamType::POSITIONAL);
	}

	expr->identifier = identifier;
	transformer.SetParamCount(MaxValue<idx_t>(transformer.ParamCount(), known_param_index));
	return std::move(expr);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformColLabelParameter(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	// ColLabelParameter <- '$' ColLabel
	auto &list_pr = parse_result->Cast<ListParseResult>();
	string identifier = transformer.Transform<string>(list_pr.GetChild(1));

	auto expr = make_uniq<ParameterExpression>();
	idx_t known_param_index = DConstants::INVALID_INDEX;

	transformer.GetParam(identifier, known_param_index, PreparedParamType::NAMED);

	if (known_param_index == DConstants::INVALID_INDEX) {
		// New named parameter gets the next available index
		known_param_index = transformer.ParamCount() + 1;
		transformer.SetParam(identifier, known_param_index, PreparedParamType::NAMED);
	}

	expr->identifier = identifier;
	transformer.SetParamCount(MaxValue<idx_t>(transformer.ParamCount(), known_param_index));
	return std::move(expr);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformPositionalExpression(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto number = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.GetChild(1));
	auto &const_expr = number->Cast<ConstantExpression>();
	int32_t index = const_expr.value.GetValue<int32_t>();
	if (index <= 0) {
		throw ParserException("Positional index must be greater than 0");
	}
	return make_uniq<PositionalReferenceExpression>(NumericCast<idx_t>(index));
}

// LiteralExpression <- StringLiteral / NumberLiteral / 'NULL' / 'TRUE' / 'FALSE'
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLiteralExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &matched_rule_result = list_pr.Child<ChoiceParseResult>(0);
	if (matched_rule_result.name == "StringLiteral") {
		auto string_literal = matched_rule_result.result->Cast<StringLiteralParseResult>();
		return string_literal.ToExpression();
	}
	return transformer.Transform<unique_ptr<ParsedExpression>>(matched_rule_result.result);
}

// ParensExpression <- Parens(Expression)
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformParensExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<unique_ptr<ParsedExpression>>(extract_parens);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformConstantLiteral(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto val = transformer.TransformEnum<Value>(list_pr.Child<ChoiceParseResult>(0).result);
	if (val.IsNull()) {
		return make_uniq<ConstantExpression>(val);
	} else {
		auto varchar_val = make_uniq<ConstantExpression>(val.GetValue<string>().substr(0, 1));
		return make_uniq<CastExpression>(LogicalType::BOOLEAN, std::move(varchar_val));
	}
}

// SingleExpression <- LiteralExpression /
// Parameter /
// SubqueryExpression /
// SpecialFunctionExpression /
// ParenthesisExpression /
// IntervalLiteral /
// TypeLiteral /
// CaseExpression /
// StarExpression /
// CastExpression /
// GroupingExpression /
// MapExpression /
// FunctionExpression /
// ColumnReference /
// PrefixExpression /
// ListComprehensionExpression /
// ListExpression /
// StructExpression /
// PositionalExpression /
// DefaultExpression
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformSingleExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

ExpressionType PEGTransformerFactory::TransformOperator(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::OPERATOR) {
		return OperatorToExpressionType(choice_pr.result->Cast<OperatorParseResult>().operator_token);
	}
	return transformer.Transform<ExpressionType>(choice_pr.result);
}

ExpressionType PEGTransformerFactory::TransformConjunctionOperator(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<ExpressionType>(list_pr.Child<ChoiceParseResult>(0).result);
}

ExpressionType PEGTransformerFactory::TransformIsOperator(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool is_not = list_pr.Child<OptionalParseResult>(1).HasResult();
	bool is_distinct = list_pr.Child<OptionalParseResult>(2).HasResult();
	if (is_distinct && is_not) {
		return ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	}
	if (is_distinct) {
		return ExpressionType::COMPARE_DISTINCT_FROM;
	}
	if (is_not) {
		return ExpressionType::OPERATOR_IS_NOT_NULL;
	}
	return ExpressionType::OPERATOR_IS_NULL;
}

ExpressionType PEGTransformerFactory::TransformInOperator(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto is_not = list_pr.Child<OptionalParseResult>(0).HasResult();
	if (is_not) {
		return ExpressionType::COMPARE_NOT_IN;
	}
	return ExpressionType::COMPARE_IN;
}

ExpressionType PEGTransformerFactory::TransformLambdaOperator(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	return ExpressionType::LAMBDA;
}

ExpressionType PEGTransformerFactory::TransformBetweenOperator(PEGTransformer &transformer,
                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	if (list_pr.Child<OptionalParseResult>(0).HasResult()) {
		return ExpressionType::COMPARE_NOT_BETWEEN;
	}
	return ExpressionType::COMPARE_BETWEEN;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIndirection(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformPostfixOperator(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	vector<unique_ptr<ParsedExpression>> func_children;
	return make_uniq<FunctionExpression>("factorial", std::move(func_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCastOperator(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto type = transformer.Transform<LogicalType>(list_pr.Child<ListParseResult>(1));
	// We input a dummy constant expression but replace this later with the real expression that precedes this post-fix
	// castOperator
	return make_uniq<CastExpression>(type, make_uniq<ConstantExpression>(Value()));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformDotOperator(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto nested_list = list_pr.Child<ListParseResult>(1);
	auto choice_pr = nested_list.Child<ChoiceParseResult>(0);
	if (choice_pr.name == "ColLabel") {
		return make_uniq<ConstantExpression>(transformer.Transform<string>(choice_pr.result));
	}
	if (choice_pr.name == "MethodExpression") {
		return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr.result);
	}
	throw InternalException("Unexpected rule encountered in 'DotOperator'");
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformMethodExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto collabel = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1))->Cast<ListParseResult>();
	bool distinct = false;
	transformer.TransformOptional<bool>(extract_parens, 0, distinct);
	auto function_arg_opt = extract_parens.Child<OptionalParseResult>(1);
	vector<unique_ptr<ParsedExpression>> function_children;
	if (function_arg_opt.HasResult()) {
		auto function_argument_list = ExtractParseResultsFromList(function_arg_opt.optional_result);
		for (auto function_argument : function_argument_list) {
			function_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(function_argument));
		}
	}
	if (function_children.size() == 1 && ExpressionIsEmptyStar(*function_children[0])) {
		// COUNT(*) gets converted into COUNT()
		function_children.clear();
	}
	vector<OrderByNode> order_by;
	transformer.TransformOptional<vector<OrderByNode>>(extract_parens, 2, order_by);
	bool ignore_nulls = false;
	transformer.TransformOptional<bool>(extract_parens, 3, ignore_nulls);
	auto result =
	    make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, collabel, std::move(function_children));
	result->distinct = distinct;
	if (!order_by.empty()) {
		auto order_by_modifier = make_uniq<OrderModifier>();
		order_by_modifier->orders = std::move(order_by);
		result->order_bys = std::move(order_by_modifier);
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformSliceExpression(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto slice_bound = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(1));
	if (slice_bound.size() == 1) {
		return make_uniq<OperatorExpression>(ExpressionType::ARRAY_EXTRACT, std::move(slice_bound));
	}
	return make_uniq<OperatorExpression>(ExpressionType::ARRAY_SLICE, std::move(slice_bound));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSliceBound(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> slice_bounds;
	auto start_slice_opt = list_pr.Child<OptionalParseResult>(0);
	auto end_slice_opt = list_pr.Child<OptionalParseResult>(1);
	auto step_slice_opt = list_pr.Child<OptionalParseResult>(2);
	if (!end_slice_opt.HasResult() && !step_slice_opt.HasResult()) {
		if (start_slice_opt.HasResult()) {
			slice_bounds.push_back(
			    transformer.Transform<unique_ptr<ParsedExpression>>(start_slice_opt.optional_result));
		}
		return slice_bounds;
	}
	auto const_list = make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));
	if (start_slice_opt.HasResult()) {
		slice_bounds.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(start_slice_opt.optional_result));
	} else {
		slice_bounds.push_back(const_list->Copy());
	}
	if (end_slice_opt.HasResult()) {
		slice_bounds.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(end_slice_opt.optional_result));
	} else {
		slice_bounds.push_back(const_list->Copy());
	}
	if (step_slice_opt.HasResult()) {
		slice_bounds.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(step_slice_opt.optional_result));
	}
	return slice_bounds;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformEndSliceBound(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto nested_list_opt = list_pr.Child<OptionalParseResult>(1);
	// If either the lower or upper bound is not specified, we use an empty constant LIST,
	// which we handle in the execution.
	auto const_list = make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));
	if (nested_list_opt.HasResult()) {
		auto nested_list = nested_list_opt.optional_result->Cast<ListParseResult>();
		auto choice_pr = nested_list.Child<ChoiceParseResult>(0);
		if (choice_pr.result->type == ParseResultType::KEYWORD) {
			// We have hit the '-'
			return std::move(const_list);
		}
		if (choice_pr.result->type == ParseResultType::LIST) {
			return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr.result);
		}
		throw InternalException("Unexpected parse result type encountered");
	}
	// return empty list here
	return std::move(const_list);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformStepSliceBound(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expression_opt = list_pr.Child<OptionalParseResult>(1);
	if (expression_opt.HasResult()) {
		return transformer.Transform<unique_ptr<ParsedExpression>>(expression_opt.optional_result);
	}
	return make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));
}

unique_ptr<ColumnRefExpression>
PEGTransformerFactory::TransformTableReservedColumnName(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto table = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto column = list_pr.Child<IdentifierParseResult>(1).identifier;
	return make_uniq<ColumnRefExpression>(column, table);
}

string PEGTransformerFactory::TransformTableQualification(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformStarExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto result = make_uniq<StarExpression>();
	auto repeat_colid_opt = list_pr.Child<OptionalParseResult>(0);
	if (repeat_colid_opt.HasResult()) {
		auto repeat_colid = repeat_colid_opt.optional_result->Cast<RepeatParseResult>();
		if (repeat_colid.children.size() > 1) {
			throw ParserException("Did not expect more than one column in front of a star expression");
		}
		auto colid_list = repeat_colid.children[0]->Cast<ListParseResult>();
		result->relation_name = transformer.Transform<string>(colid_list.Child<ListParseResult>(0));
	}
	transformer.TransformOptional<qualified_column_set_t>(list_pr, 2, result->exclude_list);
	auto replace_list_opt = list_pr.Child<OptionalParseResult>(3);
	if (replace_list_opt.HasResult()) {
		result->replace_list = transformer.Transform<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(
		    replace_list_opt.optional_result);
		for (auto &replace_entry : result->replace_list) {
			if (result->exclude_list.find(QualifiedColumnName(replace_entry.first)) != result->exclude_list.end()) {
				throw ParserException("Column \"%s\" cannot occur in both EXCLUDE and REPLACE list",
				                      replace_entry.first);
			}
		}
	}
	auto rename_list_opt = list_pr.Child<OptionalParseResult>(4);
	if (rename_list_opt.HasResult()) {
		result->rename_list = transformer.Transform<qualified_column_map_t<string>>(rename_list_opt.optional_result);
		for (auto &rename_column : result->rename_list) {
			if (result->exclude_list.find(rename_column.first) != result->exclude_list.end()) {
				throw ParserException("Column \"%s\" cannot occur in both EXCLUDE and RENAME list",
				                      rename_column.first.ToString());
			}
			if (result->replace_list.find(rename_column.first.column) != result->replace_list.end()) {
				throw ParserException("Column \"%s\" cannot occur in both REPLACE and RENAME list",
				                      rename_column.first.ToString());
			}
		}
	}
	return std::move(result);
}

qualified_column_set_t PEGTransformerFactory::TransformExcludeList(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<qualified_column_set_t>(list_pr.Child<ChoiceParseResult>(1).result);
}

qualified_column_set_t PEGTransformerFactory::TransformExcludeNameList(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto exclude_name_list = ExtractParseResultsFromList(extract_parens);
	qualified_column_set_t result;
	for (auto exclude_name : exclude_name_list) {
		auto exclude_column = transformer.Transform<QualifiedColumnName>(exclude_name);
		if (result.find(exclude_column) != result.end()) {
			throw ParserException("Duplicate entry \"%s\" in EXCLUDE list", exclude_column.ToString());
		}
		result.insert(std::move(exclude_column));
	}
	return result;
}

qualified_column_set_t PEGTransformerFactory::TransformExcludeNameSingle(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	qualified_column_set_t result;
	result.insert(transformer.Transform<QualifiedColumnName>(list_pr.Child<ListParseResult>(0)));
	return result;
}

QualifiedColumnName PEGTransformerFactory::TransformExcludeName(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	if (StringUtil::CIEquals(choice_pr->name, "dottedidentifier")) {
		auto result = transformer.Transform<vector<string>>(choice_pr);
		auto result_string = StringUtil::Join(result, ".");
		return QualifiedColumnName::Parse(result_string);
	} else if (StringUtil::CIEquals(choice_pr->name, "colidorstring")) {
		auto result = transformer.Transform<string>(choice_pr);
		return QualifiedColumnName(result);
	} else {
		throw InternalException("Unexpected option encountered for ExcludeName");
	}
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformOverClause(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<WindowExpression>>(list_pr.Child<ListParseResult>(1));
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformWindowFrame(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		auto window_name = choice_pr.result->Cast<IdentifierParseResult>().identifier;
		auto it = transformer.window_clauses.find(string(window_name));
		if (it == transformer.window_clauses.end()) {
			throw ParserException("window \"%s\" does not exist", window_name);
		}
		auto copied_expr = unique_ptr_cast<ParsedExpression, WindowExpression>(it->second->Copy());

		return unique_ptr_cast<ParsedExpression, WindowExpression>(std::move(copied_expr));
	}
	return transformer.Transform<unique_ptr<WindowExpression>>(choice_pr.result);
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformParensIdentifier(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.GetChild(0));
	auto window_name = extract_parens->Cast<IdentifierParseResult>().identifier;
	auto it = transformer.window_clauses.find(string(window_name));
	if (it == transformer.window_clauses.end()) {
		throw ParserException("window \"%s\" does not exist", window_name);
	}
	auto copied_expr = it->second->Copy();
	return unique_ptr_cast<ParsedExpression, WindowExpression>(std::move(copied_expr));
}

unique_ptr<WindowExpression>
PEGTransformerFactory::TransformWindowFrameDefinition(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<WindowExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<WindowExpression>
PEGTransformerFactory::TransformWindowFrameContentsParens(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<unique_ptr<WindowExpression>>(extract_parens);
}

bool IsWindowFrameDefault(WindowBoundary start, WindowBoundary end) {
	bool start_is_default = (start == WindowBoundary::UNBOUNDED_PRECEDING);
	bool end_is_default = (end == WindowBoundary::CURRENT_ROW_RANGE);
	return start_is_default && end_is_default;
}

unique_ptr<WindowExpression>
PEGTransformerFactory::TransformWindowFrameNameContentsParens(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0))->Cast<ListParseResult>();
	string window_name;
	transformer.TransformOptional<string>(extract_parens, 0, window_name);
	auto window_frame_contents =
	    transformer.Transform<unique_ptr<WindowExpression>>(extract_parens.Child<ListParseResult>(1));
	if (window_name.empty()) {
		return window_frame_contents;
	}
	auto it = transformer.window_clauses.find(string(window_name));
	if (it == transformer.window_clauses.end()) {
		throw ParserException("window \"%s\" does not exist", window_name);
	}
	auto copied_window = unique_ptr_cast<ParsedExpression, WindowExpression>(it->second->Copy());
	if (copied_window->start_expr || copied_window->end_expr ||
	    !IsWindowFrameDefault(copied_window->start, copied_window->end)) {
		throw ParserException("cannot copy window \"%s\" because it has a frame clause", window_name);
	}
	copied_window->start = window_frame_contents->start;
	copied_window->end = window_frame_contents->end;
	copied_window->exclude_clause = window_frame_contents->exclude_clause;
	copied_window->start_expr = std::move(window_frame_contents->start_expr);
	copied_window->end_expr = std::move(window_frame_contents->end_expr);

	copied_window->offset_expr = std::move(window_frame_contents->offset_expr);
	copied_window->default_expr = std::move(window_frame_contents->default_expr);
	if (!copied_window->orders.empty() && !window_frame_contents->orders.empty()) {
		throw ParserException("Cannot override ORDER BY clause of window \"%s\"", window_name);
	}
	copied_window->orders = std::move(window_frame_contents->orders);
	if (!copied_window->partitions.empty() && !window_frame_contents->partitions.empty()) {
		throw ParserException("Cannot override PARTITION BY clause of window \"%s\"", window_name);
	}
	if (copied_window->partitions.empty()) {
		copied_window->partitions = std::move(window_frame_contents->partitions);
	}
	return copied_window;
}

string PEGTransformerFactory::TransformBaseWindowName(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

unique_ptr<WindowExpression>
PEGTransformerFactory::TransformWindowFrameContents(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	//! Create a dummy result to add modifiers to
	auto result =
	    make_uniq<WindowExpression>(ExpressionType::WINDOW_AGGREGATE, INVALID_CATALOG, INVALID_SCHEMA, string());
	auto partition_opt = list_pr.Child<OptionalParseResult>(0);
	if (partition_opt.HasResult()) {
		result->partitions = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(partition_opt.optional_result);
	}
	auto order_by_opt = list_pr.Child<OptionalParseResult>(1);
	if (order_by_opt.HasResult()) {
		result->orders = transformer.Transform<vector<OrderByNode>>(order_by_opt.optional_result);
		for (auto &order : result->orders) {
			if (order.expression->GetExpressionType() == ExpressionType::STAR) {
				auto &star = order.expression->Cast<StarExpression>();
				if (!star.expr) {
					throw ParserException("Cannot ORDER BY ALL in a window expression");
				}
			}
		}
	}
	auto frame_opt = list_pr.Child<OptionalParseResult>(2);
	if (frame_opt.HasResult()) {
		auto window_frame = transformer.Transform<WindowFrame>(frame_opt.optional_result);
		result->start = window_frame.start;
		result->end = window_frame.end;
		result->start_expr = std::move(window_frame.start_expr);
		result->end_expr = std::move(window_frame.end_expr);
		result->exclude_clause = window_frame.exclude_clause;
	} else {
		result->start = WindowBoundary::UNBOUNDED_PRECEDING;
		result->end = WindowBoundary::CURRENT_ROW_RANGE;
	}
	return result;
}

WindowFrame PEGTransformerFactory::TransformFrameClause(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	WindowFrame result;
	auto framing = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto frame_extent = transformer.Transform<vector<WindowBoundaryExpression>>(list_pr.Child<ListParseResult>(1));
	for (auto &frame : frame_extent) {
		if (StringUtil::CIEquals(framing, "rows")) {
			if (frame.boundary == WindowBoundary::CURRENT_ROW_RANGE) {
				frame.boundary = WindowBoundary::CURRENT_ROW_ROWS;
			} else if (frame.boundary == WindowBoundary::EXPR_PRECEDING_RANGE) {
				frame.boundary = WindowBoundary::EXPR_PRECEDING_ROWS;
			} else if (frame.boundary == WindowBoundary::EXPR_FOLLOWING_RANGE) {
				frame.boundary = WindowBoundary::EXPR_FOLLOWING_ROWS;
			} else if (frame.boundary == WindowBoundary::INVALID) {
				frame.boundary = WindowBoundary::CURRENT_ROW_ROWS;
			}
		} else if (StringUtil::CIEquals(framing, "groups")) {
			if (frame.boundary == WindowBoundary::CURRENT_ROW_RANGE) {
				frame.boundary = WindowBoundary::CURRENT_ROW_GROUPS;
			} else if (frame.boundary == WindowBoundary::EXPR_PRECEDING_RANGE) {
				frame.boundary = WindowBoundary::EXPR_PRECEDING_GROUPS;
			} else if (frame.boundary == WindowBoundary::EXPR_FOLLOWING_RANGE) {
				frame.boundary = WindowBoundary::EXPR_FOLLOWING_GROUPS;
			} else if (frame.boundary == WindowBoundary::INVALID) {
				frame.boundary = WindowBoundary::CURRENT_ROW_GROUPS;
			}
		} else if (StringUtil::CIEquals(framing, "range")) {
			if (frame.boundary == WindowBoundary::INVALID) {
				frame.boundary = WindowBoundary::CURRENT_ROW_RANGE;
			}
		} else {
			throw ParserException("Invalid result from frame: %s", framing);
		}
	}
	if (frame_extent[0].boundary == WindowBoundary::UNBOUNDED_FOLLOWING) {
		throw ParserException("Frame start cannot be UNBOUNDED FOLLOWING");
	}
	result.start = frame_extent[0].boundary;
	if (frame_extent[0].expr) {
		result.start_expr = std::move(frame_extent[0].expr);
	}
	if (frame_extent.size() == 2) {
		if (frame_extent[1].boundary == WindowBoundary::UNBOUNDED_PRECEDING) {
			throw ParserException("Frame end cannot be UNBOUNDED PRECEDING");
		}
		result.end = frame_extent[1].boundary;
		if (frame_extent[1].expr) {
			result.end_expr = std::move(frame_extent[1].expr);
		}
	}
	transformer.TransformOptional<WindowExcludeMode>(list_pr, 2, result.exclude_clause);
	return result;
}

vector<WindowBoundaryExpression> PEGTransformerFactory::TransformFrameExtent(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<vector<WindowBoundaryExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

vector<WindowBoundaryExpression>
PEGTransformerFactory::TransformBetweenFrameExtent(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<WindowBoundaryExpression> result;
	result.push_back(transformer.Transform<WindowBoundaryExpression>(list_pr.Child<ListParseResult>(1)));
	result.push_back(transformer.Transform<WindowBoundaryExpression>(list_pr.Child<ListParseResult>(3)));
	return result;
}

vector<WindowBoundaryExpression>
PEGTransformerFactory::TransformSingleFrameExtent(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<WindowBoundaryExpression> result;
	result.push_back(transformer.Transform<WindowBoundaryExpression>(list_pr.Child<ListParseResult>(0)));
	WindowBoundaryExpression end_current_row;
	end_current_row.boundary = WindowBoundary::INVALID;
	result.push_back(std::move(end_current_row));
	return result;
}

WindowBoundaryExpression PEGTransformerFactory::TransformFrameBound(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<WindowBoundaryExpression>(list_pr.Child<ChoiceParseResult>(0).result);
}

WindowBoundaryExpression PEGTransformerFactory::TransformFrameUnbounded(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool preceding = transformer.Transform<bool>(list_pr.Child<ListParseResult>(1));
	WindowBoundaryExpression result;
	if (preceding) {
		result.boundary = WindowBoundary::UNBOUNDED_PRECEDING;
	} else {
		result.boundary = WindowBoundary::UNBOUNDED_FOLLOWING;
	}
	return result;
}

WindowBoundaryExpression PEGTransformerFactory::TransformFrameExpression(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	WindowBoundaryExpression result;
	result.expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto is_preceding = transformer.Transform<bool>(list_pr.Child<ListParseResult>(1));
	if (is_preceding) {
		// These are placeholders and will be converted to groups/rows/range later
		result.boundary = WindowBoundary::EXPR_PRECEDING_RANGE;
	} else {
		result.boundary = WindowBoundary::EXPR_FOLLOWING_RANGE;
	}
	return result;
}

WindowBoundaryExpression PEGTransformerFactory::TransformFrameCurrentRow(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	WindowBoundaryExpression result;
	// These are placeholders and will be converted to groups/rows/range later
	result.boundary = WindowBoundary::CURRENT_ROW_RANGE;
	return result;
}

bool PEGTransformerFactory::TransformPrecedingOrFollowing(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return StringUtil::CIEquals(choice_pr->Cast<KeywordParseResult>().keyword, "preceding");
}

WindowExcludeMode PEGTransformerFactory::TransformWindowExcludeClause(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<WindowExcludeMode>(list_pr.Child<ListParseResult>(1));
}

string PEGTransformerFactory::TransformFraming(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	return choice_pr->Cast<KeywordParseResult>().keyword;
}

WindowExcludeMode PEGTransformerFactory::TransformWindowExcludeElement(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<WindowExcludeMode>(list_pr.Child<ChoiceParseResult>(0).result);
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformWindowPartition(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expression_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	vector<unique_ptr<ParsedExpression>> result;
	for (auto expression : expression_list) {
		result.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expression));
	}
	return result;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformSpecialFunctionExpression(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformCoalesceExpression(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto expr_list = ExtractParseResultsFromList(extract_parens);
	for (auto expr : expr_list) {
		result->children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformUnpackExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto result = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK);
	result->children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(extract_parens));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTryExpression(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto result = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_TRY);
	result->children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(extract_parens));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformColumnsExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool unpack = list_pr.Child<OptionalParseResult>(0).HasResult();

	auto result = make_uniq<StarExpression>();
	auto expr =
	    transformer.Transform<unique_ptr<ParsedExpression>>(ExtractResultFromParens(list_pr.Child<ListParseResult>(2)));
	if (expr->GetExpressionType() == ExpressionType::STAR) {
		auto star_expr = unique_ptr_cast<ParsedExpression, StarExpression>(std::move(expr));
		if (star_expr->columns) {
			result->expr = std::move(star_expr);
		} else {
			result = std::move(star_expr);
		}
	} else if (expr->GetExpressionType() == ExpressionType::LAMBDA) {
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(make_uniq<StarExpression>());
		children.push_back(std::move(expr));
		auto list_filter = make_uniq<FunctionExpression>("list_filter", std::move(children));
		result->expr = std::move(list_filter);
	} else {
		result->expr = std::move(expr);
	}
	result->columns = true;
	if (unpack) {
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK, std::move(result));
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExtractExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_expressions = ExtractResultFromParens(list_pr.Child<ListParseResult>(1))->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> expr_children;
	expr_children.push_back(
	    transformer.Transform<unique_ptr<ParsedExpression>>(extract_expressions.Child<ListParseResult>(0)));
	expr_children.push_back(
	    transformer.Transform<unique_ptr<ParsedExpression>>(extract_expressions.Child<ListParseResult>(2)));
	return make_uniq<FunctionExpression>(INVALID_CATALOG, "main", "date_part", std::move(expr_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExtractArgument(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	if (choice_pr->type == ParseResultType::IDENTIFIER) {
		return make_uniq<ConstantExpression>(Value(choice_pr->Cast<IdentifierParseResult>().identifier));
	}
	if (choice_pr->type == ParseResultType::STRING) {
		return make_uniq<ConstantExpression>(Value(choice_pr->Cast<StringLiteralParseResult>().result));
	}
	auto date_part = transformer.TransformEnum<DatePartSpecifier>(choice_pr);
	return make_uniq<ConstantExpression>(EnumUtil::ToString(date_part));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLambdaExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto col_id_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(1));
	vector<string> parameters;
	for (auto colid : col_id_list) {
		parameters.push_back(transformer.Transform<string>(colid));
	}
	auto rhs_expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(3));
	auto result = make_uniq<LambdaExpression>(parameters, std::move(rhs_expr));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNullIfExpression(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto nested_list = extract_parens->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> expr_children;
	expr_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(nested_list.Child<ListParseResult>(0)));
	expr_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(nested_list.Child<ListParseResult>(2)));
	return make_uniq<FunctionExpression>("nullif", std::move(expr_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformRowExpression(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto expr_list_opt = extract_parens->Cast<OptionalParseResult>();
	if (!expr_list_opt.HasResult()) {
		throw InvalidInputException("Can't pack nothing into a struct");
	}
	auto expr_list = ExtractParseResultsFromList(expr_list_opt.optional_result);
	vector<unique_ptr<ParsedExpression>> results;
	for (auto expr : expr_list) {
		results.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	auto func_expr = make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "row", std::move(results));
	return std::move(func_expr);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformSubstringExpression(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto substring_arguments = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(extract_parens);
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "substring", std::move(substring_arguments));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSubstringArguments(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ChoiceParseResult>(0).result);
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSubstringExpressionList(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> results;
	auto expr_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	for (const auto expr : expr_list) {
		results.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	return results;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSubstringParameters(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> results;
	results.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.GetChild(0)));
	results.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.GetChild(2)));
	results.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.GetChild(4)));
	return results;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTrimExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto inner_list = extract_parens->Cast<ListParseResult>();
	string function_name = "trim";
	transformer.TransformOptional<string>(inner_list, 0, function_name);
	vector<unique_ptr<ParsedExpression>> trim_expressions;
	auto expr_list = ExtractParseResultsFromList(inner_list.Child<ListParseResult>(2));
	for (auto expr : expr_list) {
		trim_expressions.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	auto trim_source_opt = inner_list.Child<OptionalParseResult>(1);
	if (trim_source_opt.HasResult()) {
		auto trim_source_expr = transformer.Transform<unique_ptr<ParsedExpression>>(trim_source_opt.optional_result);
		if (trim_source_expr) {
			trim_expressions.push_back(std::move(trim_source_expr));
		}
	}
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, function_name, std::move(trim_expressions));
}

string PEGTransformerFactory::TransformTrimDirection(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<string>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTrimSource(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr_opt = list_pr.Child<OptionalParseResult>(0);
	if (expr_opt.HasResult()) {
		return transformer.Transform<unique_ptr<ParsedExpression>>(expr_opt.optional_result);
	}
	return nullptr;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformPositionExpression(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto position_values = extract_parens->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> results;
	//! search_string IN string
	results.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(position_values.Child<ListParseResult>(2)));
	results.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(position_values.Child<ListParseResult>(0)));
	return make_uniq<FunctionExpression>("position", std::move(results));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCastExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool try_cast = transformer.Transform<bool>(list_pr.Child<ListParseResult>(0));
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1))->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(extract_parens.Child<ListParseResult>(0));
	auto type = transformer.Transform<LogicalType>(extract_parens.Child<ListParseResult>(2));
	return make_uniq<CastExpression>(type, std::move(expr), try_cast);
}

bool PEGTransformerFactory::TransformCastOrTryCast(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return StringUtil::Lower(choice_pr.result->Cast<KeywordParseResult>().keyword) == "try_cast";
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCaseExpression(PEGTransformer &transformer,
                                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<CaseExpression>();
	unique_ptr<ParsedExpression> opt_expr;
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 1, opt_expr);

	auto cases_pr = list_pr.Child<RepeatParseResult>(2).children;
	for (auto &case_pr : cases_pr) {
		auto case_expr = transformer.Transform<CaseCheck>(case_pr);
		CaseCheck new_case;
		if (opt_expr) {
			new_case.when_expr = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, opt_expr->Copy(),
			                                                     std::move(case_expr.when_expr));
		} else {
			new_case.when_expr = std::move(case_expr.when_expr);
		}
		new_case.then_expr = std::move(case_expr.then_expr);
		result->case_checks.push_back(std::move(new_case));
	}
	auto else_expr_opt = list_pr.Child<OptionalParseResult>(3);
	if (else_expr_opt.HasResult()) {
		result->else_expr = transformer.Transform<unique_ptr<ParsedExpression>>(else_expr_opt.optional_result);
	} else {
		result->else_expr = make_uniq<ConstantExpression>(Value());
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCaseElse(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

CaseCheck PEGTransformerFactory::TransformCaseWhenThen(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	CaseCheck result;
	result.when_expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	result.then_expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(3));
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTypeLiteral(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto colid = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto type = LogicalType(TransformStringToLogicalTypeId(colid));
	if (type == LogicalTypeId::UNBOUND) {
		type = LogicalType::UNBOUND(make_uniq<TypeExpression>(colid, vector<unique_ptr<ParsedExpression>>()));
	}
	auto string_literal = list_pr.Child<StringLiteralParseResult>(1).result;
	auto child = make_uniq<ConstantExpression>(Value(string_literal));
	auto result = make_uniq<CastExpression>(type, std::move(child));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformDefaultExpression(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	return make_uniq<DefaultExpression>();
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIntervalLiteral(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	DatePartSpecifier interval_unit = DatePartSpecifier::INVALID;
	transformer.TransformOptional<DatePartSpecifier>(list_pr, 2, interval_unit);
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	auto func_name = DateTruncSimplificationRule::DatePartToFunc(interval_unit);
	if (func_name.empty()) {
		expr = make_uniq<CastExpression>(LogicalType::INTERVAL, std::move(expr));
		return expr;
	}
	LogicalType parse_type = LogicalType::DOUBLE;
	expr = make_uniq<CastExpression>(parse_type, std::move(expr));
	auto target_type = GetIntervalTargetType(interval_unit);
	if (target_type != parse_type) {
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(expr));
		expr = make_uniq<FunctionExpression>("trunc", std::move(children));
		expr = make_uniq<CastExpression>(target_type, std::move(expr));
	}
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(expr));
	auto result = make_uniq<FunctionExpression>(func_name, std::move(children));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIntervalParameter(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	if (choice_pr->type == ParseResultType::STRING) {
		return make_uniq<ConstantExpression>(Value(choice_pr->Cast<StringLiteralParseResult>().result));
	}
	return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformSubqueryExpression(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool is_not = list_pr.Child<OptionalParseResult>(0).HasResult();
	bool is_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto subquery_reference = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(2));

	auto result = make_uniq<SubqueryExpression>();
	if (is_exists) {
		result->subquery_type = SubqueryType::EXISTS;
	} else {
		result->subquery_type = SubqueryType::SCALAR;
	}
	auto select_statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(make_uniq<StarExpression>());
	select_node->from_table = std::move(subquery_reference);
	select_statement->node = std::move(select_node);
	result->subquery = std::move(select_statement);
	if (is_not) {
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(result));
		auto not_operator = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(children));
		return std::move(not_operator);
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformMapExpression(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto children = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(1));
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "map", std::move(children));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformMapStructExpression(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> keys;
	vector<unique_ptr<ParsedExpression>> values;
	auto map_struct_opt = list_pr.Child<OptionalParseResult>(1);

	if (map_struct_opt.HasResult()) {
		auto field_list = ExtractParseResultsFromList(map_struct_opt.optional_result);
		for (auto &field : field_list) {
			// Get the pair {key, value} from the field transformer
			auto key_val_pair = transformer.Transform<vector<unique_ptr<ParsedExpression>>>(field);
			keys.push_back(std::move(key_val_pair[0]));
			values.push_back(std::move(key_val_pair[1]));
		}
	}
	vector<unique_ptr<ParsedExpression>> result;
	result.push_back(make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "list_value", std::move(keys)));
	result.push_back(make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "list_value", std::move(values)));
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformMapStructField(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<unique_ptr<ParsedExpression>> fields;
	fields.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0)));
	fields.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2)));
	return fields;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformListComprehensionExpression(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	// 1. Extract base components
	auto result_expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	auto col_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(3));
	auto in_expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(5));
	auto list_comprehension_filter = list_pr.Child<OptionalParseResult>(6);

	vector<string> lambda_columns;
	for (auto col : col_list) {
		lambda_columns.push_back(transformer.Transform<string>(col));
	}

	// Basic Case: No Filter
	if (!list_comprehension_filter.HasResult()) {
		auto lambda_expression = make_uniq<LambdaExpression>(lambda_columns, std::move(result_expr));
		vector<unique_ptr<ParsedExpression>> apply_children;
		apply_children.push_back(std::move(in_expr));
		apply_children.push_back(std::move(lambda_expression));

		return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "list_apply", std::move(apply_children));
	}

	// --- WITH FILTER: 3-Stage Transformation ---
	auto filter_expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_comprehension_filter.optional_result);

	// STAGE 1: list_apply(in_expr, x -> struct_pack(filter := ..., result := ...))
	filter_expr->alias = "filter";
	result_expr->alias = "result";

	vector<unique_ptr<ParsedExpression>> struct_children;
	struct_children.push_back(std::move(filter_expr));
	struct_children.push_back(std::move(result_expr));
	auto struct_pack =
	    make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "struct_pack", std::move(struct_children));

	auto stage1_lambda = make_uniq<LambdaExpression>(lambda_columns, std::move(struct_pack));
	vector<unique_ptr<ParsedExpression>> stage1_apply_args;
	stage1_apply_args.push_back(std::move(in_expr));
	stage1_apply_args.push_back(std::move(stage1_lambda));
	auto stage1_apply =
	    make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "list_apply", std::move(stage1_apply_args));

	// STAGE 2: list_filter(stage1, elem -> struct_extract(elem, 'filter'))
	auto elem_ref_filter = make_uniq<ColumnRefExpression>("elem");
	auto filter_const = make_uniq<ConstantExpression>(Value("filter"));
	vector<unique_ptr<ParsedExpression>> extract_filter_args;
	extract_filter_args.push_back(std::move(elem_ref_filter));
	extract_filter_args.push_back(std::move(filter_const));
	auto filter_extract = make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "struct_extract",
	                                                    std::move(extract_filter_args));

	auto stage2_lambda = make_uniq<LambdaExpression>(vector<string> {"elem"}, std::move(filter_extract));
	vector<unique_ptr<ParsedExpression>> stage2_filter_args;
	stage2_filter_args.push_back(std::move(stage1_apply));
	stage2_filter_args.push_back(std::move(stage2_lambda));
	auto stage2_filter =
	    make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "list_filter", std::move(stage2_filter_args));

	// STAGE 3: list_apply(stage2, elem -> struct_extract(elem, 'result'))
	auto elem_ref_result = make_uniq<ColumnRefExpression>("elem");
	auto result_const = make_uniq<ConstantExpression>(Value("result"));
	vector<unique_ptr<ParsedExpression>> extract_result_args;
	extract_result_args.push_back(std::move(elem_ref_result));
	extract_result_args.push_back(std::move(result_const));
	auto result_extract = make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "struct_extract",
	                                                    std::move(extract_result_args));

	auto stage3_lambda = make_uniq<LambdaExpression>(vector<string> {"elem"}, std::move(result_extract));
	vector<unique_ptr<ParsedExpression>> stage3_apply_args;
	stage3_apply_args.push_back(std::move(stage2_filter));
	stage3_apply_args.push_back(std::move(stage3_lambda));

	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "list_apply", std::move(stage3_apply_args));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformListComprehensionFilter(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

case_insensitive_map_t<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformReplaceList(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(
	    list_pr.Child<ListParseResult>(1));
}

case_insensitive_map_t<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformReplaceEntries(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(
	    list_pr.Child<ChoiceParseResult>(0).result);
}

case_insensitive_map_t<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformReplaceEntrySingle(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto replace_entry =
	    transformer.Transform<pair<string, unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(0));
	case_insensitive_map_t<unique_ptr<ParsedExpression>> entry_map;
	entry_map.insert(std::move(replace_entry));
	return entry_map;
}

case_insensitive_map_t<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformReplaceEntryList(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto entry_list = ExtractParseResultsFromList(extract_parens);
	case_insensitive_map_t<unique_ptr<ParsedExpression>> entry_map;
	for (auto entry : entry_list) {
		auto replace_entry = transformer.Transform<pair<string, unique_ptr<ParsedExpression>>>(entry);
		if (entry_map.find(replace_entry.first) != entry_map.end()) {
			throw ParserException("Duplicate entry \"%s\" in REPLACE list", replace_entry.first);
		}
		entry_map.insert(std::move(replace_entry));
	}
	return entry_map;
}

pair<string, unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformReplaceEntry(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto column_reference = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	if (column_reference->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
		throw InternalException("Expected a column reference in the replace entry");
	}
	auto &col_ref = column_reference->Cast<ColumnRefExpression>();
	auto column_name = col_ref.GetColumnName();
	return make_pair(column_name, std::move(expr));
}

ExpressionType PEGTransformerFactory::TransformIsDistinctFromOp(PEGTransformer &transformer,
                                                                optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	if (list_pr.Child<OptionalParseResult>(1).HasResult()) {
		return ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	}
	return ExpressionType::COMPARE_DISTINCT_FROM;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformGroupingExpression(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto expr_list = ExtractParseResultsFromList(extract_parens);
	vector<unique_ptr<ParsedExpression>> grouping_expressions;
	for (auto expr : expr_list) {
		grouping_expressions.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	auto result = make_uniq<OperatorExpression>(ExpressionType::GROUPING_FUNCTION, std::move(grouping_expressions));
	return std::move(result);
}

qualified_column_map_t<string> PEGTransformerFactory::TransformRenameList(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto inner_list = list_pr.Child<ListParseResult>(1);
	return transformer.Transform<qualified_column_map_t<string>>(inner_list.Child<ChoiceParseResult>(0).result);
}

qualified_column_map_t<string> PEGTransformerFactory::TransformRenameEntryList(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto entry_list = ExtractParseResultsFromList(extract_parens);
	qualified_column_map_t<string> result;
	for (auto entry : entry_list) {
		auto rename_entry = transformer.Transform<pair<QualifiedColumnName, string>>(entry);
		result[rename_entry.first] = rename_entry.second;
	}
	return result;
}

qualified_column_map_t<string>
PEGTransformerFactory::TransformSingleRenameEntry(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	qualified_column_map_t<string> result;
	auto rename_entry = transformer.Transform<pair<QualifiedColumnName, string>>(list_pr.GetChild(0));
	result[rename_entry.first] = rename_entry.second;
	return result;
}

pair<QualifiedColumnName, string> PEGTransformerFactory::TransformRenameEntry(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto column_name = transformer.Transform<QualifiedColumnName>(list_pr.GetChild(0));
	auto alias = list_pr.Child<IdentifierParseResult>(2).identifier;
	return make_pair(column_name, alias);
}

bool PEGTransformerFactory::TransformIgnoreOrRespectNulls(PEGTransformer &transformer,
                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<bool>(list_pr.Child<ChoiceParseResult>(0).result);
}

} // namespace duckdb
