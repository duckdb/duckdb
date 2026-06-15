#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/enums/subquery_type.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/optimizer/rule/date_trunc_simplification.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
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
#include "duckdb/common/string_util.hpp"

namespace duckdb {

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformExpressionStatement(PEGTransformer &transformer,
                                                    vector<unique_ptr<ParsedExpression>> expression_alias) {
	auto expressions = std::move(expression_alias);
	auto select_statement = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();

	bool any_column_ref = false;
	for (auto &expr : expressions) {
		if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			any_column_ref = true;
			break;
		}
	}

	if (any_column_ref && expressions.size() > 1) {
		throw ParserException("Mix of table names and expressions is not supported. "
		                      "Use SELECT to explicitly specify what you want to query.");
	}

	if (any_column_ref) {
		// Single COLUMN_REF: treat as table scan
		auto &col_expr = expressions[0]->Cast<ColumnRefExpression>();
		if (col_expr.IsQualified()) {
			if (col_expr.ColumnNames().size() >= 4) {
				throw ParserException(
				    "Too many qualifiers encountered. Expected \"catalog.schema.table\" or \"schema.table\"");
			}
			if (col_expr.ColumnNames().size() == 3) {
				auto table_description =
				    TableDescription(col_expr.ColumnNames()[0], col_expr.ColumnNames()[1], col_expr.ColumnNames()[2]);
				select_node->from_table = make_uniq<BaseTableRef>(table_description);
			} else if (col_expr.ColumnNames().size() == 2) {
				auto table_description =
				    TableDescription(INVALID_CATALOG, col_expr.ColumnNames()[0], col_expr.ColumnNames()[1]);
				select_node->from_table = make_uniq<BaseTableRef>(table_description);
			}
		} else {
			auto base_table = make_uniq<BaseTableRef>();
			base_table->table_name = col_expr.GetColumnName();
			select_node->from_table = std::move(base_table);
		}
		select_node->select_list.push_back(make_uniq<StarExpression>());
	} else {
		for (auto &expr : expressions) {
			select_node->select_list.push_back(std::move(expr));
		}
		select_node->from_table = make_uniq<EmptyTableRef>();
	}

	select_statement->node = std::move(select_node);
	return std::move(select_statement);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExpressionAlias(PEGTransformer &transformer,
                                                                             ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformBaseExpression(PEGTransformer &transformer, unique_ptr<ParsedExpression> single_expression,
                                               optional<vector<unique_ptr<ParsedExpression>>> indirection_list) {
	auto expr = std::move(single_expression);
	if (!indirection_list) {
		return expr;
	}

	bool prev_indirection_was_cast = false;
	for (auto &indirection_expr : *indirection_list) {
		if (indirection_expr->GetExpressionClass() == ExpressionClass::CAST) {
			auto cast_expr = unique_ptr_cast<ParsedExpression, CastExpression>(std::move(indirection_expr));
			cast_expr->ChildMutable() = std::move(expr);
			expr = std::move(cast_expr);
			prev_indirection_was_cast = true;
		} else if (indirection_expr->GetExpressionClass() == ExpressionClass::OPERATOR) {
			if (prev_indirection_was_cast) {
				throw ParserException(
				    "Subscript/slice cannot be applied directly after a cast operator (e.g. x::TYPE[1:3] is not "
				    "allowed). Wrap the cast in parentheses: (x::TYPE)[1:3]");
			}
			auto operator_expr = unique_ptr_cast<ParsedExpression, OperatorExpression>(std::move(indirection_expr));
			operator_expr->GetChildrenMutable().insert(operator_expr->GetChildrenMutable().begin(), std::move(expr));
			expr = std::move(operator_expr);
			prev_indirection_was_cast = false;
		} else if (indirection_expr->GetExpressionClass() == ExpressionClass::FUNCTION) {
			auto function_expr = unique_ptr_cast<ParsedExpression, FunctionExpression>(std::move(indirection_expr));
			function_expr->GetArgumentsMutable().insert(function_expr->GetArgumentsMutable().begin(), std::move(expr));
			expr = std::move(function_expr);
			prev_indirection_was_cast = false;
		} else if (indirection_expr->GetExpressionClass() == ExpressionClass::CONSTANT) {
			vector<unique_ptr<ParsedExpression>> struct_children;
			struct_children.push_back(std::move(expr));
			struct_children.push_back(std::move(indirection_expr));
			auto struct_expr =
			    make_uniq<OperatorExpression>(ExpressionType::STRUCT_EXTRACT, std::move(struct_children));
			expr = std::move(struct_expr);
			prev_indirection_was_cast = false;
		} else {
			throw NotImplementedException("Unhandled case for Base Expression with indirection");
		}
	}
	return expr;
}

// ColumnReference <- CatalogReservedSchemaTableColumnName / SchemaReservedTableColumnName / TableReservedColumnName /
// NestedColumnName
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformColumnReference(PEGTransformer &transformer,
                                                                             unique_ptr<ColumnRefExpression> child) {
	return std::move(child);
}

unique_ptr<ColumnRefExpression> PEGTransformerFactory::TransformCatalogReservedSchemaTableColumnName(
    PEGTransformer &transformer, const Identifier &catalog_qualification,
    const Identifier &reserved_schema_qualification, const Identifier &reserved_table_qualification,
    const Identifier &reserved_column_name) {
	vector<Identifier> column_names;
	column_names.push_back(catalog_qualification);
	column_names.push_back(reserved_schema_qualification);
	column_names.push_back(reserved_table_qualification);
	column_names.push_back(reserved_column_name);
	return make_uniq<ColumnRefExpression>(std::move(column_names));
}

unique_ptr<ColumnRefExpression> PEGTransformerFactory::TransformSchemaReservedTableColumnName(
    PEGTransformer &transformer, const Identifier &schema_qualification, const Identifier &reserved_table_qualification,
    const Identifier &reserved_column_name) {
	vector<Identifier> column_names;
	column_names.push_back(schema_qualification);
	column_names.push_back(reserved_table_qualification);
	column_names.push_back(reserved_column_name);
	return make_uniq<ColumnRefExpression>(std::move(column_names));
}

Identifier PEGTransformerFactory::TransformReservedTableQualification(PEGTransformer &transformer,
                                                                      const Identifier &reserved_table_name) {
	return reserved_table_name;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformFunctionExpression(PEGTransformer &transformer,
                                                                                ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto qualified_function = transformer.Transform<QualifiedName>(list_pr.Child<ListParseResult>(0));
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1)).Cast<ListParseResult>();
	bool distinct = false;
	transformer.TransformOptional<bool>(extract_parens, 0, distinct);

	auto &function_arg_opt = extract_parens.Child<OptionalParseResult>(1);
	vector<FunctionArgument> function_children;
	if (function_arg_opt.HasResult()) {
		auto function_argument_list = ExtractParseResultsFromList(function_arg_opt.GetResult());
		for (auto function_argument : function_argument_list) {
			function_children.push_back(transformer.Transform<FunctionArgument>(function_argument));
		}
	}
	auto order_modifier = make_uniq<OrderModifier>();
	transformer.TransformOptional<vector<OrderByNode>>(extract_parens, 2, order_modifier->orders);

	unique_ptr<ParsedExpression> filter_expr;
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 3, filter_expr);
	bool ignore_nulls = false;
	bool has_ignore_nulls_result = extract_parens.Child<OptionalParseResult>(3).HasResult();
	transformer.TransformOptional<bool>(extract_parens, 3, ignore_nulls);

	auto &export_opt = list_pr.Child<OptionalParseResult>(4);
	if (function_children.size() == 1 && ExpressionIsEmptyStar(*function_children[0].GetExpressionMutable()) &&
	    !distinct && order_modifier->orders.empty()) {
		// COUNT(*) gets converted into COUNT()
		function_children.clear();
	}
	auto lowercase_name = StringUtil::Lower(qualified_function.name.GetIdentifierName());

	auto &over_opt = list_pr.Child<OptionalParseResult>(5);
	if (over_opt.HasResult()) {
		if (transformer.in_window_definition) {
			throw ParserException("window functions are not allowed in window definitions");
		}
		//	We map first/last OVER() to first_value/last_value.
		//	Not sure the semantics match, but we are stuck with it.
		if (lowercase_name == "first" || lowercase_name == "last") {
			lowercase_name += "_value";
		}

		if (export_opt.HasResult()) {
			throw ParserException("EXPORT_STATE is not supported for window functions!");
		}

		transformer.in_window_definition = true;
		auto expr = transformer.Transform<unique_ptr<WindowExpression>>(over_opt.GetResult());
		expr->CatalogMutable() = qualified_function.catalog;
		expr->SchemaMutable() = qualified_function.schema;
		expr->SetFunctionName(lowercase_name);

		for (auto &arg : function_children) {
			expr->GetArgumentsMutable().push_back(std::move(arg));
		}

		expr->HasIgnoreNullsMutable() = has_ignore_nulls_result;
		expr->IgnoreNullsMutable() = ignore_nulls;
		expr->FilterMutable() = std::move(filter_expr);
		expr->ArgOrdersMutable() = std::move(order_modifier->orders);
		expr->DistinctMutable() = distinct;
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
		for (auto &arg : function_children) {
			if (arg.HasName()) {
				throw ParserException("Named arguments are not supported in IF expressions");
			}
		}

		auto expr = make_uniq<CaseExpression>();
		CaseCheck check;
		check.when_expr = std::move(function_children[0].GetExpressionMutable());
		check.then_expr = std::move(function_children[1].GetExpressionMutable());
		expr->CaseChecksMutable().push_back(std::move(check));
		expr->ElseMutable() = std::move(function_children[2].GetExpressionMutable());
		return std::move(expr);
	}
	if (lowercase_name == "unpack") {
		if (function_children.size() != 1) {
			throw ParserException("Wrong number of arguments to the UNPACK operator");
		}
		auto expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK);
		for (auto &arg : function_children) {
			if (arg.HasName()) {
				throw ParserException("Named arguments are not supported in UNPACK operator");
			}
			expr->GetChildrenMutable().push_back(std::move(arg.GetExpressionMutable()));
		}
		return std::move(expr);
	}
	if (lowercase_name == "try") {
		if (function_children.size() != 1) {
			throw ParserException("Wrong number of arguments provided to TRY expression");
		}
		auto try_expression = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_TRY);
		for (auto &arg : function_children) {
			if (arg.HasName()) {
				throw ParserException("Named arguments are not supported in TRY expression");
			}
			try_expression->GetChildrenMutable().push_back(std::move(arg.GetExpressionMutable()));
		}
		return std::move(try_expression);
	}
	if (lowercase_name == "construct_array") {
		auto construct_array = make_uniq<OperatorExpression>(ExpressionType::ARRAY_CONSTRUCTOR);
		for (auto &arg : function_children) {
			if (arg.HasName()) {
				throw ParserException("Named arguments are not supported in array constructors");
			}
			construct_array->GetChildrenMutable().push_back(std::move(arg.GetExpressionMutable()));
		}
		return std::move(construct_array);
	}
	if (lowercase_name == "ifnull") {
		if (function_children.size() != 2) {
			throw ParserException("Wrong number of arguments to IFNULL.");
		}
		for (auto &arg : function_children) {
			if (arg.HasName()) {
				throw ParserException("Named arguments are not supported in IFNULL expressions");
			}
		}

		//  Two-argument COALESCE
		auto coalesce_op = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
		coalesce_op->GetChildrenMutable().push_back(std::move(function_children[0].GetExpressionMutable()));
		coalesce_op->GetChildrenMutable().push_back(std::move(function_children[1].GetExpressionMutable()));
		return std::move(coalesce_op);
	}
	if (lowercase_name == "date") {
		if (function_children.size() != 1) {
			throw ParserException("Wrong number of arguments provided to DATE function");
		}
		return std::move(
		    make_uniq<CastExpression>(LogicalType::DATE, std::move(function_children[0].GetExpressionMutable())));
	}
	if (has_ignore_nulls_result) {
		throw ParserException("RESPECT/IGNORE NULLS is not supported for non-window functions");
	}
	auto &within_group_opt = list_pr.Child<OptionalParseResult>(2);
	if (within_group_opt.HasResult()) {
		auto order_by_clause = transformer.Transform<vector<OrderByNode>>(within_group_opt.GetResult());
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
	auto result = make_uniq<FunctionExpression>(
	    qualified_function.catalog, qualified_function.schema, Identifier(lowercase_name), std::move(function_children),
	    std::move(filter_expr), std::move(order_modifier), distinct, false, export_opt.HasResult());

	return std::move(result);
}

vector<OrderByNode> PEGTransformerFactory::TransformWithinGroupClause(PEGTransformer &transformer,
                                                                      vector<OrderByNode> order_by_clause) {
	return order_by_clause;
}

bool PEGTransformerFactory::TransformDistinctKeyword(PEGTransformer &transformer) {
	return true;
}

bool PEGTransformerFactory::TransformAllKeyword(PEGTransformer &transformer) {
	return false;
}

QualifiedName PEGTransformerFactory::TransformFunctionIdentifier(PEGTransformer &transformer,
                                                                 ParseResult &choice_result) {
	if (choice_result.type == ParseResultType::IDENTIFIER) {
		QualifiedName result;
		result.catalog = INVALID_CATALOG;
		result.schema = INVALID_SCHEMA;
		result.name = choice_result.Cast<IdentifierParseResult>().identifier;
		return result;
	}
	return transformer.Transform<QualifiedName>(choice_result);
}

QualifiedName PEGTransformerFactory::TransformSchemaReservedFunctionName(PEGTransformer &transformer,
                                                                         const Identifier &schema_qualification,
                                                                         const Identifier &reserved_function_name) {
	QualifiedName result;
	result.catalog = INVALID_CATALOG;
	result.schema = schema_qualification;
	result.name = reserved_function_name;
	return result;
}

QualifiedName PEGTransformerFactory::TransformCatalogReservedSchemaFunctionName(
    PEGTransformer &transformer, const Identifier &catalog_qualification,
    const optional<Identifier> &reserved_schema_qualification, const Identifier &reserved_function_name) {
	QualifiedName result;
	if (reserved_schema_qualification) {
		result.catalog = catalog_qualification;
		result.schema = *reserved_schema_qualification;
	} else {
		result.catalog = INVALID_CATALOG;
		result.schema = catalog_qualification;
	}
	result.name = reserved_function_name;
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformArrayBoundedListExpression(
    PEGTransformer &transformer, const bool &has_result, vector<unique_ptr<ParsedExpression>> bounded_list_expression) {
	bool is_array = has_result;
	if (!is_array) {
		return make_uniq<FunctionExpression>(INVALID_CATALOG, "main", "list_value", std::move(bounded_list_expression));
	}
	return make_uniq<OperatorExpression>(ExpressionType::ARRAY_CONSTRUCTOR, std::move(bounded_list_expression));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformFilterClause(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(1));
	auto &inner_list = extract_parens.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(inner_list.Child<ListParseResult>(1));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformParenthesisExpression(PEGTransformer &transformer,
                                                      vector<unique_ptr<ParsedExpression>> expression) {
	// ParenthesisExpression <- Parens(List(Expression))
	if (expression.size() == 1) {
		return std::move(expression[0]);
	}
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "row", std::move(expression));
}

void PEGTransformerFactory::RemoveOrderQualificationRecursive(unique_ptr<ParsedExpression> &root_expr) {
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(
	    *root_expr, [&](ColumnRefExpression &col_ref) {
		    auto &col_names = col_ref.ColumnNamesMutable();
		    if (col_names.size() > 1) {
			    col_names = vector<Identifier> {col_names.back()};
		    }
	    });
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformArrayParensSelect(PEGTransformer &transformer,
                                                  unique_ptr<SelectStatement> select_statement_internal) {
	auto subquery_expr = make_uniq<SubqueryExpression>();
	subquery_expr->SubqueryMutable() = std::move(select_statement_internal);
	// ARRAY expression
	// wrap subquery into
	// "SELECT CASE WHEN ARRAY_AGG(col) IS NULL THEN [] ELSE ARRAY_AGG(col) END FROM (...) tbl"
	auto select_node = make_uniq<SelectNode>();
	unique_ptr<ParsedExpression> array_agg_child;
	optional_ptr<SelectNode> sub_select;
	if (subquery_expr->Subquery()->node->type == QueryNodeType::SELECT_NODE) {
		// easy case - subquery is a SELECT
		sub_select = subquery_expr->Subquery()->node->Cast<SelectNode>();
		if (sub_select->select_list.size() != 1) {
			throw BinderException(*subquery_expr, "Subquery returns %zu columns - expected 1",
			                      sub_select->select_list.size());
		}
		array_agg_child = make_uniq<PositionalReferenceExpression>(1ULL);
	} else {
		// subquery is not a SELECT but a UNION or CTE
		// we can still support this but it is more challenging since we can't push columns for the ORDER BY
		auto columns_star = make_uniq<StarExpression>();
		columns_star->IsColumnsMutable() = true;
		array_agg_child = std::move(columns_star);
	}

	// ARRAY_AGG(COLUMNS(*))
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(array_agg_child));
	auto aggr = make_uniq<FunctionExpression>("array_agg", std::move(children));
	// push ORDER BY modifiers into the array_agg
	for (auto &modifier : subquery_expr->SubqueryMutable()->node->modifiers) {
		if (modifier->type == ResultModifierType::ORDER_MODIFIER) {
			aggr->OrderByMutable() = unique_ptr_cast<ResultModifier, OrderModifier>(modifier->Copy());
			break;
		}
	}
	// transform constants (e.g. ORDER BY 1) into positional references (ORDER BY #1)
	idx_t array_idx = 0;
	if (aggr->OrderBy()) {
		for (auto &order : aggr->OrderByMutable()->orders) {
			if (order.expression->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
				auto &constant_expr = order.expression->Cast<ConstantExpression>();
				Value bigint_value;
				string error;
				if (constant_expr.GetValue().DefaultTryCastAs(LogicalType::BIGINT, bigint_value, &error)) {
					int64_t order_index = BigIntValue::Get(bigint_value);
					idx_t positional_index = order_index < 0 ? NumericLimits<idx_t>::Maximum() : idx_t(order_index);
					order.expression = make_uniq<PositionalReferenceExpression>(positional_index);
				}
			} else if (sub_select) {
				// if we have a SELECT we can push the ORDER BY clause into the SELECT list and reference it
				auto alias = "__array_internal_idx_" + to_string(++array_idx);
				order.expression->SetAlias(Identifier(alias));
				sub_select->select_list.push_back(std::move(order.expression));
				order.expression = make_uniq<ColumnRefExpression>(Identifier(alias));
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
	case_expr->CaseChecksMutable().push_back(std::move(check));
	case_expr->ElseMutable() = std::move(aggr);

	select_node->select_list.push_back(std::move(case_expr));

	// FROM (...) tbl
	auto child_subquery = make_uniq<SubqueryRef>(std::move(subquery_expr->SubqueryMutable()));
	select_node->from_table = std::move(child_subquery);

	auto new_subquery = make_uniq<SelectStatement>();
	new_subquery->node = std::move(select_node);
	subquery_expr->SubqueryMutable() = std::move(new_subquery);

	subquery_expr->GetSubqueryTypeMutable() = SubqueryType::SCALAR;
	return std::move(subquery_expr);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformStructExpression(PEGTransformer &transformer,
                                                                              vector<FunctionArgument> struct_field) {
	auto func_name = "struct_pack";
	return make_uniq<FunctionExpression>(INVALID_CATALOG, "main", func_name, std::move(struct_field));
}

FunctionArgument PEGTransformerFactory::TransformStructField(PEGTransformer &transformer,
                                                             const Identifier &col_id_or_string,
                                                             unique_ptr<ParsedExpression> expression) {
	auto alias = col_id_or_string.GetIdentifierName();
	expression->SetAlias(Identifier(alias));

	return FunctionArgument(Identifier(std::move(alias)), std::move(expression));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformBoundedListExpression(PEGTransformer &transformer,
                                                      optional<vector<unique_ptr<ParsedExpression>>> expression) {
	if (expression) {
		return std::move(*expression);
	}
	return vector<unique_ptr<ParsedExpression>>();
}

// Expression <- LambdaArrowExpression
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExpression(PEGTransformer &transformer,
                                                                        ParseResult &parse_result) {
	auto stack_check = transformer.StackCheck();
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLambdaArrowExpression(PEGTransformer &transformer,
                                                                                   ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &lambda_opt = list_pr.Child<OptionalParseResult>(1);
	if (!lambda_opt.HasResult()) {
		return expr;
	}
	// Each child is a SingleArrowPair ListParseResult: ['->', LogicalOrExpression]
	auto &inner_lambda_list = lambda_opt.GetResult().Cast<RepeatParseResult>();
	for (auto pair_node : inner_lambda_list.GetChildren()) {
		auto &pair_list = pair_node.get().Cast<ListParseResult>(); // SingleArrowPair
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(pair_list.Child<ListParseResult>(1));
		expr = make_uniq<LambdaExpression>(std::move(expr), std::move(right_expr));
	}
	return expr;
}

// LogicalOrExpression <- LogicalAndExpression ('OR' LogicalAndExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLogicalOrExpression(PEGTransformer &transformer,
                                                                                 ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &or_expr_opt = list_pr.Child<OptionalParseResult>(1);
	if (!or_expr_opt.HasResult()) {
		return expr;
	}
	auto &or_expr_repeat = or_expr_opt.GetResult().Cast<RepeatParseResult>();
	auto or_depth_guard = transformer.StackCheck(or_expr_repeat.GetChildren().size());
	for (auto &or_expr : or_expr_repeat.GetChildren()) {
		auto &inner_list_pr = or_expr.get().Cast<ListParseResult>();
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		expr = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(expr), std::move(right_expr));
	}
	return expr;
}

// LogicalAndExpression <- LogicalNotExpression ('AND' LogicalNotExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLogicalAndExpression(PEGTransformer &transformer,
                                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &and_expr_opt = list_pr.Child<OptionalParseResult>(1);
	if (!and_expr_opt.HasResult()) {
		return expr;
	}
	auto &and_expr_repeat = and_expr_opt.GetResult().Cast<RepeatParseResult>();
	auto and_depth_guard = transformer.StackCheck(and_expr_repeat.GetChildren().size());
	for (auto &and_expr : and_expr_repeat.GetChildren()) {
		auto &inner_list_pr = and_expr.get().Cast<ListParseResult>();
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		expr =
		    make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(expr), std::move(right_expr));
	}
	return expr;
}

// LogicalNotExpression <- 'NOT'* IsExpression
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLogicalNotExpression(PEGTransformer &transformer,
                                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	auto &not_expr_opt = list_pr.Child<OptionalParseResult>(0);
	if (!not_expr_opt.HasResult()) {
		return expr;
	}
	auto &not_expr_repeat = not_expr_opt.GetResult().Cast<RepeatParseResult>();
	size_t n = not_expr_repeat.GetChildren().size();
	for (size_t i = 0; i < n; i++) {
		vector<unique_ptr<ParsedExpression>> inner_list_children;
		inner_list_children.push_back(std::move(expr));
		expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(inner_list_children));
	}
	return expr;
}

// IsExpression <- IsDistinctFromExpression IsTest*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsExpression(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &is_test_opt = list_pr.Child<OptionalParseResult>(1);
	if (!is_test_opt.HasResult()) {
		return expr;
	}
	auto &is_test_expr_repeat = is_test_opt.GetResult().Cast<RepeatParseResult>();
	for (auto &is_test_expr : is_test_expr_repeat.GetChildren()) {
		auto is_expr = transformer.Transform<unique_ptr<ParsedExpression>>(is_test_expr);
		if (is_expr->GetExpressionClass() == ExpressionClass::COMPARISON) {
			auto compare_expr = unique_ptr_cast<ParsedExpression, ComparisonExpression>(std::move(is_expr));
			compare_expr->LeftMutable() = make_uniq<CastExpression>(LogicalType::BOOLEAN, std::move(expr));
			expr = std::move(compare_expr);
		} else if (is_expr->GetExpressionClass() == ExpressionClass::OPERATOR) {
			auto operator_expr = unique_ptr_cast<ParsedExpression, OperatorExpression>(std::move(is_expr));
			operator_expr->GetChildrenMutable().insert(operator_expr->GetChildrenMutable().begin(), std::move(expr));
			expr = std::move(operator_expr);
		} else {
			throw InternalException("Unexpected expression encountered in IsExpression: %s",
			                        ExpressionClassToString(is_expr->GetExpressionClass()));
		}
	}
	return expr;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsTest(PEGTransformer &transformer,
                                                                    ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsLiteral(PEGTransformer &transformer,
                                                                       ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &not_expr = list_pr.Child<OptionalParseResult>(1);
	auto &inner_list_pr = list_pr.Child<ListParseResult>(2);
	auto literal_value = transformer.Transform<Value>(inner_list_pr.Child<ChoiceParseResult>(0).GetResult());
	if (literal_value.IsNull()) {
		auto expr_type = not_expr.HasResult() ? ExpressionType::OPERATOR_IS_NOT_NULL : ExpressionType::OPERATOR_IS_NULL;
		return make_uniq<OperatorExpression>(expr_type, nullptr);
	}
	auto expr_type =
	    not_expr.HasResult() ? ExpressionType::COMPARE_DISTINCT_FROM : ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	return make_uniq<ComparisonExpression>(expr_type, nullptr, make_uniq<ConstantExpression>(literal_value));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformNotNull(PEGTransformer &transformer,
                                                                     ParseResult &parse_result) {
	return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, nullptr);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsNull(PEGTransformer &transformer,
                                                                    ParseResult &parse_result) {
	return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, nullptr);
}

// IsDistinctFromExpression <- ComparisonExpression (IsDistinctFromOp ComparisonExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIsDistinctFromExpression(PEGTransformer &transformer,
                                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &is_test_opt = list_pr.Child<OptionalParseResult>(1);
	if (!is_test_opt.HasResult()) {
		return expr;
	}
	auto &is_distinct_repeat = is_test_opt.GetResult().Cast<RepeatParseResult>();
	for (auto &is_distinct : is_distinct_repeat.GetChildren()) {
		auto &distinct_list = is_distinct.get().Cast<ListParseResult>();
		auto distinct_type = transformer.Transform<ExpressionType>(distinct_list.Child<ListParseResult>(0));
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(distinct_list.Child<ListParseResult>(1));
		auto distinct_operator = make_uniq<ComparisonExpression>(distinct_type, std::move(expr), std::move(right_expr));
		expr = std::move(distinct_operator);
	}
	return expr;
}

// ComparisonExpression <- BetweenInLikeExpression (ComparisonOperator 'NOT'* BetweenInLikeExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformComparisonExpression(PEGTransformer &transformer,
                                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &comparison_opt = list_pr.Child<OptionalParseResult>(1);
	if (!comparison_opt.HasResult()) {
		return expr;
	}
	auto &comparison_repeat = comparison_opt.GetResult().Cast<RepeatParseResult>();
	auto cmp_depth_guard = transformer.StackCheck(comparison_repeat.GetChildren().size());
	for (auto &comparison_expr : comparison_repeat.GetChildren()) {
		auto &inner_list_pr = comparison_expr.get().Cast<ListParseResult>();
		auto comparison_operator = transformer.Transform<ExpressionType>(inner_list_pr.Child<ListParseResult>(0));
		auto &not_expr_opt = inner_list_pr.Child<OptionalParseResult>(1);
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(2));
		if (not_expr_opt.HasResult()) {
			auto &not_expr_repeat = not_expr_opt.GetResult().Cast<RepeatParseResult>();
			for (size_t i = 0; i < not_expr_repeat.GetChildren().size(); i++) {
				vector<unique_ptr<ParsedExpression>> inner_list_children;
				inner_list_children.push_back(std::move(right_expr));
				right_expr =
				    make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(inner_list_children));
			}
		}
		expr = make_uniq<ComparisonExpression>(comparison_operator, std::move(expr), std::move(right_expr));
	}
	return expr;
}

ExpressionType PEGTransformerFactory::TransformComparisonOperator(PEGTransformer &transformer,
                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<ExpressionType>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

bool TryNegateLikeFunction(Identifier &function_name) {
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
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBetweenInLikeExpression(PEGTransformer &transformer,
                                                                                     ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &between_in_like_opt = list_pr.Child<OptionalParseResult>(1);
	if (!between_in_like_opt.HasResult()) {
		return expr;
	}
	auto between_in_like_expr = transformer.Transform<unique_ptr<ParsedExpression>>(between_in_like_opt.GetResult());
	auto &op_list = between_in_like_opt.GetResult().Cast<ListParseResult>();
	bool has_not = op_list.Child<OptionalParseResult>(0).HasResult();
	if (between_in_like_expr->GetExpressionClass() == ExpressionClass::BETWEEN) {
		auto between_expr = unique_ptr_cast<ParsedExpression, BetweenExpression>(std::move(between_in_like_expr));
		between_expr->InputMutable() = std::move(expr);
		if (has_not) {
			expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(between_expr));
		} else {
			expr = std::move(between_expr);
		}
	} else if (between_in_like_expr->GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto func_expr = unique_ptr_cast<ParsedExpression, FunctionExpression>(std::move(between_in_like_expr));
		if (func_expr->FunctionName() == "contains") {
			func_expr->GetArgumentsMutable().push_back(std::move(expr));
		} else {
			func_expr->GetArgumentsMutable().insert(func_expr->GetArgumentsMutable().begin(), std::move(expr));
		}
		if (has_not) {
			if (!TryNegateLikeFunction(func_expr->FunctionNameMutable())) {
				// If it wasn't a special "Like" function, wrap it in a standard NOT operator
				expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(func_expr));
			} else {
				expr = std::move(func_expr);
			}
		} else if (func_expr->FunctionName() == "!~") {
			func_expr->FunctionNameMutable() = "regexp_full_match";
			func_expr->IsOperatorMutable() = false;
			expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(func_expr));
		} else {
			expr = std::move(func_expr);
		}
	} else if (between_in_like_expr->GetExpressionClass() == ExpressionClass::OPERATOR) {
		auto &operator_expr = between_in_like_expr->Cast<OperatorExpression>();
		operator_expr.GetChildrenMutable().insert(operator_expr.GetChildrenMutable().begin(), std::move(expr));
		if (has_not) {
			expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(between_in_like_expr));
		} else {
			expr = std::move(between_in_like_expr);
		}
	} else if (between_in_like_expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery_expr = between_in_like_expr->Cast<SubqueryExpression>();
		subquery_expr.GetChildMutable() = std::move(expr);
		if (has_not) {
			expr = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(between_in_like_expr));
		} else {
			expr = std::move(between_in_like_expr);
		}
	}
	return expr;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBetweenInLikeOp(PEGTransformer &transformer,
                                                                             ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &inner_list = list_pr.Child<ListParseResult>(1);
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list.Child<ChoiceParseResult>(0).GetResult());
	return expr;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformInClause(PEGTransformer &transformer,
                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformInExpression(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	if (StringUtil::CIEquals(choice_pr.name, "InExpressionList") ||
	    StringUtil::CIEquals(choice_pr.name, "InSelectStatement")) {
		return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr);
	}
	auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr);
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(right_expr));
	return make_uniq<FunctionExpression>("contains", std::move(children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformInExpressionList(PEGTransformer &transformer,
                                                                              ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto expr_list_pr = ExtractParseResultsFromList(extract_parens);
	vector<unique_ptr<ParsedExpression>> in_children;
	for (auto &expr : expr_list_pr) {
		in_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	if (in_children.size() == 1 && in_children[0]->GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery_expr = in_children[0]->Cast<SubqueryExpression>();
		auto result = make_uniq<SubqueryExpression>();
		result->GetSubqueryTypeMutable() = SubqueryType::ANY;
		result->GetComparisonTypeMutable() = ExpressionType::COMPARE_EQUAL;
		result->SubqueryMutable() = std::move(subquery_expr.SubqueryMutable());
		return std::move(result);
	}
	auto result = make_uniq<OperatorExpression>(ExpressionType::COMPARE_IN, std::move(in_children));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformInSelectStatement(PEGTransformer &transformer,
                                                                               ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto result = make_uniq<SubqueryExpression>();
	result->GetSubqueryTypeMutable() = SubqueryType::ANY;
	result->GetComparisonTypeMutable() = ExpressionType::COMPARE_EQUAL;
	result->SubqueryMutable() = transformer.Transform<unique_ptr<SelectStatement>>(extract_parens);
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBetweenClause(PEGTransformer &transformer,
                                                                           ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto lower = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1));
	auto higher = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(3));
	auto result = make_uniq<BetweenExpression>(nullptr, std::move(lower), std::move(higher));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLikeClause(PEGTransformer &transformer,
                                                                        ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	string like_variation = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	vector<unique_ptr<ParsedExpression>> like_children;
	like_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(1)));
	auto &escape_opt = list_pr.Child<OptionalParseResult>(2);
	if (escape_opt.HasResult()) {
		if (like_variation == "~~") {
			like_variation = "like_escape";
		} else if (like_variation == "~~*") {
			like_variation = "ilike_escape";
		}
		like_children.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(escape_opt.GetResult()));
	}
	auto result = make_uniq<FunctionExpression>(Identifier(like_variation), std::move(like_children));
	if (like_variation != "regexp_full_match") {
		result->IsOperatorMutable() = true;
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformEscapeClause(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.GetChild(1));
}

string PEGTransformerFactory::TransformLikeVariations(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<string>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

// OtherOperatorExpression <- BitwiseExpression (OtherOperator BitwiseExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformOtherOperatorExpression(PEGTransformer &transformer,
                                                                                     ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &other_operator_opt = list_pr.Child<OptionalParseResult>(1);
	if (!other_operator_opt.HasResult()) {
		return expr;
	}
	auto &other_operator_repeat = other_operator_opt.GetResult().Cast<RepeatParseResult>();
	for (auto &other_operator_expr : other_operator_repeat.GetChildren()) {
		auto &inner_list_pr = other_operator_expr.get().Cast<ListParseResult>();
		auto right_expr = transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		auto &other_operator_pr = inner_list_pr.Child<ListParseResult>(0);
		auto &other_operator_choice = other_operator_pr.Child<ChoiceParseResult>(0).GetResult();
		if (StringUtil::CIEquals(other_operator_choice.name, "AnyAllOperator")) {
			auto any_all = transformer.Transform<pair<string, bool>>(other_operator_choice);
			auto op_string = any_all.first;
			auto is_any = any_all.second;

			// Map operator string to ExpressionType (INVALID if not a comparison operator)
			auto expression_type = OperatorToExpressionType(op_string);

			auto subquery_expr = make_uniq<SubqueryExpression>();
			if (right_expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
				if (expression_type == ExpressionType::INVALID) {
					throw ParserException("ANY and ALL operators require one of =,<>,>,<,>=,<= comparisons!");
				}
				subquery_expr->GetSubqueryTypeMutable() = SubqueryType::ANY;
				subquery_expr->GetComparisonTypeMutable() = expression_type;
				auto &right_expr_subquery = right_expr->Cast<SubqueryExpression>();
				subquery_expr->SubqueryMutable() = std::move(right_expr_subquery.SubqueryMutable());
				subquery_expr->GetChildMutable() = std::move(expr);
				if (!is_any) {
					// ALL sublink is equivalent to NOT(ANY) with inverted comparison
					// e.g. [= ALL()] is equivalent to [NOT(<> ANY())]
					// first invert the comparison type
					subquery_expr->GetComparisonTypeMutable() =
					    NegateComparisonExpression(subquery_expr->GetComparisonType());
					return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(subquery_expr));
				}
				expr = std::move(subquery_expr);
			} else {
				// left=ANY(right)
				// we turn this into left=ANY((SELECT UNNEST(right)))
				if (expression_type == ExpressionType::INVALID) {
					throw ParserException("Unsupported comparison \"%s\" for ANY/ALL subquery", op_string);
				}
				auto select_statement = make_uniq<SelectStatement>();
				auto select_node = make_uniq<SelectNode>();
				vector<unique_ptr<ParsedExpression>> children;
				children.push_back(std::move(right_expr));

				select_node->select_list.push_back(make_uniq<FunctionExpression>("UNNEST", std::move(children)));
				select_node->from_table = make_uniq<EmptyTableRef>();
				select_statement->node = std::move(select_node);
				subquery_expr->SubqueryMutable() = std::move(select_statement);
				subquery_expr->GetSubqueryTypeMutable() = SubqueryType::ANY;
				subquery_expr->GetChildMutable() = std::move(expr);
				subquery_expr->GetComparisonTypeMutable() = expression_type;
				if (!is_any) {
					// ALL sublink is equivalent to NOT(ANY) with inverted comparison
					// e.g. [= ALL()] is equivalent to [NOT(<> ANY())]
					// first invert the comparison type
					subquery_expr->GetComparisonTypeMutable() =
					    NegateComparisonExpression(subquery_expr->GetComparisonType());
					return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(subquery_expr));
				}
				return std::move(subquery_expr);
			}
		} else {
			auto other_operator = transformer.Transform<string>(other_operator_pr);
			vector<unique_ptr<ParsedExpression>> children_function;
			children_function.push_back(std::move(expr));
			children_function.push_back(std::move(right_expr));
			vector split_operator = StringUtil::Split(other_operator, ".");
			string schema_name = INVALID_SCHEMA;
			string func_name = "";
			if (split_operator.size() == 1) {
				func_name = split_operator[0];
			} else if (split_operator.size() == 2) {
				schema_name = split_operator[0];
				func_name = split_operator[1];
			} else {
				throw ParserException("Too many identifiers found, expected schema.operator or operator");
			}

			auto func_expr =
			    make_uniq<FunctionExpression>(INVALID_CATALOG, Identifier(std::move(schema_name)),
			                                  Identifier(std::move(func_name)), std::move(children_function));
			func_expr->IsOperatorMutable() = true;
			expr = std::move(func_expr);
		}
	}
	return expr;
}

string PEGTransformerFactory::TransformOtherOperator(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &child = list_pr.Child<ChoiceParseResult>(0).GetResult();
	// OperatorLiteral matches any operator token and produces an OperatorParseResult directly
	if (child.type == ParseResultType::OPERATOR) {
		return child.Cast<OperatorParseResult>().operator_token;
	}
	return transformer.Transform<string>(child);
}

// QualifiedOperator <- 'OPERATOR' Parens(ColId* AnyOp)
string PEGTransformerFactory::TransformQualifiedOperator(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &any_op_pr = ExtractResultFromParens(list_pr.GetChild(1)).Cast<ListParseResult>();
	auto &repeat_colid_opt = any_op_pr.Child<OptionalParseResult>(0);
	vector<string> result;
	if (repeat_colid_opt.HasResult()) {
		auto &repeat_colid = repeat_colid_opt.GetResult().Cast<RepeatParseResult>();
		for (auto &colid : repeat_colid.GetChildren()) {
			result.push_back(transformer.Transform<string>(colid));
		}
	}
	result.push_back(transformer.Transform<string>(any_op_pr.GetChild(1)));
	return StringUtil::Join(result, ".");
}

// AnyOp <- '!~~*' / '>>=' / ... / '!'
string PEGTransformerFactory::TransformAnyOp(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return choice_pr.Cast<KeywordParseResult>().keyword;
}

string PEGTransformerFactory::TransformJsonOperator(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return list_pr.Child<KeywordParseResult>(0).keyword;
}

string PEGTransformerFactory::TransformInetOperator(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return choice_pr.Cast<KeywordParseResult>().keyword;
}

string PEGTransformerFactory::TransformStringOperator(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return choice_pr.Cast<KeywordParseResult>().keyword;
}

string PEGTransformerFactory::TransformListOperator(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return choice_pr.Cast<KeywordParseResult>().keyword;
}

pair<string, bool> PEGTransformerFactory::TransformAnyAllOperator(PEGTransformer &transformer,
                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto op_string = transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
	auto subquery_type = transformer.Transform<bool>(list_pr.Child<ListParseResult>(1));
	return make_pair(op_string, subquery_type);
}

bool PEGTransformerFactory::TransformAnyOrAll(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<bool>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

// BitwiseExpression <- AdditiveExpression (BitOperator AdditiveExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformBitwiseExpression(PEGTransformer &transformer,
                                                                               ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &bit_operator_opt = list_pr.Child<OptionalParseResult>(1);
	if (!bit_operator_opt.HasResult()) {
		return expr;
	}
	auto &bit_repeat = bit_operator_opt.GetResult().Cast<RepeatParseResult>();
	auto bit_depth_guard = transformer.StackCheck(bit_repeat.GetChildren().size());
	for (auto &bit_expr : bit_repeat.GetChildren()) {
		auto &inner_list_pr = bit_expr.get().Cast<ListParseResult>();
		auto bit = transformer.Transform<string>(inner_list_pr.Child<ListParseResult>(0));
		vector<unique_ptr<ParsedExpression>> bit_children;
		bit_children.push_back(std::move(expr));
		bit_children.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1)));
		auto func_expr = make_uniq<FunctionExpression>(Identifier(std::move(bit)), std::move(bit_children));
		func_expr->IsOperatorMutable() = true;
		expr = std::move(func_expr);
	}
	return expr;
}

string PEGTransformerFactory::TransformBitOperator(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return choice_pr.Cast<KeywordParseResult>().keyword;
}

// AdditiveExpression <- MultiplicativeExpression (Term MultiplicativeExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformAdditiveExpression(PEGTransformer &transformer,
                                                                                ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &term_opt = list_pr.Child<OptionalParseResult>(1);
	if (!term_opt.HasResult()) {
		return expr;
	}
	auto &term_repeat = term_opt.GetResult().Cast<RepeatParseResult>();
	auto add_depth_guard = transformer.StackCheck(term_repeat.GetChildren().size());
	for (auto &term_expr : term_repeat.GetChildren()) {
		auto &inner_list_pr = term_expr.get().Cast<ListParseResult>();
		auto term = transformer.Transform<string>(inner_list_pr.Child<ListParseResult>(0));
		vector<unique_ptr<ParsedExpression>> term_children;
		term_children.push_back(std::move(expr));
		term_children.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1)));
		auto func_expr = make_uniq<FunctionExpression>(Identifier(std::move(term)), std::move(term_children));
		func_expr->IsOperatorMutable() = true;
		if (inner_list_pr.offset.IsValid()) {
			transformer.SetQueryLocation(*func_expr, inner_list_pr.offset);
		}
		expr = std::move(func_expr);
	}
	return expr;
}

string PEGTransformerFactory::TransformTerm(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return choice_pr.Cast<KeywordParseResult>().keyword;
}

// MultiplicativeExpression <- ExponentiationExpression (Factor ExponentiationExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformMultiplicativeExpression(PEGTransformer &transformer,
                                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &factor_opt = list_pr.Child<OptionalParseResult>(1);
	if (!factor_opt.HasResult()) {
		return expr;
	}
	auto &factor_repeat = factor_opt.GetResult().Cast<RepeatParseResult>();
	auto mul_depth_guard = transformer.StackCheck(factor_repeat.GetChildren().size());
	for (auto &factor_expr : factor_repeat.GetChildren()) {
		auto &inner_list_pr = factor_expr.get().Cast<ListParseResult>();
		auto factor = transformer.Transform<string>(inner_list_pr.Child<ListParseResult>(0));
		if (factor == "/" && transformer.options.integer_division) {
			factor = "//";
		}
		vector<unique_ptr<ParsedExpression>> factor_children;
		factor_children.push_back(std::move(expr));
		factor_children.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1)));
		auto func_expr = make_uniq<FunctionExpression>(Identifier(std::move(factor)), std::move(factor_children));
		func_expr->IsOperatorMutable() = true;
		expr = std::move(func_expr);
	}
	return expr;
}

string PEGTransformerFactory::TransformFactor(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return choice_pr.Cast<KeywordParseResult>().keyword;
}

// ExponentiationExpression <- CollateExpression (ExponentOperator CollateExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExponentiationExpression(PEGTransformer &transformer,
                                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &exponent_opt = list_pr.Child<OptionalParseResult>(1);
	if (!exponent_opt.HasResult()) {
		return expr;
	}
	auto &exponent_repeat = exponent_opt.GetResult().Cast<RepeatParseResult>();
	for (auto &exponent_expr : exponent_repeat.GetChildren()) {
		auto &inner_list_pr = exponent_expr.get().Cast<ListParseResult>();
		auto exponent = transformer.Transform<string>(inner_list_pr.Child<ListParseResult>(0));
		vector<unique_ptr<ParsedExpression>> exponent_children;
		exponent_children.push_back(std::move(expr));
		exponent_children.push_back(
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1)));
		auto func_expr = make_uniq<FunctionExpression>(Identifier(std::move(exponent)), std::move(exponent_children));
		func_expr->IsOperatorMutable() = true;
		expr = std::move(func_expr);
	}
	return expr;
}

string PEGTransformerFactory::TransformExponentOperator(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return choice_pr.Cast<KeywordParseResult>().keyword;
}

// CollateExpression <- AtTimeZoneExpression (CollateOperator AtTimeZoneExpression)*
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCollateExpression(PEGTransformer &transformer,
                                                                               ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &collate_opt = list_pr.Child<OptionalParseResult>(1);
	if (!collate_opt.HasResult()) {
		return expr;
	}
	auto &collate_expr_repeat = collate_opt.GetResult().Cast<RepeatParseResult>();
	for (auto &collate_expr_pr : collate_expr_repeat.GetChildren()) {
		auto &inner_list_pr = collate_expr_pr.get().Cast<ListParseResult>();
		vector<unique_ptr<ParsedExpression>> collate_children;
		auto collate_string_expr =
		    transformer.Transform<unique_ptr<ParsedExpression>>(inner_list_pr.Child<ListParseResult>(1));
		string collate_string;
		if (collate_string_expr->GetExpressionClass() == ExpressionClass::CONSTANT) {
			auto &const_expr = collate_string_expr->Cast<ConstantExpression>();
			collate_string = const_expr.GetValue().GetValue<string>();
		} else if (collate_string_expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			auto &col_ref = collate_string_expr->Cast<ColumnRefExpression>();
			collate_string = StringUtil::Join(col_ref.ColumnNames(), ".");
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
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformAtTimeZoneExpression(PEGTransformer &transformer,
                                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(0));
	auto &at_time_zone_opt = list_pr.Child<OptionalParseResult>(1);
	if (!at_time_zone_opt.HasResult()) {
		return expr;
	}
	auto &at_time_zone_repeat = at_time_zone_opt.GetResult().Cast<RepeatParseResult>();
	for (auto &time_zone_expr : at_time_zone_repeat.GetChildren()) {
		auto &inner_list_pr = time_zone_expr.get().Cast<ListParseResult>();
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

bool IsNumberLiteral(ParseResult &pr) {
	if (pr.name == "BaseExpression") {
		auto &list = pr.Cast<ListParseResult>();
		if (list.GetChild(1).Cast<OptionalParseResult>().HasResult()) {
			return false;
		}
		return IsNumberLiteral(list.GetChild(0));
	}
	if (pr.name == "SingleExpression") {
		auto &list = pr.Cast<ListParseResult>();
		return IsNumberLiteral(list.GetChild(0).Cast<ChoiceParseResult>().GetResult());
	}
	if (pr.name == "LiteralExpression") {
		auto &list = pr.Cast<ListParseResult>();
		return IsNumberLiteral(list.GetChild(0).Cast<ChoiceParseResult>().GetResult());
	}
	return pr.name == "NumberLiteral";
}

string GetRawText(ParseResult &pr) {
	if (pr.name == "NumberLiteral") {
		return pr.Cast<NumberParseResult>().number;
	}
	if (pr.name == "BaseExpression") {
		return GetRawText(pr.Cast<ListParseResult>().GetChild(0));
	}
	if (pr.name == "SingleExpression" || pr.name == "LiteralExpression") {
		auto &list = pr.Cast<ListParseResult>();
		return GetRawText(list.GetChild(0).Cast<ChoiceParseResult>().GetResult());
	}
	return "";
}

// PrefixExpression <- PrefixOperator* BaseExpression
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformPrefixExpression(PEGTransformer &transformer,
                                                                              ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &prefix_opt = list_pr.Child<OptionalParseResult>(0);
	auto &base_expr_pr = list_pr.Child<ListParseResult>(1);

	if (!prefix_opt.HasResult()) {
		return transformer.Transform<unique_ptr<ParsedExpression>>(base_expr_pr);
	}

	auto &prefix_repeat = prefix_opt.GetResult().Cast<RepeatParseResult>();

	// --- SPECIAL CASE: Handle -<Number> atomically to prevent overflow/precision loss ---
	// We only do this if there is exactly one prefix and it is a minus.
	if (prefix_repeat.GetChildren().size() == 1) {
		auto prefix = transformer.Transform<string>(prefix_repeat.GetChildren()[0]);
		if (prefix == "-" && IsNumberLiteral(base_expr_pr)) {
			string raw_number = GetRawText(base_expr_pr);
			string full_text = "-" + raw_number;
			return ConvertNumberToValue(full_text);
		}
	}

	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(base_expr_pr);

	vector<string> prefixes;
	for (auto &child_ref : prefix_repeat.GetChildren()) {
		prefixes.push_back(transformer.Transform<string>(child_ref));
	}

	for (auto it = prefixes.rbegin(); it != prefixes.rend(); ++it) {
		const string &prefix = *it;

		if (prefix == "-" && expr->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			auto &const_expr = expr->Cast<ConstantExpression>();
			if (auto negated_expr = TryNegateValue(const_expr)) {
				expr = std::move(negated_expr);
				continue;
			}
		}

		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(expr));
		auto func_expr = make_uniq<FunctionExpression>(Identifier(prefix), std::move(children));
		func_expr->IsOperatorMutable() = true;
		expr = std::move(func_expr);
	}
	return expr;
}
string PEGTransformerFactory::TransformPrefixOperator(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (StringUtil::CIEquals(choice_pr.GetResult().name, "QualifiedOperator")) {
		return transformer.Transform<string>(choice_pr.GetResult());
	}
	return transformer.TransformEnum<string>(choice_pr.GetResult());
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformAnonymousParameter(PEGTransformer &transformer) {
	// AnonymousParameter <- '?'
	auto expr = make_uniq<ParameterExpression>();

	// Auto-increment the parameter count
	idx_t known_param_index = transformer.ParamCount() + 1;
	string identifier = StringUtil::Format("%d", known_param_index);

	// Register it
	transformer.SetParam(Identifier(identifier), known_param_index, PreparedParamType::AUTO_INCREMENT);
	transformer.SetParamCount(MaxValue<idx_t>(transformer.ParamCount(), known_param_index));

	expr->IdentifierMutable() = Identifier(identifier);
	return std::move(expr);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformQuestionMarkNumberedParameter(PEGTransformer &transformer,
                                                              unique_ptr<ParsedExpression> number_literal) {
	// QuestionMarkNumberedParameter <- '?' NumberLiteral
	auto &const_expr = number_literal->Cast<ConstantExpression>();
	int32_t param_number = const_expr.GetValue().GetValue<int32_t>();

	if (param_number <= 0) {
		throw ParserException("Parameter numbers must be greater than 0");
	}

	auto expr = make_uniq<ParameterExpression>();
	string identifier = const_expr.GetValue().ToString();
	idx_t known_param_index = DConstants::INVALID_INDEX;

	transformer.GetParam(Identifier(identifier), known_param_index, PreparedParamType::POSITIONAL);

	if (known_param_index == DConstants::INVALID_INDEX) {
		known_param_index = NumericCast<idx_t>(param_number);
		transformer.SetParam(Identifier(identifier), known_param_index, PreparedParamType::POSITIONAL);
	}

	expr->IdentifierMutable() = Identifier(identifier);
	transformer.SetParamCount(MaxValue<idx_t>(transformer.ParamCount(), known_param_index));
	return std::move(expr);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformNumberedParameter(PEGTransformer &transformer,
                                                  unique_ptr<ParsedExpression> number_literal) {
	// NumberedParameter <- '$' NumberLiteral
	auto &const_expr = number_literal->Cast<ConstantExpression>();
	int32_t param_number = const_expr.GetValue().GetValue<int32_t>();

	if (param_number <= 0) {
		throw ParserException("Parameter numbers must be greater than 0");
	}

	auto expr = make_uniq<ParameterExpression>();
	string identifier = const_expr.GetValue().ToString();
	idx_t known_param_index = DConstants::INVALID_INDEX;

	transformer.GetParam(Identifier(identifier), known_param_index, PreparedParamType::POSITIONAL);

	if (known_param_index == DConstants::INVALID_INDEX) {
		known_param_index = NumericCast<idx_t>(param_number);
		transformer.SetParam(Identifier(identifier), known_param_index, PreparedParamType::POSITIONAL);
	}

	expr->IdentifierMutable() = Identifier(identifier);
	transformer.SetParamCount(MaxValue<idx_t>(transformer.ParamCount(), known_param_index));
	return std::move(expr);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformColLabelParameter(PEGTransformer &transformer,
                                                                               const string &col_label) {
	// ColLabelParameter <- '$' ColLabel
	string identifier = col_label;

	auto expr = make_uniq<ParameterExpression>();
	idx_t known_param_index = DConstants::INVALID_INDEX;

	transformer.GetParam(Identifier(identifier), known_param_index, PreparedParamType::NAMED);

	if (known_param_index == DConstants::INVALID_INDEX) {
		// New named parameter gets the next available index
		known_param_index = transformer.ParamCount() + 1;
		transformer.SetParam(Identifier(identifier), known_param_index, PreparedParamType::NAMED);
	}

	expr->IdentifierMutable() = Identifier(identifier);
	transformer.SetParamCount(MaxValue<idx_t>(transformer.ParamCount(), known_param_index));
	return std::move(expr);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformPositionalExpression(PEGTransformer &transformer,
                                                     unique_ptr<ParsedExpression> number_literal) {
	auto &const_expr = number_literal->Cast<ConstantExpression>();
	int32_t index = const_expr.GetValue().GetValue<int32_t>();
	if (index <= 0) {
		throw ParserException("Positional reference node needs to be >= 1");
	}
	return make_uniq<PositionalReferenceExpression>(NumericCast<idx_t>(index));
}

// LiteralExpression <- StringLiteral / NumberLiteral / 'NULL' / 'TRUE' / 'FALSE'
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLiteralExpression(PEGTransformer &transformer,
                                                                               ParseResult &choice_result) {
	if (choice_result.name == "StringLiteral") {
		auto &string_literal = choice_result.Cast<StringLiteralParseResult>();
		return string_literal.ToExpression();
	}
	return transformer.Transform<unique_ptr<ParsedExpression>>(choice_result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformParensExpression(PEGTransformer &transformer,
                                                                              unique_ptr<ParsedExpression> expression) {
	return expression;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformConstantLiteral(PEGTransformer &transformer,
                                                                             const Value &child) {
	return make_uniq<ConstantExpression>(child);
}

Value PEGTransformerFactory::TransformFalseLiteral(PEGTransformer &transformer) {
	return Value(false);
}

Value PEGTransformerFactory::TransformTrueLiteral(PEGTransformer &transformer) {
	return Value(true);
}

Value PEGTransformerFactory::TransformNullLiteral(PEGTransformer &transformer) {
	return Value();
}

Value PEGTransformerFactory::TransformUnknownLiteral(PEGTransformer &transformer) {
	return Value();
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
                                                                              ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

ExpressionType PEGTransformerFactory::TransformLambdaOperator(PEGTransformer &transformer, ParseResult &parse_result) {
	return ExpressionType::LAMBDA;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformPostfixOperator(PEGTransformer &transformer) {
	vector<unique_ptr<ParsedExpression>> func_children;
	return make_uniq<FunctionExpression>("factorial", std::move(func_children));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCastOperator(PEGTransformer &transformer,
                                                                          const LogicalType &type) {
	// We input a dummy constant expression but replace this later with the real expression that precedes this post-fix
	// castOperator
	return make_uniq<CastExpression>(type, make_uniq<ConstantExpression>(Value()));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformDotMethodOperator(PEGTransformer &transformer,
                                                  unique_ptr<ParsedExpression> method_expression) {
	return method_expression;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformDotColumnOperator(PEGTransformer &transformer,
                                                                               const string &col_label) {
	return make_uniq<ConstantExpression>(col_label);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformMethodExpression(PEGTransformer &transformer, const string &col_label,
                                                 MethodArguments method_expression_arguments) {
	if (method_expression_arguments.arguments.size() == 1 &&
	    ExpressionIsEmptyStar(method_expression_arguments.arguments[0].GetExpression())) {
		// COUNT(*) gets converted into COUNT()
		method_expression_arguments.arguments.clear();
	}
	if (method_expression_arguments.has_ignore_nulls) {
		throw ParserException("RESPECT/IGNORE NULLS is not supported for non-window functions");
	}
	auto result = make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, Identifier(col_label),
	                                            std::move(method_expression_arguments.arguments));
	result->DistinctMutable() = method_expression_arguments.distinct;
	if (!method_expression_arguments.order_bys.empty()) {
		auto order_by_modifier = make_uniq<OrderModifier>();
		order_by_modifier->orders = std::move(method_expression_arguments.order_bys);
		result->OrderByMutable() = std::move(order_by_modifier);
	}
	return std::move(result);
}

MethodArguments PEGTransformerFactory::TransformMethodExpressionArguments(PEGTransformer &transformer,
                                                                          MethodArguments method_expression_argument_list) {
	return method_expression_argument_list;
}

MethodArguments PEGTransformerFactory::TransformMethodExpressionArgumentList(
    PEGTransformer &transformer, const optional<bool> &distinct_or_all,
    optional<vector<FunctionArgument>> method_function_arguments, optional<vector<OrderByNode>> order_by_clause,
    const optional<bool> &ignore_or_respect_nulls) {
	MethodArguments result;
	if (distinct_or_all) {
		result.distinct = *distinct_or_all;
	}
	if (method_function_arguments) {
		result.arguments = std::move(*method_function_arguments);
	}
	if (order_by_clause) {
		result.order_bys = std::move(*order_by_clause);
	}
	result.has_ignore_nulls = ignore_or_respect_nulls.has_value();
	return result;
}

vector<FunctionArgument> PEGTransformerFactory::TransformMethodFunctionArguments(
    PEGTransformer &transformer, vector<FunctionArgument> function_argument) {
	return function_argument;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformSliceExpression(PEGTransformer &transformer,
                                                vector<unique_ptr<ParsedExpression>> slice_bound) {
	if (slice_bound.empty()) {
		throw ParserException("Empty subscript '[]' is not allowed");
	}
	if (slice_bound.size() == 1) {
		return make_uniq<OperatorExpression>(ExpressionType::ARRAY_EXTRACT, std::move(slice_bound));
	}
	return make_uniq<OperatorExpression>(ExpressionType::ARRAY_SLICE, std::move(slice_bound));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSliceBound(PEGTransformer &transformer, optional<unique_ptr<ParsedExpression>> expression,
                                           optional<unique_ptr<ParsedExpression>> end_slice_bound,
                                           optional<unique_ptr<ParsedExpression>> step_slice_bound) {
	vector<unique_ptr<ParsedExpression>> slice_bounds;
	if (!end_slice_bound && !step_slice_bound) {
		if (expression && *expression) {
			slice_bounds.push_back(std::move(*expression));
		}
		return slice_bounds;
	}
	if (expression && *expression) {
		slice_bounds.push_back(std::move(*expression));
	} else {
		slice_bounds.push_back(make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>())));
	}
	if (end_slice_bound && *end_slice_bound) {
		slice_bounds.push_back(std::move(*end_slice_bound));
	} else {
		slice_bounds.push_back(make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>())));
	}
	if (step_slice_bound && *step_slice_bound) {
		slice_bounds.push_back(std::move(*step_slice_bound));
	}
	return slice_bounds;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformEndSliceBound(PEGTransformer &transformer,
                                                                           optional<unique_ptr<ParsedExpression>>
                                                                               end_slice_value) {
	// If either the lower or upper bound is not specified, we use an empty constant LIST,
	// which we handle in the execution.
	if (end_slice_value && *end_slice_value) {
		return std::move(*end_slice_value);
	}
	return make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformEndSliceMinus(PEGTransformer &transformer) {
	return make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformStepSliceBound(PEGTransformer &transformer,
                                               optional<unique_ptr<ParsedExpression>> expression) {
	if (expression && *expression) {
		return std::move(*expression);
	}
	return make_uniq<ConstantExpression>(Value::LIST(LogicalType::INTEGER, vector<Value>()));
}

unique_ptr<ColumnRefExpression> PEGTransformerFactory::TransformTableReservedColumnName(
    PEGTransformer &transformer, const Identifier &table_qualification, const Identifier &reserved_column_name) {
	return make_uniq<ColumnRefExpression>(reserved_column_name, table_qualification);
}

Identifier PEGTransformerFactory::TransformTableQualification(PEGTransformer &transformer,
                                                              const Identifier &table_name) {
	return table_name;
}

string PEGTransformerFactory::TransformColIdDot(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<string>(list_pr.GetChild(0));
}
unique_ptr<ParsedExpression> PEGTransformerFactory::TransformStarExpression(PEGTransformer &transformer,
                                                                            ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();

	auto result = make_uniq<StarExpression>();
	auto &repeat_colid_opt = list_pr.Child<OptionalParseResult>(0);
	if (repeat_colid_opt.HasResult()) {
		auto &repeat_colid = repeat_colid_opt.GetResult().Cast<RepeatParseResult>();
		if (repeat_colid.GetChildren().size() > 1) {
			throw ParserException("Did not expect more than one column in front of a star expression");
		}
		result->RelationNameMutable() =
		    Identifier(transformer.Transform<string>(repeat_colid.Child<ListParseResult>(0)));
	}
	transformer.TransformOptional<qualified_column_set_t>(list_pr, 2, result->ExcludeListMutable());
	auto &replace_list_opt = list_pr.Child<OptionalParseResult>(3);
	if (replace_list_opt.HasResult()) {
		auto replace_string_map =
		    transformer.Transform<case_insensitive_map_t<unique_ptr<ParsedExpression>>>(replace_list_opt.GetResult());
		for (auto &replace_entry : replace_string_map) {
			result->ReplaceListMutable()[Identifier(replace_entry.first)] = std::move(replace_entry.second);
		}
		for (auto &replace_entry : result->ReplaceList()) {
			if (result->ExcludeList().find(QualifiedColumnName(replace_entry.first)) != result->ExcludeList().end()) {
				throw ParserException("Column \"%s\" cannot occur in both EXCLUDE and REPLACE list",
				                      replace_entry.first);
			}
		}
	}
	auto &rename_list_opt = list_pr.Child<OptionalParseResult>(4);
	if (rename_list_opt.HasResult()) {
		auto rename_string_map = transformer.Transform<qualified_column_map_t<string>>(rename_list_opt.GetResult());
		for (auto &rename_entry : rename_string_map) {
			result->RenameListMutable()[rename_entry.first] = Identifier(rename_entry.second);
		}
		for (auto &rename_column : result->RenameList()) {
			if (result->ExcludeList().find(rename_column.first) != result->ExcludeList().end()) {
				throw ParserException("Column \"%s\" cannot occur in both EXCLUDE and RENAME list",
				                      rename_column.first.ToString());
			}
			if (result->ReplaceList().find(rename_column.first.column) != result->ReplaceList().end()) {
				throw ParserException("Column \"%s\" cannot occur in both REPLACE and RENAME list",
				                      rename_column.first.ToString());
			}
		}
	}
	return std::move(result);
}

qualified_column_set_t PEGTransformerFactory::TransformExcludeList(PEGTransformer &transformer,
                                                                   const qualified_column_set_t &exclude_names) {
	return exclude_names;
}

qualified_column_set_t
PEGTransformerFactory::TransformExcludeNameList(PEGTransformer &transformer,
                                                const vector<QualifiedColumnName> &exclude_name) {
	qualified_column_set_t result;
	for (auto &exclude_column : exclude_name) {
		if (result.find(exclude_column) != result.end()) {
			throw ParserException("Duplicate entry \"%s\" in EXCLUDE list", exclude_column.ToString());
		}
		result.insert(exclude_column);
	}
	return result;
}

qualified_column_set_t PEGTransformerFactory::TransformExcludeNameSingle(PEGTransformer &transformer,
                                                                         const QualifiedColumnName &exclude_name) {
	qualified_column_set_t result;
	result.insert(exclude_name);
	return result;
}

QualifiedColumnName PEGTransformerFactory::TransformExcludeDottedName(PEGTransformer &transformer,
                                                                      const vector<string> &dotted_identifier) {
	auto result_string = StringUtil::Join(dotted_identifier, ".");
	return QualifiedColumnName::Parse(result_string);
}

QualifiedColumnName PEGTransformerFactory::TransformExcludeColumnName(PEGTransformer &transformer,
                                                                      const Identifier &col_id_or_string) {
	return QualifiedColumnName(col_id_or_string);
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformOverClause(PEGTransformer &transformer,
                                                                        ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<WindowExpression>>(list_pr.Child<ListParseResult>(1));
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformWindowFrame(PEGTransformer &transformer,
                                                                         ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.GetResult().type == ParseResultType::IDENTIFIER) {
		auto window_name = choice_pr.GetResult().Cast<IdentifierParseResult>().identifier;
		return transformer.GetWindowClause(window_name);
	}
	return transformer.Transform<unique_ptr<WindowExpression>>(choice_pr.GetResult());
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformParensIdentifier(PEGTransformer &transformer,
                                                                              ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.GetChild(0));
	auto window_name = extract_parens.Cast<IdentifierParseResult>().identifier;
	auto window_clause = transformer.GetWindowClause(window_name);
	if (window_clause->StartExpr() || window_clause->EndExpr() ||
	    !transformer.IsWindowFrameDefault(window_clause->WindowStart(), window_clause->WindowEnd())) {
		throw ParserException("cannot copy window \"%s\" because it has a frame clause", window_name);
	}
	return window_clause;
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformWindowFrameDefinition(PEGTransformer &transformer,
                                                                                   ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<WindowExpression>>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformWindowFrameContentsParens(PEGTransformer &transformer,
                                                                                       ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<unique_ptr<WindowExpression>>(extract_parens);
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformWindowFrameNameContentsParens(PEGTransformer &transformer,
                                                                                           ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0)).Cast<ListParseResult>();
	string window_name;
	transformer.TransformOptional<string>(extract_parens, 0, window_name);
	auto lower_name = StringUtil::Lower(window_name);
	if (lower_name == "partition" || lower_name == "range" || lower_name == "rows" || lower_name == "groups") {
		throw ParserException("Invalid window name \"%s\"", window_name);
	}
	auto window_frame_contents =
	    transformer.Transform<unique_ptr<WindowExpression>>(extract_parens.Child<ListParseResult>(1));
	if (window_name.empty()) {
		return window_frame_contents;
	}
	auto copied_window = transformer.GetWindowClause(Identifier(window_name));
	if (copied_window->StartExpr() || copied_window->EndExpr() ||
	    !transformer.IsWindowFrameDefault(copied_window->WindowStart(), copied_window->WindowEnd())) {
		throw ParserException("cannot copy window \"%s\" because it has a frame clause", window_name);
	}
	copied_window->WindowStartMutable() = window_frame_contents->WindowStart();
	copied_window->WindowEndMutable() = window_frame_contents->WindowEnd();
	copied_window->WindowExcludeMutable() = window_frame_contents->WindowExclude();
	copied_window->StartExprMutable() = std::move(window_frame_contents->StartExprMutable());
	copied_window->EndExprMutable() = std::move(window_frame_contents->EndExprMutable());

	if (!copied_window->OrderBy().empty() && !window_frame_contents->OrderBy().empty()) {
		throw ParserException("Cannot override ORDER BY clause of window \"%s\"", window_name);
	}
	if (copied_window->OrderBy().empty()) {
		copied_window->OrderByMutable() = std::move(window_frame_contents->OrderByMutable());
	}
	if (!copied_window->Partitions().empty() && !window_frame_contents->Partitions().empty()) {
		throw ParserException("Cannot override PARTITION BY clause of window \"%s\"", window_name);
	}
	if (copied_window->Partitions().empty()) {
		copied_window->PartitionsMutable() = std::move(window_frame_contents->PartitionsMutable());
	}
	return copied_window;
}

string PEGTransformerFactory::TransformBaseWindowName(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier.GetIdentifierName();
}

unique_ptr<WindowExpression> PEGTransformerFactory::TransformWindowFrameContents(PEGTransformer &transformer,
                                                                                 ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	//! Create a dummy result to add modifiers to
	auto result = make_uniq<WindowExpression>(INVALID_CATALOG, INVALID_SCHEMA, string());
	auto &partition_opt = list_pr.Child<OptionalParseResult>(0);
	if (partition_opt.HasResult()) {
		result->PartitionsMutable() =
		    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(partition_opt.GetResult());
	}
	auto &order_by_opt = list_pr.Child<OptionalParseResult>(1);
	if (order_by_opt.HasResult()) {
		result->OrderByMutable() = transformer.Transform<vector<OrderByNode>>(order_by_opt.GetResult());
		for (auto &order : result->OrderByMutable()) {
			if (order.expression->GetExpressionType() == ExpressionType::STAR) {
				auto &star = order.expression->Cast<StarExpression>();
				if (!star.Expression()) {
					throw ParserException("Cannot ORDER BY ALL in a window expression");
				}
			}
		}
	}
	auto &frame_opt = list_pr.Child<OptionalParseResult>(2);
	if (frame_opt.HasResult()) {
		auto window_frame = transformer.Transform<WindowFrame>(frame_opt.GetResult());
		result->WindowStartMutable() = window_frame.start;
		result->WindowEndMutable() = window_frame.end;
		result->StartExprMutable() = std::move(window_frame.start_expr);
		result->EndExprMutable() = std::move(window_frame.end_expr);
		result->WindowExcludeMutable() = window_frame.exclude_clause;
	} else {
		result->WindowStartMutable() = WindowBoundary::UNBOUNDED_PRECEDING;
		result->WindowEndMutable() = WindowBoundary::CURRENT_ROW_RANGE;
	}
	return result;
}

WindowFrame PEGTransformerFactory::TransformFrameClause(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
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
                                                                             ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<vector<WindowBoundaryExpression>>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

vector<WindowBoundaryExpression> PEGTransformerFactory::TransformBetweenFrameExtent(PEGTransformer &transformer,
                                                                                    ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	vector<WindowBoundaryExpression> result;
	result.push_back(transformer.Transform<WindowBoundaryExpression>(list_pr.Child<ListParseResult>(1)));
	result.push_back(transformer.Transform<WindowBoundaryExpression>(list_pr.Child<ListParseResult>(3)));
	return result;
}

vector<WindowBoundaryExpression> PEGTransformerFactory::TransformSingleFrameExtent(PEGTransformer &transformer,
                                                                                   ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	vector<WindowBoundaryExpression> result;
	result.push_back(transformer.Transform<WindowBoundaryExpression>(list_pr.Child<ListParseResult>(0)));
	WindowBoundaryExpression end_current_row;
	end_current_row.boundary = WindowBoundary::INVALID;
	result.push_back(std::move(end_current_row));
	return result;
}

WindowBoundaryExpression PEGTransformerFactory::TransformFrameBound(PEGTransformer &transformer,
                                                                    ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<WindowBoundaryExpression>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

WindowBoundaryExpression PEGTransformerFactory::TransformFrameUnbounded(PEGTransformer &transformer,
                                                                        ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
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
                                                                         ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
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
                                                                         ParseResult &parse_result) {
	WindowBoundaryExpression result;
	// These are placeholders and will be converted to groups/rows/range later
	result.boundary = WindowBoundary::CURRENT_ROW_RANGE;
	return result;
}

bool PEGTransformerFactory::TransformPrecedingOrFollowing(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return StringUtil::CIEquals(choice_pr.Cast<KeywordParseResult>().keyword, "preceding");
}

WindowExcludeMode PEGTransformerFactory::TransformWindowExcludeClause(PEGTransformer &transformer,
                                                                      ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<WindowExcludeMode>(list_pr.Child<ListParseResult>(1));
}

string PEGTransformerFactory::TransformFraming(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	return choice_pr.Cast<KeywordParseResult>().keyword;
}

WindowExcludeMode PEGTransformerFactory::TransformWindowExcludeElement(PEGTransformer &transformer,
                                                                       ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<WindowExcludeMode>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

vector<unique_ptr<ParsedExpression>> PEGTransformerFactory::TransformWindowPartition(PEGTransformer &transformer,
                                                                                     ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto expression_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(2));
	vector<unique_ptr<ParsedExpression>> result;
	for (auto expression : expression_list) {
		result.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expression));
	}
	return result;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformCoalesceExpression(PEGTransformer &transformer,
                                                   vector<unique_ptr<ParsedExpression>> expression) {
	auto result = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE);
	for (auto &expr : expression) {
		result->GetChildrenMutable().push_back(std::move(expr));
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformUnpackExpression(PEGTransformer &transformer,
                                                                              unique_ptr<ParsedExpression> expression) {
	auto result = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK);
	result->GetChildrenMutable().push_back(std::move(expression));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTryExpression(PEGTransformer &transformer,
                                                                           unique_ptr<ParsedExpression> expression) {
	auto result = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_TRY);
	result->GetChildrenMutable().push_back(std::move(expression));
	return std::move(result);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformColumnsExpression(PEGTransformer &transformer, const bool &has_result,
                                                  unique_ptr<ParsedExpression> expression) {
	bool unpack = has_result;
	auto result = make_uniq<StarExpression>();
	if (expression->GetExpressionType() == ExpressionType::STAR) {
		auto star_expr = unique_ptr_cast<ParsedExpression, StarExpression>(std::move(expression));
		if (star_expr->IsColumns()) {
			result->ExpressionMutable() = std::move(star_expr);
		} else {
			result = std::move(star_expr);
		}
	} else if (expression->GetExpressionType() == ExpressionType::LAMBDA) {
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(make_uniq<StarExpression>());
		children.push_back(std::move(expression));
		auto list_filter = make_uniq<FunctionExpression>("list_filter", std::move(children));
		result->ExpressionMutable() = std::move(list_filter);
	} else {
		result->ExpressionMutable() = std::move(expression);
	}
	result->IsColumnsMutable() = true;
	if (unpack) {
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_UNPACK, std::move(result));
	}
	return std::move(result);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformExtractExpression(PEGTransformer &transformer,
                                                  vector<unique_ptr<ParsedExpression>> extract_arguments) {
	return make_uniq<FunctionExpression>(INVALID_CATALOG, "main", "date_part", std::move(extract_arguments));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformExtractArguments(PEGTransformer &transformer,
                                                 unique_ptr<ParsedExpression> extract_argument,
                                                 unique_ptr<ParsedExpression> expression) {
	vector<unique_ptr<ParsedExpression>> result;
	result.push_back(std::move(extract_argument));
	result.push_back(std::move(expression));
	return result;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformExtractDatePartArgument(PEGTransformer &transformer,
                                                        const DatePartSpecifier &extract_date_part) {
	return make_uniq<ConstantExpression>(EnumUtil::ToString(extract_date_part));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExtractIdentifierArgument(PEGTransformer &transformer,
                                                                                       const Identifier &identifier) {
	return make_uniq<ConstantExpression>(Value(identifier.GetIdentifierName()));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformExtractStringArgument(PEGTransformer &transformer,
                                                                                   const string &string_literal) {
	return make_uniq<ConstantExpression>(Value(string_literal));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformLambdaExpression(
    PEGTransformer &transformer, const vector<Identifier> &col_id_or_string, unique_ptr<ParsedExpression> expression) {
	vector<string> parameters;
	for (auto &parameter : col_id_or_string) {
		parameters.push_back(parameter.GetIdentifierName());
	}
	auto result = make_uniq<LambdaExpression>(parameters, std::move(expression));
	return std::move(result);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformNullIfExpression(PEGTransformer &transformer,
                                                 vector<unique_ptr<ParsedExpression>> null_if_arguments) {
	return make_uniq<FunctionExpression>("nullif", std::move(null_if_arguments));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformNullIfArguments(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
                                                unique_ptr<ParsedExpression> expression_1) {
	vector<unique_ptr<ParsedExpression>> result;
	result.push_back(std::move(expression));
	result.push_back(std::move(expression_1));
	return result;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformRowExpression(PEGTransformer &transformer,
                                              optional<vector<unique_ptr<ParsedExpression>>> expression) {
	if (!expression) {
		return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "row",
		                                     vector<unique_ptr<ParsedExpression>>());
	}
	auto func_expr = make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "row", std::move(*expression));
	return std::move(func_expr);
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformSubstringExpression(PEGTransformer &transformer,
                                                    vector<unique_ptr<ParsedExpression>> substring_arguments) {
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "substring", std::move(substring_arguments));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSubstringExpressionList(PEGTransformer &transformer,
                                                        vector<unique_ptr<ParsedExpression>> expression) {
	return expression;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSubstringParameters(PEGTransformer &transformer,
                                                    unique_ptr<ParsedExpression> expression,
                                                    vector<unique_ptr<ParsedExpression>> substring_from_for) {
	vector<unique_ptr<ParsedExpression>> results;
	results.push_back(std::move(expression));
	for (auto &arg : substring_from_for) {
		results.push_back(std::move(arg));
	}
	return results;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSubstringFromOptionalFor(PEGTransformer &transformer,
                                                         unique_ptr<ParsedExpression> from_expression,
                                                         optional<unique_ptr<ParsedExpression>> for_expression) {
	vector<unique_ptr<ParsedExpression>> results;
	results.push_back(std::move(from_expression));
	if (for_expression && *for_expression) {
		results.push_back(std::move(*for_expression));
	}
	return results;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformSubstringFor(PEGTransformer &transformer, unique_ptr<ParsedExpression> for_expression) {
	vector<unique_ptr<ParsedExpression>> results;
	results.push_back(make_uniq<ConstantExpression>(Value::INTEGER(1)));
	results.push_back(std::move(for_expression));
	return results;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTrimExpression(PEGTransformer &transformer,
                                                                            TrimArguments trim_arguments) {
	string function_name = "trim";
	if (trim_arguments.trim_direction) {
		function_name = *trim_arguments.trim_direction;
	}
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, Identifier(function_name),
	                                     std::move(trim_arguments.expressions));
}

TrimArguments PEGTransformerFactory::TransformTrimArguments(PEGTransformer &transformer,
                                                            const optional<string> &trim_direction,
                                                            optional<unique_ptr<ParsedExpression>> trim_source,
                                                            vector<unique_ptr<ParsedExpression>> expression) {
	TrimArguments result;
	result.trim_direction = trim_direction;
	result.expressions = std::move(expression);
	if (trim_source && *trim_source) {
		result.expressions.push_back(std::move(*trim_source));
	}
	return result;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformTrimSource(PEGTransformer &transformer,
                                           optional<unique_ptr<ParsedExpression>> expression) {
	if (expression) {
		return std::move(*expression);
	}
	return nullptr;
}

string PEGTransformerFactory::TransformTrimBoth(PEGTransformer &transformer) {
	return "trim";
}

string PEGTransformerFactory::TransformTrimLeading(PEGTransformer &transformer) {
	return "ltrim";
}

string PEGTransformerFactory::TransformTrimTrailing(PEGTransformer &transformer) {
	return "rtrim";
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformOverlayExpression(PEGTransformer &transformer,
                                                  vector<unique_ptr<ParsedExpression>> overlay_arguments) {
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "overlay", std::move(overlay_arguments));
}

vector<unique_ptr<ParsedExpression>> PEGTransformerFactory::TransformOverlayParameters(
    PEGTransformer &transformer, unique_ptr<ParsedExpression> expression, unique_ptr<ParsedExpression> expression_1,
    unique_ptr<ParsedExpression> from_expression, optional<unique_ptr<ParsedExpression>> for_expression) {
	vector<unique_ptr<ParsedExpression>> results;
	results.push_back(std::move(expression));
	results.push_back(std::move(expression_1));
	results.push_back(std::move(from_expression));
	if (for_expression && *for_expression) {
		results.push_back(std::move(*for_expression));
	}
	return results;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformFromExpression(PEGTransformer &transformer,
                                                                            unique_ptr<ParsedExpression> expression) {
	return expression;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformForExpression(PEGTransformer &transformer,
                                                                           unique_ptr<ParsedExpression> expression) {
	return expression;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformOverlayExpressionList(PEGTransformer &transformer,
                                                      vector<unique_ptr<ParsedExpression>> expression) {
	return expression;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformPositionExpression(PEGTransformer &transformer,
                                                   vector<unique_ptr<ParsedExpression>> position_arguments) {
	return make_uniq<FunctionExpression>("position", std::move(position_arguments));
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformPositionArguments(PEGTransformer &transformer,
                                                  unique_ptr<ParsedExpression> single_expression,
                                                  unique_ptr<ParsedExpression> single_expression_1) {
	vector<unique_ptr<ParsedExpression>> result;
	result.push_back(std::move(single_expression_1));
	result.push_back(std::move(single_expression));
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCastExpression(PEGTransformer &transformer,
                                                                            const bool &cast_or_try_cast,
                                                                            CastArguments cast_arguments) {
	return make_uniq<CastExpression>(cast_arguments.type, std::move(cast_arguments.expression), cast_or_try_cast);
}

CastArguments PEGTransformerFactory::TransformCastArguments(PEGTransformer &transformer,
                                                            unique_ptr<ParsedExpression> expression,
                                                            const LogicalType &type) {
	CastArguments result;
	result.expression = std::move(expression);
	result.type = type;
	return result;
}

bool PEGTransformerFactory::TransformCastKeyword(PEGTransformer &transformer) {
	return false;
}

bool PEGTransformerFactory::TransformTryCastKeyword(PEGTransformer &transformer) {
	return true;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCaseExpression(
    PEGTransformer &transformer, optional<unique_ptr<ParsedExpression>> expression, vector<CaseCheck> case_when_then,
    optional<unique_ptr<ParsedExpression>> case_else) {
	auto result = make_uniq<CaseExpression>();

	for (auto &case_expr : case_when_then) {
		CaseCheck new_case;
		if (expression) {
			new_case.when_expr = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, (*expression)->Copy(),
			                                                     std::move(case_expr.when_expr));
		} else {
			new_case.when_expr = std::move(case_expr.when_expr);
		}
		new_case.then_expr = std::move(case_expr.then_expr);
		result->CaseChecksMutable().push_back(std::move(new_case));
	}
	if (case_else) {
		result->ElseMutable() = std::move(*case_else);
	} else {
		result->ElseMutable() = make_uniq<ConstantExpression>(Value());
	}
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCaseElse(PEGTransformer &transformer,
                                                                      unique_ptr<ParsedExpression> expression) {
	return expression;
}

CaseCheck PEGTransformerFactory::TransformCaseWhenThen(PEGTransformer &transformer,
                                                       unique_ptr<ParsedExpression> expression,
                                                       unique_ptr<ParsedExpression> expression_1) {
	CaseCheck result;
	result.when_expr = std::move(expression);
	result.then_expr = std::move(expression_1);
	return result;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformTypeLiteral(PEGTransformer &transformer,
                                                                         const Identifier &col_id,
                                                                         const string &string_literal) {
	auto colid = col_id.GetIdentifierName();
	auto type = LogicalType(TransformStringToLogicalTypeId(colid));
	if (type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::STRUCT) {
		throw ParserException("Cannot convert to type %s, requires exactly one type modifier",
		                      EnumUtil::ToString(type.id()));
	}
	if (type == LogicalTypeId::UNBOUND || type.InternalType() == PhysicalType::INVALID) {
		type = LogicalType::UNBOUND(make_uniq<TypeExpression>(colid, vector<unique_ptr<ParsedExpression>>()));
	}
	auto child = make_uniq<ConstantExpression>(Value(string_literal));
	auto result = make_uniq<CastExpression>(type, std::move(child));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformDefaultExpression(PEGTransformer &transformer) {
	return make_uniq<DefaultExpression>();
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformIntervalLiteral(PEGTransformer &transformer,
                                                unique_ptr<ParsedExpression> interval_parameter,
                                                const optional<DatePartSpecifier> &interval) {
	DatePartSpecifier interval_unit = DatePartSpecifier::INVALID;
	if (interval) {
		interval_unit = *interval;
	}
	auto expr = std::move(interval_parameter);
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
	auto result = make_uniq<FunctionExpression>(Identifier(func_name), std::move(children));
	return std::move(result);
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformIntervalStringParameter(PEGTransformer &transformer,
                                                                                     const string &string_literal) {
	return make_uniq<ConstantExpression>(Value(string_literal));
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformSubqueryExpression(PEGTransformer &transformer, const optional<bool> &subquery_not,
                                                   const optional<bool> &subquery_exists,
                                                   unique_ptr<TableRef> subquery_reference) {
	bool is_not = subquery_not ? *subquery_not : false;
	bool is_exists = subquery_exists ? *subquery_exists : false;
	auto result = make_uniq<SubqueryExpression>();
	if (is_exists) {
		result->GetSubqueryTypeMutable() = SubqueryType::EXISTS;
	} else {
		result->GetSubqueryTypeMutable() = SubqueryType::SCALAR;
	}
	if (subquery_reference->type == TableReferenceType::SUBQUERY) {
		auto &subquery_ref = subquery_reference->Cast<SubqueryRef>();
		result->SubqueryMutable() = std::move(subquery_ref.subquery);
	} else {
		auto select_statement = make_uniq<SelectStatement>();
		auto select_node = make_uniq<SelectNode>();
		select_node->select_list.push_back(make_uniq<StarExpression>());
		select_node->from_table = std::move(subquery_reference);
		select_statement->node = std::move(select_node);
		result->SubqueryMutable() = std::move(select_statement);
	}
	if (is_not) {
		vector<unique_ptr<ParsedExpression>> children;
		children.push_back(std::move(result));
		auto not_operator = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(children));
		return std::move(not_operator);
	}
	return std::move(result);
}

bool PEGTransformerFactory::TransformSubqueryNot(PEGTransformer &transformer) {
	return true;
}

bool PEGTransformerFactory::TransformSubqueryExists(PEGTransformer &transformer) {
	return true;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformMapExpression(PEGTransformer &transformer,
                                              vector<unique_ptr<ParsedExpression>> map_struct_expression) {
	return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "map", std::move(map_struct_expression));
}

vector<unique_ptr<ParsedExpression>> PEGTransformerFactory::TransformMapStructExpression(
    PEGTransformer &transformer, optional<vector<vector<unique_ptr<ParsedExpression>>>> map_struct_field) {
	vector<unique_ptr<ParsedExpression>> keys;
	vector<unique_ptr<ParsedExpression>> values;

	if (map_struct_field) {
		for (auto &key_val_pair : *map_struct_field) {
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
PEGTransformerFactory::TransformMapStructField(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
                                               unique_ptr<ParsedExpression> expression_1) {
	vector<unique_ptr<ParsedExpression>> fields;
	fields.push_back(std::move(expression));
	fields.push_back(std::move(expression_1));
	return fields;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformListComprehensionExpression(
    PEGTransformer &transformer, unique_ptr<ParsedExpression> expression, const vector<Identifier> &col_id_or_string,
    unique_ptr<ParsedExpression> expression_1, optional<unique_ptr<ParsedExpression>> list_comprehension_filter) {
	auto result_expr = std::move(expression);
	auto in_expr = std::move(expression_1);
	vector<string> lambda_columns;
	for (auto &col : col_id_or_string) {
		lambda_columns.push_back(col.GetIdentifierName());
	}

	if (!list_comprehension_filter || !*list_comprehension_filter) {
		auto lambda_expression = make_uniq<LambdaExpression>(lambda_columns, std::move(result_expr));
		vector<unique_ptr<ParsedExpression>> apply_children;
		apply_children.push_back(std::move(in_expr));
		apply_children.push_back(std::move(lambda_expression));

		return make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "list_apply", std::move(apply_children));
	}

	auto filter_expr = std::move(*list_comprehension_filter);

	// STAGE 1: list_apply(in_expr, x -> struct_pack(filter := ..., result := ...))
	vector<FunctionArgument> struct_children;
	struct_children.emplace_back("filter", std::move(filter_expr));
	struct_children.emplace_back("result", std::move(result_expr));
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
                                                        unique_ptr<ParsedExpression> expression) {
	return expression;
}

case_insensitive_map_t<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformReplaceList(PEGTransformer &transformer,
                                            case_insensitive_map_t<unique_ptr<ParsedExpression>> replace_entries) {
	return replace_entries;
}

case_insensitive_map_t<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformReplaceEntrySingle(PEGTransformer &transformer,
                                                   pair<string, unique_ptr<ParsedExpression>> replace_entry) {
	case_insensitive_map_t<unique_ptr<ParsedExpression>> entry_map;
	entry_map.insert(std::move(replace_entry));
	return entry_map;
}

case_insensitive_map_t<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformReplaceEntryList(PEGTransformer &transformer,
                                                 vector<pair<string, unique_ptr<ParsedExpression>>> replace_entry) {
	case_insensitive_map_t<unique_ptr<ParsedExpression>> entry_map;
	for (auto &replace_entry : replace_entry) {
		if (entry_map.find(replace_entry.first) != entry_map.end()) {
			throw ParserException("Duplicate entry \"%s\" in REPLACE list", replace_entry.first);
		}
		entry_map.insert(std::move(replace_entry));
	}
	return entry_map;
}

pair<string, unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformReplaceEntry(PEGTransformer &transformer, unique_ptr<ParsedExpression> expression,
                                             unique_ptr<ParsedExpression> column_reference) {
	if (column_reference->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
		throw InternalException("Expected a column reference in the replace entry");
	}
	auto &col_ref = column_reference->Cast<ColumnRefExpression>();
	auto column_name = col_ref.GetColumnName();
	return make_pair(column_name.GetIdentifierName(), std::move(expression));
}

ExpressionType PEGTransformerFactory::TransformIsDistinctFromOp(PEGTransformer &transformer,
                                                                ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	if (list_pr.Child<OptionalParseResult>(1).HasResult()) {
		return ExpressionType::COMPARE_NOT_DISTINCT_FROM;
	}
	return ExpressionType::COMPARE_DISTINCT_FROM;
}

unique_ptr<ParsedExpression>
PEGTransformerFactory::TransformGroupingExpression(PEGTransformer &transformer, const bool &grouping_or_grouping_id,
                                                   optional<vector<unique_ptr<ParsedExpression>>> expression) {
	vector<unique_ptr<ParsedExpression>> grouping_expressions;
	if (expression) {
		grouping_expressions = std::move(*expression);
	}

	auto result = make_uniq<OperatorExpression>(ExpressionType::GROUPING_FUNCTION, std::move(grouping_expressions));
	return std::move(result);
}

bool PEGTransformerFactory::TransformGroupingKeyword(PEGTransformer &transformer) {
	return false;
}

bool PEGTransformerFactory::TransformGroupingIdKeyword(PEGTransformer &transformer) {
	return true;
}

qualified_column_map_t<string>
PEGTransformerFactory::TransformRenameList(PEGTransformer &transformer,
                                           const qualified_column_map_t<string> &rename_entries) {
	return rename_entries;
}

qualified_column_map_t<string>
PEGTransformerFactory::TransformRenameEntryList(PEGTransformer &transformer,
                                                const vector<pair<QualifiedColumnName, string>> &rename_entry) {
	qualified_column_map_t<string> result;
	for (auto &rename_entry : rename_entry) {
		result[rename_entry.first] = rename_entry.second;
	}
	return result;
}

qualified_column_map_t<string>
PEGTransformerFactory::TransformSingleRenameEntry(PEGTransformer &transformer,
                                                  const pair<QualifiedColumnName, string> &rename_entry) {
	qualified_column_map_t<string> result;
	result[rename_entry.first] = rename_entry.second;
	return result;
}

pair<QualifiedColumnName, string> PEGTransformerFactory::TransformRenameEntry(PEGTransformer &transformer,
                                                                              const QualifiedColumnName &exclude_name,
                                                                              const Identifier &identifier) {
	return make_pair(exclude_name, identifier.GetIdentifierName());
}

bool PEGTransformerFactory::TransformIgnoreOrRespectNulls(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<bool>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

} // namespace duckdb
