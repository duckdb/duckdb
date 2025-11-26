#include "duckdb/parser/statement/update_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformUpdateStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<UpdateStatement>();
	auto with_opt = list_pr.Child<OptionalParseResult>(0);
	if (with_opt.HasResult()) {
		result->cte_map = transformer.Transform<CommonTableExpressionMap>(with_opt.optional_result);
	}
	result->table = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(2));
	result->set_info = transformer.Transform<unique_ptr<UpdateSetInfo>>(list_pr.Child<ListParseResult>(3));
	transformer.TransformOptional<unique_ptr<TableRef>>(list_pr, 4, result->from_table);
	transformer.TransformOptional<unique_ptr<ParsedExpression>>(list_pr, 5, result->set_info->condition);
	transformer.TransformOptional<vector<unique_ptr<ParsedExpression>>>(list_pr, 6, result->returning_list);
	return result;
}

unique_ptr<TableRef> PEGTransformerFactory::TransformUpdateTarget(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<TableRef> PEGTransformerFactory::TransformBaseTableSet(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(0));
}

unique_ptr<TableRef> PEGTransformerFactory::TransformBaseTableAliasSet(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto table_ref = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<string>(list_pr, 1, table_ref->alias);
	return table_ref;
}

string PEGTransformerFactory::TransformUpdateAlias(PEGTransformer &transformer,
                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<string>(list_pr.Child<ListParseResult>(1));
}

unique_ptr<UpdateSetInfo> PEGTransformerFactory::TransformUpdateSetClause(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<UpdateSetInfo>>(list_pr.Child<ChoiceParseResult>(0).result);
}

unique_ptr<UpdateSetInfo> PEGTransformerFactory::TransformUpdateSetTuple(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto column_list = ExtractParseResultsFromList(extract_parens);
	auto result = make_uniq<UpdateSetInfo>();
	result->columns.reserve(column_list.size());
	for (auto &column : column_list) {
		result->columns.push_back(column->Cast<IdentifierParseResult>().identifier);
	}

	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	bool is_row_assignment = false;
	if (expr->GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &func_ref = expr->Cast<FunctionExpression>();
		if (StringUtil::CIEquals(func_ref.function_name, "row")) {
			is_row_assignment = true;
		}
	}

	if (is_row_assignment) {
		auto &func_expr = expr->Cast<FunctionExpression>();
		if (func_expr.children.size() != result->columns.size()) {
			throw ParserException("Could not perform assignment, expected %d values, got %d", result->columns.size(),
			                      func_expr.children.size());
		}
		result->expressions = std::move(func_expr.children);
	} else {
		result->expressions.reserve(result->columns.size());
		for (idx_t i = 0; i < result->columns.size(); i++) {
			result->expressions.push_back(expr->Copy());
		}
	}

	return result;
}

unique_ptr<UpdateSetInfo> PEGTransformerFactory::TransformUpdateSetElementList(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<UpdateSetInfo>();
	auto update_element_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	for (auto &update_element : update_element_list) {
		auto column_expr = transformer.Transform<pair<string, unique_ptr<ParsedExpression>>>(update_element);
		result->columns.push_back(column_expr.first);
		result->expressions.push_back(std::move(column_expr.second));
	}
	return result;
}

pair<string, unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformUpdateSetElement(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto column_name = list_pr.Child<IdentifierParseResult>(0).identifier;
	auto expr = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	return make_pair(column_name, std::move(expr));
}
} // namespace duckdb
