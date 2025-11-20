#include "duckdb/parser/statement/update_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

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
	for (auto &column : column_list) {
		result->columns.push_back(column->Cast<IdentifierParseResult>().identifier);
	}
	result->expressions.push_back(
	    transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2)));
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
