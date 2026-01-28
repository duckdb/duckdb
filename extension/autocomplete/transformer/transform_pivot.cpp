#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/select_statement.hpp"

namespace duckdb {

unique_ptr<SelectStatement> PEGTransformerFactory::TransformPivotStatement(PEGTransformer &transformer,
                                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto result = make_uniq<SelectStatement>();
	auto select_node = make_uniq<SelectNode>();
	select_node->from_table = transformer.Transform<unique_ptr<TableRef>>(list_pr.GetChild(1));
	transformer.TransformOptional<GroupByNode>(list_pr, 4, select_node->groups);
	for (auto &group : select_node->groups.group_expressions) {
		if (group->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
			throw ParserException("Expected columns in the GROUP BY for PIVOT");
		}
		auto col_ref = group->Cast<ColumnRefExpression>();
		select_node->select_list.push_back(col_ref.Copy());
	}
	vector<unique_ptr<ParsedExpression>> select_list;
	transformer.TransformOptional<vector<unique_ptr<ParsedExpression>>>(list_pr, 3, select_list);
	for (auto &col : select_list) {
		select_node->select_list.push_back(std::move(col));
	}
	result->node = std::move(select_node);
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformPivotUsing(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.GetChild(1));
}

vector<string> PEGTransformerFactory::TransformUnpivotHeader(PEGTransformer &transformer,
                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<vector<string>>(list_pr.Child<ChoiceParseResult>(0).result);
}

vector<string> PEGTransformerFactory::TransformUnpivotHeaderSingle(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> result;
	result.push_back(transformer.Transform<string>(list_pr.GetChild(0)));
	return result;
}

vector<string> PEGTransformerFactory::TransformUnpivotHeaderList(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.GetChild(0));
	auto col_list = ExtractParseResultsFromList(extract_parens);
	vector<string> result;
	for (auto col : col_list) {
		result.push_back(transformer.Transform<string>(col));
	}
	return result;
}

bool PEGTransformerFactory::TransformIncludeOrExcludeNulls(PEGTransformer &transformer,
                                                           optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<bool>(list_pr.Child<ChoiceParseResult>(0).result);
}

} // namespace duckdb
