#include "transformer/peg_transformer.hpp"

namespace duckdb {

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
