#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformFunctionArgument(PEGTransformer &transformer,
                                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ChoiceParseResult>(0).result);
}

} // namespace duckdb
