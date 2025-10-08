#include "transformer/peg_transformer.hpp"

namespace duckdb {

// IdentifierOrStringLiteral <- Identifier / StringLiteral
string PEGTransformerFactory::TransformIdentifierOrStringLiteral(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		return choice_pr.result->Cast<IdentifierParseResult>().identifier;
	}
	if (choice_pr.result->type == ParseResultType::STRING) {
		return choice_pr.result->Cast<StringLiteralParseResult>().result;
	}
	throw NotImplementedException("Unexpected type encountered: %s", ParseResultToString(choice_pr.result->type));
}

string PEGTransformerFactory::TransformColIdOrString(PEGTransformer &transformer,
                                                     optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	return transformer.Transform<string>(choice_pr.result);
}

string PEGTransformerFactory::TransformColId(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0);
	if (choice_pr.result->type == ParseResultType::IDENTIFIER) {
		return choice_pr.result->Cast<IdentifierParseResult>().identifier;
	}
	return transformer.Transform<string>(choice_pr.result);
}

string PEGTransformerFactory::TransformIdentifier(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

} // namespace duckdb
