#include "transformer/peg_transformer.hpp"

namespace duckdb {

QualifiedName PEGTransformerFactory::TransformQualifiedSequenceName(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	QualifiedName result;
	transformer.TransformOptional<string>(list_pr, 0, result.catalog);
	transformer.TransformOptional<string>(list_pr, 1, result.schema);
	result.name = transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	return result;
}

string PEGTransformerFactory::TransformSequenceName(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}
} // namespace duckdb
