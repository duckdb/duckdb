#include "transformer/peg_transformer.hpp"

namespace duckdb {


// UseStatement <- 'USE' UseTarget
unique_ptr<SQLStatement> PEGTransformerFactory::TransformUseStatement(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'UseStatement' has not been implemented yet");
}

// UseTarget <- (CatalogName '.' ReservedSchemaName) / SchemaName / CatalogName
unique_ptr<SQLStatement> PEGTransformerFactory::TransformUseTarget(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
    auto &list_pr = parse_result->Cast<ListParseResult>();
    throw NotImplementedException("Rule 'UseTarget' has not been implemented yet");
}
} // namespace duckdb
