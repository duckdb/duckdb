#include "duckdb/parser/statement/pragma_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformImportStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto name = list_pr.Child<StringLiteralParseResult>(2).result;
	auto result = make_uniq<PragmaStatement>();
	result->info->name = "import_database";
	result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(name)));
	return std::move(result);
}

} // namespace duckdb
