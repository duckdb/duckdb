#include "duckdb/parser/statement/prepare_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformPrepareStatement(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<PrepareStatement>();
	result->name = list_pr.Child<IdentifierParseResult>(1).identifier;
	auto type_list_opt = list_pr.Child<OptionalParseResult>(2);
	if (type_list_opt.HasResult()) {
		throw NotImplementedException("TypeList for prepared statement has not been implemented.");
	}
	result->statement = transformer.Transform<unique_ptr<SQLStatement>>(list_pr.Child<ListParseResult>(4));
	transformer.ClearParameters();
	return std::move(result);
}

} // namespace duckdb
