#include "duckdb/function/function.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCallStatement(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto table_function_name = list_pr.Child<IdentifierParseResult>(1).identifier;
	auto function_children =
	    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(list_pr.Child<ListParseResult>(2));
	auto result = make_uniq<CallStatement>();
	auto function_expression = make_uniq<FunctionExpression>(table_function_name, std::move(function_children));
	result->function = std::move(function_expression);
	return std::move(result);
}

} // namespace duckdb
