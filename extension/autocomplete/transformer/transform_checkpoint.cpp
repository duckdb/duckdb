#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCheckpointStatement(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto &force_pr = list_pr.Child<OptionalParseResult>(0);
	auto checkpoint_name = force_pr.HasResult() ? "force_checkpoint" : "checkpoint";
	auto result = make_uniq<CallStatement>();
	vector<unique_ptr<ParsedExpression>> children;
	auto function = make_uniq<FunctionExpression>(checkpoint_name, std::move(children));
	function->catalog = SYSTEM_CATALOG;
	function->schema = DEFAULT_SCHEMA;

	auto catalog_name_pr = list_pr.Child<OptionalParseResult>(2);
	if (catalog_name_pr.HasResult()) {
		function->children.push_back(
		    make_uniq<ConstantExpression>(catalog_name_pr.optional_result->Cast<IdentifierParseResult>().identifier));
	}
	result->function = std::move(function);
	return std::move(result);
}

} // namespace duckdb
