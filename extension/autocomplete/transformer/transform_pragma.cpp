#include "duckdb/parser/statement/pragma_statement.hpp"

namespace duckdb {
unique_ptr<SQLStatement> PEGTransformerFactory::TransformPragmaStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	// Rule: PragmaStatement <- 'PRAGMA'i (PragmaAssign / PragmaFunction)
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &matched_child = list_pr.Child<ListParseResult>(1).Child<ChoiceParseResult>(0);
	return transformer.Transform<unique_ptr<SQLStatement>>(matched_child.result);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformPragmaAssign(PEGTransformer &transformer,
                                                                      optional_ptr<ParseResult> parse_result) {
	// Rule: PragmaAssign <- SettingName '=' Expression
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<PragmaStatement>();
	auto &info = *result->info;
	info.name = list_pr.Child<IdentifierParseResult>(0).identifier;
	auto value = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	info.parameters.push_back(std::move(value));

	auto set_statement = make_uniq<SetVariableStatement>(info.name, std::move(info.parameters[0]), SetScope::AUTOMATIC);
	return std::move(set_statement);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformPragmaFunction(PEGTransformer &transformer,
                                                                        optional_ptr<ParseResult> parse_result) {
	// Rule: PragmaFunction <- PragmaName PragmaParameters?
	auto result = make_uniq<PragmaStatement>();
	auto &list_pr = parse_result->Cast<ListParseResult>();
	result->info->name = list_pr.Child<IdentifierParseResult>(0).identifier;
	auto &optional_parameters_pr = list_pr.Child<OptionalParseResult>(1);
	if (optional_parameters_pr.HasResult()) {
		result->info->parameters =
		    transformer.Transform<vector<unique_ptr<ParsedExpression>>>(optional_parameters_pr.optional_result);
	}
	return result;
}

vector<unique_ptr<ParsedExpression>>
PEGTransformerFactory::TransformPragmaParameters(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	// TODO(Dtenwolde) Check about named parameters
	// PragmaParameters <- List(Expression)
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto expr_list = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	vector<unique_ptr<ParsedExpression>> parameters;
	for (auto expr : expr_list) {
		parameters.push_back(transformer.Transform<unique_ptr<ParsedExpression>>(expr));
	}
	return parameters;
}

} // namespace duckdb
