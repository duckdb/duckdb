#include "ast/analyze_target.hpp"
#include "duckdb/parser/parsed_data/vacuum_info.hpp"
#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/vacuum_statement.hpp"

namespace duckdb {
unique_ptr<SQLStatement> PEGTransformerFactory::TransformAnalyzeStatement(PEGTransformer &transformer,
																   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	VacuumOptions vacuum_options;
	vacuum_options.analyze = true;
	auto result = make_uniq<VacuumStatement>(vacuum_options);
	auto target_opt = list_pr.Child<OptionalParseResult>(2);
	if (target_opt.HasResult()) {
		auto target = transformer.Transform<AnalyzeTarget>(target_opt.optional_result);
	}
	return result;
}

AnalyzeTarget PEGTransformerFactory::TransformAnalyzeTarget(PEGTransformer &transformer,
																   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	AnalyzeTarget result;
	result.ref = transformer.Transform<unique_ptr<TableRef>>(list_pr.Child<ListParseResult>(0));
	transformer.TransformOptional<vector<string>>(list_pr, 1, result.columns);
	return result;
}

// TODO(Dtenwolde) Move this to transform_vacuum.cpp
vector<string> PEGTransformerFactory::TransformNameList(PEGTransformer &transformer,
																   optional_ptr<ParseResult> parse_result) {
	throw NotImplementedException("TransformName has not yet been implemented");
}

} // namespace duckdb