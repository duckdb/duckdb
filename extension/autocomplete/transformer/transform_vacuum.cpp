#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/vacuum_statement.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformVacuumStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	VacuumOptions options;
	transformer.TransformOptional<VacuumOptions>(list_pr, 1, options);
	auto result = make_uniq<VacuumStatement>(options);
	auto target_opt = list_pr.Child<OptionalParseResult>(2);
	if (target_opt.HasResult()) {
		auto target = transformer.Transform<AnalyzeTarget>(target_opt.optional_result);
		result->info->columns = target.columns;
		result->info->ref = std::move(target.ref);
		result->info->has_table = true;
	}
	return std::move(result);
}

VacuumOptions PEGTransformerFactory::TransformVacuumOptions(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<VacuumOptions>(list_pr.Child<ChoiceParseResult>(0).result);
}

VacuumOptions PEGTransformerFactory::TransformVacuumLegacyOptions(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	VacuumOptions options;
	options.vacuum = true;
	options.analyze = list_pr.Child<OptionalParseResult>(3).HasResult();
	if (list_pr.Child<OptionalParseResult>(0).HasResult()) {
		throw NotImplementedException("FULL is not yet implemented");
	}
	if (list_pr.Child<OptionalParseResult>(1).HasResult()) {
		throw NotImplementedException("FREEZE is not yet implemented");
	}
	if (list_pr.Child<OptionalParseResult>(2).HasResult()) {
		throw NotImplementedException("VERBOSE is not yet implemented");
	}
	return options;
}

VacuumOptions PEGTransformerFactory::TransformVacuumParensOptions(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto option_list = ExtractParseResultsFromList(extract_parens);
	VacuumOptions options;
	options.vacuum = true;
	for (auto &option : option_list) {
		string option_name = transformer.Transform<string>(option);
		if (StringUtil::CIEquals(option_name, "disable_page_skipping")) {
			throw NotImplementedException("Disable Page Skipping vacuum option");
		}
		if (StringUtil::CIEquals(option_name, "freeze")) {
			throw NotImplementedException("FREEZE is not yet implemented");
		}
		if (StringUtil::CIEquals(option_name, "full")) {
			throw NotImplementedException("FULL is not yet implemented");
		}
		if (StringUtil::CIEquals(option_name, "verbose")) {
			throw NotImplementedException("VERBOSE is not yet implemented");
		}
		if (StringUtil::CIEquals(option_name, "analyze")) {
			options.analyze = true;
		}
	}
	return options;
}

string PEGTransformerFactory::TransformVacuumOption(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto option = list_pr.Child<ChoiceParseResult>(0).result;
	if (option->type == ParseResultType::IDENTIFIER) {
		return option->Cast<IdentifierParseResult>().identifier;
	}
	return transformer.TransformEnum<string>(option);
}

vector<string> PEGTransformerFactory::TransformNameList(PEGTransformer &transformer,
                                                        optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	vector<string> result;
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto colid_list = ExtractParseResultsFromList(extract_parens);
	for (auto &colid : colid_list) {
		result.push_back(transformer.Transform<string>(colid));
	}
	return result;
}

} // namespace duckdb
