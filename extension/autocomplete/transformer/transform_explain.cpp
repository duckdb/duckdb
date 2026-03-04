#include "transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/explain_statement.hpp"

namespace duckdb {

ExplainFormat ParseExplainFormat(const Value &val) {
	if (val.type().id() != LogicalTypeId::VARCHAR) {
		throw InvalidInputException("Expected a string as argument to FORMAT");
	}
	auto format_val = val.GetValue<string>();
	case_insensitive_map_t<ExplainFormat> format_mapping {
	    {"default", ExplainFormat::DEFAULT}, {"text", ExplainFormat::TEXT},         {"json", ExplainFormat::JSON},
	    {"html", ExplainFormat::HTML},       {"graphviz", ExplainFormat::GRAPHVIZ}, {"yaml", ExplainFormat::YAML},
	    {"mermaid", ExplainFormat::MERMAID}};
	auto it = format_mapping.find(format_val);
	if (it != format_mapping.end()) {
		return it->second;
	}
	vector<string> options_list;
	for (auto &it : format_mapping) {
		options_list.push_back(it.first);
	}
	auto allowed_options = StringUtil::Join(options_list, ", ");
	throw InvalidInputException("\"%s\" is not a valid FORMAT argument, valid options are: %s", format_val,
	                            allowed_options);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformExplainStatement(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto explain_analyze = list_pr.Child<OptionalParseResult>(1);
	auto explain_type = explain_analyze.HasResult() ? ExplainType::EXPLAIN_ANALYZE : ExplainType::EXPLAIN_STANDARD;
	bool format_is_set = false;
	auto explain_format = ExplainFormat::DEFAULT;
	auto explain_options_opt = list_pr.Child<OptionalParseResult>(2);
	if (explain_options_opt.HasResult()) {
		auto options = transformer.Transform<vector<GenericCopyOption>>(explain_options_opt.optional_result);
		for (auto option : options) {
			auto option_name = StringUtil::Lower(option.name);
			if (option_name == "format") {
				if (format_is_set) {
					throw InvalidInputException("FORMAT can not be provided more than once");
				}
				explain_format = ParseExplainFormat(option.children[0]);
				format_is_set = true;
			} else if (option_name == "analyze") {
				explain_type = ExplainType::EXPLAIN_ANALYZE;
			}
		}
	}
	auto statement = transformer.Transform<unique_ptr<SQLStatement>>(list_pr.Child<ListParseResult>(3));
	return make_uniq<ExplainStatement>(std::move(statement), explain_type, explain_format);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformExplainableStatements(PEGTransformer &transformer,
                                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto choice_pr = list_pr.Child<ChoiceParseResult>(0).result;
	if (StringUtil::CIEquals(choice_pr->name, "SelectStatementInternal")) {
		return transformer.Transform<unique_ptr<SelectStatement>>(list_pr.Child<ChoiceParseResult>(0).result);
	}
	return transformer.Transform<unique_ptr<SQLStatement>>(list_pr.Child<ChoiceParseResult>(0).result);
}

} // namespace duckdb
