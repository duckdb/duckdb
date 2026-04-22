#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/statement/export_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformExportStatement(PEGTransformer &transformer,
                                                                         ParseResult &parse_result) {
	auto info = make_uniq<CopyInfo>();
	auto &list_pr = parse_result.Cast<ListParseResult>();
	info->file_path = list_pr.Child<StringLiteralParseResult>(3).result;
	info->format = "csv";
	info->is_from = false;

	auto &parens = list_pr.Child<OptionalParseResult>(4);
	if (parens.HasResult()) {
		auto option_list = transformer.Transform<vector<GenericCopyOption>>(parens.GetResult());
		for (auto &option : option_list) {
			if (option.name == "format") {
				info->format = option.children[0].GetValue<string>();
				info->is_format_auto_detected = false;
			} else if (option.expression) {
				info->parsed_options[StringUtil::Upper(option.name)] = std::move(option.expression);
			} else {
				info->options[StringUtil::Upper(option.name)] = option.children;
			}
		}
	}

	auto result = make_uniq<ExportStatement>(std::move(info));
	auto &database_result = list_pr.Child<OptionalParseResult>(2);
	if (database_result.HasResult()) {
		result->database = transformer.Transform<string>(database_result.GetResult());
	}
	return std::move(result);
}

string PEGTransformerFactory::TransformExportSource(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

} // namespace duckdb
