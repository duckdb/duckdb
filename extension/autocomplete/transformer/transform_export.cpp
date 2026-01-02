#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/parser/statement/export_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformExportStatement(PEGTransformer &transformer,
                                                                         optional_ptr<ParseResult> parse_result) {
	auto info = make_uniq<CopyInfo>();
	auto &list_pr = parse_result->Cast<ListParseResult>();
	info->file_path = list_pr.Child<StringLiteralParseResult>(3).result;
	info->format = "csv";
	info->is_from = false;

	auto &parens = list_pr.Child<OptionalParseResult>(4);
	if (parens.HasResult()) {
		auto &generic_copy_option_list = parens.optional_result->Cast<ListParseResult>().Child<ListParseResult>(1);
		auto option_list = transformer.Transform<unordered_map<string, vector<Value>>>(generic_copy_option_list);
		case_insensitive_map_t<vector<Value>> option_result;
		for (auto &option : option_list) {
			option_result[option.first] = option.second;
		}
		info->options = option_result;
	}

	auto result = make_uniq<ExportStatement>(std::move(info));
	auto database_result = list_pr.Child<OptionalParseResult>(2);
	if (database_result.HasResult()) {
		result->database = transformer.Transform<string>(database_result.optional_result);
	}
	return std::move(result);
}

string PEGTransformerFactory::TransformExportSource(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(0).identifier;
}

} // namespace duckdb
