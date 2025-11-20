#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCopyStatement(PEGTransformer &transformer,
                                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto &copy_mode = list_pr.Child<ListParseResult>(1);
	return transformer.Transform<unique_ptr<SQLStatement>>(copy_mode.Child<ChoiceParseResult>(0).result);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCopySelect(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	throw NotImplementedException("Copy SELECT has not yet been implemented");
	auto select_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(select_parens);
	auto result = make_uniq<CopyStatement>();
	auto info = make_uniq<CopyInfo>();
	info->is_from = false;
	info->file_path = transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	auto options_opt = list_pr.Child<OptionalParseResult>(3);
	if (options_opt.HasResult()) {
		info->options = transformer.Transform<case_insensitive_map_t<vector<Value>>>(options_opt.optional_result);
	}
	info->select_statement = std::move(select_statement->node);
	result->info = std::move(info);
	return std::move(result);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCopyFromDatabase(PEGTransformer &transformer,
                                                                          optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto from_database = transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	auto to_database = transformer.Transform<string>(list_pr.Child<ListParseResult>(4));

	auto copy_database_flag = list_pr.Child<OptionalParseResult>(5);
	if (copy_database_flag.HasResult()) {
		auto copy_type = transformer.Transform<CopyDatabaseType>(copy_database_flag.optional_result);
		return make_uniq<CopyDatabaseStatement>(from_database, to_database, copy_type);
	}
	auto result = make_uniq<PragmaStatement>();
	result->info->name = "copy_database";
	result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(from_database)));
	result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(to_database)));
	return std::move(result);
}

CopyDatabaseType PEGTransformerFactory::TransformCopyDatabaseFlag(PEGTransformer &transformer,
                                                                  optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0))->Cast<ListParseResult>();
	auto schema_or_data = extract_parens.Child<ChoiceParseResult>(0);
	return transformer.TransformEnum<CopyDatabaseType>(schema_or_data.result);
}

string PEGTransformerFactory::ExtractFormat(const string &file_path) {
	auto format = StringUtil::Lower(file_path);
	// We first remove extension suffixes
	if (StringUtil::EndsWith(format, CompressionExtensionFromType(FileCompressionType::GZIP))) {
		format = format.substr(0, format.size() - 3);
	} else if (StringUtil::EndsWith(format, CompressionExtensionFromType(FileCompressionType::ZSTD))) {
		format = format.substr(0, format.size() - 4);
	}
	// Now lets check for the last .
	size_t dot_pos = format.rfind('.');
	if (dot_pos == std::string::npos || dot_pos == format.length() - 1) {
		// No format found
		return "";
	}
	// We found something
	return format.substr(dot_pos + 1);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCopyTable(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();

	auto result = make_uniq<CopyStatement>();
	auto info = make_uniq<CopyInfo>();

	auto base_table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(0));
	info->table = base_table->table_name;
	info->schema = base_table->schema_name;
	info->catalog = base_table->catalog_name;
	auto insert_column_list = list_pr.Child<OptionalParseResult>(1);
	if (insert_column_list.HasResult()) {
		info->select_list = transformer.Transform<vector<string>>(insert_column_list.optional_result);
	}
	info->is_from = transformer.Transform<bool>(list_pr.Child<ListParseResult>(2));
	info->file_path = transformer.Transform<string>(list_pr.Child<ListParseResult>(3));
	info->format = ExtractFormat(info->file_path);

	auto &copy_options_pr = list_pr.Child<OptionalParseResult>(4);
	if (copy_options_pr.HasResult()) {
		info->options = transformer.Transform<case_insensitive_map_t<vector<Value>>>(copy_options_pr.optional_result);
		auto format_option = info->options.find("format");
		if (format_option != info->options.end()) {
			info->format = format_option->second[0].GetValue<string>();
			info->is_format_auto_detected = false;
			info->options.erase(format_option);
		}
	}

	result->info = std::move(info);
	return std::move(result);
}

bool PEGTransformerFactory::TransformFromOrTo(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto from_or_to = list_pr.Child<ChoiceParseResult>(0).result;
	auto keyword = from_or_to->Cast<KeywordParseResult>();
	return StringUtil::CIEquals(keyword.keyword, "from");
}

string PEGTransformerFactory::TransformCopyFileName(PEGTransformer &transformer,
                                                    optional_ptr<ParseResult> parse_result) {
	// TODO(dtenwolde) support stdin and stdout
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<string>(list_pr.Child<ChoiceParseResult>(0).result);
}

string PEGTransformerFactory::TransformIdentifierColId(PEGTransformer &transformer,
                                                       optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	string result;
	result += list_pr.Child<IdentifierParseResult>(0).name;
	result += ".";
	result += transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	return result;
}

case_insensitive_map_t<vector<Value>>
PEGTransformerFactory::TransformCopyOptions(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	// CopyOptions <- 'WITH'i? Parens(GenericCopyOptionList) / SpecializedOption+
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto copy_option_pr = list_pr.Child<ChoiceParseResult>(1).result;
	return transformer.Transform<case_insensitive_map_t<vector<Value>>>(copy_option_pr);
}

case_insensitive_map_t<vector<Value>>
PEGTransformerFactory::TransformGenericCopyOptionListParens(PEGTransformer &transformer,
                                                            optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto generic_options = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto generic_options_transformed = transformer.Transform<unordered_map<string, vector<Value>>>(generic_options);
	case_insensitive_map_t<vector<Value>> result;
	for (auto option : generic_options_transformed) {
		result[option.first] = {option.second};
	}
	return result;
}

case_insensitive_map_t<vector<Value>>
PEGTransformerFactory::TransformSpecializedOptionList(PEGTransformer &transformer,
                                                      optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto options = ExtractParseResultsFromList(list_pr.Child<ListParseResult>(0));
	case_insensitive_map_t<vector<Value>> result;
	for (auto option : options) {
		auto option_result = transformer.Transform<GenericCopyOption>(option);
		result[option_result.name] = option_result.children;
	}

	return result;
}

GenericCopyOption PEGTransformerFactory::TransformSpecializedOption(PEGTransformer &transformer,
                                                                    optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<GenericCopyOption>(list_pr.Child<ChoiceParseResult>(0).result);
}

GenericCopyOption PEGTransformerFactory::TransformSingleOption(PEGTransformer &transformer,
                                                               optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.TransformEnum<GenericCopyOption>(list_pr.Child<ChoiceParseResult>(0).result);
}

GenericCopyOption PEGTransformerFactory::TransformEncodingOption(PEGTransformer &transformer,
                                                                 optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto string_literal = list_pr.Child<StringLiteralParseResult>(1).result;
	return GenericCopyOption("encoding", string_literal);
}

GenericCopyOption PEGTransformerFactory::TransformForceQuoteOption(PEGTransformer &transformer,
                                                                   optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	bool force_quote = list_pr.Child<OptionalParseResult>(0).HasResult();
	string func_name = force_quote ? "force_quote" : "quote";
	// TODO(dtenwolde) continue with options here. Need to return ParsedExpressions rather than Value
	return GenericCopyOption(func_name, Value());
}

} // namespace duckdb
