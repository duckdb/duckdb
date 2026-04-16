#include "duckdb/common/enums/file_compression_type.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/copy_database_statement.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCopyStatement(PEGTransformer &transformer,
                                                                       ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &copy_mode = list_pr.Child<ListParseResult>(1);
	return transformer.Transform<unique_ptr<SQLStatement>>(copy_mode.Child<ChoiceParseResult>(0).GetResult());
}

void SetCopyOptions(unique_ptr<CopyInfo> &info, vector<GenericCopyOption> &options) {
	case_insensitive_string_set_t option_names;
	for (auto &option : options) {
		if (option_names.find(option.name) != option_names.end()) {
			throw ParserException("Unexpected duplicate option \"%s\"", option.name);
		}
		option_names.insert(option.name);
		if (StringUtil::CIEquals(option.name, "PARTITION_BY") || StringUtil::CIEquals(option.name, "FORCE_QUOTE") ||
		    StringUtil::CIEquals(option.name, "FORCE_NOT_NULL") || StringUtil::CIEquals(option.name, "FORCE_NULL")) {
			if (option.expression) {
				info->parsed_options[option.name] = std::move(option.expression);
			} else {
				if (option.children.empty()) {
					throw BinderException("\"%s\" expects a column list or * as parameter", option.name);
				}
				vector<unique_ptr<ParsedExpression>> func_children;
				for (const auto &partition : option.children) {
					func_children.push_back(make_uniq<ColumnRefExpression>(partition.GetValue<string>()));
				}
				auto row_func =
				    make_uniq<FunctionExpression>(INVALID_CATALOG, DEFAULT_SCHEMA, "row", std::move(func_children));
				info->parsed_options[option.name] = std::move(row_func);
			}
		} else if (StringUtil::CIEquals(option.name, "HEADER") || StringUtil::CIEquals(option.name, "ESCAPE")) {
			if (option.children.empty()) {
				info->parsed_options[option.name] = nullptr;
			} else {
				info->parsed_options[option.name] = make_uniq<ConstantExpression>(option.children[0]);
			}
		} else if (StringUtil::CIEquals(option.name, "NULL") || StringUtil::CIEquals(option.name, "NULLSTR")) {
			if (option.children.empty()) {
				info->parsed_options[option.name] = std::move(option.expression);
			} else {
				info->parsed_options[option.name] = make_uniq<ConstantExpression>(option.children[0]);
			}
		} else {
			if (option.expression) {
				info->parsed_options[option.name] = std::move(option.expression);
			} else {
				info->options[option.name] = option.children;
			}
		}
	}
	auto format_option = info->options.find("format");
	if (format_option != info->options.end()) {
		if (format_option->second.empty()) {
			throw ParserException("Unsupported parameter type for FORMAT: expected e.g. FORMAT 'csv', 'parquet'");
		}
		info->format = format_option->second[0].GetValue<string>();
		info->is_format_auto_detected = false;
		info->options.erase(format_option);
	}
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCopySelect(PEGTransformer &transformer,
                                                                    ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &select_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	auto select_statement = transformer.Transform<unique_ptr<SelectStatement>>(select_parens);
	auto result = make_uniq<CopyStatement>();
	auto info = make_uniq<CopyInfo>();
	info->is_from = false;
	auto file_name = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(2));
	if (file_name->GetExpressionClass() == ExpressionClass::CONSTANT) {
		auto &const_expr = file_name->Cast<ConstantExpression>();
		info->file_path = const_expr.value.GetValue<string>();
	} else {
		info->file_path_expression = std::move(file_name);
	}
	auto &options_opt = list_pr.Child<OptionalParseResult>(3);
	if (options_opt.HasResult()) {
		auto options = transformer.Transform<vector<GenericCopyOption>>(options_opt.GetResult());
		SetCopyOptions(info, options);
	}
	info->select_statement = std::move(select_statement->node);
	result->info = std::move(info);
	return std::move(result);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCopyFromDatabase(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();

	auto from_database = transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	auto to_database = transformer.Transform<string>(list_pr.Child<ListParseResult>(4));

	auto &copy_database_flag = list_pr.Child<OptionalParseResult>(5);
	if (copy_database_flag.HasResult()) {
		auto copy_type = transformer.Transform<CopyDatabaseType>(copy_database_flag.GetResult());
		return make_uniq<CopyDatabaseStatement>(from_database, to_database, copy_type);
	}
	auto result = make_uniq<PragmaStatement>();
	result->info->name = "copy_database";
	result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(from_database)));
	result->info->parameters.emplace_back(make_uniq<ConstantExpression>(Value(to_database)));
	return std::move(result);
}

CopyDatabaseType PEGTransformerFactory::TransformCopyDatabaseFlag(PEGTransformer &transformer,
                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &extract_parens = ExtractResultFromParens(list_pr.Child<ListParseResult>(0));
	return transformer.Transform<CopyDatabaseType>(extract_parens);
}

CopyDatabaseType PEGTransformerFactory::TransformSchemaOrData(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<CopyDatabaseType>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

string PEGTransformerFactory::ExtractFormat(const string &file_path) {
	auto format = StringUtil::Lower(file_path);
	if (StringUtil::EndsWith(format, CompressionExtensionFromType(FileCompressionType::GZIP))) {
		format = format.substr(0, format.size() - 3);
	} else if (StringUtil::EndsWith(format, CompressionExtensionFromType(FileCompressionType::ZSTD))) {
		format = format.substr(0, format.size() - 4);
	}
	size_t dot_pos = format.rfind('.');
	if (dot_pos == std::string::npos || dot_pos == format.length() - 1) {
		// No format found
		return "";
	}
	return format.substr(dot_pos + 1);
}

unique_ptr<SQLStatement> PEGTransformerFactory::TransformCopyTable(PEGTransformer &transformer,
                                                                   ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();

	auto result = make_uniq<CopyStatement>();
	auto info = make_uniq<CopyInfo>();

	auto base_table = transformer.Transform<unique_ptr<BaseTableRef>>(list_pr.Child<ListParseResult>(0));
	info->table = base_table->table_name;
	info->schema = base_table->schema_name;
	info->catalog = base_table->catalog_name;
	auto &insert_column_list = list_pr.Child<OptionalParseResult>(1);
	if (insert_column_list.HasResult()) {
		info->select_list = transformer.Transform<vector<string>>(insert_column_list.GetResult());
	}
	info->is_from = transformer.Transform<bool>(list_pr.Child<ListParseResult>(2));
	auto file_name = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(3));
	if (file_name->GetExpressionClass() == ExpressionClass::CONSTANT) {
		auto &const_expr = file_name->Cast<ConstantExpression>();
		info->file_path = const_expr.value.GetValue<string>();
	} else {
		info->file_path_expression = std::move(file_name);
	}
	info->format = ExtractFormat(info->file_path);

	auto &copy_options_pr = list_pr.Child<OptionalParseResult>(4);
	if (copy_options_pr.HasResult()) {
		auto generic_options = transformer.Transform<vector<GenericCopyOption>>(copy_options_pr.GetResult());
		SetCopyOptions(info, generic_options);
	}

	result->info = std::move(info);
	return std::move(result);
}

bool PEGTransformerFactory::TransformFromOrTo(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &from_or_to = list_pr.Child<ChoiceParseResult>(0).GetResult();
	auto &keyword = from_or_to.Cast<KeywordParseResult>();
	return StringUtil::CIEquals(keyword.keyword, "from");
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformCopyFileName(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &choice_pr = list_pr.Child<ChoiceParseResult>(0).GetResult();
	if (choice_pr.name == "ParensExpression" || choice_pr.name == "Parameter") {
		return transformer.Transform<unique_ptr<ParsedExpression>>(choice_pr);
	}
	string file_name;
	if (choice_pr.type == ParseResultType::IDENTIFIER) {
		file_name = choice_pr.Cast<IdentifierParseResult>().identifier;
	} else {
		file_name = transformer.Transform<string>(choice_pr);
	}
	return make_uniq<ConstantExpression>(Value(file_name));
}

string PEGTransformerFactory::TransformIdentifierColId(PEGTransformer &transformer,
                                                       ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	string result;
	result += list_pr.Child<IdentifierParseResult>(0).name;
	result += ".";
	result += transformer.Transform<string>(list_pr.Child<ListParseResult>(2));
	return result;
}

vector<GenericCopyOption> PEGTransformerFactory::TransformCopyOptions(PEGTransformer &transformer,
                                                                      ParseResult &parse_result) {
	// CopyOptions <- 'WITH'? GenericCopyOptionList / SpecializedOptions
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<vector<GenericCopyOption>>(list_pr.Child<ChoiceParseResult>(1).GetResult());
}

vector<GenericCopyOption> PEGTransformerFactory::TransformSpecializedOptionList(PEGTransformer &transformer,
                                                                                ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &options_opt = list_pr.Child<OptionalParseResult>(0);
	if (!options_opt.HasResult()) {
		return {};
	}
	auto &options = options_opt.GetResult().Cast<RepeatParseResult>();
	vector<GenericCopyOption> result;
	for (auto option : options.GetChildren()) {
		result.push_back(transformer.Transform<GenericCopyOption>(option));
	}
	return result;
}

GenericCopyOption PEGTransformerFactory::TransformSpecializedOption(PEGTransformer &transformer,
                                                                    ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<GenericCopyOption>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

GenericCopyOption PEGTransformerFactory::TransformSingleOption(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.TransformEnum<GenericCopyOption>(list_pr.Child<ChoiceParseResult>(0).GetResult());
}

GenericCopyOption PEGTransformerFactory::TransformEncodingOption(PEGTransformer &transformer,
                                                                 ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto string_literal = list_pr.Child<StringLiteralParseResult>(1).result;
	return GenericCopyOption("encoding", string_literal);
}

GenericCopyOption PEGTransformerFactory::TransformForceQuoteOption(PEGTransformer &transformer,
                                                                   ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	bool force_quote = list_pr.Child<OptionalParseResult>(0).HasResult();
	string func_name = force_quote ? "force_quote" : "quote";
	auto &star_or_column_list_pr = list_pr.Child<ListParseResult>(2);
	auto &star_or_column_list = star_or_column_list_pr.Child<ChoiceParseResult>(0).GetResult();
	auto result = GenericCopyOption();
	result.name = func_name;
	if (StringUtil::CIEquals(star_or_column_list.name, "StarSymbol")) {
		result.expression = make_uniq<StarExpression>();
	} else if (StringUtil::CIEquals(star_or_column_list.name, "ColumnList")) {
		auto column_list = transformer.Transform<vector<string>>(star_or_column_list);
		for (auto &col : column_list) {
			result.children.push_back(Value(col));
		}
	}

	return result;
}

GenericCopyOption PEGTransformerFactory::TransformQuoteAsOption(PEGTransformer &transformer,
                                                                ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto string_literal = list_pr.Child<StringLiteralParseResult>(2).result;
	return GenericCopyOption("quote", string_literal);
}

GenericCopyOption PEGTransformerFactory::TransformForceNullOption(PEGTransformer &transformer,
                                                                  ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	bool is_not = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto result = GenericCopyOption();
	result.name = is_not ? "force_not_null" : "force_null";
	auto column_list = transformer.Transform<vector<string>>(list_pr.Child<ListParseResult>(3));
	for (auto &col : column_list) {
		result.children.push_back(Value(col));
	}
	return result;
}

GenericCopyOption PEGTransformerFactory::TransformPartitionByOption(PEGTransformer &transformer,
                                                                    ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto result = GenericCopyOption();
	auto &star_or_column_list_pr = list_pr.Child<ListParseResult>(2);
	auto &star_or_column_list = star_or_column_list_pr.Child<ChoiceParseResult>(0).GetResult();
	result.name = "partition_by";
	if (StringUtil::CIEquals(star_or_column_list.name, "StarSymbol")) {
		result.expression = make_uniq<StarExpression>();
	} else if (StringUtil::CIEquals(star_or_column_list.name, "ColumnList")) {
		auto column_list = transformer.Transform<vector<string>>(star_or_column_list);
		for (auto &col : column_list) {
			result.children.push_back(Value(col));
		}
	}
	return result;
}

GenericCopyOption PEGTransformerFactory::TransformNullAsOption(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto string_literal = list_pr.Child<StringLiteralParseResult>(2).result;
	return GenericCopyOption("null", string_literal);
}

GenericCopyOption PEGTransformerFactory::TransformDelimiterAsOption(PEGTransformer &transformer,
                                                                    ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto string_literal = list_pr.Child<StringLiteralParseResult>(2).result;
	return GenericCopyOption("delimiter", string_literal);
}

GenericCopyOption PEGTransformerFactory::TransformEscapeAsOption(PEGTransformer &transformer,
                                                                 ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto string_literal = list_pr.Child<StringLiteralParseResult>(2).result;
	return GenericCopyOption("escape", string_literal);
}

} // namespace duckdb
