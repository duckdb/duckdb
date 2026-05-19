#include "duckdb/parser/peg/ast/generic_copy_option.hpp"
#include "duckdb/parser/statement/attach_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement> PEGTransformerFactory::TransformAttachStatement(PEGTransformer &transformer,
                                                                         ParseResult &parse_result) {
	auto result = make_uniq<AttachStatement>();
	auto info = make_uniq<AttachInfo>();

	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto or_replace = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto if_not_exists = list_pr.Child<OptionalParseResult>(2).HasResult();

	if (or_replace && if_not_exists) {
		throw ParserException("Cannot specify both OR REPLACE and IF NOT EXISTS at the same time");
	}

	if (or_replace) {
		info->on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;
	} else if (if_not_exists) {
		info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	} else {
		info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	}

	info->parsed_path = transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.Child<ListParseResult>(4));
	transformer.TransformOptional<string>(list_pr, 5, info->name);
	vector<GenericCopyOption> copy_options;
	transformer.TransformOptional<vector<GenericCopyOption>>(list_pr, 6, copy_options);
	for (auto &copy_option : copy_options) {
		if (copy_option.expression) {
			info->parsed_options[copy_option.name] = std::move(copy_option.expression);
			continue;
		}
		if (copy_option.children.empty()) {
			info->options[copy_option.name] = Value(true);
		} else if (copy_option.children.size() == 1) {
			auto val = copy_option.children[0];
			if (val.IsNull()) {
				throw BinderException("NULL is not supported as a valid option for ATTACH option \"%s\"",
				                      copy_option.name);
			}
			info->options[copy_option.name] = std::move(copy_option.children[0]);
		} else {
			throw ParserException("Option %s can only have one argument", copy_option.name);
		}
	}
	result->info = std::move(info);
	return std::move(result);
}

string PEGTransformerFactory::TransformAttachAlias(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	auto &col_id = list_pr.Child<ListParseResult>(1).Child<ChoiceParseResult>(0);
	if (col_id.GetResult().type == ParseResultType::IDENTIFIER) {
		return col_id.GetResult().Cast<IdentifierParseResult>().identifier;
	}
	return transformer.Transform<string>(col_id.GetResult());
}

vector<GenericCopyOption> PEGTransformerFactory::TransformAttachOptions(PEGTransformer &transformer,
                                                                        ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<vector<GenericCopyOption>>(list_pr.Child<ListParseResult>(0));
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformDatabasePath(PEGTransformer &transformer,
                                                                          ParseResult &parse_result) {
	auto &list_pr = parse_result.Cast<ListParseResult>();
	return transformer.Transform<unique_ptr<ParsedExpression>>(list_pr.GetChild(0));
}

} // namespace duckdb
