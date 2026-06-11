#include "duckdb/parser/peg/ast/generic_copy_option.hpp"
#include "duckdb/parser/statement/attach_statement.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

unique_ptr<SQLStatement>
PEGTransformerFactory::TransformAttachStatement(PEGTransformer &transformer, const bool &or_replace,
                                                const bool &if_not_exists, unique_ptr<ParsedExpression> database_path,
                                                const Identifier &attach_alias,
                                                const vector<GenericCopyOption> &attach_options) {
	auto result = make_uniq<AttachStatement>();
	auto info = make_uniq<AttachInfo>();

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

	info->parsed_path = std::move(database_path);
	info->name = Identifier(attach_alias);
	for (const auto &attach_option : attach_options) {
		if (attach_option.expression) {
			info->parsed_options[attach_option.name.GetIdentifierName()] = attach_option.expression->Copy();
			continue;
		}
		if (attach_option.children.empty()) {
			info->options[attach_option.name.GetIdentifierName()] = Value(true);
		} else if (attach_option.children.size() == 1) {
			auto val = attach_option.children[0];
			if (val.IsNull()) {
				throw BinderException("NULL is not supported as a valid option for ATTACH option \"%s\"",
				                      attach_option.name);
			}
			info->options[attach_option.name.GetIdentifierName()] = attach_option.children[0];
		} else {
			throw ParserException("Option %s can only have one argument", attach_option.name);
		}
	}
	result->info = std::move(info);
	return std::move(result);
}

Identifier PEGTransformerFactory::TransformAttachAlias(PEGTransformer &transformer, const Identifier &col_id) {
	return Identifier(col_id);
}

vector<GenericCopyOption>
PEGTransformerFactory::TransformAttachOptions(PEGTransformer &transformer,
                                              const vector<GenericCopyOption> &generic_copy_option_list) {
	return generic_copy_option_list;
}

unique_ptr<ParsedExpression> PEGTransformerFactory::TransformDatabasePath(PEGTransformer &transformer,
                                                                          unique_ptr<ParsedExpression> expression) {
	return expression;
}

} // namespace duckdb
