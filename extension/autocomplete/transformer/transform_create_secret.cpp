#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "transformer/peg_transformer.hpp"

namespace duckdb {

Value PEGTransformerFactory::GetConstantExpressionValue(unique_ptr<ParsedExpression> &expr) {
	if (expr->type == ExpressionType::VALUE_CONSTANT) {
		return expr->Cast<ConstantExpression>().value;
	}
	if (expr->type == ExpressionType::COLUMN_REF) {
		return expr->Cast<ColumnRefExpression>().GetName();
	}
	return Value();
}

unique_ptr<CreateStatement> PEGTransformerFactory::TransformCreateSecretStmt(PEGTransformer &transformer,
                                                                             optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	auto result = make_uniq<CreateStatement>();
	auto if_not_exists = list_pr.Child<OptionalParseResult>(1).HasResult();
	auto on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	auto info = make_uniq<CreateSecretInfo>(on_conflict, SecretPersistType::DEFAULT);
	auto secret_name_pr = list_pr.Child<OptionalParseResult>(2);
	if (secret_name_pr.HasResult()) {
		info->name = transformer.Transform<string>(secret_name_pr.optional_result);
	}
	auto secret_storage_specifier_pr = list_pr.Child<OptionalParseResult>(3);
	if (secret_storage_specifier_pr.HasResult()) {
		info->storage_type =
		    StringUtil::Lower(transformer.Transform<string>(secret_storage_specifier_pr.optional_result));
	}
	auto option_list = transformer.Transform<vector<GenericCopyOption>>(list_pr.Child<ListParseResult>(4));
	for (auto option : option_list) {
		auto lower_name = StringUtil::Lower(option.name);
		if (lower_name == "scope") {
			info->scope = option.GetFirstChildOrExpression();
			continue;
		}
		if (lower_name == "type") {
			info->type = option.GetFirstChildOrExpression();
			continue;
		}
		if (lower_name == "provider") {
			info->provider = option.GetFirstChildOrExpression();
			continue;
		}
		if (info->options.find(lower_name) != info->options.end()) {
			throw BinderException("Duplicate query param found while parsing create secret: '%s'", lower_name);
		}
		info->options.insert({lower_name, option.GetFirstChildOrExpression()});
	}
	if (info->name.empty()) {
		auto value = GetConstantExpressionValue(info->type);
		if (value.IsNull()) {
			throw InvalidInputException(
			    "Can not combine a non-constant expression for the secret type with a default-named secret. Either "
			    "provide an explicit secret name or use a constant expression for the secret type.");
		}
		info->name = "__default_" + StringUtil::Lower(value.ToString());
	}
	result->info = std::move(info);
	return result;
}

string PEGTransformerFactory::TransformSecretName(PEGTransformer &transformer, optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return transformer.Transform<string>(list_pr.Child<ListParseResult>(0));
}

string PEGTransformerFactory::TransformSecretStorageSpecifier(PEGTransformer &transformer,
                                                              optional_ptr<ParseResult> parse_result) {
	auto &list_pr = parse_result->Cast<ListParseResult>();
	return list_pr.Child<IdentifierParseResult>(1).identifier;
}

} // namespace duckdb
