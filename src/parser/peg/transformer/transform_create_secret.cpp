#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"

namespace duckdb {

Value PEGTransformerFactory::GetConstantExpressionValue(unique_ptr<ParsedExpression> &expr) {
	if (expr->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		return expr->Cast<ConstantExpression>().GetValue();
	}
	if (expr->GetExpressionType() == ExpressionType::COLUMN_REF) {
		return expr->Cast<ColumnRefExpression>().GetName();
	}
	return Value();
}

unique_ptr<CreateStatement>
PEGTransformerFactory::TransformCreateSecretStmt(PEGTransformer &transformer, const bool &if_not_exists,
                                                 const string &secret_name, const string &secret_storage_specifier,
                                                 const vector<GenericCopyOption> &generic_copy_option_list) {
	auto result = make_uniq<CreateStatement>();
	auto on_conflict = if_not_exists ? OnCreateConflict::IGNORE_ON_CONFLICT : OnCreateConflict::ERROR_ON_CONFLICT;
	auto info = make_uniq<CreateSecretInfo>(on_conflict, SecretPersistType::DEFAULT);
	if (!secret_name.empty()) {
		info->name = secret_name;
	}
	if (!secret_storage_specifier.empty()) {
		info->storage_type = StringUtil::Lower(secret_storage_specifier);
	}
	for (const auto &option : generic_copy_option_list) {
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
		if (!info->type) {
			throw ParserException("Failed to create secret - secret must have a type defined");
		}
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

string PEGTransformerFactory::TransformSecretStorageSpecifier(PEGTransformer &transformer, const string &identifier) {
	return identifier;
}

string PEGTransformerFactory::TransformSecretName(PEGTransformer &transformer, const string &col_id) {
	return col_id;
}

} // namespace duckdb
