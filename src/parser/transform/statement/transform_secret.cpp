#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

static Value GetConstantExpressionValue(ParsedExpression &expr) {
	if (expr.type == ExpressionType::VALUE_CONSTANT) {
		return expr.Cast<ConstantExpression>().value;
	}
	if (expr.type == ExpressionType::COLUMN_REF) {
		return expr.Cast<ColumnRefExpression>().GetName();
	}
	return Value();
}

void Transformer::TransformCreateSecretOptions(CreateSecretInfo &info,
                                               optional_ptr<duckdb_libpgquery::PGList> options) {
	if (!options) {
		return;
	}

	duckdb_libpgquery::PGListCell *cell;

	// iterate over each option
	for_each_cell(cell, options->head) {
		auto def_elem = PGPointerCast<duckdb_libpgquery::PGDefElem>(cell->data.ptr_value);
		auto lower_name = StringUtil::Lower(def_elem->defname);
		if (lower_name == "scope") {
			info.scope = TransformExpression(def_elem->arg);
			continue;
		}
		if (lower_name == "type") {
			info.type = TransformExpression(def_elem->arg);
			if (info.type->type == ExpressionType::COLUMN_REF) {
				info.type = make_uniq<ConstantExpression>(GetConstantExpressionValue(*info.type));
			}
			continue;
		}
		if (lower_name == "provider") {
			info.provider = TransformExpression(def_elem->arg);
			if (info.provider->type == ExpressionType::COLUMN_REF) {
				info.provider = make_uniq<ConstantExpression>(GetConstantExpressionValue(*info.provider));
			}
			continue;
		}

		// All the other options end up in the generic
		case_insensitive_map_t<vector<ParsedExpression>> vector_options;

		if (info.options.find(lower_name) != info.options.end()) {
			throw BinderException("Duplicate query param found while parsing create secret: '%s'", lower_name);
		}

		auto expr = TransformExpression(def_elem->arg);
		if (expr->type == ExpressionType::COLUMN_REF) {
			expr = make_uniq<ConstantExpression>(GetConstantExpressionValue(*expr));
		}
		info.options[lower_name] = std::move(expr);
	}
}

unique_ptr<CreateStatement> Transformer::TransformSecret(duckdb_libpgquery::PGCreateSecretStmt &stmt) {
	auto result = make_uniq<CreateStatement>();

	auto create_secret_info =
	    make_uniq<CreateSecretInfo>(TransformOnConflict(stmt.onconflict),
	                                EnumUtil::FromString<SecretPersistType>(StringUtil::Upper(stmt.persist_type)));

	if (stmt.secret_name) {
		create_secret_info->name = StringUtil::Lower(stmt.secret_name);
	}

	if (stmt.secret_storage) {
		create_secret_info->storage_type = StringUtil::Lower(stmt.secret_storage);
	}

	if (stmt.options) {
		TransformCreateSecretOptions(*create_secret_info, stmt.options);
	}

	if (!create_secret_info->type) {
		throw ParserException("Failed to create secret - secret must have a type defined");
	}
	if (create_secret_info->name.empty()) {
		auto value = GetConstantExpressionValue(*create_secret_info->type);
		if (value.IsNull()) {
			throw InvalidInputException(
			    "Can not combine a non-constant expression for the secret type with a default-named secret. Either "
			    "provide an explicit secret name or use a constant expression for the secret type.");
		}
		create_secret_info->name = "__default_" + StringUtil::Lower(value.ToString());
	}

	result->info = std::move(create_secret_info);

	return result;
}

} // namespace duckdb
