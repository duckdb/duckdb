#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

static Value GetConstantExpressionValue(ParsedExpression &expr, const string& field) {
	if (expr.type == ExpressionType::VALUE_CONSTANT) {
		return expr.Cast<ConstantExpression>().value;
	}
	if (expr.type == ExpressionType::COLUMN_REF) {
		return expr.Cast<ColumnRefExpression>().GetName();
	}
	throw InvalidInputException("Unsupported expression passed to field '%s'", field);
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
			auto scope_expr = TransformExpression(def_elem->arg);
			auto value = GetConstantExpressionValue(*scope_expr, lower_name);
			for (const auto &child :ListValue::GetChildren(value)) {
				info.scope.push_back(child.GetValue<string>());
			}
		} else if (lower_name == "type") {
			auto type_expr = TransformExpression(def_elem->arg);
			info.type = GetConstantExpressionValue(*type_expr, lower_name).GetValue<string>();
			continue;
		} else if (lower_name == "provider") {
			auto provider_expression = TransformExpression(def_elem->arg);
			info.provider = GetConstantExpressionValue(*provider_expression, lower_name).GetValue<string>();
			continue;
		}

		// All the other options end up in the generic
		case_insensitive_map_t<vector<ParsedExpression>> vector_options;

		if (info.options.find(def_elem->defname) != info.options.end()) {
			throw BinderException("Duplicate query param found while parsing create secret: '%s'", def_elem->defname);
		}

		info.options[def_elem->defname] = TransformExpression(def_elem->arg);
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

	if (create_secret_info->type.empty()) {
		throw ParserException("Failed to create secret - secret must have a type defined");
	}
	if (create_secret_info->name.empty()) {
		create_secret_info->name = "__default_" + create_secret_info->type;
	}

	result->info = std::move(create_secret_info);

	return result;
}

} // namespace duckdb
