#include "duckdb/parser/statement/create_secret_statement.hpp"
#include "duckdb/parser/transformer.hpp"

namespace duckdb {

unique_ptr<CreateSecretStatement> Transformer::TransformSecret(duckdb_libpgquery::PGCreateSecretStmt &stmt) {
	auto result = make_uniq<CreateSecretStatement>(
	    TransformOnConflict(stmt.onconflict),
	    EnumUtil::FromString<SecretPersistMode>(StringUtil::Upper(stmt.persist_option)));

	if (stmt.secret_name) {
		result->info->name = StringUtil::Lower(stmt.secret_name);
	}

	if (stmt.options) {
		for (auto cell = stmt.options->head; cell; cell = cell->next) {
			auto option_list = PGPointerCast<duckdb_libpgquery::PGList>(cell->data.ptr_value);
			D_ASSERT(option_list->length == 2);
			string key = StringUtil::Lower(reinterpret_cast<char*>(option_list->head->data.ptr_value));
			auto value_node = PGPointerCast<duckdb_libpgquery::PGNode>(option_list->tail->data.ptr_value);
			if (key == "scope") {
				if (value_node->type == duckdb_libpgquery::T_PGString) {
					auto &val = PGCast<duckdb_libpgquery::PGValue>(*value_node);
					string value = val.val.str;
					result->info->scope.push_back(value);
					continue;
				} else if (value_node->type != duckdb_libpgquery::T_PGList) {
					throw ParserException("%s has to be a string, or a list of strings", key);
				}
				auto &list = PGCast<duckdb_libpgquery::PGList>(*value_node);
				for (auto scope_cell = list.head; scope_cell; scope_cell = scope_cell->next) {
					auto scope_val = PGPointerCast<duckdb_libpgquery::PGValue>(scope_cell->data.ptr_value);
					result->info->scope.push_back(scope_val->val.str);
				}
				continue;
			}
			if (value_node->type != duckdb_libpgquery::T_PGString) {
				throw ParserException("%s has to be a string", key);
			}
			auto &val = PGCast<duckdb_libpgquery::PGValue>(*value_node);
			string value = val.val.str;
			if (key == "type") {
				result->info->type = StringUtil::Lower(value);
			} else if (key == "provider") {
				result->info->provider = StringUtil::Lower(value);
			} else {
				result->info->named_parameters[key] = value;
			}
		}
	}
	if (result->info->type.empty()) {
		throw ParserException("Failed to create secret - secret must have a type defined");
	}
	if (result->info->name.empty()) {
		result->info->name = "__default_" + result->info->type;
	}

	return result;
}

} // namespace duckdb
