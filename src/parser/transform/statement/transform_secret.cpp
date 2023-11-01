#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/create_secret_statement.hpp"

namespace duckdb {

unique_ptr<CreateSecretStatement> Transformer::TransformSecret(duckdb_libpgquery::PGCreateSecretStmt &stmt) {
	auto result = make_uniq<CreateSecretStatement>(StringUtil::Lower(stmt.secret_type), TransformOnConflict(stmt.onconflict));

	if (stmt.secret_name) {
		result->info->name = StringUtil::Lower(stmt.secret_name);
	}

	if (stmt.secret_provider) {
		result->info->provider = StringUtil::Lower(stmt.secret_provider);
	}

	if (stmt.scope) {
		for (auto cell = stmt.scope->head; cell; cell = cell->next) {
			string scope = (char*)cell->data.ptr_value;
			result->info->scope.push_back(scope);
		}
	}

	if (stmt.options) {
		for (auto cell = stmt.options->head; cell; cell = cell->next) {
			auto option_list = PGPointerCast<duckdb_libpgquery::PGList>(cell->data.ptr_value);
			D_ASSERT(option_list->length == 2);
			string key = (char *) option_list->head->data.ptr_value;
			string value = (char *) option_list->tail->data.ptr_value;
			result->info->named_parameters[StringUtil::Lower(key)] = StringUtil::Lower(value);
		}
	}

	// Pull up the scope param as its used to identify the correct function TODO: clean this up and make list
	auto lu = result->info->named_parameters.find("scope");
	if (lu != result->info->named_parameters.end()) {
		result->info->scope = {lu->second.ToString()};
		result->info->named_parameters.erase("scope");
	}

	return result;
}

} // namespace duckdb
