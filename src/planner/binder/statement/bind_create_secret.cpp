#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "duckdb/planner/operator/logical_create_secret.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/statement/create_secret_statement.hpp"

namespace duckdb {

BoundStatement Binder::Bind(CreateSecretStatement &stmt) {
	auto &secret_manager = context.db->config.secret_manager;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return secret_manager->BindCreateSecret(stmt);
}

} // namespace duckdb
