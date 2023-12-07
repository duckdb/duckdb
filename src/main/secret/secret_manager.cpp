#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

SecretManager &SecretManager::Get(ClientContext &context) {
	return *DBConfig::GetConfig(context).secret_manager;
}

void SecretManager::DropSecretByName(ClientContext &context, const string &name, bool missing_ok) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	return DropSecretByName(transaction, name, missing_ok);
}

} // namespace duckdb
