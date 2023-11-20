#include "duckdb/main/secret/secret_manager.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/parser/statement/create_secret_statement.hpp"

namespace duckdb {

void DebugSecretManager::Initialize(DatabaseInstance &db) {
	return base_secret_manager->Initialize(db);
}

unique_ptr<BaseSecret> DebugSecretManager::DeserializeSecret(CatalogTransaction transaction, Deserializer &deserializer) {
	auto secret = base_secret_manager->DeserializeSecret(transaction, deserializer);
	printf("DeserializeSecret %s\n", secret->ToString(false).c_str());
	return secret;
}

void DebugSecretManager::RegisterSecretType(SecretType &type) {
	printf("RegisterSecretType %s\n", type.name.c_str());
	base_secret_manager->RegisterSecretType(type);
}

optional_ptr<SecretEntry> DebugSecretManager::RegisterSecret(CatalogTransaction transaction, unique_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
                                        SecretPersistMode persist_mode) {
	printf("RegisterSecret %s\n", secret->ToString(false).c_str());
	return base_secret_manager->RegisterSecret(transaction, std::move(secret), on_conflict, persist_mode);
}

void DebugSecretManager::RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) {
	printf("RegisterSecretFunction %s %s\n", function.secret_type.c_str(), function.provider.c_str());
	base_secret_manager->RegisterSecretFunction(function, on_conflict);
}

optional_ptr<SecretEntry> DebugSecretManager::CreateSecret(ClientContext &context, const CreateSecretInfo &info) {
	printf("CreateSecret %s %s\n", info.type.c_str(), info.provider.c_str());
	return base_secret_manager->CreateSecret(context, info);
}

BoundStatement DebugSecretManager::BindCreateSecret(CreateSecretStatement &stmt) {
	printf("BindCreateSecret %s %s\n", stmt.info->type.c_str(), stmt.info->provider.c_str());
	return base_secret_manager->BindCreateSecret(stmt);
}

optional_ptr<SecretEntry> DebugSecretManager::GetSecretByPath(CatalogTransaction transaction, const string &path, const string &type) {
	auto reg_secret = base_secret_manager->GetSecretByPath(transaction, path, type);
	if (reg_secret->secret) {
		printf("GetSecretByPath %s\n", reg_secret->secret->ToString(false).c_str());
	}
	return reg_secret;
}

optional_ptr<SecretEntry> DebugSecretManager::GetSecretByName(CatalogTransaction transaction, const string &name) {
	auto reg_secret = base_secret_manager->GetSecretByName(transaction, name);
	printf("GetSecretByName %s\n", reg_secret->secret->ToString(false).c_str());
	return reg_secret;
}

void DebugSecretManager::DropSecretByName(CatalogTransaction transaction, const string &name, bool missing_ok) {
	printf("DropSecretByName %s\n", name.c_str());
	return base_secret_manager->DropSecretByName(transaction, name, missing_ok);
}

SecretType DebugSecretManager::LookupType(const string &type) {
	auto type_str = base_secret_manager->LookupType(type);
	printf("LookupType %s\n", type_str.name.c_str());
	return type_str;
}

vector<SecretEntry *> DebugSecretManager::AllSecrets(CatalogTransaction transaction) {
	auto result = base_secret_manager->AllSecrets(transaction);

	printf("AllSecrets:\n");
	for (const auto &res : result) {
		printf(" - %s\n", res->secret->ToString(false).c_str());
	}

	return base_secret_manager->AllSecrets(transaction);
}

bool DebugSecretManager::AllowConfigChanges() {
	return base_secret_manager->AllowConfigChanges();
}

} // namespace duckdb
