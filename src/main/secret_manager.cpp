#include "duckdb/main/secret_manager.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/parser/statement/create_secret_statement.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/planner/operator/logical_create_secret.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

bool CreateSecretFunctionSet::ProviderExists(const string& provider_name) {
	return functions.find(provider_name) != functions.end();
}

void CreateSecretFunctionSet::AddFunction(CreateSecretFunction function, OnCreateConflict on_conflict) {
	if (ProviderExists(function.provider)) {
		if(on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw InternalException("Attempted to override a Create Secret Function with OnCreateConflict::ERROR_ON_CONFLICT for: '%s'", function.provider);
		} else if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
			functions[function.provider] = function;
		} else if (on_conflict == OnCreateConflict::ALTER_ON_CONFLICT) {
			throw NotImplementedException("ALTER_ON_CONFLICT not implemented for CreateSecretFunctionSet");
		}
	} else {
		functions[function.provider] = function;
	}
}

CreateSecretFunction& CreateSecretFunctionSet::GetFunction(const string& provider) {
	const auto& lookup = functions.find(provider);

	if (lookup == functions.end()) {
		throw InternalException("Could not find Create Secret Function with provider %s");
	}

	return lookup->second;
}

unique_ptr<BaseSecret> DuckSecretManager::DeserializeSecret(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<string>(100, "type");
	auto provider = deserializer.ReadProperty<string>(101, "provider");
	auto name = deserializer.ReadProperty<string>(102, "name");
	vector<string> scope;
	deserializer.ReadList(103, "scope",
	                      [&](Deserializer::List &list, idx_t i) { scope.push_back(list.ReadElement<string>()); });

	auto secret_type = LookupType(type);

	if (!secret_type.deserializer) {
		throw InternalException(
		    "Attempted to deserialize secret type '%s' which does not have a deserialization method", type);
	}

	return secret_type.deserializer(deserializer, {scope, type, provider, name});
}

void DuckSecretManager::RegisterSecretType(SecretType &type) {
	lock_guard<mutex> lck(lock);

	if (registered_types.find(type.name) != registered_types.end()) {
		throw InternalException("Attempted to register an already registered secret type: '%s'", type.name);
	}

	registered_types[type.name] = type;
}

void DuckSecretManager::RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) {
	lock_guard<mutex> lck(lock);

	const auto& lu = registered_functions.find(function.secret_type);
	if (lu != registered_functions.end()) {
		auto& functions_for_type = lu->second;
		functions_for_type.AddFunction(function, on_conflict);
	}

	CreateSecretFunctionSet new_set(function.secret_type);
	new_set.AddFunction(function, OnCreateConflict::ERROR_ON_CONFLICT);
	registered_functions.insert(std::make_pair(function.secret_type, std::move(new_set)));
}

void DuckSecretManager::RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict, SecretPersistMode persist_mode) {
	lock_guard<mutex> lck(lock);

	bool conflict = false;
	idx_t conflict_idx;

	//! Ensure we only create secrets for known types;
	LookupTypeInternal(secret->GetType());

	// Assert the alias does not exist already
	if (!secret->GetName().empty()) {
		for (idx_t cred_idx = 0; cred_idx < registered_secrets.size(); cred_idx++) {
			const auto &cred = registered_secrets[cred_idx];
			if (cred.secret->GetName() == secret->GetName()) {
				conflict = true;
				conflict_idx = cred_idx;
				break;
			}
		}
	}

	if (persist_mode == SecretPersistMode::PERMANENT) {
		throw NotImplementedException("DuckSecretManager does not implement persistent secrets yet!");
	}

	if (conflict) {
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw InvalidInputException("Secret with alias '" + secret->GetName() + "' already exists!");
		} else if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return;
		} else if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
			RegisteredSecret reg_secret(secret);
			reg_secret.persistent = false;
			reg_secret.storage_mode = "in-memory";
			registered_secrets[conflict_idx] = std::move(reg_secret);
			return;
		} else {
			throw InternalException("unknown OnCreateConflict found while registering secret");
		}
	}

	RegisteredSecret reg_secret(secret);
	reg_secret.persistent = false;
	reg_secret.storage_mode = "in-memory";
	registered_secrets.push_back(std::move(reg_secret));
}

CreateSecretFunction* DuckSecretManager::LookupFunctionInternal(const string& type, const string& provider) {
	const auto& lookup = registered_functions.find(type);
	if (lookup == registered_functions.end()) {
		return nullptr;
	}

	if (!lookup->second.ProviderExists(provider)) {
		return nullptr;
	}

	return &lookup->second.GetFunction(provider);
}

void DuckSecretManager::CreateSecret(ClientContext &context, const CreateSecretInfo& info) {
	// TODO; register secret grabs lock too
//		lock_guard<mutex> lck(lock);

	// Make a copy to set the provider to default if necessary
	CreateSecretInput function_input {info.type, info.provider,  info.persist_mode, info.name, info.scope, info.named_parameters};
	if (function_input.provider.empty()) {
		auto secret_type = LookupTypeInternal(function_input.type);
		function_input.provider = secret_type.default_provider;
	}

	// Lookup function
	auto function_lookup = LookupFunctionInternal(function_input.type, function_input.provider);
	if (!function_lookup) {
		throw InvalidInputException("Could not find CreateSecretFunction for type: '%s' and provider: '%s'", info.type, info.provider);
	}

	// TODO probably don't hold lock while running this
	// Call the function
	auto secret = function_lookup->function(context, function_input);

	if (!secret) {
		throw InternalException("CreateSecretFunction for type: '%s' and provider: '%s' did not return a secret!", info.type, info.provider);
	}

	// Register the secret at the secret_manager
	RegisterSecret(std::move(secret), info.on_conflict, info.persist_mode);
}

BoundStatement DuckSecretManager::BindCreateSecret(CreateSecretStatement &stmt) {
	lock_guard<mutex> lck(lock);

	auto type = stmt.info->type;
	auto provider = stmt.info->provider;
	bool default_provider = false;

	if (provider.empty()) {
		default_provider = true;
		auto secret_type = LookupTypeInternal(type);
		provider = secret_type.default_provider;
	}

	string default_string = default_provider ? "default " : "";

	auto function = LookupFunctionInternal(type, provider);

	if (!function) {
		throw BinderException("Could not find create secret function for secret type '%s' with %sprovider '%s'", type, default_string, provider);
	}

	for (const auto& param : stmt.info->named_parameters) {
		if (function->named_parameters.find(param.first) == function->named_parameters.end()) {
			throw BinderException("Unknown parameter '%s' for secret type '%s' with %sprovider '%s'", param.first, type, default_string, provider);
		}
	}

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_uniq<LogicalCreateSecret>(*function, *stmt.info);
	return result;
}

RegisteredSecret DuckSecretManager::GetSecretByPath(const string &path, const string &type) {
	lock_guard<mutex> lck(lock);

	int best_match_score = -1;
	RegisteredSecret* best_match = nullptr;

	for (auto &secret : registered_secrets) {
		if (secret.secret->GetType() != type) {
			continue;
		}
		auto match = secret.secret->MatchScore(path);

		if (match > best_match_score) {
			best_match_score = MaxValue<idx_t>(match, best_match_score);
			best_match = &secret;
		}
	}

	if (best_match) {
		return *best_match;
	}

	return {nullptr};
}

RegisteredSecret DuckSecretManager::GetSecretByName(const string &name) {
	lock_guard<mutex> lck(lock);

	for (const auto &reg_secret : registered_secrets) {
		if (reg_secret.secret->GetName() == name) {
			return reg_secret;
		}
	}

	throw InternalException("GetSecretByName called on unknown secret: %s", name);
}

void DuckSecretManager::DropSecretByName(const string &name, bool missing_ok) {
	lock_guard<mutex> lck(lock);
	bool deleted = false;

	std::vector<RegisteredSecret>::iterator iter;
	for (iter = registered_secrets.begin(); iter != registered_secrets.end();) {
		if (iter->secret->GetName() == name) {
			registered_secrets.erase(iter);
			deleted = true;
			break;
		}
		++iter;
	}

	if (!deleted && !missing_ok) {
		throw InvalidInputException("Failed to remove non-existent secret with name '%s'", name);
	}
}

SecretType DuckSecretManager::LookupType(const string &type) {
	lock_guard<mutex> lck(lock);
	return LookupTypeInternal(type);
}

SecretType DuckSecretManager::LookupTypeInternal(const string &type) {
	auto lu = registered_types.find(type);

	if (lu == registered_types.end()) {
		throw InvalidInputException("Secret type '%s' not found", type);
	}

	return lu->second;
}

vector<RegisteredSecret> DuckSecretManager::AllSecrets() {
	return registered_secrets;
}

unique_ptr<BaseSecret> DebugSecretManager::DeserializeSecret(Deserializer &deserializer) {
	return base_secret_manager->DeserializeSecret(deserializer);
}

void DebugSecretManager::RegisterSecretType(SecretType &type) {
	base_secret_manager->RegisterSecretType(type);
}

void DebugSecretManager::RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict, SecretPersistMode persist_mode) {
	return base_secret_manager->RegisterSecret(secret, on_conflict, persist_mode);
}

void DebugSecretManager::RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) {
	base_secret_manager->RegisterSecretFunction(function, on_conflict);
}

void DebugSecretManager::CreateSecret(ClientContext &context, const CreateSecretInfo &info) {
	base_secret_manager->CreateSecret(context, info);
}

BoundStatement DebugSecretManager::BindCreateSecret(CreateSecretStatement &stmt) {
	return base_secret_manager->BindCreateSecret(stmt);
}

RegisteredSecret DebugSecretManager::GetSecretByPath(const string &path, const string &type) {
	return base_secret_manager->GetSecretByPath(path, type);
}

RegisteredSecret DebugSecretManager::GetSecretByName(const string &name) {
	return base_secret_manager->GetSecretByName(name);
}

void DebugSecretManager::DropSecretByName(const string &name, bool missing_ok) {
	return base_secret_manager->DropSecretByName(name, missing_ok);
}

SecretType DebugSecretManager::LookupType(const string &type) {
	return base_secret_manager->LookupType(type);
}

vector<RegisteredSecret> DebugSecretManager::AllSecrets() {
	return base_secret_manager->AllSecrets();
}

} // namespace duckdb
