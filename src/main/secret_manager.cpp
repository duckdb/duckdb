#include "duckdb/main/secret_manager.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

unique_ptr<BaseSecret> SecretManager::DeserializeSecret(Deserializer &deserializer) {
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

void SecretManager::RegisterSecretType(SecretType &type) {
	lock_guard<mutex> lck(lock);

	if (registered_types.find(type.name) != registered_types.end()) {
		throw InternalException("Attempted to register an already registered secret type: '%s'", type.name);
	}

	registered_types[type.name] = type;
}

void SecretManager::RegisterSecret(shared_ptr<BaseSecret> secret, OnCreateConflict on_conflict) {
	lock_guard<mutex> lck(lock);

	bool conflict = false;
	idx_t conflict_idx;

	//! Ensure we only create secrets for known types;
	LookupTypeInternal(secret->type);

	// Assert the alias does not exist already
	if (!secret->GetName().empty()) {
		for (idx_t cred_idx = 0; cred_idx < registered_secrets.size(); cred_idx++) {
			const auto &cred = registered_secrets[cred_idx];
			if (cred->GetName() == secret->GetName()) {
				conflict = true;
				conflict_idx = cred_idx;
			}
		}
	}

	if (conflict) {
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw InvalidInputException("Secret with alias '" + secret->GetName() + "' already exists!");
		} else if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return;
		} else if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
			registered_secrets[conflict_idx] = secret;
		} else {
			throw InternalException("unknown OnCreateConflict found while registering secret");
		}
	} else {
		registered_secrets.push_back(std::move(secret));
	}
}

shared_ptr<BaseSecret> SecretManager::GetSecretByPath(const string &path, const string &type) {
	lock_guard<mutex> lck(lock);

	int best_match_score = -1;
	shared_ptr<BaseSecret> best_match;

	for (const auto &secret : registered_secrets) {
		if (secret->GetType() != type) {
			continue;
		}
		auto match = secret->MatchScore(path);

		if (match > best_match_score) {
			best_match_score = MaxValue<idx_t>(match, best_match_score);
			best_match = secret;
		}
	}
	return best_match;
}

shared_ptr<BaseSecret> SecretManager::GetSecretByName(const string &name) {
	lock_guard<mutex> lck(lock);

	for (const auto &secret : registered_secrets) {
		if (secret->name == name) {
			return secret;
		}
	}

	throw InternalException("GetSecretByName called on unknown secret: %s", name);
}

void SecretManager::DropSecretByName(const string &name, bool missing_ok) {
	lock_guard<mutex> lck(lock);
	bool deleted = false;

	std::vector<shared_ptr<BaseSecret>>::iterator iter;
	for (iter = registered_secrets.begin(); iter != registered_secrets.end();) {
		if (iter->get()->GetName() == name) {
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

SecretType SecretManager::LookupType(const string &type) {
	lock_guard<mutex> lck(lock);
	return LookupTypeInternal(type);
}

SecretType SecretManager::LookupTypeInternal(const string &type) {
	auto lu = registered_types.find(type);

	if (lu == registered_types.end()) {
		throw InvalidInputException("Secret type '%s' not found", type);
	}

	return lu->second;
}

vector<shared_ptr<BaseSecret>> &SecretManager::AllSecrets() {
	return registered_secrets;
}

shared_ptr<BaseSecret> DebugSecretManager::GetSecretByPath(const string &path, const string &type) {
	//	printf("\n  [GetSecretByPath] path=%s type=%s", path.c_str(), type.c_str());
	return SecretManager::GetSecretByPath(path, type);
}

void DebugSecretManager::RegisterSecret(shared_ptr<BaseSecret> secret, OnCreateConflict on_conflict) {
	//	printf("\n  [RegisterSecret]  name=%s type=%s provider=%s", secret->GetName().c_str(),
	// secret->GetType().c_str(), secret->GetProvider().c_str());
	SecretManager::RegisterSecret(secret, on_conflict);
}

} // namespace duckdb
