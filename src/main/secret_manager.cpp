#include "duckdb/main/secret_manager.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

unique_ptr<RegisteredSecret> SecretManager::DeserializeSecret(Deserializer& deserializer) {
	auto type = deserializer.ReadProperty<string>(100, "type");
	auto provider = deserializer.ReadProperty<string>(101, "provider");
	auto name = deserializer.ReadProperty<string>(102, "name");
	vector<string> scope;
	deserializer.ReadList(103, "scope", [&](Deserializer::List &list, idx_t i) {
		scope.push_back(list.ReadElement<string>());
	});

	auto secret_type = LookupType(type);

	if (!secret_type.deserializer) {
		throw InternalException("Attempted to deserialize secret type '%s' which does not have a deserialization method", type);
	}

	return secret_type.deserializer(deserializer, {scope, type, provider, name});
}

void SecretManager::SerializeSecret(RegisteredSecret& secret, Serializer& serializer) {
	throw NotImplementedException("SerializeSecret");
}

void SecretManager::RegisterSecretType(SecretType& type) {
	if (registered_types.find(type.name) != registered_types.end()) {
		throw InternalException("Attempted to register an already registered secret type: '%s'", type.name);
	}

	registered_types[type.name] = type;
}

void SecretManager::RegisterSecret(shared_ptr<RegisteredSecret> secret, OnCreateConflict on_conflict) {
	bool conflict = false;
	idx_t conflict_idx;

	//! Ensure we only create secrets for known types;
	LookupType(secret->type);

	// Assert the alias does not exist already
	if (!secret->GetName().empty()) {
		for(idx_t cred_idx = 0; cred_idx < registered_secrets.size(); cred_idx++) {
			const auto& cred = registered_secrets[cred_idx];
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

shared_ptr<RegisteredSecret> SecretManager::GetSecretByPath(string& path, string type) {
	int longest_match = -1;
	shared_ptr<RegisteredSecret> best_match;

	for (const auto& secret: registered_secrets) {
		if (secret->GetType() != type) {
			continue;
		}
		auto match = secret->LongestMatch(path);

		if (match > longest_match) {
			longest_match = MaxValue<idx_t>(match, longest_match);
			best_match = secret;
		}
	}
	return best_match;
}

shared_ptr<RegisteredSecret> SecretManager::GetSecretByName(string& name) {
	throw NotImplementedException("GetSecretByName");
}

SecretType SecretManager::LookupType(string &type) {
	auto lu = registered_types.find(type);

	if (lu == registered_types.end()) {
		throw InvalidInputException("Secret type '%s' not found", type);
	}

	return lu->second;
}

vector<shared_ptr<RegisteredSecret>>& SecretManager::AllSecrets() {
	return registered_secrets;
}

} // namespace duckdb
