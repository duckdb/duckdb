#include "duckdb/main/secret_manager.hpp"

namespace duckdb {

void SecretManager::RegisterSecret(shared_ptr<RegisteredSecret> secret, OnCreateConflict on_conflict) {
	bool conflict = false;
	idx_t conflict_idx;

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

shared_ptr<RegisteredSecret> GetSecretByName(string& name) {
	throw NotImplementedException("GetSecretByName");
}

vector<shared_ptr<RegisteredSecret>>& SecretManager::AllSecrets() {
	return registered_secrets;
}

} // namespace duckdb
