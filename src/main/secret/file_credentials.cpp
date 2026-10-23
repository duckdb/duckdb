#include "duckdb/main/secret/file_credentials.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {

namespace {

//! Revokes the credentials created in a transaction once its transaction managers have committed or rolled back, so
//! that a catalog's commit can still write with them
class FileCredentialsState : public ClientContextState {
public:
	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		RevokeAll();
	}
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		RevokeAll();
	}

	vector<weak_ptr<FileCredentials>> created;

private:
	void RevokeAll() {
		for (auto &entry : created) {
			auto credentials = entry.lock();
			if (credentials) {
				credentials->Revoke();
			}
		}
		created.clear();
	}
};

//! Whether `path` has a ".." segment, which a server may resolve to a path outside the scope
bool HasParentSegment(const string &path) {
	return StringUtil::Contains("/" + StringUtil::Replace(path, "\\", "/") + "/", "/../");
}

} // namespace

FileCredentials::FileCredentials(ClientContext &context, string scope_p, vend_t vend_p)
    : owner(context.shared_from_this()), scope(std::move(scope_p)), vend(std::move(vend_p)) {
}

shared_ptr<FileCredentials> FileCredentials::Create(ClientContext &context, string scope, vend_t vend) {
	if (scope.empty() || !vend || !context.transaction.HasActiveTransaction()) {
		throw InternalException("FileCredentials need a scope, a vend function and an active transaction");
	}
	auto credentials = make_shared_ptr<FileCredentials>(context, std::move(scope), std::move(vend));
	context.registered_state->GetOrCreate<FileCredentialsState>("file_credentials")->created.push_back(credentials);
	return credentials;
}

ClientContext &FileCredentials::CheckClient(optional_ptr<FileOpener> opener) const {
	auto context = FileOpener::TryGetClientContext(opener);
	if (!context || context.get() != owner.lock().get()) {
		throw InvalidConfigurationException("The credentials of '%s' serve only the connection that created them",
		                                    scope);
	}
	return *context;
}

shared_ptr<const KeyValueSecret> FileCredentials::GetSecret(optional_ptr<FileOpener> opener, const string &path) {
	auto &context = CheckClient(opener);
	if (!StringUtil::StartsWith(path, scope) || HasParentSegment(path)) {
		throw InvalidConfigurationException("The credentials of '%s' cannot be used for '%s'", scope, path);
	}
	// held while vending, so that concurrent first uses share one vend
	lock_guard<mutex> guard(lock);
	if (!vend) {
		throw InvalidConfigurationException("The credentials of '%s' ended with the transaction that created them",
		                                    scope);
	}
	if (!secret) {
		secret = vend(context);
	}
	return secret;
}

bool FileCredentials::Refresh(optional_ptr<FileOpener> opener) {
	auto &context = CheckClient(opener);
	lock_guard<mutex> guard(lock);
	if (!vend) {
		return false;
	}
	// a vend that throws keeps the current secret
	secret = vend(context);
	return true;
}

void FileCredentials::Revoke() {
	lock_guard<mutex> guard(lock);
	vend = nullptr;
	secret.reset();
}

} // namespace duckdb
