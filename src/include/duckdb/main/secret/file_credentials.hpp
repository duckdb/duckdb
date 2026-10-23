//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret/file_credentials.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"

#include <functional>

namespace duckdb {
class ClientContext;
class FileOpener;
class KeyValueSecret;

//! The credentials of one table's files. A catalog puts them on every file and listing of the table
//! (ExtendedOpenFileInfo::credentials, FileGlobInput::credentials), and a file system signs requests for such a file
//! with them alone: no secret is looked up by path and no setting fills a key they lack. Nothing registers them, so no
//! SQL can name, list or look them up. They serve only the client that created them, for paths under their scope with
//! no ".." segment, until the transaction they were created in ends.
class FileCredentials {
public:
	//! Returns the secret: on first use, and again on a refresh
	using vend_t = std::function<shared_ptr<const KeyValueSecret>(ClientContext &context)>;

	FileCredentials(ClientContext &context, string scope, vend_t vend);

	//! Credentials for the paths that start with `scope`, revoked when the current transaction of `context` ends
	DUCKDB_API static shared_ptr<FileCredentials> Create(ClientContext &context, string scope, vend_t vend);

	//! The secret to sign a request for `path` with; throws when the client of `opener` may not use it for `path`
	DUCKDB_API shared_ptr<const KeyValueSecret> GetSecret(optional_ptr<FileOpener> opener, const string &path);
	//! Vends again after the store rejected the secret as expired; false once revoked
	DUCKDB_API bool Refresh(optional_ptr<FileOpener> opener);
	void Revoke();

private:
	ClientContext &CheckClient(optional_ptr<FileOpener> opener) const;

	weak_ptr<ClientContext> owner;
	const string scope;
	mutex lock;
	//! Null once revoked
	vend_t vend;
	shared_ptr<const KeyValueSecret> secret;
};

} // namespace duckdb
