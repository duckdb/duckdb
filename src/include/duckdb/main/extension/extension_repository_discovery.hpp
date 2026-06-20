//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/extension/extension_repository_discovery.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;

//! A single signing key advertised by a repository discovery document
struct ExtensionRepositorySigningKey {
	string kid;
	string algorithm;
	string public_key; // PEM
	string valid_to;   // ISO date, optional
};

//! Parsed `.well-known/duckdb-extensions.json` discovery document
struct ExtensionRepositoryDiscovery {
	static constexpr const char *WELL_KNOWN_PATH = "/.well-known/duckdb-extensions.json";

	int64_t schema_version = 0;
	string repository;   //! advertised repository name (optional)
	string url;          //! advertised canonical base url (optional)
	string url_template; //! advertised install-path layout (optional)
	vector<ExtensionRepositorySigningKey> signing_keys;

	//! Normalize a user-provided url into the discovery-document url:
	//! ends with `.json` -> used as-is; otherwise `<url>/.well-known/duckdb-extensions.json`.
	static string NormalizeDiscoveryUrl(const string &url);
	//! Compute the SHA256 fingerprint (hex) of a PEM public key
	static string KeyFingerprint(const string &public_key);
	//! Parse a discovery document from JSON content
	static ExtensionRepositoryDiscovery Parse(const string &json_content, const string &source);
	//! Fetch (via the FileSystem, honoring access secrets/protocols) and parse the discovery document
	static ExtensionRepositoryDiscovery FetchAndParse(ClientContext &context, const string &discovery_url);
};

} // namespace duckdb
