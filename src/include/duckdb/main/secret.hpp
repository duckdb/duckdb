//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//! Base class from which RegisteredSecret classes can be made. TODO thread-safety?
class RegisteredSecret {
	friend class SecretManager;

public:
	RegisteredSecret(vector<string> &prefix_paths, string &type, string &provider, string &name)
	    : prefix_paths(prefix_paths), type(type), provider(provider), name(name) {};
	virtual ~RegisteredSecret() = default;

	//! Returns the longest prefix that matches, -1 for no match
	//! Note: some secrets may want to override this function when paths are platform dependent
	virtual int LongestMatch(const string &path);

	//! The ToString method prints the secret, the redact option determines whether secret data is allowed to be printed
	//! in clear text. This is to be decided by the secret implementation
	virtual string ToString(bool redact) {
		return "";
	}

	//! Getters
	vector<string> &GetScope() {
		return prefix_paths;
	}
	const string &GetType() {
		return type;
	}
	const string &GetProvider() {
		return provider;
	}
	const string &GetName() const {
		return name;
	}

protected:
	//! prefixes to which the secret applies
	vector<string> prefix_paths;

	//! Type of secret
	string type;
	//! Provider of the secret
	string provider;
	//! Name of the secret
	string name;
};

} // namespace duckdb
