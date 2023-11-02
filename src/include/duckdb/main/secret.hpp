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
#include "duckdb/main/secret_manager.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

//! Base class from which RegisteredSecret classes can be made. TODO thread-safety?
class RegisteredSecret {
	friend class SecretManager;

public:
	RegisteredSecret(vector<string> &prefix_paths, const string &type, const string &provider, const string &name)
	    : prefix_paths(prefix_paths), type(type), provider(provider), name(name), serializable(false) {};
	virtual ~RegisteredSecret() = default;

	//! Returns the longest prefix that matches, -1 for no match
	virtual int LongestMatch(const string &path);

	//! The ToString method prints the secret, the redact option determines whether secret data is allowed to be printed
	//! in clear text. This is to be decided by the secret implementation
	virtual string ToString(bool redact) {
		return "";
	}

	//! Serialize this secret
	virtual void Serialize(Serializer &serializer) const {
	    throw InternalException("Attempted to serialize secret without serialize");
	};

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
	const string &GetName() {
		return name;
	}
	bool IsSerializable() const {
		return serializable;
	}

protected:
	//! Helper function to serialize the base RegisteredSecret class variables
	virtual void SerializeBaseSecret(Serializer &serializer) const final {
		serializer.WriteProperty(100, "type", type);
		serializer.WriteProperty(101, "provider", provider);
		serializer.WriteProperty(102, "name", name);
		serializer.WriteList(103, "scope", prefix_paths.size(),
		                     [&](Serializer::List &list, idx_t i) { list.WriteElement(prefix_paths[i]); });
	};

	//! prefixes to which the secret applies
	vector<string> prefix_paths;

	//! Type of secret
	string type;
	//! Provider of the secret
	string provider;
	//! Name of the secret
	string name;

	//! Whether the secret can be serialized/deserialized
	bool serializable;
};

} // namespace duckdb
