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
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

//! Base class from which BaseSecret classes can be made.
class BaseSecret {
	friend class SecretManager;

public:
	BaseSecret(vector<string> &prefix_paths, const string &type, const string &provider, const string &name)
	    : prefix_paths(prefix_paths), type(type), provider(provider), name(name), serializable(false) {
		D_ASSERT(!type.empty());
	}
	BaseSecret(BaseSecret &other)
	    : prefix_paths(other.prefix_paths), type(other.type), provider(other.provider), name(other.name),
	      serializable(other.serializable) {
		D_ASSERT(!type.empty());
	}
	virtual ~BaseSecret() = default;

	//! The score of how well this secret's scope matches the path (by default: the length of the longest matching
	//! prefix)
	virtual int MatchScore(const string &path);

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
	//! Helper function to serialize the base BaseSecret class variables
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

//! The BaseKeyValueSecret is a base class that implements a Secret as a set of key -> value strings. This class
//! implements some features that all secret implementations that consist of only a key->value map of strings need.
class BaseKeyValueSecret : public BaseSecret {
public:
	BaseKeyValueSecret(vector<string> &prefix_paths, const string &type, const string &provider, const string &name)
	    : BaseSecret(prefix_paths, type, provider, name) {
		D_ASSERT(!type.empty());
		serializable = true;
	}
	BaseKeyValueSecret(BaseSecret &secret)
	    : BaseSecret(secret.GetScope(), secret.GetType(), secret.GetProvider(), secret.GetName()) {};
	BaseKeyValueSecret(BaseKeyValueSecret &secret)
	    : BaseSecret(secret.GetScope(), secret.GetType(), secret.GetProvider(), secret.GetName()) {
		secret_map = secret.secret_map;
	};

	virtual string ToString(bool redact) override;
	void Serialize(Serializer &serializer) const override;

	template <class TYPE>
	static unique_ptr<BaseSecret> Deserialize(Deserializer &deserializer, BaseSecret base_secret) {
		auto result = make_uniq<TYPE>(base_secret);
		Value secret_map_value;
		deserializer.ReadProperty(201, "secret_map", secret_map_value);

		auto list_of_map = ListValue::GetChildren(secret_map_value);
		for (const auto &entry : list_of_map) {
			auto kv_struct = StructValue::GetChildren(entry);
			result->secret_map[kv_struct[0].ToString()] = kv_struct[1].ToString();
		}

		return result;
	}

protected:
	//! the map of key -> values that make up the secret
	case_insensitive_map_t<string> secret_map;

	//! (optionally) a set of keys to be redacted for this type
	case_insensitive_set_t redact_keys;
};

} // namespace duckdb
