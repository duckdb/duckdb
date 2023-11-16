//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret/secret.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/named_parameter_map.hpp"

namespace duckdb {
class BaseSecret;

//! Input passed to a CreateSecretFunction
struct CreateSecretInput {
	//! type
	string type;
	//! mode
	string provider;
	//! should the secret be persisted?
	SecretPersistMode persist;
	//! (optional) alias provided by user
	string name;
	//! (optional) scope provided by user
	vector<string> scope;
	//! (optional) named parameter map, each create secret function has defined it's own set of these
	named_parameter_map_t named_parameters;
};

//! Deserialize Function
typedef unique_ptr<BaseSecret> (*secret_deserializer_t)(Deserializer &deserializer, BaseSecret base_secret);
//! Create Secret Function
typedef unique_ptr<BaseSecret> (*create_secret_function_t)(ClientContext &context, CreateSecretInput &input);

//! A CreateSecretFunction is a function that can produce secrets of a specific type using a provider.
class CreateSecretFunction {
public:
	string secret_type;
	string provider;
	create_secret_function_t function;
	named_parameter_type_map_t named_parameters;
};

//! CreateSecretFunctionsSet contains multiple functions of a specific type, identified by the provider. The provider
//! should be seen as the method of secret creation. (e.g. user-provided config, env variables, auto-detect)
class CreateSecretFunctionSet {
public:
	CreateSecretFunctionSet(string& name) : name(name){};
	bool ProviderExists(const string& provider_name);
	void AddFunction(CreateSecretFunction function, OnCreateConflict on_conflict);
	CreateSecretFunction& GetFunction(const string& provider);

protected:
	//! Create Secret Function type name
	string name;
	//! Maps of provider -> function
	case_insensitive_map_t<CreateSecretFunction> functions;
};

enum class SecretPersistMode : uint8_t {
	DEFAULT,
	TEMPORARY,
	PERMANENT
};

//! Secret types contain the base settings of a secret
struct SecretType {
	//! Unique name identifying the secret type
	string name;
	//! The deserialization function for the type
	secret_deserializer_t deserializer;
	//! Provider to use when non is specified
	string default_provider;
};

//! Base class from which BaseSecret classes can be made.
class BaseSecret {
	friend class SecretManager;

public:
	BaseSecret(const vector<string> &prefix_paths, const string &type, const string &provider, const string &name)
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
	virtual int MatchScore(const string &path) const;
	//! The ToString method prints the secret, the redact option determines whether secret data is allowed to be printed
	//! in clear text. This is to be decided by the secret implementation
	virtual string ToString(bool redact) const;
	//! Serialize this secret
	virtual void Serialize(Serializer &serializer) const;

	//! Getters
	const vector<string> &GetScope() const {
		return prefix_paths;
	}
	const string &GetType() const {
		return type;
	}
	const string &GetProvider() const {
		return provider;
	}
	const string &GetName() const {
		return name;
	}
	bool IsSerializable() const {
		return serializable;
	}

protected:
	//! Helper function to serialize the base BaseSecret class variables
	virtual void SerializeBaseSecret(Serializer &serializer) const final;

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
	    :  BaseSecret(prefix_paths, type, provider, name) {
		D_ASSERT(!type.empty());
		serializable = true;
	}
	BaseKeyValueSecret(BaseSecret &secret)
	    : BaseSecret(secret.GetScope(), secret.GetType(), secret.GetProvider(), secret.GetName()) {
		serializable = true;
  	};
	BaseKeyValueSecret(BaseKeyValueSecret &secret)
	    : BaseSecret(secret.GetScope(), secret.GetType(), secret.GetProvider(), secret.GetName()) {
		secret_map = secret.secret_map;
		serializable = true;
	};

	//! Print the secret as a key value map in the format 'key1=value;key2=value2'
	virtual string ToString(bool redact) const override;
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
	map<string, string> secret_map;

	//! (optionally) a set of keys to be redacted for this type
	case_insensitive_set_t redact_keys;
};

} // namespace duckdb
