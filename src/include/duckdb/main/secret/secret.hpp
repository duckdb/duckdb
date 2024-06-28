//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret/secret.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/named_parameter_map.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {
class BaseSecret;

//! Whether a secret is persistent or temporary
enum class SecretPersistType : uint8_t { DEFAULT, TEMPORARY, PERSISTENT };

//! Input passed to a CreateSecretFunction
struct CreateSecretInput {
	//! type
	string type;
	//! mode
	string provider;
	//! should the secret be persisted?
	string storage_type;
	//! (optional) alias provided by user
	string name;
	//! (optional) scope provided by user
	vector<string> scope;
	//! (optional) named parameter map, each create secret function has defined it's own set of these
	case_insensitive_map_t<Value> options;
};

typedef unique_ptr<BaseSecret> (*secret_deserializer_t)(Deserializer &deserializer, BaseSecret base_secret);
typedef unique_ptr<BaseSecret> (*create_secret_function_t)(ClientContext &context, CreateSecretInput &input);

//! A CreateSecretFunction is a function adds a provider for a secret type.
class CreateSecretFunction {
public:
	string secret_type;
	string provider;
	create_secret_function_t function;
	named_parameter_type_map_t named_parameters;
};

//! CreateSecretFunctionsSet contains multiple functions of a single type, identified by the provider. The provider
//! should be seen as the method of secret creation. (e.g. user-provided config, env variables, auto-detect)
class CreateSecretFunctionSet {
public:
	explicit CreateSecretFunctionSet(string &name) : name(name) {};

public:
	bool ProviderExists(const string &provider_name);
	void AddFunction(CreateSecretFunction &function, OnCreateConflict on_conflict);
	CreateSecretFunction &GetFunction(const string &provider);

protected:
	//! Create Secret Function type name
	string name;
	//! Maps of provider -> function
	case_insensitive_map_t<CreateSecretFunction> functions;
};

//! Determines whether the secrets are allowed to be shown
enum class SecretDisplayType : uint8_t { REDACTED, UNREDACTED };

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
	BaseSecret(vector<string> prefix_paths_p, string type_p, string provider_p, string name_p)
	    : prefix_paths(std::move(prefix_paths_p)), type(std::move(type_p)), provider(std::move(provider_p)),
	      name(std::move(name_p)), serializable(false) {
		D_ASSERT(!type.empty());
	}
	BaseSecret(const BaseSecret &other)
	    : prefix_paths(other.prefix_paths), type(other.type), provider(other.provider), name(other.name),
	      serializable(other.serializable) {
		D_ASSERT(!type.empty());
	}
	virtual ~BaseSecret() = default;

	//! The score of how well this secret's scope matches the path (by default: the length of the longest matching
	//! prefix)
	virtual int64_t MatchScore(const string &path) const;
	//! Prints the secret as a string
	virtual string ToString(SecretDisplayType mode = SecretDisplayType::REDACTED) const;
	//! Serialize this secret
	virtual void Serialize(Serializer &serializer) const;

	virtual unique_ptr<const BaseSecret> Clone() const {
		D_ASSERT(typeid(BaseSecret) == typeid(*this));
		return make_uniq<BaseSecret>(*this);
	}

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

//! The KeyValueSecret is a class that implements a Secret as a set of key -> values. This class can be used
//! for most use-cases of secrets as secrets generally tend to fit in a key value map.
class KeyValueSecret : public BaseSecret {
public:
	KeyValueSecret(const vector<string> &prefix_paths, const string &type, const string &provider, const string &name)
	    : BaseSecret(prefix_paths, type, provider, name) {
		D_ASSERT(!type.empty());
		serializable = true;
	}
	explicit KeyValueSecret(const BaseSecret &secret)
	    : BaseSecret(secret.GetScope(), secret.GetType(), secret.GetProvider(), secret.GetName()) {
		serializable = true;
	};
	KeyValueSecret(const KeyValueSecret &secret)
	    : BaseSecret(secret.GetScope(), secret.GetType(), secret.GetProvider(), secret.GetName()) {
		secret_map = secret.secret_map;
		redact_keys = secret.redact_keys;
		serializable = true;
	};
	KeyValueSecret(KeyValueSecret &&secret) noexcept
	    : BaseSecret(std::move(secret.prefix_paths), std::move(secret.type), std::move(secret.provider),
	                 std::move(secret.name)) {
		secret_map = std::move(secret.secret_map);
		redact_keys = std::move(secret.redact_keys);
		serializable = true;
	};

	//! Print the secret as a key value map in the format 'key1=value;key2=value2'
	string ToString(SecretDisplayType mode = SecretDisplayType::REDACTED) const override;
	void Serialize(Serializer &serializer) const override;

	//! Tries to get the value at key <key>, depending on error_on_missing will throw or return Value()
	Value TryGetValue(const string &key, bool error_on_missing = false) const;

	// FIXME: use serialization scripts
	template <class TYPE>
	static unique_ptr<BaseSecret> Deserialize(Deserializer &deserializer, BaseSecret base_secret) {
		auto result = make_uniq<TYPE>(base_secret);
		Value secret_map_value;
		deserializer.ReadProperty(201, "secret_map", secret_map_value);

		for (const auto &entry : ListValue::GetChildren(secret_map_value)) {
			auto kv_struct = StructValue::GetChildren(entry);
			result->secret_map[kv_struct[0].ToString()] = kv_struct[1].ToString();
		}

		Value redact_set_value;
		deserializer.ReadProperty(202, "redact_keys", redact_set_value);
		for (const auto &entry : ListValue::GetChildren(redact_set_value)) {
			result->redact_keys.insert(entry.ToString());
		}

		return duckdb::unique_ptr_cast<TYPE, BaseSecret>(std::move(result));
	}

	unique_ptr<const BaseSecret> Clone() const override {
		return make_uniq<KeyValueSecret>(*this);
	}

	//! the map of key -> values that make up the secret
	case_insensitive_tree_t<Value> secret_map;
	//! keys that are sensitive and should be redacted
	case_insensitive_set_t redact_keys;
};

} // namespace duckdb
