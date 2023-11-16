//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/secret_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/main/secret.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {
class ClientContext;
class BaseSecret;
class CreateSecretFunction;
struct CreateSecretInfo;
struct CreateSecretInput;
struct BoundStatement;
class FileSystem;

//! Deserialize Function
typedef unique_ptr<BaseSecret> (*secret_deserializer_t)(Deserializer &deserializer, BaseSecret base_secret);
//! Create Secret Function
typedef unique_ptr<BaseSecret> (*create_secret_function_t)(ClientContext &context, CreateSecretInput &input);

//! Secret types contain the base settings of a secret
struct SecretType {
	//! Unique name identifying the secret type
	string name;
	//! The deserialization function for the type
	secret_deserializer_t deserializer;
	//! Provider to use when non is specified
	string default_provider;
};

//! Registered secret is a wrapper around a secret containing metadata from the secret manager
struct RegisteredSecret {
public:
	RegisteredSecret(shared_ptr<const BaseSecret> secret) : secret(secret){};
	//! Whether this secret is persistent
	bool persistent;
	//! Metadata for user on how the secret is stored. (DuckSecretManager will set this to the path)
	string storage_mode;
	//! The secret pointer
	shared_ptr<const BaseSecret> secret;
};

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

//! Base secret manager class
class SecretManager {
	friend struct RegisteredSecret;

public:
	virtual ~SecretManager() = default;

	//! Deserialize the secret. Will look up the deserialized type, then call the deserialize for the registered type.
	DUCKDB_API virtual unique_ptr<BaseSecret> DeserializeSecret(Deserializer &deserializer) = 0;
	//! Registers a secret type
	DUCKDB_API virtual void RegisterSecretType(SecretType &type) = 0;
	//! Get the registered type
	DUCKDB_API virtual SecretType LookupType(const string &type) = 0;
	//! Registers a create secret function
	DUCKDB_API virtual void RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) = 0;
	//! Register a Secret directly
	DUCKDB_API virtual void RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict, SecretPersistMode persist_mode) = 0;
	//! Create & Register a secret by looking up the function
	DUCKDB_API virtual void CreateSecret(ClientContext &context, const CreateSecretInfo &input) = 0;
	//! Binds a create secret statement
	DUCKDB_API virtual BoundStatement BindCreateSecret(CreateSecretStatement &stmt) = 0;
	//! Get the secret whose scope best matches the path.
	DUCKDB_API virtual RegisteredSecret GetSecretByPath(const string &path, const string &type) = 0;
	//! Get a secret by name
	DUCKDB_API virtual RegisteredSecret GetSecretByName(const string &name) = 0;
	//! Drop a secret by name
	DUCKDB_API virtual void DropSecretByName(const string &name, bool missing_ok) = 0;
	//! Get a vector of all registered secrets
	DUCKDB_API virtual vector<RegisteredSecret> AllSecrets() = 0;
};

//! The Main secret manager implementation for DuckDB. The secret manager can handle both temporary and permanent secrets
class DuckSecretManager : public SecretManager {
	friend struct RegisteredSecret;

public:
	explicit DuckSecretManager(DatabaseInstance& instance);
	virtual ~DuckSecretManager() override = default;

	//! SecretManager API
	DUCKDB_API virtual unique_ptr<BaseSecret> DeserializeSecret(Deserializer &deserializer) override;
	DUCKDB_API virtual void RegisterSecretType(SecretType &type) override;
	DUCKDB_API virtual SecretType LookupType(const string &type) override;
	DUCKDB_API virtual void RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) override;
	DUCKDB_API virtual void RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict, SecretPersistMode persist_mode) override;
	DUCKDB_API virtual void CreateSecret(ClientContext &context, const CreateSecretInfo &info) override;
	DUCKDB_API virtual BoundStatement BindCreateSecret(CreateSecretStatement &stmt) override;
	DUCKDB_API virtual RegisteredSecret GetSecretByPath(const string &path, const string &type) override;
	DUCKDB_API virtual RegisteredSecret GetSecretByName(const string &name) override;
	DUCKDB_API virtual void DropSecretByName(const string &name, bool missing_ok) override;

	//! Get a vector of all registered secrets
	DUCKDB_API virtual vector<RegisteredSecret> AllSecrets() override;

private:
	//! Deserialize a secret
	unique_ptr<BaseSecret> DeserializeSecretInternal(Deserializer &deserializer);
	//! Lookup a SecretType
	SecretType LookupTypeInternal(const string &type);
	//! Lookup a CreateSecretFunction
	CreateSecretFunction* LookupFunctionInternal(const string& type, const string& provider);
	//! Register a new Secret
	void RegisterSecretInternal(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict, SecretPersistMode persist_mode);

	//! Write a secret to the FileSystem
	void WriteSecretToFile(const BaseSecret& secret);

	//! Lazily preloads the permanent secrets f
	void PreloadPermanentSecrets();

	//! Fully loads ALL lazily preloaded permanent secrets that have not yet been preloaded
	void LoadPreloadedSecrets();
	//! Fully load a lazily preloaded permanent secret by path
	void LoadSecret(const string& path, SecretPersistMode persist_mode);
	//! Fully load a lazily preloaded permanent secret by name
	void LoadSecretFromPreloaded(const string& name);

	//! Checks if the secret_directory changed, if so this reloads all permanent secrets (lazily)
	void SyncPermanentSecrets(bool force = false);

	//! Return secret directory
	string GetSecretDirectory();

	//! Current SecretTypes and CreatSecretFunctions
	case_insensitive_map_t<SecretType> registered_types;
	case_insensitive_map_t<CreateSecretFunctionSet> create_secret_functions;

	//! The currently loaded secrets: can contain both temporary and permanent secrets.
	vector<RegisteredSecret> registered_secrets;
	//! Maps secret name -> permanent secret file path. Permanent secrets are loaded lazily into this map. Then loaded into
	//! registered_secrets when they are needed
	case_insensitive_map_t<string> permanent_secret_files;
	//! The permanent secret directory is scanned once. When the secret path changes in DuckDB the secret manager will
	//! need to reload the permanent secrets;
	string last_secret_directory;
	//! The secret manager requires access to the DatabaseInstance for the FileSystem and DBConfig
	DatabaseInstance &db_instance;
	//! Secret manager needs to be thread-safe
	mutex lock;
};

//! The debug secret manager demonstrates how the Base Secret Manager can be extended
class DebugSecretManager : public SecretManager {
public:
	virtual ~DebugSecretManager() override = default;
	DebugSecretManager(unique_ptr<SecretManager> secret_manager) : base_secret_manager(std::move(secret_manager)){};

	DUCKDB_API virtual unique_ptr<BaseSecret> DeserializeSecret(Deserializer &deserializer) override;
	DUCKDB_API virtual void RegisterSecretType(SecretType &type) override;
	DUCKDB_API virtual void RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) override;
	DUCKDB_API virtual void RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict, SecretPersistMode persist_mode) override;
	DUCKDB_API virtual void CreateSecret(ClientContext &context, const CreateSecretInfo &info) override;
	DUCKDB_API virtual BoundStatement BindCreateSecret(CreateSecretStatement &stmt) override;
	DUCKDB_API virtual RegisteredSecret GetSecretByPath(const string &path, const string &type) override;
	DUCKDB_API virtual RegisteredSecret GetSecretByName(const string &name) override;
	DUCKDB_API virtual void DropSecretByName(const string &name, bool missing_ok) override;
	DUCKDB_API virtual SecretType LookupType(const string &type) override;
	DUCKDB_API virtual vector<RegisteredSecret> AllSecrets() override;

protected:
	unique_ptr<SecretManager> base_secret_manager;
};

} // namespace duckdb
