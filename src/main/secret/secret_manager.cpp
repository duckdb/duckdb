#include "duckdb/main/secret/secret_manager.hpp"

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_storage.hpp"
#include "duckdb/main/secret/default_secrets.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/operator/logical_create_secret.hpp"

namespace duckdb {

SecretCatalogEntry::SecretCatalogEntry(unique_ptr<SecretEntry> secret_p, Catalog &catalog)
    : InCatalogEntry(CatalogType::SECRET_ENTRY, catalog, secret_p->secret->GetName()), secret(std::move(secret_p)) {
	internal = true;
}

SecretCatalogEntry::SecretCatalogEntry(unique_ptr<const BaseSecret> secret_p, Catalog &catalog)
    : InCatalogEntry(CatalogType::SECRET_ENTRY, catalog, secret_p->GetName()) {
	internal = true;
	secret = make_uniq<SecretEntry>(std::move(secret_p));
}

const BaseSecret &SecretMatch::GetSecret() const {
	return *secret_entry->secret;
}

constexpr const char *SecretManager::TEMPORARY_STORAGE_NAME;
constexpr const char *SecretManager::LOCAL_FILE_STORAGE_NAME;

void SecretManager::Initialize(DatabaseInstance &db) {
	lock_guard<mutex> lck(manager_lock);

	// Construct default path
	LocalFileSystem fs;
	config.default_secret_path = fs.GetHomeDirectory();
	vector<string> path_components = {".duckdb", "stored_secrets"};
	for (auto &path_ele : path_components) {
		config.default_secret_path = fs.JoinPath(config.default_secret_path, path_ele);
	}
	// Use default path if none has been specified by the user configuration
	if (config.secret_path.empty()) {
		config.secret_path = config.default_secret_path;
	}

	// Set the defaults for persistent storage
	config.default_persistent_storage = LOCAL_FILE_STORAGE_NAME;

	// Store the current db for enabling autoloading
	this->db = &db;

	// Register default types
	for (auto &type : CreateHTTPSecretFunctions::GetDefaultSecretTypes()) {
		RegisterSecretTypeInternal(type);
	}

	// Register default functions
	for (auto &function : CreateHTTPSecretFunctions::GetDefaultSecretFunctions()) {
		RegisterSecretFunctionInternal(function, OnCreateConflict::ERROR_ON_CONFLICT);
	}
}

void SecretManager::LoadSecretStorage(unique_ptr<SecretStorage> storage) {
	lock_guard<mutex> lck(manager_lock);
	return LoadSecretStorageInternal(std::move(storage));
}

void SecretManager::LoadSecretStorageInternal(unique_ptr<SecretStorage> storage) {
	if (secret_storages.find(storage->GetName()) != secret_storages.end()) {
		throw InvalidConfigurationException("Secret Storage with name '%s' already registered!", storage->GetName());
	}

	// Check for tie-break offset collisions to ensure we can always tie-break cleanly
	for (const auto &storage_ptr : secret_storages) {
		if (storage_ptr.second->tie_break_offset == storage->tie_break_offset) {
			throw InvalidConfigurationException(
			    "Failed to load secret storage '%s', tie break score collides with '%s'", storage->GetName(),
			    storage_ptr.second->GetName());
		}
	}

	secret_storages[storage->GetName()] = std::move(storage);
}

// FIXME: use serialization scripts?
unique_ptr<BaseSecret> SecretManager::DeserializeSecret(Deserializer &deserializer, const string &secret_path) {
	auto type = deserializer.ReadProperty<string>(100, "type");
	auto provider = deserializer.ReadProperty<string>(101, "provider");
	auto name = deserializer.ReadProperty<string>(102, "name");
	vector<string> scope;
	deserializer.ReadList(103, "scope",
	                      [&](Deserializer::List &list, idx_t i) { scope.push_back(list.ReadElement<string>()); });
	auto serialization_type = deserializer.ReadPropertyWithExplicitDefault(104, "serialization_type",
	                                                                       SecretSerializationType::KEY_VALUE_SECRET);

	switch (serialization_type) {
	// This allows us to skip looking up the secret type for deserialization altogether
	case SecretSerializationType::KEY_VALUE_SECRET:
		return KeyValueSecret::Deserialize<KeyValueSecret>(deserializer, {scope, type, provider, name});
	// Continues below: we need to do a type lookup to find the secret deserialize method
	case SecretSerializationType::CUSTOM:
		break;
	default:
		throw IOException("Unrecognized secret serialization type found in secret '%s': %s", secret_path,
		                  EnumUtil::ToString(serialization_type));
	}

	SecretType deserialized_type;
	if (!TryLookupTypeInternal(type, deserialized_type)) {
		ThrowTypeNotFoundError(type, secret_path);
	}

	if (!deserialized_type.deserializer) {
		throw InvalidConfigurationException(
		    "Attempted to deserialize secret type '%s' which does not have a deserialization method", type);
	}

	return deserialized_type.deserializer(deserializer, {scope, type, provider, name});
}

void SecretManager::RegisterSecretType(SecretType &type) {
	lock_guard<mutex> lck(manager_lock);
	RegisterSecretTypeInternal(type);
}

void SecretManager::RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) {
	unique_lock<mutex> lck(manager_lock);
	RegisterSecretFunctionInternal(std::move(function), on_conflict);
}

unique_ptr<SecretEntry> SecretManager::RegisterSecret(CatalogTransaction transaction,
                                                      unique_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
                                                      SecretPersistType persist_type, const string &storage) {
	InitializeSecrets(transaction);
	return RegisterSecretInternal(transaction, std::move(secret), on_conflict, persist_type, storage);
}

unique_ptr<SecretEntry> SecretManager::RegisterSecretInternal(CatalogTransaction transaction,
                                                              unique_ptr<const BaseSecret> secret,
                                                              OnCreateConflict on_conflict,
                                                              SecretPersistType persist_type, const string &storage) {
	//! Ensure we only create secrets for known types;
	LookupTypeInternal(secret->GetType());

	//! Handle default for persist type
	if (persist_type == SecretPersistType::DEFAULT) {
		if (storage.empty()) {
			persist_type = config.default_persist_type;
		} else if (storage == TEMPORARY_STORAGE_NAME) {
			persist_type = SecretPersistType::TEMPORARY;
		} else {
			persist_type = SecretPersistType::PERSISTENT;
		}
	}

	//! Resolve storage
	string resolved_storage;
	if (storage.empty()) {
		resolved_storage =
		    persist_type == SecretPersistType::PERSISTENT ? config.default_persistent_storage : TEMPORARY_STORAGE_NAME;
	} else {
		resolved_storage = storage;
	}

	//! Lookup which backend to store the secret in
	auto backend = GetSecretStorage(resolved_storage);
	if (!backend) {
		if (!config.allow_persistent_secrets &&
		    (persist_type == SecretPersistType::PERSISTENT || storage == LOCAL_FILE_STORAGE_NAME)) {
			throw InvalidInputException("Persistent secrets are disabled. Restart DuckDB and enable persistent secrets "
			                            "through 'SET allow_persistent_secrets=true'");
		}
		throw InvalidInputException("Secret storage '%s' not found!", resolved_storage);
	}

	// Validation on both allow_persistent_secrets and storage backend's own persist type
	if (persist_type == SecretPersistType::PERSISTENT) {
		if (backend->persistent) {
			if (!config.allow_persistent_secrets) {
				throw InvalidInputException(
				    "Persistent secrets are currently disabled. To enable them, restart duckdb and "
				    "run 'SET allow_persistent_secrets=true'");
			}
		} else { // backend is temp
			throw InvalidInputException("Cannot create persistent secrets in a temporary secret storage!");
		}
	} else { // SecretPersistType::TEMPORARY
		if (backend->persistent) {
			throw InvalidInputException("Cannot create temporary secrets in a persistent secret storage!");
		}
	}
	return backend->StoreSecret(std::move(secret), on_conflict, &transaction);
}

optional_ptr<CreateSecretFunction> SecretManager::LookupFunctionInternal(const string &type, const string &provider) {
	unique_lock<mutex> lck(manager_lock);
	auto lookup = secret_functions.find(type);

	if (lookup != secret_functions.end()) {
		if (lookup->second.ProviderExists(provider)) {
			return &lookup->second.GetFunction(provider);
		}
	}

	// Try autoloading
	lck.unlock();
	AutoloadExtensionForFunction(type, provider);
	lck.lock();

	lookup = secret_functions.find(type);

	if (lookup != secret_functions.end()) {
		if (lookup->second.ProviderExists(provider)) {
			return &lookup->second.GetFunction(provider);
		}
	}

	return nullptr;
}

unique_ptr<SecretEntry> SecretManager::CreateSecret(ClientContext &context, const CreateSecretInput &input) {
	// Note that a context is required for CreateSecret, as the CreateSecretFunction expects one
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	InitializeSecrets(transaction);

	// Make a copy to set the provider to default if necessary
	auto function_input = input;
	if (function_input.provider.empty()) {
		auto secret_type = LookupTypeInternal(function_input.type);
		function_input.provider = secret_type.default_provider;
	}

	// Lookup function
	auto function_lookup = LookupFunctionInternal(function_input.type, function_input.provider);
	if (!function_lookup) {
		ThrowProviderNotFoundError(input.type, input.provider);
	}

	// Call the function
	auto secret = function_lookup->function(context, function_input);

	if (!secret) {
		throw InternalException("CreateSecretFunction for type: '%s' and provider: '%s' did not return a secret!",
		                        input.type, input.provider);
	}

	// Register the secret at the secret_manager
	return RegisterSecretInternal(transaction, std::move(secret), input.on_conflict, input.persist_type,
	                              input.storage_type);
}

BoundStatement SecretManager::BindCreateSecret(CatalogTransaction transaction, CreateSecretInput &info) {
	InitializeSecrets(transaction);

	auto type = info.type;
	auto provider = info.provider;
	bool default_provider = false;

	if (provider.empty()) {
		default_provider = true;
		auto secret_type = LookupTypeInternal(type);
		provider = secret_type.default_provider;
	}

	string default_string = default_provider ? "default " : "";

	auto function = LookupFunctionInternal(type, provider);

	if (!function) {
		ThrowProviderNotFoundError(info.type, info.provider, default_provider);
	}

	auto bound_info = info;
	bound_info.options.clear();

	// We cast the passed parameters
	for (const auto &param : info.options) {
		auto matched_param = function->named_parameters.find(param.first);
		if (matched_param == function->named_parameters.end()) {
			throw BinderException("Unknown parameter '%s' for secret type '%s' with %sprovider '%s'", param.first, type,
			                      default_string, provider);
		}

		// Cast the provided value to the expected type
		string error_msg;
		Value cast_value;
		if (!param.second.DefaultTryCastAs(matched_param->second, cast_value, &error_msg)) {
			throw BinderException("Failed to cast option '%s' to type '%s': '%s'", matched_param->first,
			                      matched_param->second.ToString(), error_msg);
		}

		bound_info.options[matched_param->first] = {cast_value};
	}

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_uniq<LogicalCreateSecret>(std::move(bound_info));
	return result;
}

SecretMatch SecretManager::LookupSecret(CatalogTransaction transaction, const string &path, const string &type) {
	InitializeSecrets(transaction);

	int64_t best_match_score = NumericLimits<int64_t>::Minimum();
	unique_ptr<SecretEntry> best_match = nullptr;

	for (const auto &storage_ref : GetSecretStorages()) {
		if (!storage_ref.get().IncludeInLookups()) {
			continue;
		}
		auto match = storage_ref.get().LookupSecret(path, StringUtil::Lower(type), &transaction);
		if (match.HasMatch() && match.score > best_match_score) {
			best_match = std::move(match.secret_entry);
			best_match_score = match.score;
		}
	}

	if (best_match) {
		return SecretMatch(*best_match, best_match_score);
	}

	return SecretMatch();
}

unique_ptr<SecretEntry> SecretManager::GetSecretByName(CatalogTransaction transaction, const string &name,
                                                       const string &storage) {
	InitializeSecrets(transaction);

	unique_ptr<SecretEntry> result = nullptr;
	bool found = false;

	if (!storage.empty()) {
		auto storage_lookup = GetSecretStorage(storage);

		if (!storage_lookup) {
			throw InvalidInputException("Unknown secret storage found: '%s'", storage);
		}

		return storage_lookup->GetSecretByName(name, &transaction);
	}

	for (const auto &storage_ref : GetSecretStorages()) {
		auto lookup = storage_ref.get().GetSecretByName(name, &transaction);
		if (lookup) {
			if (found) {
				throw InvalidConfigurationException(
				    "Ambiguity detected for secret name '%s', secret occurs in multiple storage backends.", name);
			}

			result = std::move(lookup);
			found = true;
		}
	}

	return result;
}

void SecretManager::DropSecretByName(CatalogTransaction transaction, const string &name,
                                     OnEntryNotFound on_entry_not_found, SecretPersistType persist_type,
                                     const string &storage) {
	InitializeSecrets(transaction);

	vector<reference<SecretStorage>> matches;

	// storage to drop from was specified directly
	if (!storage.empty()) {
		auto storage_lookup = GetSecretStorage(storage);
		if (!storage_lookup) {
			throw InvalidInputException("Unknown storage type found for drop secret: '%s'", storage);
		}
		matches.push_back(*storage_lookup.get());
	} else {
		for (const auto &storage_ref : GetSecretStorages()) {
			if (persist_type == SecretPersistType::PERSISTENT && !storage_ref.get().Persistent()) {
				continue;
			}
			if (persist_type == SecretPersistType::TEMPORARY && storage_ref.get().Persistent()) {
				continue;
			}

			auto lookup = storage_ref.get().GetSecretByName(name, &transaction);
			if (lookup) {
				matches.push_back(storage_ref.get());
			}
		}
	}

	if (matches.size() > 1) {
		string list_of_matches;
		for (const auto &match : matches) {
			list_of_matches += match.get().GetName() + ",";
		}
		list_of_matches.pop_back(); // trailing comma

		throw InvalidInputException(
		    "Ambiguity found for secret name '%s', secret occurs in multiple storages: [%s] Please specify which "
		    "secret to drop using: 'DROP <PERSISTENT|TEMPORARY> SECRET [FROM <storage>]'.",
		    name, list_of_matches);
	}

	if (matches.empty()) {
		if (on_entry_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			string storage_str;
			if (!storage.empty()) {
				storage_str = " for storage '" + storage + "'";
			}
			throw InvalidInputException("Failed to remove non-existent secret with name '%s'%s", name, storage_str);
		}
		// Do nothing on OnEntryNotFound::RETURN_NULL...
	} else {
		matches[0].get().DropSecretByName(name, on_entry_not_found, &transaction);
	}
}

SecretType SecretManager::LookupType(const string &type) {
	return LookupTypeInternal(type);
}

void SecretManager::RegisterSecretTypeInternal(SecretType &type) {
	auto lookup = secret_types.find(type.name);
	if (lookup != secret_types.end()) {
		throw InternalException("Attempted to register an already registered secret type: '%s'", type.name);
	}
	secret_types[type.name] = type;
}

bool SecretManager::TryLookupTypeInternal(const string &type, SecretType &type_out) {
	unique_lock<mutex> lck(manager_lock);
	auto lookup = secret_types.find(type);
	if (lookup != secret_types.end()) {
		type_out = lookup->second;
		return true;
	}

	// Try autoloading
	lck.unlock();
	AutoloadExtensionForType(type);
	lck.lock();

	lookup = secret_types.find(type);
	if (lookup != secret_types.end()) {
		type_out = lookup->second;
		return true;
	}

	return false;
}

SecretType SecretManager::LookupTypeInternal(const string &type) {
	SecretType return_value;
	if (!TryLookupTypeInternal(type, return_value)) {
		ThrowTypeNotFoundError(type);
	}
	return return_value;
}

void SecretManager::RegisterSecretFunctionInternal(CreateSecretFunction function, OnCreateConflict on_conflict) {
	auto lookup = secret_functions.find(function.secret_type);
	if (lookup != secret_functions.end()) {
		lookup->second.AddFunction(function, on_conflict);
		return;
	}
	CreateSecretFunctionSet new_set(function.secret_type);
	new_set.AddFunction(function, OnCreateConflict::ERROR_ON_CONFLICT);
	secret_functions.insert({function.secret_type, new_set});
}

vector<SecretEntry> SecretManager::AllSecrets(CatalogTransaction transaction) {
	InitializeSecrets(transaction);

	vector<SecretEntry> result;

	// Add results from all backends to the result set
	for (const auto &backend : secret_storages) {
		auto backend_result = backend.second->AllSecrets(&transaction);
		for (const auto &it : backend_result) {
			result.push_back(it);
		}
	}

	return result;
}

vector<SecretType> SecretManager::AllSecretTypes() {
	unique_lock<mutex> lck(manager_lock);
	vector<SecretType> result;

	for (const auto &secret : secret_types) {
		result.push_back(secret.second);
	}

	return result;
}

void SecretManager::ThrowOnSettingChangeIfInitialized() {
	if (initialized) {
		throw InvalidInputException(
		    "Changing Secret Manager settings after the secret manager is used is not allowed!");
	}
}

void SecretManager::SetEnablePersistentSecrets(bool enabled) {
	ThrowOnSettingChangeIfInitialized();
	config.allow_persistent_secrets = enabled;
}

void SecretManager::ResetEnablePersistentSecrets() {
	ThrowOnSettingChangeIfInitialized();
	config.allow_persistent_secrets = SecretManagerConfig::DEFAULT_ALLOW_PERSISTENT_SECRETS;
}

bool SecretManager::PersistentSecretsEnabled() {
	return config.allow_persistent_secrets;
}

void SecretManager::SetDefaultStorage(const string &storage) {
	config.default_persistent_storage = storage;
}

void SecretManager::ResetDefaultStorage() {
	config.default_persistent_storage = SecretManager::LOCAL_FILE_STORAGE_NAME;
}

string SecretManager::DefaultStorage() {
	return config.default_persistent_storage;
}

void SecretManager::SetPersistentSecretPath(const string &path) {
	ThrowOnSettingChangeIfInitialized();
	config.secret_path = path;
}

void SecretManager::ResetPersistentSecretPath() {
	ThrowOnSettingChangeIfInitialized();
	config.secret_path = config.default_secret_path;
}

string SecretManager::PersistentSecretPath() {
	return config.secret_path;
}

void SecretManager::InitializeSecrets(CatalogTransaction transaction) {
	if (!initialized) {
		lock_guard<mutex> lck(manager_lock);
		if (initialized) {
			// some sneaky other thread beat us to it
			return;
		}

		// load the tmp storage
		LoadSecretStorageInternal(make_uniq<TemporarySecretStorage>(TEMPORARY_STORAGE_NAME, *transaction.db));

		if (config.allow_persistent_secrets) {
			// load the persistent storage if enabled
			LoadSecretStorageInternal(
			    make_uniq<LocalFileSecretStorage>(*this, *transaction.db, LOCAL_FILE_STORAGE_NAME, config.secret_path));
		}

		initialized = true;
	}
}

void SecretManager::AutoloadExtensionForType(const string &type) {
	ExtensionHelper::TryAutoloadFromEntry(*db, StringUtil::Lower(type), EXTENSION_SECRET_TYPES);
}

void SecretManager::ThrowTypeNotFoundError(const string &type, const string &secret_path) {
	auto entry = ExtensionHelper::FindExtensionInEntries(StringUtil::Lower(type), EXTENSION_SECRET_TYPES);
	string error_message;

	if (!entry.empty() && db) {
		error_message = "Secret type '" + type + "' does not exist, but it exists in the " + entry + " extension.";
		error_message = ExtensionHelper::AddExtensionInstallHintToErrorMsg(*db, error_message, entry);

		if (!secret_path.empty()) {
			error_message += "\n\nAlternatively, ";
		}
	} else {
		error_message = StringUtil::Format("Secret type '%s' not found", type);

		if (!secret_path.empty()) {
			error_message += ", ";
		}
	}

	if (!secret_path.empty()) {
		error_message += StringUtil::Format("try removing the secret at path '%s'.", secret_path);
	}

	throw InvalidInputException(error_message);
}

void SecretManager::AutoloadExtensionForFunction(const string &type, const string &provider) {
	ExtensionHelper::TryAutoloadFromEntry(*db, StringUtil::Lower(type) + "/" + StringUtil::Lower(provider),
	                                      EXTENSION_SECRET_PROVIDERS);
}

void SecretManager::ThrowProviderNotFoundError(const string &type, const string &provider, bool was_default) {
	auto entry = ExtensionHelper::FindExtensionInEntries(StringUtil::Lower(type) + "/" + StringUtil::Lower(provider),
	                                                     EXTENSION_SECRET_PROVIDERS);
	if (!entry.empty() && db) {
		string error_message = was_default ? "Default secret provider" : "Secret provider";
		error_message +=
		    " '" + provider + "' for type '" + type + "' does not exist, but it exists in the " + entry + " extension.";
		error_message = ExtensionHelper::AddExtensionInstallHintToErrorMsg(*db, error_message, entry);

		throw InvalidInputException(error_message);
	}
	throw InvalidInputException("Secret provider '%s' not found for type '%s'", provider, type);
}

optional_ptr<SecretStorage> SecretManager::GetSecretStorage(const string &name) {
	lock_guard<mutex> lock(manager_lock);

	auto lookup = secret_storages.find(name);
	if (lookup != secret_storages.end()) {
		return lookup->second.get();
	}

	return nullptr;
}

vector<reference<SecretStorage>> SecretManager::GetSecretStorages() {
	lock_guard<mutex> lock(manager_lock);

	vector<reference<SecretStorage>> result;

	for (const auto &storage : secret_storages) {
		result.push_back(*storage.second);
	}

	return result;
}

DefaultSecretGenerator::DefaultSecretGenerator(Catalog &catalog, SecretManager &secret_manager,
                                               case_insensitive_set_t &persistent_secrets)
    : DefaultGenerator(catalog), secret_manager(secret_manager), persistent_secrets(persistent_secrets) {
}

unique_ptr<CatalogEntry> DefaultSecretGenerator::CreateDefaultEntryInternal(const string &entry_name) {
	lock_guard<mutex> guard(lock);
	auto secret_lu = persistent_secrets.find(entry_name);
	if (secret_lu == persistent_secrets.end()) {
		return nullptr;
	}

	LocalFileSystem fs;

	string base_secret_path = secret_manager.PersistentSecretPath();
	string secret_path = fs.JoinPath(base_secret_path, entry_name + ".duckdb_secret");

	// Note each file should contain 1 secret
	try {
		auto file_reader = BufferedFileReader(fs, secret_path.c_str());

		if (!LocalFileSystem::IsPrivateFile(secret_path, nullptr)) {
			throw IOException(
			    "The secret file '%s' has incorrect permissions! Please set correct permissions or remove file",
			    secret_path);
		}

		if (!file_reader.Finished()) {
			BinaryDeserializer deserializer(file_reader);

			deserializer.Begin();
			auto deserialized_secret = secret_manager.DeserializeSecret(deserializer, secret_path);
			deserializer.End();

			auto entry = make_uniq<SecretCatalogEntry>(std::move(deserialized_secret), catalog);
			entry->secret->storage_mode = SecretManager::LOCAL_FILE_STORAGE_NAME;
			entry->secret->persist_type = SecretPersistType::PERSISTENT;

			// Finally: we remove the default entry from the persistent_secrets, otherwise we aren't able to drop it
			// later
			persistent_secrets.erase(secret_lu);

			return std::move(entry);
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		switch (error.Type()) {
		case ExceptionType::SERIALIZATION:
			throw SerializationException(
			    "Failed to deserialize the persistent secret file: '%s'. The file maybe be "
			    "corrupt, please remove the file, restart and try again. (error message: '%s')",
			    secret_path, error.RawMessage());
		case ExceptionType::IO:
			throw IOException(
			    "Failed to open the persistent secret file: '%s'. Some other process may have removed it, "
			    "please restart and try again. (error message: '%s')",
			    secret_path, error.RawMessage());
		default:
			throw;
		}
	}

	throw SerializationException("Failed to deserialize secret '%s' from '%s': file appears empty! Please remove the "
	                             "file, restart and try again",
	                             entry_name, secret_path);
}

unique_ptr<CatalogEntry> DefaultSecretGenerator::CreateDefaultEntry(CatalogTransaction transaction,
                                                                    const string &entry_name) {
	return CreateDefaultEntryInternal(entry_name);
}

unique_ptr<CatalogEntry> DefaultSecretGenerator::CreateDefaultEntry(ClientContext &context, const string &entry_name) {
	return CreateDefaultEntryInternal(entry_name);
}

vector<string> DefaultSecretGenerator::GetDefaultEntries() {
	vector<string> ret;

	lock_guard<mutex> guard(lock);
	for (const auto &res : persistent_secrets) {
		ret.push_back(res);
	}

	return ret;
}

SecretManager &SecretManager::Get(ClientContext &context) {
	return *DBConfig::GetConfig(context).secret_manager;
}
SecretManager &SecretManager::Get(DatabaseInstance &db) {
	return *DBConfig::GetConfig(db).secret_manager;
}

void SecretManager::DropSecretByName(ClientContext &context, const string &name, OnEntryNotFound on_entry_not_found,
                                     SecretPersistType persist_type, const string &storage) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	return DropSecretByName(transaction, name, on_entry_not_found, persist_type, storage);
}

} // namespace duckdb
