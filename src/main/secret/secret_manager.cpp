#include "duckdb/catalog/catalog_entry/secret_function_entry.hpp"
#include "duckdb/catalog/catalog_entry/secret_type_entry.hpp"
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
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/operator/logical_create_secret.hpp"

namespace duckdb {

optional_ptr<SecretEntry> CatalogSetSecretStorage::StoreSecret(CatalogTransaction transaction,
                                                               unique_ptr<const BaseSecret> secret,
                                                               OnCreateConflict on_conflict) {
	if (secrets->GetEntry(transaction, secret->GetName())) {
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			string persist_string = persistent ? "Persistent" : "Temporary";
			string storage_string = persistent ? " in secret storage '" + storage_name + "'" : "";
			throw InvalidInputException("%s secret with name '%s' already exists%s!", persist_string, secret->GetName(),
			                            storage_string);
		} else if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		} else if (on_conflict == OnCreateConflict::ALTER_ON_CONFLICT) {
			throw InternalException("unknown OnCreateConflict found while registering secret");
		} else if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
			secrets->DropEntry(transaction, secret->GetName(), true, true);
		}
	}

	// Call write function
	WriteSecret(transaction, *secret);

	auto secret_name = secret->GetName();
	auto secret_entry =
	    make_uniq<SecretEntry>(std::move(secret), Catalog::GetSystemCatalog(*transaction.db), secret_name);
	secret_entry->temporary = !persistent;
	secret_entry->storage_mode = storage_name;
	secret_entry->persist_type = persistent ? SecretPersistType::PERSISTENT : SecretPersistType::TEMPORARY;
	DependencyList l;
	secrets->CreateEntry(transaction, secret_name, std::move(secret_entry), l);
	return &secrets->GetEntry(transaction, secret_name)->Cast<SecretEntry>();
}

vector<reference<SecretEntry>> CatalogSetSecretStorage::AllSecrets(CatalogTransaction transaction) {
	vector<reference<SecretEntry>> ret_value;
	const std::function<void(CatalogEntry &)> callback = [&](CatalogEntry &entry) {
		auto &cast_entry = entry.Cast<SecretEntry>();
		ret_value.push_back(cast_entry);
	};
	secrets->Scan(transaction, callback);
	return ret_value;
}

void CatalogSetSecretStorage::DropSecretByName(CatalogTransaction transaction, const string &name,
                                               OnEntryNotFound on_entry_not_found) {
	auto entry = secrets->GetEntry(transaction, name);
	if (!entry && on_entry_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		string persist_string = persistent ? "persistent" : "temporary";
		string storage_string = persistent ? " in secret storage '" + storage_name + "'" : "";
		throw InvalidInputException("Failed to remove non-existent %s secret '%s'%s", persist_string, name,
		                            storage_string);
	}

	secrets->DropEntry(transaction, name, true, true);
	RemoveSecret(transaction, name);
}

SecretMatch CatalogSetSecretStorage::GetSecretByPath(CatalogTransaction transaction, const string &path,
                                                     const string &type) {
	int64_t best_match_score = -1;
	SecretEntry *best_match = nullptr;

	const std::function<void(CatalogEntry &)> callback = [&](CatalogEntry &entry) {
		auto &cast_entry = entry.Cast<SecretEntry>();

		if (cast_entry.secret->GetType() == type) {
			auto match = cast_entry.secret->MatchScore(path);

			if (match > best_match_score) {
				best_match_score = match;
				best_match = &cast_entry;
			}
		}
	};
	secrets->Scan(transaction, callback);

	if (best_match) {
		return SecretMatch(*best_match, best_match_score);
	}

	return SecretMatch();
}

optional_ptr<SecretEntry> CatalogSetSecretStorage::GetSecretByName(CatalogTransaction transaction, const string &name) {
	auto res = secrets->GetEntry(transaction, name);

	if (res) {
		auto &cast_entry = res->Cast<SecretEntry>();
		return &cast_entry;
	}

	return nullptr;
}

LocalFileSecretStorage::LocalFileSecretStorage(SecretManager &manager, DatabaseInstance &db, const string &name_p,
                                               const string &secret_path)
    : CatalogSetSecretStorage(name_p), secret_path(secret_path) {
	persistent = true;

	LocalFileSystem fs;

	if (!fs.DirectoryExists(secret_path)) {
		fs.CreateDirectory(secret_path);
	}

	if (persistent_secrets.empty()) {
		fs.ListFiles(secret_path, [&](const string &fname, bool is_dir) {
			string full_path = fs.JoinPath(secret_path, fname);

			if (StringUtil::EndsWith(full_path, ".duckdb_secret")) {
				string secret_name = fname.substr(0, fname.size() - 14); // size of file ext
				persistent_secrets.insert(secret_name);
			}
		});
	}

	auto &catalog = Catalog::GetSystemCatalog(db);
	secrets = make_uniq<CatalogSet>(Catalog::GetSystemCatalog(db),
	                                make_uniq<DefaultSecretGenerator>(catalog, manager, persistent_secrets));
};

void CatalogSetSecretStorage::WriteSecret(CatalogTransaction transaction, const BaseSecret &secret) {
	// By default, this writes nothing
}
void CatalogSetSecretStorage::RemoveSecret(CatalogTransaction transaction, const string &name) {
	// By default, this writes nothing
}

void LocalFileSecretStorage::WriteSecret(CatalogTransaction transaction, const BaseSecret &secret) {
	LocalFileSystem fs;
	auto file_path = fs.JoinPath(secret_path, secret.GetName() + ".duckdb_secret");

	if (fs.FileExists(file_path)) {
		fs.RemoveFile(file_path);
	}

	auto file_writer = BufferedFileWriter(fs, file_path);

	auto serializer = BinarySerializer(file_writer);
	serializer.Begin();
	secret.Serialize(serializer);
	serializer.End();

	file_writer.Flush();
}

void LocalFileSecretStorage::RemoveSecret(CatalogTransaction transaction, const string &secret) {
	LocalFileSystem fs;
	string file = fs.JoinPath(secret_path, secret + ".duckdb_secret");
	persistent_secrets.erase(secret);
	try {
		fs.RemoveFile(file);
	} catch (IOException &e) {
		throw IOException("Failed to remove secret file '%s', the file may have been removed by another duckdb "
		                  "instance. (original error: '%s')",
		                  file, e.RawMessage());
	}
}

constexpr const char *SecretManager::TEMPORARY_STORAGE_NAME;
constexpr const char *SecretManager::LOCAL_FILE_STORAGE_NAME;

void SecretManager::Initialize(DatabaseInstance &db) {
	lock_guard<mutex> lck(initialize_lock);

	auto &catalog = Catalog::GetSystemCatalog(db);
	secret_functions = make_uniq<CatalogSet>(catalog);
	secret_types = make_uniq<CatalogSet>(catalog);

	// Construct default path
	LocalFileSystem fs;
	config.default_secret_path = fs.GetHomeDirectory();
	vector<string> path_components = {".duckdb", "stored_secrets", ExtensionHelper::GetVersionDirectoryName()};
	for (auto &path_ele : path_components) {
		config.default_secret_path = fs.JoinPath(config.default_secret_path, path_ele);
		if (!fs.DirectoryExists(config.default_secret_path)) {
			fs.CreateDirectory(config.default_secret_path);
		}
	}
	config.secret_path = config.default_secret_path;

	// Set the defaults for persistent storage
	config.default_persistent_storage = LOCAL_FILE_STORAGE_NAME;
}

unique_ptr<BaseSecret> SecretManager::DeserializeSecret(CatalogTransaction transaction, Deserializer &deserializer) {
	InitializeSecrets(transaction);
	return DeserializeSecretInternal(transaction, deserializer);
}

// FIXME: use serialization scripts
unique_ptr<BaseSecret> SecretManager::DeserializeSecretInternal(CatalogTransaction transaction,
                                                                Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<string>(100, "type");
	auto provider = deserializer.ReadProperty<string>(101, "provider");
	auto name = deserializer.ReadProperty<string>(102, "name");
	vector<string> scope;
	deserializer.ReadList(103, "scope",
	                      [&](Deserializer::List &list, idx_t i) { scope.push_back(list.ReadElement<string>()); });

	auto secret_type = LookupTypeInternal(transaction, type);

	if (!secret_type.deserializer) {
		throw InternalException(
		    "Attempted to deserialize secret type '%s' which does not have a deserialization method", type);
	}

	return secret_type.deserializer(deserializer, {scope, type, provider, name});
}

void SecretManager::RegisterSecretType(CatalogTransaction transaction, SecretType &type) {
	auto &catalog = Catalog::GetSystemCatalog(*transaction.db);
	auto entry = make_uniq<SecretTypeEntry>(catalog, type);
	DependencyList l;
	auto res = secret_types->CreateEntry(transaction, type.name, std::move(entry), l);
	if (!res) {
		throw InternalException("Attempted to register an already registered secret type: '%s'", type.name);
	}
}

void SecretManager::RegisterSecretFunction(CatalogTransaction transaction, CreateSecretFunction function,
                                           OnCreateConflict on_conflict) {

	auto entry = secret_functions->GetEntry(transaction, function.secret_type);
	if (entry) {
		auto &cast_entry = entry->Cast<CreateSecretFunctionEntry>();
		cast_entry.function_set.AddFunction(function, on_conflict);
	}

	CreateSecretFunctionSet new_set(function.secret_type);
	new_set.AddFunction(function, OnCreateConflict::ERROR_ON_CONFLICT);
	auto &catalog = Catalog::GetSystemCatalog(*transaction.db);
	auto new_entry = make_uniq<CreateSecretFunctionEntry>(catalog, new_set, function.secret_type);
	DependencyList l;
	secret_functions->CreateEntry(transaction, function.secret_type, std::move(new_entry), l);
}

optional_ptr<SecretEntry> SecretManager::RegisterSecret(CatalogTransaction transaction,
                                                        unique_ptr<const BaseSecret> secret,
                                                        OnCreateConflict on_conflict, SecretPersistType persist_type,
                                                        const string &storage) {
	InitializeSecrets(transaction);
	return RegisterSecretInternal(transaction, std::move(secret), on_conflict, persist_type, storage);
}

optional_ptr<SecretEntry> SecretManager::RegisterSecretInternal(CatalogTransaction transaction,
                                                                unique_ptr<const BaseSecret> secret,
                                                                OnCreateConflict on_conflict,
                                                                SecretPersistType persist_type, const string &storage) {
	//! Ensure we only create secrets for known types;
	LookupTypeInternal(transaction, secret->GetType());

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

	if (resolved_storage != TEMPORARY_STORAGE_NAME && persist_type == SecretPersistType::TEMPORARY) {
		throw InvalidInputException("Can not set secret storage for temporary secrets!");
	}

	//! Lookup which backend to store the secret in
	auto backend = storage_backends.find(resolved_storage);
	if (backend != storage_backends.end()) {
		return backend->second->StoreSecret(transaction, std::move(secret), on_conflict);
	}

	if (resolved_storage == LOCAL_FILE_STORAGE_NAME) {
		if (!config.allow_persistent_secrets) {
			throw InvalidInputException("Persistent secrets are currently disabled. To enable them, restart duckdb and "
			                            "run 'SET allow_persistent_secrets=true'");
		} else {
			throw InternalException("The default local file storage for secrets was not found.");
		}
	}

	throw InvalidInputException("Secret storage '%s' not found!", resolved_storage);
}

optional_ptr<CreateSecretFunction> SecretManager::LookupFunctionInternal(CatalogTransaction transaction,
                                                                         const string &type, const string &provider) {
	auto lookup = secret_functions->GetEntry(transaction, type);

	if (lookup) {
		auto &cast_entry = lookup->Cast<CreateSecretFunctionEntry>();
		if (cast_entry.function_set.ProviderExists(provider)) {
			return &cast_entry.function_set.GetFunction(provider);
		}
	}

	// Not found, try autoloading. TODO: with a refactor, we can make this work without a context
	if (transaction.context) {
		AutoloadExtensionForFunction(*transaction.context, type, provider);
		lookup = secret_functions->GetEntry(transaction, type);

		if (lookup) {
			auto &cast_entry = lookup->Cast<CreateSecretFunctionEntry>();
			if (cast_entry.function_set.ProviderExists(provider)) {
				return &cast_entry.function_set.GetFunction(provider);
			}
		}
	}

	return nullptr;
}

optional_ptr<SecretEntry> SecretManager::CreateSecret(ClientContext &context, const CreateSecretInfo &info) {
	// Note that a context is required for CreateSecret, as the CreateSecretFunction expects one
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	InitializeSecrets(transaction);

	// Make a copy to set the provider to default if necessary
	CreateSecretInput function_input {info.type, info.provider, info.storage_type, info.name, info.scope, info.options};
	if (function_input.provider.empty()) {
		auto secret_type = LookupTypeInternal(transaction, function_input.type);
		function_input.provider = secret_type.default_provider;
	}

	// Lookup function
	auto function_lookup = LookupFunctionInternal(transaction, function_input.type, function_input.provider);
	if (!function_lookup) {
		throw InvalidInputException("Could not find CreateSecretFunction for type: '%s' and provider: '%s'", info.type,
		                            info.provider);
	}

	// Call the function
	auto secret = function_lookup->function(context, function_input);

	if (!secret) {
		throw InternalException("CreateSecretFunction for type: '%s' and provider: '%s' did not return a secret!",
		                        info.type, info.provider);
	}

	// Register the secret at the secret_manager
	return RegisterSecretInternal(transaction, std::move(secret), info.on_conflict, info.persist_type,
	                              info.storage_type);
}

BoundStatement SecretManager::BindCreateSecret(CatalogTransaction transaction, CreateSecretInfo &info) {
	InitializeSecrets(transaction);

	auto type = info.type;
	auto provider = info.provider;
	bool default_provider = false;

	if (provider.empty()) {
		default_provider = true;
		auto secret_type = LookupTypeInternal(transaction, type);
		provider = secret_type.default_provider;
	}

	string default_string = default_provider ? "default " : "";

	auto function = LookupFunctionInternal(transaction, type, provider);

	if (!function) {
		throw BinderException("Could not find create secret function for secret type '%s' with %sprovider '%s'", type,
		                      default_string, provider);
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
	result.plan = make_uniq<LogicalCreateSecret>(*function, std::move(bound_info));
	return result;
}

optional_ptr<SecretEntry> SecretManager::GetSecretByPath(CatalogTransaction transaction, const string &path,
                                                         const string &type) {
	InitializeSecrets(transaction);

	int64_t best_match_score = -1;
	SecretEntry *best_match = nullptr;

	for (const auto &backend : storage_backends) {
		if (!backend.second->IncludeInLookups()) {
			continue;
		}
		auto match = backend.second->GetSecretByPath(transaction, path, type);
		if (match.secret && match.score > best_match_score) {
			best_match = match.secret.get();
		}
	}

	if (best_match) {
		return best_match;
	}

	return nullptr;
}

// TODO test this
optional_ptr<SecretEntry> SecretManager::GetSecretByName(CatalogTransaction transaction, const string &name,
                                                         const string &storage) {
	InitializeSecrets(transaction);

	optional_ptr<SecretEntry> result;
	bool found = false;

	if (!storage.empty()) {
		auto storage_lookup = storage_backends.find(storage);

		if (storage_lookup == storage_backends.end()) {
			throw InvalidInputException("Unknown secret storage found: '%s'", storage);
		}

		return storage_lookup->second->GetSecretByName(transaction, name);
	}

	for (const auto &backend : storage_backends) {
		auto lookup = backend.second->GetSecretByName(transaction, name);
		if (lookup) {
			if (found) {
				throw InternalException(
				    "Ambiguity detected for secret name '%s', secret occurs in multiple storage backends.", name);
			}

			result = lookup;
			found = true;
			;
		}
	}

	return result;
}

void SecretManager::DropSecretByName(CatalogTransaction transaction, const string &name,
                                     OnEntryNotFound on_entry_not_found, const string &storage) {
	InitializeSecrets(transaction);

	vector<SecretStorage *> matches;

	// storage to drop from was specified directly
	if (!storage.empty()) {
		auto storage_lookup = storage_backends.find(storage);
		if (storage_lookup == storage_backends.end()) {
			throw InvalidInputException("Unknown storage type found for drop secret: '%s'", storage);
		}
		matches.push_back(storage_lookup->second.get());
	} else {
		for (const auto &backend : storage_backends) {
			auto lookup = backend.second->GetSecretByName(transaction, name);
			if (lookup) {
				matches.push_back(backend.second.get());
			}
		}
	}

	if (matches.size() > 1) {
		throw InvalidInputException(
		    "Ambiguity detected for secret name '%s', secret occurs in multiple secret storages. Please specify which "
		    "secret to drop using: 'DROP <PERSISTENT | LOCAL> SECRET [FROM <secret_storage>]'",
		    name);
	}

	if (matches.empty() && on_entry_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		string storage_str;
		if (!storage.empty()) {
			storage_str = " for storage '" + storage + "'";
		}

		throw InvalidInputException("Failed to remove non-existent secret with name '%s'%s", name, storage_str);
	}

	matches[0]->DropSecretByName(transaction, name, on_entry_not_found);
}

SecretType SecretManager::LookupType(CatalogTransaction transaction, const string &type) {
	return LookupTypeInternal(transaction, type);
}

SecretType SecretManager::LookupTypeInternal(CatalogTransaction transaction, const string &type) {

	auto lookup = secret_types->GetEntry(transaction, type);
	if (!lookup) {
		// Retry with autoload TODO this can work without context
		if (transaction.context) {
			AutoloadExtensionForType(*transaction.context, type);

			lookup = secret_types->GetEntry(transaction, type);
			if (!lookup) {
				throw InvalidInputException("Secret type '%s' not found", type);
			}
		}
	}

	return lookup->Cast<SecretTypeEntry>().type;
}

vector<reference<SecretEntry>> SecretManager::AllSecrets(CatalogTransaction transaction) {
	InitializeSecrets(transaction);

	vector<reference<SecretEntry>> result;

	// Add results from all backends to the result set
	for (const auto &backend : storage_backends) {
		auto backend_result = backend.second->AllSecrets(transaction);
		for (const auto &it : backend_result) {
			result.push_back(it);
		}
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

void SecretManager::SetDefaultStorage(string storage) {
	ThrowOnSettingChangeIfInitialized();
	config.default_persistent_storage = storage;
}

void SecretManager::ResetDefaultStorage() {
	ThrowOnSettingChangeIfInitialized();
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

void SecretManager::WriteSecretToFile(CatalogTransaction transaction, const BaseSecret &secret) {
	LocalFileSystem fs;
	auto file_path = fs.JoinPath(config.secret_path, secret.GetName() + ".duckdb_secret");

	if (fs.FileExists(file_path)) {
		fs.RemoveFile(file_path);
	}

	auto file_writer = BufferedFileWriter(fs, file_path);

	auto serializer = BinarySerializer(file_writer);
	serializer.Begin();
	secret.Serialize(serializer);
	serializer.End();

	file_writer.Flush();
}

void SecretManager::InitializeSecrets(CatalogTransaction transaction) {
	if (!initialized) {
		lock_guard<mutex> lck(initialize_lock);
		if (initialized) {
			// some sneaky other thread beat us to it
			return;
		}

		// load the tmp storage
		storage_backends[TEMPORARY_STORAGE_NAME] =
		    make_uniq<TemporarySecretStorage>(TEMPORARY_STORAGE_NAME, *transaction.db);

		// load the persistent storage if enabled
		if (config.allow_persistent_secrets) {
			storage_backends[LOCAL_FILE_STORAGE_NAME] =
			    make_uniq<LocalFileSecretStorage>(*this, *transaction.db, LOCAL_FILE_STORAGE_NAME, config.secret_path);
			SecretManager::LoadPersistentSecretsMap(transaction);
		}

		initialized = true;
	}
}

void SecretManager::LoadPersistentSecretsMap(CatalogTransaction transaction) {
	LocalFileSystem fs;

	auto secret_dir = config.secret_path;

	if (!fs.DirectoryExists(secret_dir)) {
		fs.CreateDirectory(secret_dir);
	}

	if (persistent_secrets.empty()) {
		fs.ListFiles(secret_dir, [&](const string &fname, bool is_dir) {
			string full_path = fs.JoinPath(secret_dir, fname);

			if (StringUtil::EndsWith(full_path, ".duckdb_secret")) {
				string secret_name = fname.substr(0, fname.size() - 14); // size of file ext
				persistent_secrets.insert(secret_name);
			}
		});
	}
}

void SecretManager::AutoloadExtensionForType(ClientContext &context, const string &type) {
	ExtensionHelper::TryAutoloadFromEntry(context, type, EXTENSION_SECRET_TYPES);
}

void SecretManager::AutoloadExtensionForFunction(ClientContext &context, const string &type, const string &provider) {
	ExtensionHelper::TryAutoloadFromEntry(context, type + "/" + provider, EXTENSION_SECRET_PROVIDERS);
}

DefaultSecretGenerator::DefaultSecretGenerator(Catalog &catalog, SecretManager &secret_manager,
                                               case_insensitive_set_t &persistent_secrets)
    : DefaultGenerator(catalog), secret_manager(secret_manager), persistent_secrets(persistent_secrets) {
}

unique_ptr<CatalogEntry> DefaultSecretGenerator::CreateDefaultEntry(ClientContext &context, const string &entry_name) {

	auto secret_lu = persistent_secrets.find(entry_name);
	if (secret_lu == persistent_secrets.end()) {
		return nullptr;
	}

	LocalFileSystem fs;
	auto &catalog = Catalog::GetSystemCatalog(context);

	string base_secret_path = secret_manager.PersistentSecretPath();
	string secret_path = fs.JoinPath(base_secret_path, entry_name + ".duckdb_secret");

	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);

	// Note each file should contain 1 secret
	try {
		auto file_reader = BufferedFileReader(fs, secret_path.c_str());
		if (!file_reader.Finished()) {
			BinaryDeserializer deserializer(file_reader);

			deserializer.Begin();
			auto deserialized_secret = secret_manager.DeserializeSecret(transaction, deserializer);
			deserializer.End();

			auto name = deserialized_secret->GetName();
			auto entry = make_uniq<SecretEntry>(std::move(deserialized_secret), catalog, name);
			entry->storage_mode = SecretManager::LOCAL_FILE_STORAGE_NAME;
			entry->persist_type = SecretPersistType::PERSISTENT;

			// Finally: we remove the default entry from the persistent_secrets, otherwise we aren't able to drop it
			// later
			persistent_secrets.erase(secret_lu);

			return std::move(entry);
		}
	} catch (SerializationException &e) {
		throw SerializationException("Failed to deserialize the persistent secret file: '%s'. The file maybe be "
		                             "corrupt, please remove the file, restart and try again. (error message: '%s')",
		                             secret_path, e.RawMessage());
	} catch (IOException &e) {
		throw IOException("Failed to open the persistent secret file: '%s'. Some other process may have removed it, "
		                  "please restart and try again. (error message: '%s')",
		                  secret_path, e.RawMessage());
	}

	throw SerializationException("Failed to deserialize secret '%s' from '%s': file appears empty! Please remove the "
	                             "file, restart and try again",
	                             entry_name, secret_path);
}

vector<string> DefaultSecretGenerator::GetDefaultEntries() {
	vector<string> ret;

	for (const auto &res : persistent_secrets) {
		ret.push_back(res);
	}

	return ret;
}

SecretManager &SecretManager::Get(ClientContext &context) {
	return *DBConfig::GetConfig(context).secret_manager;
}

void SecretManager::DropSecretByName(ClientContext &context, const string &name, OnEntryNotFound on_entry_not_found,
                                     const string &storage) {
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	return DropSecretByName(transaction, name, on_entry_not_found, storage);
}

} // namespace duckdb
