#include "duckdb/main/secret/duck_secret_manager.hpp"

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
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/operator/logical_create_secret.hpp"

namespace duckdb {

void DuckSecretManager::Initialize(DatabaseInstance &db) {
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
}

unique_ptr<BaseSecret> DuckSecretManager::DeserializeSecret(CatalogTransaction transaction,
                                                            Deserializer &deserializer) {
	InitializeSecrets(transaction);
	return DeserializeSecretInternal(transaction, deserializer);
}

unique_ptr<BaseSecret> DuckSecretManager::DeserializeSecretInternal(CatalogTransaction transaction,
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

void DuckSecretManager::RegisterSecretType(CatalogTransaction transaction, SecretType &type) {
	auto &catalog = Catalog::GetSystemCatalog(*transaction.db);
	auto entry = make_uniq<SecretTypeEntry>(catalog, type);
	DependencyList l;
	auto res = secret_types->CreateEntry(transaction, type.name, std::move(entry), l);
	if (!res) {
		throw InternalException("Attempted to register an already registered secret type: '%s'", type.name);
	}
}

void DuckSecretManager::RegisterSecretFunction(CatalogTransaction transaction, CreateSecretFunction function,
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

optional_ptr<SecretEntry> DuckSecretManager::RegisterSecret(CatalogTransaction transaction,
                                                            unique_ptr<const BaseSecret> secret,
                                                            OnCreateConflict on_conflict,
                                                            SecretPersistMode persist_mode) {
	InitializeSecrets(transaction);
	return RegisterSecretInternal(transaction, std::move(secret), on_conflict, persist_mode);
}

optional_ptr<SecretEntry> DuckSecretManager::RegisterSecretInternal(CatalogTransaction transaction,
                                                                    unique_ptr<const BaseSecret> secret,
                                                                    OnCreateConflict on_conflict,
                                                                    SecretPersistMode persist_mode) {
	//! Ensure we only create secrets for known types;
	LookupTypeInternal(transaction, secret->GetType());

	bool persist = persist_mode == SecretPersistMode::PERMANENT;

	string storage_str;
	if (persist_mode == SecretPersistMode::PERMANENT) {
		storage_str = config.secret_path;
	} else {
		storage_str = ":memory:";
	}

	if (secrets->GetEntry(transaction, secret->GetName())) {
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw InvalidInputException("Secret with name '" + secret->GetName() + "' already exists!");
		} else if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		} else if (on_conflict == OnCreateConflict::ALTER_ON_CONFLICT) {
			throw InternalException("unknown OnCreateConflict found while registering secret");
		} else if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
			secrets->DropEntry(transaction, secret->GetName(), true, true);
		}
	}

	if (persist_mode == SecretPersistMode::PERMANENT) {
		WriteSecretToFile(transaction, *secret);
	}

	// Creating entry
	auto secret_name = secret->GetName();
	auto secret_entry =
	    make_uniq<SecretEntry>(std::move(secret), Catalog::GetSystemCatalog(*transaction.db), secret_name);
	secret_entry->temporary = !persist;
	secret_entry->storage_mode = storage_str;
	DependencyList l;
	secrets->CreateEntry(transaction, secret_name, std::move(secret_entry), l);

	return &secrets->GetEntry(transaction, secret_name)->Cast<SecretEntry>();
}

optional_ptr<CreateSecretFunction>
DuckSecretManager::LookupFunctionInternal(CatalogTransaction transaction, const string &type, const string &provider) {
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

optional_ptr<SecretEntry> DuckSecretManager::CreateSecret(ClientContext &context, const CreateSecretInfo &info) {
	// Note that a context is required for CreateSecret, as the CreateSecretFunction expects one
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	InitializeSecrets(transaction);

	// Make a copy to set the provider to default if necessary
	CreateSecretInput function_input {info.type, info.provider, info.persist_mode, info.name, info.scope, info.options};
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
	return RegisterSecretInternal(transaction, std::move(secret), info.on_conflict, info.persist_mode);
}

BoundStatement DuckSecretManager::BindCreateSecret(CatalogTransaction transaction, CreateSecretInfo &info) {
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

optional_ptr<SecretEntry> DuckSecretManager::GetSecretByPath(CatalogTransaction transaction, const string &path,
                                                             const string &type) {
	InitializeSecrets(transaction);

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
		return best_match;
	}

	return nullptr;
}

optional_ptr<SecretEntry> DuckSecretManager::GetSecretByName(CatalogTransaction transaction, const string &name) {
	InitializeSecrets(transaction);

	auto res = secrets->GetEntry(transaction, name);

	if (res) {
		auto &cast_entry = res->Cast<SecretEntry>();
		return &cast_entry;
	}

	throw InternalException("GetSecretByName called on unknown secret: %s", name);
}

void DuckSecretManager::DropSecretByName(CatalogTransaction transaction, const string &name, OnEntryNotFound on_entry_not_found) {
	InitializeSecrets(transaction);

	bool deleted;
	bool was_persistent;

	auto entry = secrets->GetEntry(transaction, name);
	if (!entry && on_entry_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw InvalidInputException("Failed to remove non-existent secret with name '%s'", name);
	}

	const auto &cast_entry = entry->Cast<SecretEntry>();
	was_persistent = !cast_entry.temporary;
	deleted = secrets->DropEntry(transaction, name, true, true);

	if (!deleted || was_persistent) {
		LocalFileSystem fs;
		deleted = true;
		string file = fs.JoinPath(config.secret_path, name + ".duckdb_secret");
		permanent_secrets.erase(name);
		try {
			fs.RemoveFile(file);
		} catch (IOException &e) {
			throw IOException("Failed to remove secret file '%s', the file may have been removed by another duckdb "
							  "instance. (original error: '%s')",
							  file, e.RawMessage());
		}
	}

	if (!deleted && on_entry_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw InvalidInputException("Failed to remove non-existent secret with name '%s'", name);
	}
}

SecretType DuckSecretManager::LookupType(CatalogTransaction transaction, const string &type) {
	return LookupTypeInternal(transaction, type);
}

SecretType DuckSecretManager::LookupTypeInternal(CatalogTransaction transaction, const string &type) {

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

vector<reference<SecretEntry>> DuckSecretManager::AllSecrets(CatalogTransaction transaction) {
	InitializeSecrets(transaction);

	vector<reference<SecretEntry>> ret_value;
	const std::function<void(CatalogEntry &)> callback = [&](CatalogEntry &entry) {
		auto &cast_entry = entry.Cast<SecretEntry>();
		ret_value.push_back(cast_entry);
	};
	secrets->Scan(transaction, callback);

	return ret_value;
}

void DuckSecretManager::ThrowOnSettingChangeIfInitialized() {
	if (initialized) {
		throw InvalidInputException(
		    "Changing Secret Manager settings after the secret manager is used is not allowed!");
	}
}

void DuckSecretManager::SetEnablePermanentSecrets(bool enabled) {
	ThrowOnSettingChangeIfInitialized();
	config.allow_permanent_secrets = enabled;
}

void DuckSecretManager::ResetEnablePermanentSecrets() {
	ThrowOnSettingChangeIfInitialized();
	config.allow_permanent_secrets = DuckSecretManagerConfig::DEFAULT_ALLOW_PERMANENT_SECRETS;
}

bool DuckSecretManager::PermanentSecretsEnabled() {
	return config.allow_permanent_secrets;
}

void DuckSecretManager::SetPermanentSecretPath(const string &path) {
	ThrowOnSettingChangeIfInitialized();
	config.secret_path = path;
}

void DuckSecretManager::ResetPermanentSecretPath() {
	ThrowOnSettingChangeIfInitialized();
	config.secret_path = config.default_secret_path;
}

string DuckSecretManager::PermanentSecretPath() {
	return config.secret_path;
}

void DuckSecretManager::WriteSecretToFile(CatalogTransaction transaction, const BaseSecret &secret) {
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

void DuckSecretManager::InitializeSecrets(CatalogTransaction transaction) {
	if (!initialized) {
		lock_guard<mutex> lck(initialize_lock);
		if (initialized) {
			// some sneaky other thread beat us to it
			return;
		}

		if (config.allow_permanent_secrets) {
			DuckSecretManager::LoadPermanentSecretsMap(transaction);
		}

		auto &catalog = Catalog::GetSystemCatalog(*transaction.db);
		secrets =
		    make_uniq<CatalogSet>(catalog, make_uniq<DefaultDuckSecretGenerator>(catalog, *this, permanent_secrets));
		initialized = true;
	}
}

void DuckSecretManager::LoadPermanentSecretsMap(CatalogTransaction transaction) {
	LocalFileSystem fs;

	auto secret_dir = config.secret_path;

	if (!fs.DirectoryExists(secret_dir)) {
		fs.CreateDirectory(secret_dir);
	}

	if (permanent_secrets.empty()) {
		fs.ListFiles(secret_dir, [&](const string &fname, bool is_dir) {
			string full_path = fs.JoinPath(secret_dir, fname);

			if (StringUtil::EndsWith(full_path, ".duckdb_secret")) {
				string secret_name = fname.substr(0, fname.size() - 14); // size of file ext
				permanent_secrets.insert(secret_name);
			}
		});
	}
}

void DuckSecretManager::AutoloadExtensionForType(ClientContext &context, const string &type) {
	ExtensionHelper::TryAutoloadFromEntry(context, type, EXTENSION_SECRET_TYPES);
}

void DuckSecretManager::AutoloadExtensionForFunction(ClientContext &context, const string &type,
                                                     const string &provider) {
	ExtensionHelper::TryAutoloadFromEntry(context, type + "/" + provider, EXTENSION_SECRET_PROVIDERS);
}

DefaultDuckSecretGenerator::DefaultDuckSecretGenerator(Catalog &catalog, DuckSecretManager &secret_manager,
                                                       case_insensitive_set_t &permanent_secrets)
    : DefaultGenerator(catalog), secret_manager(secret_manager), permanent_secrets(permanent_secrets) {
}

unique_ptr<CatalogEntry> DefaultDuckSecretGenerator::CreateDefaultEntry(ClientContext &context,
                                                                        const string &entry_name) {

	auto secret_lu = permanent_secrets.find(entry_name);
	if (secret_lu == permanent_secrets.end()) {
		return nullptr;
	}

	LocalFileSystem fs;
	auto &catalog = Catalog::GetSystemCatalog(context);

	string base_secret_path = secret_manager.PermanentSecretPath();
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
			entry->storage_mode = base_secret_path;

			return std::move(entry);
		}
	} catch (SerializationException &e) {
		throw SerializationException("Failed to deserialize the permanent secret file: '%s'. The file maybe be "
		                             "corrupt, please remove the file, restart and try again. (error message: '%s')",
		                             secret_path, e.RawMessage());
	} catch (IOException &e) {
		throw IOException("Failed to open the permanent secret file: '%s'. Some other process may have removed it, "
		                  "please restart and try again. (error message: '%s')",
		                  secret_path, e.RawMessage());
	}

	throw SerializationException("Failed to deserialize secret '%s' from '%s': file appears empty! Please remove the "
	                             "file, restart and try again",
	                             entry_name, secret_path);
}

vector<string> DefaultDuckSecretGenerator::GetDefaultEntries() {
	vector<string> ret;

	for (const auto &res : permanent_secrets) {
		ret.push_back(res);
	}

	return ret;
}

} // namespace duckdb
