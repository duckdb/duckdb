#include "duckdb/main/secret/secret_manager.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/file_system.hpp"
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
#include "duckdb/parser/statement/create_secret_statement.hpp"
#include "duckdb/planner/operator/logical_create_secret.hpp"

namespace duckdb {

void DuckSecretManager::Initialize(DatabaseInstance &db) {
	if (registered_secrets) {
		throw InternalException("SecretManager can only be initialized once!");
	}
	registered_secrets = make_uniq<CatalogSet>(Catalog::GetSystemCatalog(db));
}

unique_ptr<BaseSecret> DuckSecretManager::DeserializeSecret(CatalogTransaction transaction,
                                                            Deserializer &deserializer) {
	lock_guard<mutex> lck(lock);
	return DeserializeSecretInternal(deserializer);
}

unique_ptr<BaseSecret> DuckSecretManager::DeserializeSecretInternal(Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<string>(100, "type");
	auto provider = deserializer.ReadProperty<string>(101, "provider");
	auto name = deserializer.ReadProperty<string>(102, "name");
	vector<string> scope;
	deserializer.ReadList(103, "scope",
	                      [&](Deserializer::List &list, idx_t i) { scope.push_back(list.ReadElement<string>()); });

	auto secret_type = LookupTypeInternal(type);

	if (!secret_type.deserializer) {
		throw InternalException(
		    "Attempted to deserialize secret type '%s' which does not have a deserialization method", type);
	}

	return secret_type.deserializer(deserializer, {scope, type, provider, name});
}

void DuckSecretManager::RegisterSecretType(SecretType &type) {
	lock_guard<mutex> lck(lock);

	if (registered_types.find(type.name) != registered_types.end()) {
		throw InternalException("Attempted to register an already registered secret type: '%s'", type.name);
	}

	registered_types[type.name] = type;
}

void DuckSecretManager::RegisterSecretFunction(CreateSecretFunction function, OnCreateConflict on_conflict) {
	lock_guard<mutex> lck(lock);

	const auto &lu = create_secret_functions.find(function.secret_type);
	if (lu != create_secret_functions.end()) {
		auto &functions_for_type = lu->second;
		functions_for_type.AddFunction(function, on_conflict);
	}

	CreateSecretFunctionSet new_set(function.secret_type);
	new_set.AddFunction(function, OnCreateConflict::ERROR_ON_CONFLICT);
	create_secret_functions.insert(std::make_pair(function.secret_type, std::move(new_set)));
}

optional_ptr<SecretEntry> DuckSecretManager::RegisterSecret(CatalogTransaction transaction,
                                                            unique_ptr<const BaseSecret> secret,
                                                            OnCreateConflict on_conflict,
                                                            SecretPersistMode persist_mode) {
	lock_guard<mutex> lck(lock);
	return RegisterSecretInternal(transaction, std::move(secret), on_conflict, persist_mode);
}

optional_ptr<SecretEntry> DuckSecretManager::RegisterSecretInternal(CatalogTransaction transaction,
                                                                    unique_ptr<const BaseSecret> secret,
                                                                    OnCreateConflict on_conflict,
                                                                    SecretPersistMode persist_mode) {
	bool conflict = false;

	//! Ensure we only create secrets for known types;
	LookupTypeInternal(secret->GetType());

	if (registered_secrets->GetEntry(transaction, secret->GetName())) {
		conflict = true;
	}

	// Assert the secret does not exist as a persistent secret either
	if (permanent_secret_files.find(secret->GetName()) != permanent_secret_files.end()) {
		conflict = true;
	}

	bool persist = persist_mode == SecretPersistMode::PERMANENT;

	string storage_str;
	if (persist_mode == SecretPersistMode::PERMANENT) {
		storage_str = GetSecretDirectory(transaction.db->config);
	} else {
		storage_str = "in-memory";
	};

	if (conflict) {
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw InvalidInputException("Secret with name '" + secret->GetName() + "' already exists!");
		} else if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		} else if (on_conflict == OnCreateConflict::ALTER_ON_CONFLICT) {
			throw InternalException("unknown OnCreateConflict found while registering secret");
		} else if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
			registered_secrets->DropEntry(transaction, secret->GetName(), true, true);
		}
	}

	if (persist_mode == SecretPersistMode::PERMANENT) {
		WriteSecretToFile(transaction, *secret);
	}

	// Creating entry
	auto secret_name = secret->GetName();
	auto secret_entry =
	    make_uniq<SecretEntry>(std::move(secret), Catalog::GetSystemCatalog(*transaction.db), secret->GetName());
	secret_entry->temporary = !persist;
	secret_entry->storage_mode = storage_str;
	DependencyList l;
	registered_secrets->CreateEntry(transaction, secret_name, std::move(secret_entry), l);

	return &registered_secrets->GetEntry(transaction, secret_name)->Cast<SecretEntry>();
}

CreateSecretFunction *DuckSecretManager::LookupFunctionInternal(const string &type, const string &provider) {
	const auto &lookup = create_secret_functions.find(type);
	if (lookup == create_secret_functions.end()) {
		return nullptr;
	}

	if (!lookup->second.ProviderExists(provider)) {
		return nullptr;
	}

	return &lookup->second.GetFunction(provider);
}

optional_ptr<SecretEntry> DuckSecretManager::CreateSecret(ClientContext &context, const CreateSecretInfo &info) {
	lock_guard<mutex> lck(lock);

	// We're creating a secret: therefore we need to refresh our view on the permanent secrets to ensure there are no
	// name collisions
	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
	SyncPermanentSecrets(transaction);

	// Make a copy to set the provider to default if necessary
	CreateSecretInput function_input {info.type, info.provider, info.persist_mode,
	                                  info.name, info.scope,    info.named_parameters};
	if (function_input.provider.empty()) {
		auto secret_type = LookupTypeInternal(function_input.type);
		function_input.provider = secret_type.default_provider;
	}

	// Lookup function
	auto function_lookup = LookupFunctionInternal(function_input.type, function_input.provider);
	if (!function_lookup) {
		throw InvalidInputException("Could not find CreateSecretFunction for type: '%s' and provider: '%s'", info.type,
		                            info.provider);
	}

	// Ensure the secret doesn't collide with any of the lazy loaded persistent secrets by first lazy loading the
	// requested secret
	if (!permanent_secret_files.empty()) {
		const auto &lu = permanent_secret_files.find(info.name);
		if (lu != permanent_secret_files.end()) {
			LoadSecretFromPreloaded(transaction, lu->first);
		}
	}

	// TODO probably don't hold lock while running this
	// Call the function
	auto secret = function_lookup->function(context, function_input);

	if (!secret) {
		throw InternalException("CreateSecretFunction for type: '%s' and provider: '%s' did not return a secret!",
		                        info.type, info.provider);
	}

	// Register the secret at the secret_manager
	return RegisterSecretInternal(transaction, std::move(secret), info.on_conflict, info.persist_mode);
}

BoundStatement DuckSecretManager::BindCreateSecret(CreateSecretStatement &stmt) {
	lock_guard<mutex> lck(lock);

	auto type = stmt.info->type;
	auto provider = stmt.info->provider;
	bool default_provider = false;

	if (provider.empty()) {
		default_provider = true;
		auto secret_type = LookupTypeInternal(type);
		provider = secret_type.default_provider;
	}

	string default_string = default_provider ? "default " : "";

	auto function = LookupFunctionInternal(type, provider);

	if (!function) {
		throw BinderException("Could not find create secret function for secret type '%s' with %sprovider '%s'", type,
		                      default_string, provider);
	}

	for (const auto &param : stmt.info->named_parameters) {
		if (function->named_parameters.find(param.first) == function->named_parameters.end()) {
			throw BinderException("Unknown parameter '%s' for secret type '%s' with %sprovider '%s'", param.first, type,
			                      default_string, provider);
		}
	}

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_uniq<LogicalCreateSecret>(*function, *stmt.info);
	return result;
}

optional_ptr<SecretEntry> DuckSecretManager::GetSecretByPath(CatalogTransaction transaction, const string &path,
                                                             const string &type) {
	lock_guard<mutex> lck(lock);

	// Synchronize the permanent secrets
	SyncPermanentSecrets(transaction);

	// We need to fully load all permanent secrets now: we will be searching and comparing their types and scopes
	LoadPreloadedSecrets(transaction);

	int best_match_score = -1;
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
	registered_secrets->Scan(transaction, callback);

	if (best_match) {
		return best_match;
	}

	return nullptr;
}

optional_ptr<SecretEntry> DuckSecretManager::GetSecretByName(CatalogTransaction transaction, const string &name) {
	lock_guard<mutex> lck(lock);

	//! Synchronize the permanent secrets to ensure we have an up to date view on them
	SyncPermanentSecrets(transaction);

	//! If the secret in in our lazy preload map, this is the time we must read and deserialize it.
	if (permanent_secret_files.find(name) != permanent_secret_files.end()) {
		LoadSecretFromPreloaded(transaction, name);
	}

	auto res = registered_secrets->GetEntry(transaction, name);

	if (res) {
		auto &cast_entry = res->Cast<SecretEntry>();
		return &cast_entry;
	}

	throw InternalException("GetSecretByName called on unknown secret: %s", name);
}

void DuckSecretManager::DropSecretByName(CatalogTransaction transaction, const string &name, bool missing_ok) {
	lock_guard<mutex> lck(lock);

	bool deleted = false;
	bool was_persistent = false;

	auto entry = registered_secrets->GetEntry(transaction, name);
	if (!entry && !missing_ok) {
		throw InvalidInputException("Failed to remove non-existent secret with name '%s'", name);
	}

	const auto &cast_entry = entry->Cast<SecretEntry>();
	was_persistent = !cast_entry.temporary;
	deleted = registered_secrets->DropEntry(transaction, name, true, true);

	// For deleting persistent secrets, we need to do a sync to ensure we have the path in permanent_secret_files
	if (was_persistent) {
		SyncPermanentSecrets(transaction, true);
	}

	if (!deleted || was_persistent) {
		auto &fs = *transaction.db->config.file_system;
		auto file_lu = permanent_secret_files.find(name);

		if (file_lu != permanent_secret_files.end()) {
			deleted = true;
			fs.RemoveFile(file_lu->second);
			permanent_secret_files.erase(file_lu);
		}
	}

	if (!deleted && !missing_ok) {
		throw InvalidInputException("Failed to remove non-existent secret with name '%s'", name);
	}
}

SecretType DuckSecretManager::LookupType(const string &type) {
	lock_guard<mutex> lck(lock);
	return LookupTypeInternal(type);
}

SecretType DuckSecretManager::LookupTypeInternal(const string &type) {
	auto lu = registered_types.find(type);

	if (lu == registered_types.end()) {
		throw InvalidInputException("Secret type '%s' not found", type);
	}

	return lu->second;
}

vector<SecretEntry *> DuckSecretManager::AllSecrets(CatalogTransaction transaction) {
	SyncPermanentSecrets(transaction);
	LoadPreloadedSecrets(transaction);

	vector<SecretEntry *> ret_value;

	const std::function<void(CatalogEntry &)> callback = [&](CatalogEntry &entry) {
		auto &cast_entry = entry.Cast<SecretEntry>();
		ret_value.push_back(&cast_entry);
	};
	registered_secrets->Scan(transaction, callback);

	return ret_value;
}

bool DuckSecretManager::AllowConfigChanges() {
	return initialized_fs;
};

// TODO: switch to single file for secrets
void DuckSecretManager::WriteSecretToFile(CatalogTransaction transaction, const BaseSecret &secret) {
	auto &fs = *transaction.db->config.file_system;
	auto secret_dir = GetSecretDirectory(transaction.db->config);
	auto file_path = fs.JoinPath(secret_dir, secret.GetName() + ".duckdb_secret");

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

void DuckSecretManager::PreloadPermanentSecrets(CatalogTransaction transaction) {
	auto &fs = *transaction.db->config.file_system;
	auto secret_dir = GetSecretDirectory(transaction.db->config);
	fs.ListFiles(secret_dir, [&](const string &fname, bool is_dir) {
		string full_path = fs.JoinPath(secret_dir, fname);

		if (StringUtil::EndsWith(full_path, ".duckdb_secret")) {
			string secret_name = fname.substr(0, fname.size() - 14); // size of file ext

			if (registered_secrets->GetEntry(transaction, fname)) {
				throw IOException("Trying to preload permanent secrets '%s' which has a name that already exists!",
				                  full_path);
			}

			permanent_secret_files[secret_name] = full_path;
		}
	});
}

void DuckSecretManager::LoadPreloadedSecrets(CatalogTransaction transaction) {
	vector<string> to_load;

	// Find the permanent secrets that need to be loaded
	for (auto &it : permanent_secret_files) {
		bool found = false;

		if (registered_secrets->GetEntry(transaction, it.first)) {
			found = true;
		}

		if (!found) {
			to_load.push_back(it.second);
		}
	}
	permanent_secret_files.clear();

	for (const auto &path : to_load) {
		LoadSecret(transaction, path, SecretPersistMode::PERMANENT);
	}
}

void DuckSecretManager::LoadSecret(CatalogTransaction transaction, const string &path, SecretPersistMode persist_mode) {
	auto &fs = *transaction.db->config.file_system;
	auto file_reader = BufferedFileReader(fs, path.c_str());
	while (!file_reader.Finished()) {
		BinaryDeserializer deserializer(file_reader);
		deserializer.Begin();
		auto deserialized_secret = DeserializeSecretInternal(deserializer);
		RegisterSecretInternal(transaction, std::move(deserialized_secret), OnCreateConflict::ERROR_ON_CONFLICT,
		                       persist_mode);
		deserializer.End();
	}
}

void DuckSecretManager::LoadSecretFromPreloaded(CatalogTransaction transaction, const string &name) {
	auto path = permanent_secret_files[name];
	permanent_secret_files.erase(name);
	LoadSecret(transaction, path, SecretPersistMode::PERMANENT);
}

void DuckSecretManager::SyncPermanentSecrets(CatalogTransaction transaction, bool force) {
	auto permanent_secrets_enabled = transaction.db->config.options.allow_permanent_secrets;

	if (force || !permanent_secrets_enabled || !initialized_fs) {

		vector<string> delete_list;
		const std::function<void(CatalogEntry &)> callback = [&](CatalogEntry &entry) {
			const auto &cast_entry = entry.Cast<SecretEntry>();
			if (!cast_entry.temporary) {
				delete_list.push_back(cast_entry.secret->GetName());
			}
		};
		registered_secrets->Scan(transaction, callback);

		for (const auto &name : delete_list) {
			registered_secrets->DropEntry(transaction, name, true, true);
		}

		permanent_secret_files.clear();

		if (permanent_secrets_enabled) {
			DuckSecretManager::PreloadPermanentSecrets(transaction);
		}
	}

	initialized_fs = true;
}

string DuckSecretManager::GetSecretDirectory(DBConfig &config) {
	string directory = config.options.secret_directory;
	auto &fs = *config.file_system;

	if (directory.empty()) {
		directory = fs.GetHomeDirectory();
		if (!fs.DirectoryExists(directory)) {
			throw IOException(
			    "Can't find the home directory for storing permanent secrets. Either specify a secret directory "
			    "using SET secret_directory='/path/to/dir' or specify a home directory using the SET "
			    "home_directory='/path/to/dir' option.",
			    directory);
		}

		// TODO: make secret version num instead, add a test that reads same as storage info test
		vector<string> path_components = {".duckdb", "stored_secrets", ExtensionHelper::GetVersionDirectoryName()};
		for (auto &path_ele : path_components) {
			directory = fs.JoinPath(directory, path_ele);
			if (!fs.DirectoryExists(directory)) {
				fs.CreateDirectory(directory);
			}
		}
	} else if (!fs.DirectoryExists(directory)) {
		// This is mostly for our unittest to easily allow making unique directories for test secret storage
		fs.CreateDirectory(directory);
	}

	if (fs.IsRemoteFile(directory)) {
		throw InternalException("Cannot set the secret_directory to a remote filesystem: '%s'", directory);
	}

	return directory;
}

} // namespace duckdb
