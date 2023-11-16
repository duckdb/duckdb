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

DuckSecretManager::DuckSecretManager(DatabaseInstance &instance) : db_instance(instance) {
	SyncPermanentSecrets(true);
}

unique_ptr<BaseSecret> DuckSecretManager::DeserializeSecret(Deserializer &deserializer) {
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

void DuckSecretManager::RegisterSecret(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
                                       SecretPersistMode persist_mode) {
	lock_guard<mutex> lck(lock);
	return RegisterSecretInternal(std::move(secret), on_conflict, persist_mode);
}

void DuckSecretManager::RegisterSecretInternal(shared_ptr<const BaseSecret> secret, OnCreateConflict on_conflict,
                                               SecretPersistMode persist_mode) {
	bool conflict = false;
	idx_t conflict_idx = DConstants::INVALID_INDEX;

	//! Ensure we only create secrets for known types;
	LookupTypeInternal(secret->GetType());

	// Assert the name does not exist already
	for (idx_t cred_idx = 0; cred_idx < registered_secrets.size(); cred_idx++) {
		const auto &cred = registered_secrets[cred_idx];
		if (cred.secret->GetName() == secret->GetName()) {
			conflict = true;
			conflict_idx = cred_idx;
			break;
		}
	}

	// Assert the secret does not exist as a persistent secret either
	if (permanent_secret_files.find(secret->GetName()) != permanent_secret_files.end()) {
		conflict = true;
	}

	bool persist = persist_mode == SecretPersistMode::PERMANENT;

	string storage_str;
	if (persist_mode == SecretPersistMode::PERMANENT) {
		storage_str = GetSecretDirectory();
	} else {
		storage_str = "in-memory";
	};

	if (conflict) {
		if (on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw InvalidInputException("Secret with name '" + secret->GetName() + "' already exists!");
		} else if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return;
		} else if (on_conflict == OnCreateConflict::ALTER_ON_CONFLICT) {
			throw InternalException("unknown OnCreateConflict found while registering secret");
		}
	}

	if (persist_mode == SecretPersistMode::PERMANENT) {
		WriteSecretToFile(*secret);
	}

	RegisteredSecret reg_secret(secret);
	reg_secret.persistent = persist;
	reg_secret.storage_mode = storage_str;

	if (conflict_idx != DConstants::INVALID_INDEX) {
		registered_secrets[conflict_idx] = std::move(reg_secret);
	} else {
		registered_secrets.push_back(std::move(reg_secret));
	}
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

void DuckSecretManager::CreateSecret(ClientContext &context, const CreateSecretInfo &info) {
	lock_guard<mutex> lck(lock);

	// We're creating a secret: therefore we need to refresh our view on the permanent secrets to ensure there are no
	// name collisions
	SyncPermanentSecrets();

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
			LoadSecretFromPreloaded(lu->first);
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
	RegisterSecretInternal(std::move(secret), info.on_conflict, info.persist_mode);
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

RegisteredSecret DuckSecretManager::GetSecretByPath(const string &path, const string &type) {
	lock_guard<mutex> lck(lock);

	// Synchronize the permanent secrets
	SyncPermanentSecrets();

	// We need to fully load all permanent secrets now: we will be searching and comparing their types and scopes
	LoadPreloadedSecrets();

	int best_match_score = -1;
	RegisteredSecret *best_match = nullptr;

	for (auto &secret : registered_secrets) {
		if (secret.secret->GetType() != type) {
			continue;
		}
		auto match = secret.secret->MatchScore(path);

		if (match > best_match_score) {
			best_match_score = MaxValue<idx_t>(match, best_match_score);
			best_match = &secret;
		}
	}

	if (best_match) {
		return *best_match;
	}

	return {nullptr};
}

RegisteredSecret DuckSecretManager::GetSecretByName(const string &name) {
	lock_guard<mutex> lck(lock);

	//! Synchronize the permanent secrets to ensure we have an up to date view on them
	SyncPermanentSecrets();

	//! If the secret in in our lazy preload map, this is the time we must read and deserialize it.
	if (permanent_secret_files.find(name) != permanent_secret_files.end()) {
		LoadSecretFromPreloaded(name);
	}

	for (const auto &reg_secret : registered_secrets) {
		if (reg_secret.secret->GetName() == name) {
			return reg_secret;
		}
	}

	throw InternalException("GetSecretByName called on unknown secret: %s", name);
}

void DuckSecretManager::DropSecretByName(const string &name, bool missing_ok) {
	lock_guard<mutex> lck(lock);

	bool deleted = false;
	bool was_persistent = false;

	std::vector<RegisteredSecret>::iterator iter;
	for (iter = registered_secrets.begin(); iter != registered_secrets.end();) {
		if (iter->secret->GetName() == name) {
			was_persistent = iter->persistent;
			registered_secrets.erase(iter);
			deleted = true;
			break;
		}
		++iter;
	}

	// For deleting persistent secrets, we need to do a sync to ensure we have the path in permanent_secret_files
	if (was_persistent) {
		SyncPermanentSecrets(true);
	}

	if (!deleted || was_persistent) {
		auto &fs = FileSystem::GetFileSystem(db_instance);
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

vector<RegisteredSecret> DuckSecretManager::AllSecrets() {
	SyncPermanentSecrets();
	LoadPreloadedSecrets();
	return registered_secrets;
}

void DuckSecretManager::WriteSecretToFile(const BaseSecret &secret) {
	auto &fs = FileSystem::GetFileSystem(db_instance);
	auto secret_dir = GetSecretDirectory();
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

void DuckSecretManager::PreloadPermanentSecrets() {
	auto &fs = FileSystem::GetFileSystem(db_instance);
	auto secret_dir = GetSecretDirectory();
	fs.ListFiles(secret_dir, [&](const string &fname, bool is_dir) {
		string full_path = fs.JoinPath(secret_dir, fname);

		if (StringUtil::EndsWith(full_path, ".duckdb_secret")) {
			string secret_name = fname.substr(0, fname.size() - 14); // size of file ext

			// Check for name collisions
			for (const auto &secret : registered_secrets) {
				if (secret.secret->GetName() == fname) {
					throw IOException("Trying to preload permanent secrets '%s' which has a name that already exists!",
					                  full_path);
				}
			}

			permanent_secret_files[secret_name] = full_path;
		}
	});
}

void DuckSecretManager::LoadPreloadedSecrets() {
	vector<string> to_load;

	// Find the permanent secrets that need to be loaded
	for (auto &it : permanent_secret_files) {
		bool found = false;

		for (const auto &secret : registered_secrets) {
			if (secret.secret->GetName() == it.first) {
				found = true;
			}
		}

		if (!found) {
			to_load.push_back(it.second);
		}
	}
	permanent_secret_files.clear();

	for (const auto &path : to_load) {
		LoadSecret(path, SecretPersistMode::PERMANENT);
	}
}

void DuckSecretManager::LoadSecret(const string &path, SecretPersistMode persist_mode) {
	auto &fs = FileSystem::GetFileSystem(db_instance);
	auto file_reader = BufferedFileReader(fs, path.c_str());
	while (!file_reader.Finished()) {
		BinaryDeserializer deserializer(file_reader);
		deserializer.Begin();
		shared_ptr<BaseSecret> deserialized_secret = DeserializeSecretInternal(deserializer);
		RegisterSecretInternal(deserialized_secret, OnCreateConflict::ERROR_ON_CONFLICT, persist_mode);
		deserializer.End();
	}
}

void DuckSecretManager::LoadSecretFromPreloaded(const string &name) {
	auto path = permanent_secret_files[name];
	permanent_secret_files.erase(name);
	LoadSecret(path, SecretPersistMode::PERMANENT);
}

void DuckSecretManager::SyncPermanentSecrets(bool force) {
	auto &current_directory = db_instance.config.options.secret_directory;
	auto permanent_secrets_enabled = db_instance.config.options.allow_permanent_secrets;
	bool require_flush = current_directory != last_secret_directory;

	if (force || !permanent_secrets_enabled || require_flush) {

		//! Remove current persistent secrets.
		std::vector<RegisteredSecret>::iterator iter;
		for (iter = registered_secrets.begin(); iter != registered_secrets.end();) {
			if (iter->persistent) {
				registered_secrets.erase(iter);
			} else {
				++iter;
			}
		}

		permanent_secret_files.clear();

		if (permanent_secrets_enabled) {
			DuckSecretManager::PreloadPermanentSecrets();
		}
	}

	last_secret_directory = current_directory;
}

string DuckSecretManager::GetSecretDirectory() {
	auto &fs = FileSystem::GetFileSystem(db_instance);

	string directory = DBConfig::GetConfig(db_instance).options.secret_directory;

	if (directory.empty()) {
		directory = fs.GetHomeDirectory();
		if (!fs.DirectoryExists(directory)) {
			throw IOException(
			    "Can't find the home directory for storing permanent secrets. Either specify a secret directory "
			    "using SET secret_directory='/path/to/dir' or specify a home directory using the SET "
			    "home_directory='/path/to/dir' option.",
			    directory);
		}

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
