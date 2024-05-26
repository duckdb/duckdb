#include "duckdb/main/database.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/database_path_and_type.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/main/database_file_opener.hpp"

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif

namespace duckdb {

DBConfig::DBConfig() {
	compression_functions = make_uniq<CompressionFunctionSet>();
	cast_functions = make_uniq<CastFunctionSet>(*this);
	index_types = make_uniq<IndexTypeSet>();
	error_manager = make_uniq<ErrorManager>();
	secret_manager = make_uniq<SecretManager>();
}

DBConfig::DBConfig(bool read_only) : DBConfig::DBConfig() {
	if (read_only) {
		options.access_mode = AccessMode::READ_ONLY;
	}
}

DBConfig::DBConfig(const case_insensitive_map_t<Value> &config_dict, bool read_only) : DBConfig::DBConfig(read_only) {
	SetOptionsByName(config_dict);
}

DBConfig::~DBConfig() {
}

DatabaseInstance::DatabaseInstance() {
}

DatabaseInstance::~DatabaseInstance() {
	// destroy all attached databases
	GetDatabaseManager().ResetDatabases(scheduler);
	// destroy child elements
	connection_manager.reset();
	object_cache.reset();
	scheduler.reset();
	db_manager.reset();
	buffer_manager.reset();
	// finally, flush allocations
	Allocator::FlushAll();
}

BufferManager &BufferManager::GetBufferManager(DatabaseInstance &db) {
	return db.GetBufferManager();
}

const BufferManager &BufferManager::GetBufferManager(const DatabaseInstance &db) {
	return db.GetBufferManager();
}

BufferManager &BufferManager::GetBufferManager(AttachedDatabase &db) {
	return BufferManager::GetBufferManager(db.GetDatabase());
}

DatabaseInstance &DatabaseInstance::GetDatabase(ClientContext &context) {
	return *context.db;
}

const DatabaseInstance &DatabaseInstance::GetDatabase(const ClientContext &context) {
	return *context.db;
}

DatabaseManager &DatabaseInstance::GetDatabaseManager() {
	if (!db_manager) {
		throw InternalException("Missing DB manager");
	}
	return *db_manager;
}

Catalog &Catalog::GetSystemCatalog(DatabaseInstance &db) {
	return db.GetDatabaseManager().GetSystemCatalog();
}

Catalog &Catalog::GetCatalog(AttachedDatabase &db) {
	return db.GetCatalog();
}

FileSystem &FileSystem::GetFileSystem(DatabaseInstance &db) {
	return db.GetFileSystem();
}

FileSystem &FileSystem::Get(AttachedDatabase &db) {
	return FileSystem::GetFileSystem(db.GetDatabase());
}

DBConfig &DBConfig::GetConfig(DatabaseInstance &db) {
	return db.config;
}

ClientConfig &ClientConfig::GetConfig(ClientContext &context) {
	return context.config;
}

DBConfig &DBConfig::Get(AttachedDatabase &db) {
	return DBConfig::GetConfig(db.GetDatabase());
}

const DBConfig &DBConfig::GetConfig(const DatabaseInstance &db) {
	return db.config;
}

const ClientConfig &ClientConfig::GetConfig(const ClientContext &context) {
	return context.config;
}

TransactionManager &TransactionManager::Get(AttachedDatabase &db) {
	return db.GetTransactionManager();
}

ConnectionManager &ConnectionManager::Get(DatabaseInstance &db) {
	return db.GetConnectionManager();
}

ConnectionManager &ConnectionManager::Get(ClientContext &context) {
	return ConnectionManager::Get(DatabaseInstance::GetDatabase(context));
}

unique_ptr<AttachedDatabase> DatabaseInstance::CreateAttachedDatabase(ClientContext &context, const AttachInfo &info,
                                                                      const string &type, AccessMode access_mode) {
	unique_ptr<AttachedDatabase> attached_database;
	if (!type.empty()) {
		// find the storage extension
		auto extension_name = ExtensionHelper::ApplyExtensionAlias(type);
		auto entry = config.storage_extensions.find(extension_name);
		if (entry == config.storage_extensions.end()) {
			throw BinderException("Unrecognized storage type \"%s\"", type);
		}

		if (entry->second->attach != nullptr && entry->second->create_transaction_manager != nullptr) {
			// use storage extension to create the initial database
			attached_database = make_uniq<AttachedDatabase>(*this, Catalog::GetSystemCatalog(*this), *entry->second,
			                                                context, info.name, info, access_mode);
		} else {
			attached_database =
			    make_uniq<AttachedDatabase>(*this, Catalog::GetSystemCatalog(*this), info.name, info.path, access_mode);
		}
	} else {
		// check if this is an in-memory database or not
		attached_database =
		    make_uniq<AttachedDatabase>(*this, Catalog::GetSystemCatalog(*this), info.name, info.path, access_mode);
	}
	return attached_database;
}

void DatabaseInstance::CreateMainDatabase() {
	AttachInfo info;
	info.name = AttachedDatabase::ExtractDatabaseName(config.options.database_path, GetFileSystem());
	info.path = config.options.database_path;

	optional_ptr<AttachedDatabase> initial_database;
	{
		Connection con(*this);
		con.BeginTransaction();
		initial_database =
		    db_manager->AttachDatabase(*con.context, info, config.options.database_type, config.options.access_mode);
		con.Commit();
	}

	initial_database->SetInitialDatabase();
	initial_database->Initialize();
}

void ThrowExtensionSetUnrecognizedOptions(const unordered_map<string, Value> &unrecognized_options) {
	auto unrecognized_options_iter = unrecognized_options.begin();
	string unrecognized_option_keys = unrecognized_options_iter->first;
	while (++unrecognized_options_iter != unrecognized_options.end()) {
		unrecognized_option_keys = "," + unrecognized_options_iter->first;
	}
	throw InvalidInputException("Unrecognized configuration property \"%s\"", unrecognized_option_keys);
}

void DatabaseInstance::Initialize(const char *database_path, DBConfig *user_config) {
	DBConfig default_config;
	DBConfig *config_ptr = &default_config;
	if (user_config) {
		config_ptr = user_config;
	}

	Configure(*config_ptr, database_path);

	if (user_config && !user_config->options.use_temporary_directory) {
		// temporary directories explicitly disabled
		config.options.temporary_directory = string();
	}

	db_file_system = make_uniq<DatabaseFileSystem>(*this);
	db_manager = make_uniq<DatabaseManager>(*this);
	if (config.buffer_manager) {
		buffer_manager = config.buffer_manager;
	} else {
		buffer_manager = make_uniq<StandardBufferManager>(*this, config.options.temporary_directory);
	}
	scheduler = make_uniq<TaskScheduler>(*this);
	object_cache = make_uniq<ObjectCache>();
	connection_manager = make_uniq<ConnectionManager>();

	// initialize the secret manager
	config.secret_manager->Initialize(*this);

	// resolve the type of teh database we are opening
	auto &fs = FileSystem::GetFileSystem(*this);
	DBPathAndType::ResolveDatabaseType(fs, config.options.database_path, config.options.database_type);

	// initialize the system catalog
	db_manager->InitializeSystemCatalog();

	if (!config.options.database_type.empty()) {
		// if we are opening an extension database - load the extension
		if (!config.file_system) {
			throw InternalException("No file system!?");
		}
		ExtensionHelper::LoadExternalExtension(*this, *config.file_system, config.options.database_type);
	}

	if (!config.options.unrecognized_options.empty()) {
		ThrowExtensionSetUnrecognizedOptions(config.options.unrecognized_options);
	}

	if (!db_manager->HasDefaultDatabase()) {
		CreateMainDatabase();
	}

	// only increase thread count after storage init because we get races on catalog otherwise
	scheduler->SetThreads(config.options.maximum_threads, config.options.external_threads);
	scheduler->RelaunchThreads();
}

DuckDB::DuckDB(const char *path, DBConfig *new_config) : instance(make_shared_ptr<DatabaseInstance>()) {
	instance->Initialize(path, new_config);
	if (instance->config.options.load_extensions) {
		ExtensionHelper::LoadAllExtensions(*this);
	}
}

DuckDB::DuckDB(const string &path, DBConfig *config) : DuckDB(path.c_str(), config) {
}

DuckDB::DuckDB(DatabaseInstance &instance_p) : instance(instance_p.shared_from_this()) {
}

DuckDB::~DuckDB() {
}

SecretManager &DatabaseInstance::GetSecretManager() {
	return *config.secret_manager;
}

BufferManager &DatabaseInstance::GetBufferManager() {
	return *buffer_manager;
}

const BufferManager &DatabaseInstance::GetBufferManager() const {
	return *buffer_manager;
}

BufferPool &DatabaseInstance::GetBufferPool() const {
	return *config.buffer_pool;
}

DatabaseManager &DatabaseManager::Get(DatabaseInstance &db) {
	return db.GetDatabaseManager();
}

DatabaseManager &DatabaseManager::Get(ClientContext &db) {
	return DatabaseManager::Get(*db.db);
}

TaskScheduler &DatabaseInstance::GetScheduler() {
	return *scheduler;
}

ObjectCache &DatabaseInstance::GetObjectCache() {
	return *object_cache;
}

FileSystem &DatabaseInstance::GetFileSystem() {
	return *db_file_system;
}

ConnectionManager &DatabaseInstance::GetConnectionManager() {
	return *connection_manager;
}

FileSystem &DuckDB::GetFileSystem() {
	return instance->GetFileSystem();
}

Allocator &Allocator::Get(ClientContext &context) {
	return Allocator::Get(*context.db);
}

Allocator &Allocator::Get(DatabaseInstance &db) {
	return *db.config.allocator;
}

Allocator &Allocator::Get(AttachedDatabase &db) {
	return Allocator::Get(db.GetDatabase());
}

void DatabaseInstance::Configure(DBConfig &new_config, const char *database_path) {
	config.options = new_config.options;

	if (new_config.options.duckdb_api.empty()) {
		config.SetOptionByName("duckdb_api", "cpp");
	}

	if (database_path) {
		config.options.database_path = database_path;
	} else {
		config.options.database_path.clear();
	}

	if (new_config.options.temporary_directory.empty()) {
		config.SetDefaultTempDirectory();
	}

	if (config.options.access_mode == AccessMode::UNDEFINED) {
		config.options.access_mode = AccessMode::READ_WRITE;
	}
	config.extension_parameters = new_config.extension_parameters;
	if (new_config.file_system) {
		config.file_system = std::move(new_config.file_system);
	} else {
		config.file_system = make_uniq<VirtualFileSystem>();
	}
	if (new_config.secret_manager) {
		config.secret_manager = std::move(new_config.secret_manager);
	}
	if (config.options.maximum_memory == DConstants::INVALID_INDEX) {
		config.SetDefaultMaxMemory();
	}
	if (new_config.options.maximum_threads == DConstants::INVALID_INDEX) {
		config.options.maximum_threads = config.GetSystemMaxThreads(*config.file_system);
	}
	config.allocator = std::move(new_config.allocator);
	if (!config.allocator) {
		config.allocator = make_uniq<Allocator>();
	}
	config.replacement_scans = std::move(new_config.replacement_scans);
	config.parser_extensions = std::move(new_config.parser_extensions);
	config.error_manager = std::move(new_config.error_manager);
	if (!config.error_manager) {
		config.error_manager = make_uniq<ErrorManager>();
	}
	if (!config.default_allocator) {
		config.default_allocator = Allocator::DefaultAllocatorReference();
	}
	if (new_config.buffer_pool) {
		config.buffer_pool = std::move(new_config.buffer_pool);
	} else {
		config.buffer_pool = make_shared_ptr<BufferPool>(config.options.maximum_memory,
		                                                 config.options.buffer_manager_track_eviction_timestamps);
	}
}

DBConfig &DBConfig::GetConfig(ClientContext &context) {
	return context.db->config;
}

const DBConfig &DBConfig::GetConfig(const ClientContext &context) {
	return context.db->config;
}

idx_t DatabaseInstance::NumberOfThreads() {
	return NumericCast<idx_t>(scheduler->NumberOfThreads());
}

const unordered_set<std::string> &DatabaseInstance::LoadedExtensions() {
	return loaded_extensions;
}

const unordered_map<std::string, ExtensionInstallInfo> &DatabaseInstance::LoadedExtensionsData() {
	return loaded_extensions_data;
}

idx_t DuckDB::NumberOfThreads() {
	return instance->NumberOfThreads();
}

bool DatabaseInstance::ExtensionIsLoaded(const std::string &name) {
	auto extension_name = ExtensionHelper::GetExtensionName(name);
	return loaded_extensions.find(extension_name) != loaded_extensions.end();
}

bool DuckDB::ExtensionIsLoaded(const std::string &name) {
	return instance->ExtensionIsLoaded(name);
}

void DatabaseInstance::SetExtensionLoaded(const string &name, ExtensionInstallInfo &install_info) {
	auto extension_name = ExtensionHelper::GetExtensionName(name);
	loaded_extensions.insert(extension_name);
	loaded_extensions_data.insert({extension_name, install_info});

	auto &callbacks = DBConfig::GetConfig(*this).extension_callbacks;
	for (auto &callback : callbacks) {
		callback->OnExtensionLoaded(*this, name);
	}
}

SettingLookupResult DatabaseInstance::TryGetCurrentSetting(const std::string &key, Value &result) const {
	// check the session values
	auto &db_config = DBConfig::GetConfig(*this);
	const auto &global_config_map = db_config.options.set_variables;

	auto global_value = global_config_map.find(key);
	bool found_global_value = global_value != global_config_map.end();
	if (!found_global_value) {
		return SettingLookupResult();
	}
	result = global_value->second;
	return SettingLookupResult(SettingScope::GLOBAL);
}

ValidChecker &DatabaseInstance::GetValidChecker() {
	return db_validity;
}

ValidChecker &ValidChecker::Get(DatabaseInstance &db) {
	return db.GetValidChecker();
}

} // namespace duckdb
