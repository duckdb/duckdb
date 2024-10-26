#include "duckdb/main/database.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/execution/index/index_type_set.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database_file_opener.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/database_path_and_type.hpp"
#include "duckdb/main/db_instance_cache.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/planner/collation_binding.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/capi/extension_api.hpp"

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif

namespace duckdb {

DBConfig::DBConfig() {
	compression_functions = make_uniq<CompressionFunctionSet>();
	cast_functions = make_uniq<CastFunctionSet>(*this);
	collation_bindings = make_uniq<CollationBinding>();
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
	config.is_user_config = false;
	create_api_v0 = nullptr;
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
	// flush allocations and disable the background thread
	if (Allocator::SupportsFlush()) {
		Allocator::FlushAll();
	}
	Allocator::SetBackgroundThreads(false);
	// after all destruction is complete clear the cache entry
	db_cache_entry.reset();
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

void DatabaseInstance::SetDatabaseCacheEntry(shared_ptr<DatabaseCacheEntry> entry) {
	db_cache_entry = std::move(entry);
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
                                                                      const AttachOptions &options) {
	unique_ptr<AttachedDatabase> attached_database;
	auto &catalog = Catalog::GetSystemCatalog(*this);

	if (!options.db_type.empty()) {
		// Find the storage extension for this database file.
		auto extension_name = ExtensionHelper::ApplyExtensionAlias(options.db_type);
		auto entry = config.storage_extensions.find(extension_name);
		if (entry == config.storage_extensions.end()) {
			throw BinderException("Unrecognized storage type \"%s\"", options.db_type);
		}

		if (entry->second->attach != nullptr && entry->second->create_transaction_manager != nullptr) {
			// Use the storage extension to create the initial database.
			attached_database =
			    make_uniq<AttachedDatabase>(*this, catalog, *entry->second, context, info.name, info, options);
			return attached_database;
		}

		attached_database = make_uniq<AttachedDatabase>(*this, catalog, info.name, info.path, options);
		return attached_database;
	}

	// An empty db_type defaults to a duckdb database file.
	attached_database = make_uniq<AttachedDatabase>(*this, catalog, info.name, info.path, options);
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
		AttachOptions options(config.options);
		initial_database = db_manager->AttachDatabase(*con.context, info, options);
		con.Commit();
	}

	initial_database->SetInitialDatabase();
	initial_database->Initialize();
}

static void ThrowExtensionSetUnrecognizedOptions(const case_insensitive_map_t<Value> &unrecognized_options) {
	D_ASSERT(!unrecognized_options.empty());

	vector<string> options;
	for (auto &kv : unrecognized_options) {
		options.push_back(kv.first);
	}
	auto concatenated = StringUtil::Join(options, ", ");
	throw InvalidInputException("The following options were not recognized: " + concatenated);
}

void DatabaseInstance::LoadExtensionSettings() {
	auto &unrecognized_options = config.options.unrecognized_options;

	if (config.options.autoload_known_extensions) {
		if (unrecognized_options.empty()) {
			// Nothing to do
			return;
		}

		Connection con(*this);
		con.BeginTransaction();

		vector<string> extension_options;
		for (auto &option : unrecognized_options) {
			auto &name = option.first;
			auto &value = option.second;

			auto extension_name = ExtensionHelper::FindExtensionInEntries(name, EXTENSION_SETTINGS);
			if (extension_name.empty()) {
				continue;
			}
			if (!ExtensionHelper::TryAutoLoadExtension(*this, extension_name)) {
				throw InvalidInputException(
				    "To set the %s setting, the %s extension needs to be loaded. But it could not be autoloaded.", name,
				    extension_name);
			}
			auto it = config.extension_parameters.find(name);
			if (it == config.extension_parameters.end()) {
				throw InternalException("Extension %s did not provide the '%s' config setting", extension_name, name);
			}
			auto &context = *con.context;
			PhysicalSet::SetExtensionVariable(context, it->second, name, SetScope::GLOBAL, value);
			extension_options.push_back(name);
		}

		for (auto &option : extension_options) {
			unrecognized_options.erase(option);
		}
		con.Commit();
	}
	if (!unrecognized_options.empty()) {
		ThrowExtensionSetUnrecognizedOptions(unrecognized_options);
	}
}

static duckdb_ext_api_v0 CreateAPIv0Wrapper() {
	return CreateAPIv0();
}

void DatabaseInstance::Initialize(const char *database_path, DBConfig *user_config) {
	DBConfig default_config;
	DBConfig *config_ptr = &default_config;
	if (user_config) {
		config_ptr = user_config;
	}

	Configure(*config_ptr, database_path);

	create_api_v0 = CreateAPIv0Wrapper;

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

	LoadExtensionSettings();

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

	if (config.options.duckdb_api.empty()) {
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
		                                                 config.options.buffer_manager_track_eviction_timestamps,
		                                                 config.options.allocator_bulk_deallocation_flush_threshold);
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

const unordered_map<string, ExtensionInfo> &DatabaseInstance::GetExtensions() {
	return loaded_extensions_info;
}

void DatabaseInstance::AddExtensionInfo(const string &name, const ExtensionLoadedInfo &info) {
	loaded_extensions_info[name].load_info = make_uniq<ExtensionLoadedInfo>(info);
}

idx_t DuckDB::NumberOfThreads() {
	return instance->NumberOfThreads();
}

bool DatabaseInstance::ExtensionIsLoaded(const std::string &name) {
	auto extension_name = ExtensionHelper::GetExtensionName(name);
	auto it = loaded_extensions_info.find(extension_name);
	return it != loaded_extensions_info.end() && it->second.is_loaded;
}

bool DuckDB::ExtensionIsLoaded(const std::string &name) {
	return instance->ExtensionIsLoaded(name);
}

void DatabaseInstance::SetExtensionLoaded(const string &name, ExtensionInstallInfo &install_info) {
	auto extension_name = ExtensionHelper::GetExtensionName(name);
	loaded_extensions_info[extension_name].is_loaded = true;
	loaded_extensions_info[extension_name].install_info = make_uniq<ExtensionInstallInfo>(install_info);

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

const duckdb_ext_api_v0 DatabaseInstance::GetExtensionAPIV0() {
	D_ASSERT(create_api_v0);
	return create_api_v0();
}

ValidChecker &ValidChecker::Get(DatabaseInstance &db) {
	return db.GetValidChecker();
}

} // namespace duckdb
