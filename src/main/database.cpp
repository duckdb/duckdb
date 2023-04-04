#include "duckdb/main/database.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/magic_bytes.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/execution/operator/helper/physical_set.hpp"
#include "duckdb/storage/standard_buffer_manager.hpp"

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif

namespace duckdb {

DBConfig::DBConfig() {
	compression_functions = make_unique<CompressionFunctionSet>();
	cast_functions = make_unique<CastFunctionSet>();
	error_manager = make_unique<ErrorManager>();
}

DBConfig::DBConfig(std::unordered_map<string, string> &config_dict, bool read_only) : DBConfig::DBConfig() {
	if (read_only) {
		options.access_mode = AccessMode::READ_ONLY;
	}
	for (auto &kv : config_dict) {
		string key = kv.first;
		string val = kv.second;
		auto opt_val = Value(val);
		DBConfig::SetOptionByName(key, opt_val);
	}
}

DBConfig::~DBConfig() {
}

DatabaseInstance::DatabaseInstance() {
}

DatabaseInstance::~DatabaseInstance() {
}

BufferManager &BufferManager::GetBufferManager(DatabaseInstance &db) {
	return db.GetBufferManager();
}

BufferManager &BufferManager::GetBufferManager(AttachedDatabase &db) {
	return BufferManager::GetBufferManager(db.GetDatabase());
}

DatabaseInstance &DatabaseInstance::GetDatabase(ClientContext &context) {
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

ClientContext *ConnectionManager::GetConnection(DatabaseInstance *db) {
	for (auto &conn : connections) {
		if (conn.first->db.get() == db) {
			return conn.first;
		}
	}
	return nullptr;
}

ConnectionManager &ConnectionManager::Get(ClientContext &context) {
	return ConnectionManager::Get(DatabaseInstance::GetDatabase(context));
}

string DatabaseInstance::ExtractDatabaseType(string &path) {
	// first check if there is an existing prefix
	auto extension = ExtensionHelper::ExtractExtensionPrefixFromPath(path);
	if (!extension.empty()) {
		// path is prefixed with an extension - remove it
		path = StringUtil::Replace(path, extension + ":", "");
		return extension;
	}
	// if there isn't - check the magic bytes of the file (if any)
	auto file_type = MagicBytes::CheckMagicBytes(config.file_system.get(), path);
	if (file_type == DataFileType::SQLITE_FILE) {
		return "sqlite";
	}
	return string();
}

unique_ptr<AttachedDatabase> DatabaseInstance::CreateAttachedDatabase(AttachInfo &info, const string &type,
                                                                      AccessMode access_mode) {
	unique_ptr<AttachedDatabase> attached_database;
	if (!type.empty()) {
		// find the storage extensionon database
		auto entry = config.storage_extensions.find(type);
		if (entry == config.storage_extensions.end()) {
			throw BinderException("Unrecognized storage type \"%s\"", type);
		}

		if (entry->second->attach != nullptr && entry->second->create_transaction_manager != nullptr) {
			// use storage extension to create the initial database
			attached_database = make_unique<AttachedDatabase>(*this, Catalog::GetSystemCatalog(*this), *entry->second,
			                                                  info.name, info, access_mode);
		} else {
			attached_database = make_unique<AttachedDatabase>(*this, Catalog::GetSystemCatalog(*this), info.name,
			                                                  info.path, access_mode);
		}
	} else {
		// check if this is an in-memory database or not
		attached_database =
		    make_unique<AttachedDatabase>(*this, Catalog::GetSystemCatalog(*this), info.name, info.path, access_mode);
	}
	return attached_database;
}

void DatabaseInstance::Initialize(const char *database_path, DBConfig *user_config) {
	DBConfig default_config;
	DBConfig *config_ptr = &default_config;
	if (user_config) {
		config_ptr = user_config;
	}

	if (config_ptr->options.temporary_directory.empty() && database_path) {
		// no directory specified: use default temp path
		config_ptr->options.temporary_directory = string(database_path) + ".tmp";

		// special treatment for in-memory mode
		if (strcmp(database_path, ":memory:") == 0) {
			config_ptr->options.temporary_directory = ".tmp";
		}
	}

	if (database_path) {
		config_ptr->options.database_path = database_path;
	} else {
		config_ptr->options.database_path.clear();
	}
	Configure(*config_ptr);

	if (user_config && !user_config->options.use_temporary_directory) {
		// temporary directories explicitly disabled
		config.options.temporary_directory = string();
	}

	db_manager = make_unique<DatabaseManager>(*this);
	buffer_manager = make_unique<StandardBufferManager>(*this, config.options.temporary_directory);
	scheduler = make_unique<TaskScheduler>(*this);
	object_cache = make_unique<ObjectCache>();
	connection_manager = make_unique<ConnectionManager>();

	// check if we are opening a standard DuckDB database or an extension database
	auto database_type = ExtractDatabaseType(config.options.database_path);
	if (!database_type.empty()) {
		// we are opening an extension database, run storage_init
		ExtensionHelper::StorageInit(database_type, config);
	}
	AttachInfo info;
	info.name = AttachedDatabase::ExtractDatabaseName(config.options.database_path);
	info.path = config.options.database_path;

	auto attached_database = CreateAttachedDatabase(info, database_type, config.options.access_mode);
	auto initial_database = attached_database.get();
	{
		Connection con(*this);
		con.BeginTransaction();
		db_manager->AddDatabase(*con.context, std::move(attached_database));
		con.Commit();
	}

	// initialize the system catalog
	db_manager->InitializeSystemCatalog();
	// initialize the database
	initial_database->Initialize();

	if (!database_type.empty()) {
		// if we are opening an extension database - load the extension
		ExtensionHelper::LoadExternalExtension(*this, nullptr, database_type);
	}

	if (!config.options.unrecognized_options.empty()) {
		// check if all unrecognized options can be handled by the loaded extension(s)
		for (auto &unrecognized_option : config.options.unrecognized_options) {
			auto entry = config.extension_parameters.find(unrecognized_option.first);
			if (entry == config.extension_parameters.end()) {
				throw InvalidInputException("Unrecognized configuration property \"%s\"", unrecognized_option.first);
			}
		}

		// if so - set the options
		Connection con(*this);
		con.BeginTransaction();
		for (auto &unrecognized_option : config.options.unrecognized_options) {
			auto entry = config.extension_parameters.find(unrecognized_option.first);
			D_ASSERT(entry != config.extension_parameters.end());
			PhysicalSet::SetExtensionVariable(*con.context, entry->second, unrecognized_option.first, SetScope::GLOBAL,
			                                  unrecognized_option.second);
		}
		con.Commit();
	}

	// only increase thread count after storage init because we get races on catalog otherwise
	scheduler->SetThreads(config.options.maximum_threads);
}

DuckDB::DuckDB(const char *path, DBConfig *new_config) : instance(make_shared<DatabaseInstance>()) {
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

BufferManager &DatabaseInstance::GetBufferManager() {
	return *buffer_manager;
}

BufferPool &DatabaseInstance::GetBufferPool() {
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
	return *config.file_system;
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

void DatabaseInstance::Configure(DBConfig &new_config) {
	config.options = new_config.options;
	if (config.options.access_mode == AccessMode::UNDEFINED) {
		config.options.access_mode = AccessMode::READ_WRITE;
	}
	if (new_config.file_system) {
		config.file_system = std::move(new_config.file_system);
	} else {
		config.file_system = make_unique<VirtualFileSystem>();
	}
	if (config.options.maximum_memory == (idx_t)-1) {
		config.SetDefaultMaxMemory();
	}
	if (new_config.options.maximum_threads == (idx_t)-1) {
		config.SetDefaultMaxThreads();
	}
	config.allocator = std::move(new_config.allocator);
	if (!config.allocator) {
		config.allocator = make_unique<Allocator>();
	}
	config.replacement_scans = std::move(new_config.replacement_scans);
	config.parser_extensions = std::move(new_config.parser_extensions);
	config.error_manager = std::move(new_config.error_manager);
	if (!config.error_manager) {
		config.error_manager = make_unique<ErrorManager>();
	}
	if (!config.default_allocator) {
		config.default_allocator = Allocator::DefaultAllocatorReference();
	}
	if (new_config.buffer_pool) {
		config.buffer_pool = std::move(new_config.buffer_pool);
	} else {
		config.buffer_pool = make_shared<BufferPool>(config.options.maximum_memory);
	}
}

DBConfig &DBConfig::GetConfig(ClientContext &context) {
	return context.db->config;
}

const DBConfig &DBConfig::GetConfig(const ClientContext &context) {
	return context.db->config;
}

idx_t DatabaseInstance::NumberOfThreads() {
	return scheduler->NumberOfThreads();
}

const unordered_set<std::string> &DatabaseInstance::LoadedExtensions() {
	return loaded_extensions;
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

void DatabaseInstance::SetExtensionLoaded(const std::string &name) {
	auto extension_name = ExtensionHelper::GetExtensionName(name);
	loaded_extensions.insert(extension_name);
}

bool DatabaseInstance::TryGetCurrentSetting(const std::string &key, Value &result) {
	// check the session values
	auto &db_config = DBConfig::GetConfig(*this);
	const auto &global_config_map = db_config.options.set_variables;

	auto global_value = global_config_map.find(key);
	bool found_global_value = global_value != global_config_map.end();
	if (!found_global_value) {
		return false;
	}
	result = global_value->second;
	return true;
}

string ClientConfig::ExtractTimezone() const {
	auto entry = set_variables.find("TimeZone");
	if (entry == set_variables.end()) {
		return "UTC";
	} else {
		return entry->second.GetValue<std::string>();
	}
}

ValidChecker &DatabaseInstance::GetValidChecker() {
	return db_validity;
}

ValidChecker &ValidChecker::Get(DatabaseInstance &db) {
	return db.GetValidChecker();
}

} // namespace duckdb
