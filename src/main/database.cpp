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
#include "duckdb/main/replacement_opens.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/main/attached_database.hpp"

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif

namespace duckdb {

DBConfig::DBConfig() {
	compression_functions = make_unique<CompressionFunctionSet>();
	replacement_opens.push_back(ExtensionPrefixReplacementOpen());
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
		auto config_property = DBConfig::GetOptionByName(key);
		if (!config_property) {
			throw InvalidInputException("Unrecognized configuration property \"%s\"", key);
		}
		auto opt_val = Value(val);
		DBConfig::SetOption(*config_property, opt_val);
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

ConnectionManager &ConnectionManager::Get(ClientContext &context) {
	return ConnectionManager::Get(DatabaseInstance::GetDatabase(context));
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

	for (auto &open : config_ptr->replacement_opens) {
		if (open.pre_func) {
			open.data = open.pre_func(*config_ptr, open.static_data.get());
			if (open.data) {
				break;
			}
		}
	}
	Configure(*config_ptr);

	if (user_config && !user_config->options.use_temporary_directory) {
		// temporary directories explicitly disabled
		config.options.temporary_directory = string();
	}

	auto &config = DBConfig::GetConfig(*this);
	db_manager = make_unique<DatabaseManager>(*this);
	buffer_manager =
	    make_unique<BufferManager>(*this, config.options.temporary_directory, config.options.maximum_memory);
	scheduler = make_unique<TaskScheduler>(*this);
	object_cache = make_unique<ObjectCache>();
	connection_manager = make_unique<ConnectionManager>();

	auto database_name = AttachedDatabase::ExtractDatabaseName(config.options.database_path);
	auto attached_database = make_unique<AttachedDatabase>(*this, Catalog::GetSystemCatalog(*this), database_name,
	                                                       config.options.database_path, config.options.access_mode);
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

	// only increase thread count after storage init because we get races on catalog otherwise
	scheduler->SetThreads(config.options.maximum_threads);

	for (auto &open : config.replacement_opens) {
		if (open.post_func && open.data) {
			open.post_func(*this, open.data.get());
			break;
		}
	}
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
	config.replacement_opens = std::move(new_config.replacement_opens);
	config.parser_extensions = std::move(new_config.parser_extensions);
	config.error_manager = std::move(new_config.error_manager);
	if (!config.error_manager) {
		config.error_manager = make_unique<ErrorManager>();
	}
	if (!config.default_allocator) {
		config.default_allocator = Allocator::DefaultAllocatorReference();
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

bool DuckDB::ExtensionIsLoaded(const std::string &name) {
	return instance->loaded_extensions.find(name) != instance->loaded_extensions.end();
}
void DatabaseInstance::SetExtensionLoaded(const std::string &name) {
	loaded_extensions.insert(name);
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
