#include "duckdb/main/database.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/extension_helper.hpp"

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif

namespace duckdb {

DBConfig::DBConfig() {
	compression_functions = make_unique<CompressionFunctionSet>();
}

DBConfig::~DBConfig() {
}

DatabaseInstance::DatabaseInstance() {
}

DatabaseInstance::~DatabaseInstance() {
	if (Exception::UncaughtException()) {
		return;
	}

	// shutting down: attempt to checkpoint the database
	// but only if we are not cleaning up as part of an exception unwind
	try {
		auto &storage = StorageManager::GetStorageManager(*this);
		if (!storage.InMemory()) {
			auto &config = storage.db.config;
			if (!config.options.checkpoint_on_shutdown) {
				return;
			}
			storage.CreateCheckpoint(true);
		}
	} catch (...) {
	}
}

BufferManager &BufferManager::GetBufferManager(DatabaseInstance &db) {
	return *db.GetStorageManager().buffer_manager;
}

BlockManager &BlockManager::GetBlockManager(DatabaseInstance &db) {
	return *db.GetStorageManager().block_manager;
}

BlockManager &BlockManager::GetBlockManager(ClientContext &context) {
	return BlockManager::GetBlockManager(DatabaseInstance::GetDatabase(context));
}

DatabaseInstance &DatabaseInstance::GetDatabase(ClientContext &context) {
	return *context.db;
}

StorageManager &StorageManager::GetStorageManager(DatabaseInstance &db) {
	return db.GetStorageManager();
}

Catalog &Catalog::GetCatalog(DatabaseInstance &db) {
	return db.GetCatalog();
}

FileSystem &FileSystem::GetFileSystem(DatabaseInstance &db) {
	return db.GetFileSystem();
}

DBConfig &DBConfig::GetConfig(DatabaseInstance &db) {
	return db.config;
}

ClientConfig &ClientConfig::GetConfig(ClientContext &context) {
	return context.config;
}

const DBConfig &DBConfig::GetConfig(const DatabaseInstance &db) {
	return db.config;
}

const ClientConfig &ClientConfig::GetConfig(const ClientContext &context) {
	return context.config;
}

TransactionManager &TransactionManager::Get(ClientContext &context) {
	return TransactionManager::Get(DatabaseInstance::GetDatabase(context));
}

TransactionManager &TransactionManager::Get(DatabaseInstance &db) {
	return db.GetTransactionManager();
}

ConnectionManager &ConnectionManager::Get(DatabaseInstance &db) {
	return db.GetConnectionManager();
}

ConnectionManager &ConnectionManager::Get(ClientContext &context) {
	return ConnectionManager::Get(DatabaseInstance::GetDatabase(context));
}

void DatabaseInstance::Initialize(const char *path, DBConfig *new_config) {
	if (new_config) {
		// user-supplied configuration
		Configure(*new_config);
	} else {
		// default configuration
		DBConfig config;
		Configure(config);
	}
	if (config.options.temporary_directory.empty() && path) {
		// no directory specified: use default temp path
		config.options.temporary_directory = string(path) + ".tmp";

		// special treatment for in-memory mode
		if (strcmp(path, ":memory:") == 0) {
			config.options.temporary_directory = ".tmp";
		}
	}
	if (new_config && !new_config->options.use_temporary_directory) {
		// temporary directories explicitly disabled
		config.options.temporary_directory = string();
	}

	storage = make_unique<StorageManager>(*this, path ? string(path) : string(),
	                                      config.options.access_mode == AccessMode::READ_ONLY);
	catalog = make_unique<Catalog>(*this);
	transaction_manager = make_unique<TransactionManager>(*this);
	scheduler = make_unique<TaskScheduler>(*this);
	object_cache = make_unique<ObjectCache>();
	connection_manager = make_unique<ConnectionManager>();

	// initialize the database
	storage->Initialize();

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

StorageManager &DatabaseInstance::GetStorageManager() {
	return *storage;
}

Catalog &DatabaseInstance::GetCatalog() {
	return *catalog;
}

TransactionManager &DatabaseInstance::GetTransactionManager() {
	return *transaction_manager;
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

void DatabaseInstance::Configure(DBConfig &new_config) {
	config.options.access_mode = AccessMode::READ_WRITE;
	if (new_config.options.access_mode != AccessMode::UNDEFINED) {
		config.options.access_mode = new_config.options.access_mode;
	}
	if (new_config.file_system) {
		config.file_system = move(new_config.file_system);
	} else {
		config.file_system = make_unique<VirtualFileSystem>();
	}
	config.options.maximum_memory = new_config.options.maximum_memory;
	if (config.options.maximum_memory == (idx_t)-1) {
		auto memory = FileSystem::GetAvailableMemory();
		if (memory != DConstants::INVALID_INDEX) {
			config.options.maximum_memory = memory * 8 / 10;
		}
	}
	if (new_config.options.maximum_threads == (idx_t)-1) {
#ifndef DUCKDB_NO_THREADS
		config.options.maximum_threads = std::thread::hardware_concurrency();
#else
		config.options.maximum_threads = 1;
#endif
	} else {
		config.options.maximum_threads = new_config.options.maximum_threads;
	}
	config.options.external_threads = new_config.options.external_threads;
	config.options.load_extensions = new_config.options.load_extensions;
	config.options.force_compression = new_config.options.force_compression;
	config.allocator = move(new_config.allocator);
	if (!config.allocator) {
		config.allocator = make_unique<Allocator>();
	}
	config.options.checkpoint_wal_size = new_config.options.checkpoint_wal_size;
	config.options.use_direct_io = new_config.options.use_direct_io;
	config.options.temporary_directory = new_config.options.temporary_directory;
	config.options.collation = new_config.options.collation;
	config.options.default_order_type = new_config.options.default_order_type;
	config.options.default_null_order = new_config.options.default_null_order;
	config.options.enable_external_access = new_config.options.enable_external_access;
	config.options.allow_unsigned_extensions = new_config.options.allow_unsigned_extensions;
	config.replacement_scans = move(new_config.replacement_scans);
	config.options.initialize_default_database = new_config.options.initialize_default_database;
	config.options.disabled_optimizers = move(new_config.options.disabled_optimizers);
	config.parser_extensions = move(new_config.parser_extensions);
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
void DuckDB::SetExtensionLoaded(const std::string &name) {
	instance->loaded_extensions.insert(name);
}

string ClientConfig::ExtractTimezone() const {
	auto entry = set_variables.find("TimeZone");
	if (entry == set_variables.end()) {
		return "UTC";
	} else {
		return entry->second.GetValue<std::string>();
	}
}

void DatabaseInstance::Invalidate() {
	this->is_invalidated = true;
}
bool DatabaseInstance::IsInvalidated() {
	return this->is_invalidated;
}

} // namespace duckdb
