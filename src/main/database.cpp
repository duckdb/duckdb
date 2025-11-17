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
#include "duckdb/storage/block_allocator.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/capi/extension_api.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include "duckdb/storage/compression/empty_validity.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/http_util.hpp"
#include "mbedtls_wrapper.hpp"
#include "duckdb/main/database_file_path_manager.hpp"
#include "duckdb/main/result_set_manager.hpp"

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif

namespace duckdb {

DBConfig::DBConfig() {
	compression_functions = make_uniq<CompressionFunctionSet>();
	encoding_functions = make_uniq<EncodingFunctionSet>();
	encoding_functions->Initialize(*this);
	arrow_extensions = make_uniq<ArrowTypeExtensionSet>();
	arrow_extensions->Initialize(*this);
	cast_functions = make_uniq<CastFunctionSet>(*this);
	collation_bindings = make_uniq<CollationBinding>();
	index_types = make_uniq<IndexTypeSet>();
	error_manager = make_uniq<ErrorManager>();
	secret_manager = make_uniq<SecretManager>();
	http_util = make_shared_ptr<HTTPUtil>();
	storage_extensions["__open_file__"] = OpenFileStorageExtension::Create();
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

DatabaseInstance::DatabaseInstance() : db_validity(*this) {
	config.is_user_config = false;
	create_api_v1 = nullptr;
}

DatabaseInstance::~DatabaseInstance() {
	// destroy all attached databases
	if (db_manager) {
		db_manager->ResetDatabases(scheduler);
	}
	// destroy child elements
	connection_manager.reset();
	object_cache.reset();
	scheduler.reset();
	db_manager.reset();

	// stop the log manager, after this point Logger calls are unsafe.
	log_manager.reset();

	external_file_cache.reset();
	result_set_manager.reset();

	buffer_manager.reset();

	// flush allocations and disable the background thread
	config.block_allocator->FlushAll();
	Allocator::SetBackgroundThreads(false);
	// after all destruction is complete clear the cache entry
	config.db_cache_entry.reset();
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

shared_ptr<AttachedDatabase> DatabaseInstance::CreateAttachedDatabase(ClientContext &context, AttachInfo &info,
                                                                      AttachOptions &options) {
	shared_ptr<AttachedDatabase> attached_database;
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
			    make_shared_ptr<AttachedDatabase>(*this, catalog, *entry->second, context, info.name, info, options);
			return attached_database;
		}

		attached_database = make_shared_ptr<AttachedDatabase>(*this, catalog, info.name, info.path, options);
		return attached_database;
	}

	// An empty db_type defaults to a duckdb database file.
	attached_database = make_shared_ptr<AttachedDatabase>(*this, catalog, info.name, info.path, options);
	return attached_database;
}

void DatabaseInstance::CreateMainDatabase() {
	AttachInfo info;
	info.name = AttachedDatabase::ExtractDatabaseName(config.options.database_path, GetFileSystem());
	info.path = config.options.database_path;

	Connection con(*this);
	con.BeginTransaction();
	AttachOptions options(config.options);
	options.is_main_database = true;
	db_manager->AttachDatabase(*con.context, info, options);
	con.Commit();
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
	// copy the map, to protect against modifications during
	auto unrecognized_options_copy = config.options.unrecognized_options;

	if (config.options.autoload_known_extensions) {
		if (unrecognized_options_copy.empty()) {
			// Nothing to do
			return;
		}

		Connection con(*this);
		con.BeginTransaction();

		vector<string> extension_options;
		for (auto &option : unrecognized_options_copy) {
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
			// if the extension provided the option, it should no longer be unrecognized.
			D_ASSERT(config.options.unrecognized_options.find(name) == config.options.unrecognized_options.end());
			auto &context = *con.context;
			PhysicalSet::SetExtensionVariable(context, it->second, name, SetScope::GLOBAL, value);
			extension_options.push_back(name);
		}

		con.Commit();
	}
	if (!config.options.unrecognized_options.empty()) {
		ThrowExtensionSetUnrecognizedOptions(config.options.unrecognized_options);
	}
}

static duckdb_ext_api_v1 CreateAPIv1Wrapper() {
	return CreateAPIv1();
}

void DatabaseInstance::Initialize(const char *database_path, DBConfig *user_config) {
	DBConfig default_config;
	DBConfig *config_ptr = &default_config;
	if (user_config) {
		config_ptr = user_config;
	}

	Configure(*config_ptr, database_path);

	create_api_v1 = CreateAPIv1Wrapper;

	db_file_system = make_uniq<DatabaseFileSystem>(*this);
	db_manager = make_uniq<DatabaseManager>(*this);
	if (config.buffer_manager) {
		buffer_manager = config.buffer_manager;
	} else {
		buffer_manager = make_uniq<StandardBufferManager>(*this, config.options.temporary_directory);
	}

	log_manager = make_uniq<LogManager>(*this, LogConfig());
	log_manager->Initialize();

	external_file_cache = make_uniq<ExternalFileCache>(*this, config.options.enable_external_file_cache);
	result_set_manager = make_uniq<ResultSetManager>(*this);

	scheduler = make_uniq<TaskScheduler>(*this);
	object_cache = make_uniq<ObjectCache>();
	connection_manager = make_uniq<ConnectionManager>();
	extension_manager = make_uniq<ExtensionManager>(*this);

	// initialize the secret manager
	config.secret_manager->Initialize(*this);

	// resolve the type of the database we are opening
	auto &fs = FileSystem::GetFileSystem(*this);
	DBPathAndType::ResolveDatabaseType(fs, config.options.database_path, config.options.database_type);

	// initialize the system catalog
	db_manager->InitializeSystemCatalog();

	if (!config.options.database_type.empty() && !StringUtil::CIEquals(config.options.database_type, "duckdb")) {
		// if we are opening an extension database - load the extension
		if (!config.file_system) {
			throw InternalException("No file system!?");
		}
		auto entry = config.storage_extensions.find(config.options.database_type);
		if (entry == config.storage_extensions.end()) {
			ExtensionHelper::LoadExternalExtension(*this, *config.file_system, config.options.database_type);
		}
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
	instance->db_manager->FinalizeStartup();
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

ExternalFileCache &DatabaseInstance::GetExternalFileCache() {
	return *external_file_cache;
}

ResultSetManager &DatabaseInstance::GetResultSetManager() {
	return *result_set_manager;
}

ConnectionManager &DatabaseInstance::GetConnectionManager() {
	return *connection_manager;
}

ExtensionManager &DatabaseInstance::GetExtensionManager() {
	return *extension_manager;
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
		config.file_system = make_uniq<VirtualFileSystem>(FileSystem::CreateLocal());
	}
	if (database_path && !config.options.enable_external_access) {
		config.AddAllowedPath(database_path);
		config.AddAllowedPath(database_path + string(".wal"));
		if (!config.options.temporary_directory.empty()) {
			config.AddAllowedDirectory(config.options.temporary_directory);
		}
	}
	if (new_config.secret_manager) {
		config.secret_manager = std::move(new_config.secret_manager);
	}
	if (!new_config.storage_extensions.empty()) {
		config.storage_extensions = std::move(new_config.storage_extensions);
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
	config.block_allocator = make_uniq<BlockAllocator>(*config.allocator, config.options.default_block_alloc_size,
	                                                   DBConfig::GetSystemAvailableMemory(*config.file_system) * 8 / 10,
	                                                   config.options.block_allocator_size);
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
		config.buffer_pool = make_shared_ptr<BufferPool>(*config.block_allocator, config.options.maximum_memory,
		                                                 config.options.buffer_manager_track_eviction_timestamps,
		                                                 config.options.allocator_bulk_deallocation_flush_threshold);
	}
	config.db_cache_entry = std::move(new_config.db_cache_entry);
	config.path_manager = std::move(new_config.path_manager);
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

idx_t DuckDB::NumberOfThreads() {
	return instance->NumberOfThreads();
}

bool DatabaseInstance::ExtensionIsLoaded(const string &name) {
	return extension_manager->ExtensionIsLoaded(name);
}

bool DuckDB::ExtensionIsLoaded(const std::string &name) {
	return instance->ExtensionIsLoaded(name);
}

SettingLookupResult DatabaseInstance::TryGetCurrentSetting(const string &key, Value &result) const {
	// check the session values
	auto &db_config = DBConfig::GetConfig(*this);
	return db_config.TryGetCurrentSetting(key, result);
}

shared_ptr<EncryptionUtil> DatabaseInstance::GetEncryptionUtil() {
	if (!config.encryption_util || !config.encryption_util->SupportsEncryption()) {
		ExtensionHelper::TryAutoLoadExtension(*this, "httpfs");
	}

	if (config.encryption_util) {
		return config.encryption_util;
	}

	auto result = make_shared_ptr<duckdb_mbedtls::MbedTlsWrapper::AESStateMBEDTLSFactory>();

	return std::move(result);
}

ValidChecker &DatabaseInstance::GetValidChecker() {
	return db_validity;
}

const duckdb_ext_api_v1 DatabaseInstance::GetExtensionAPIV1() {
	D_ASSERT(create_api_v1);
	return create_api_v1();
}

LogManager &DatabaseInstance::GetLogManager() const {
	return *log_manager;
}

ValidChecker &ValidChecker::Get(DatabaseInstance &db) {
	return db.GetValidChecker();
}

} // namespace duckdb
