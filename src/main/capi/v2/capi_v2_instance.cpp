#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/database_path_and_type.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace duckdb {
namespace capiv2 {

CV2Instance::CV2Instance(CV2Environment &env) : env(env), config(make_uniq<DBConfig>()) {
}

void CV2Instance::Start() {
	if (IsStarted()) {
		return;
	}
	// Routed through the env's DBInstanceCache so every instance shares its path manager: opening one file twice is
	// detected across instances, while no instance is memoized.
	database = env.cache->CreateEmptyInstance(*config);
	config.reset();
	staged_settings.clear();
	internal_connection = make_uniq<Connection>(*database);
}

DuckDB &CV2Instance::GetDatabase() {
	Start();
	return *database;
}

//! Runs `action` inside a transaction on the internal connection, the way the ATTACH / DETACH operators run inside
//! their statement's transaction.
template <class T>
static void WithTransaction(Connection &connection, T action) {
	connection.BeginTransaction();
	try {
		action(*connection.context);
	} catch (...) {
		connection.Rollback();
		throw;
	}
	connection.Commit();
}

void CV2Instance::Attach(const string &path, const Identifier &name,
                         optional_ptr<const CV2AttachOptions> attach_options, bool make_default) {
	if (attach_options && &attach_options->instance != this) {
		throw InvalidInputException("the attach options were created from a different instance handle");
	}
	Start();
	WithTransaction(*internal_connection, [&](ClientContext &context) {
		// Mirrors PhysicalAttach for `ATTACH 'path' AS name (options)`.
		auto &instance = *database->instance;
		AttachInfo info;
		info.path = path;
		info.name = name;
		if (attach_options) {
			info.options = attach_options->options;
		}
		AttachOptions options(info.options, instance.config.options.access_mode);
		options.original_path = path;
		if (options.db_type.empty()) {
			DBPathAndType::ExtractExtensionPrefix(info.path, options.db_type);
		}
		if (info.name.empty()) {
			info.name = AttachedDatabase::ExtractDatabaseName(info.path, FileSystem::GetFileSystem(instance));
		}
		// The host opening a file is not external access: allow it the way the main database path is allowed.
		if (options.db_type.empty() && !DBConfig::IsInMemoryDatabase(info.path.c_str()) &&
		    !FileSystem::IsRemoteFile(info.path) && !Settings::Get<EnableExternalAccessSetting>(instance)) {
			instance.config.AddAllowedDatabasePath(info.path);
		}
		auto &db_manager = DatabaseManager::Get(instance);
		auto attached = db_manager.AttachDatabase(context, info, options);
		if (make_default) {
			db_manager.SetDefaultDatabase(attached->GetName());
		}
	});
}

//! Finds the database attached under `path` as a name, else the one attached from it as a path, disambiguated by
//! the name instance_attach derives from the path. Throws when nothing matches.
static shared_ptr<AttachedDatabase> FindAttachedDatabase(DatabaseInstance &instance, const string &path) {
	auto &db_manager = DatabaseManager::Get(instance);
	if (auto by_name = db_manager.GetDatabase(Identifier(path))) {
		if (!by_name->IsSystem() && !by_name->IsTemporary()) {
			return by_name;
		}
	}
	auto &fs = FileSystem::GetFileSystem(instance);
	string stripped = path;
	string db_type;
	DBPathAndType::ExtractExtensionPrefix(stripped, db_type);
	const bool in_memory = DBConfig::IsInMemoryDatabase(stripped.c_str());
	// File-based DuckDB databases are attached under their canonical path; other attaches keep the path verbatim.
	string canonical = stripped;
	if (!in_memory && db_type.empty() && !FileSystem::IsRemoteFile(stripped)) {
		try {
			canonical = fs.CanonicalizePath(stripped);
		} catch (...) { // NOLINT(bugprone-empty-catch): an uncanonicalizable path just falls back to verbatim matching
		}
	}
	const auto derived_name = AttachedDatabase::ExtractDatabaseName(stripped, fs);

	shared_ptr<AttachedDatabase> match;
	idx_t match_count = 0;
	for (auto &db : db_manager.GetDatabases()) {
		if (db->IsSystem() || db->IsTemporary()) {
			continue;
		}
		auto &catalog = db->GetCatalog();
		bool matches;
		if (in_memory) {
			matches = catalog.InMemory();
		} else {
			const auto db_path = catalog.GetDBPath();
			matches = db_path == canonical || db_path == stripped;
		}
		if (!matches) {
			continue;
		}
		match_count++;
		if (!match || db->GetName() == derived_name) {
			match = db;
		}
	}
	if (!match) {
		throw InvalidInputException("no database is attached from or under '%s'", path);
	}
	if (match_count > 1 && match->GetName() != derived_name) {
		throw InvalidInputException("several databases are attached from '%s'; refer to one by name in SQL", path);
	}
	return match;
}

void CV2Instance::Detach(const string &path) {
	if (!IsStarted()) {
		throw InvalidInputException("no database is attached from or under '%s'", path);
	}
	auto &instance = *database->instance;
	auto attached = FindAttachedDatabase(instance, path);
	WithTransaction(*internal_connection, [&](ClientContext &context) {
		DatabaseManager::Get(instance).DetachDatabase(context, attached->GetName(), OnEntryNotFound::THROW_EXCEPTION,
		                                              true);
	});
}

void CV2Instance::SetDefault(const string &path) {
	if (!IsStarted()) {
		throw InvalidInputException("no database is attached from or under '%s'", path);
	}
	auto &instance = *database->instance;
	auto attached = FindAttachedDatabase(instance, path);
	DatabaseManager::Get(instance).SetDefaultDatabase(attached->GetName());
}

void CV2Instance::SetOption(const Identifier &name, const string &setting) {
	if (!IsStarted()) {
		// Staged for startup: the only route to options that cannot change once the instance runs.
		config->SetOptionByName(name, Value(setting));
		auto option = DBConfig::GetOptionByName(name);
		staged_settings[option ? Identifier(option->name) : name] = setting;
		return;
	}
	// Force GLOBAL scope: the internal context has no LOCAL settings of its own, and instance-scoped settings only
	// make sense as GLOBAL anyway.
	PhysicalSet::SetVariable(*internal_connection->context, name, SetScope::GLOBAL, Value(setting));
}

unique_ptr<CV2Option> CV2Instance::GetOption(std::string_view name) {
	if (!IsStarted()) {
		return CV2Option::FromName(CV2OptionSource(*config, staged_settings), name);
	}
	return CV2Option::FromName(CV2OptionSource(*internal_connection->context), name);
}

idx_t CV2Instance::GetOptionCount() {
	if (!IsStarted()) {
		return CV2Option::Count(CV2OptionSource(*config, staged_settings));
	}
	return CV2Option::Count(CV2OptionSource(*internal_connection->context));
}

unique_ptr<CV2Option> CV2Instance::GetOptionByIndex(idx_t index) {
	if (!IsStarted()) {
		return CV2Option::FromIndex(CV2OptionSource(*config, staged_settings), index);
	}
	return CV2Option::FromIndex(CV2OptionSource(*internal_connection->context), index);
}

} // namespace capiv2
} // namespace duckdb

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_instance_create(duckdb_v2_environment_handle env, duckdb_v2_instance_handle *out_instance,
                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(env);
	DUCKDB_CHECK_ARG(out_instance);
	*out_instance = nullptr;
	return WithErrorHandler(err, [&]() {
		auto *env_wrapper = Convert(env);
		auto wrapper = duckdb::make_uniq<CV2Instance>(*env_wrapper);
		env_wrapper->instance_count.fetch_add(1, std::memory_order_release);
		*out_instance = Convert(wrapper.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_instance_destroy(duckdb_v2_instance_handle *instance) {
	return WithErrorHandler(nullptr, [&]() {
		if (!instance) {
			return;
		}
		if (*instance) {
			const auto *wrapper = Convert(*instance);
			auto &env = wrapper->env;
			delete wrapper;
			env.instance_count.fetch_sub(1, std::memory_order_release);
			*instance = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_instance_attach(duckdb_v2_instance_handle instance, duckdb_v2_str path,
                                          duckdb_v2_identifier_t *name, duckdb_v2_attach_options_handle options,
                                          bool make_default, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(path);
	if (name) {
		DUCKDB_CHECK_ARG(*name);
	}
	return WithErrorHandler(err, [&]() {
		auto &wrapper = *Convert(instance);
		duckdb::lock_guard<duckdb::mutex> guard(wrapper.lock);
		duckdb::Identifier attach_name = name ? duckdb::Identifier(Convert(*name)) : duckdb::Identifier();
		wrapper.Attach(duckdb::string(Convert(path)), attach_name, Convert(options), make_default);
	});
}

DUCKDB_V2_ERROR duckdb_v2_attach_options_create(duckdb_v2_instance_handle instance,
                                                duckdb_v2_attach_options_handle *out_options,
                                                duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(out_options);
	*out_options = nullptr;
	return WithErrorHandler(err, [&]() {
		auto options = duckdb::make_uniq<CV2AttachOptions>(*Convert(instance));
		*out_options = Convert(options.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_attach_options_set(duckdb_v2_attach_options_handle options, duckdb_v2_identifier_t key,
                                             duckdb_v2_str setting, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(options);
	DUCKDB_CHECK_ARG(key);
	DUCKDB_CHECK_ARG(setting);
	return WithErrorHandler(err, [&]() {
		// Keys are unquoted SQL identifiers, which the parser lowercases.
		auto lowered = duckdb::StringUtil::Lower(duckdb::string(Convert(key)));
		Convert(options)->options[lowered] = duckdb::Value(duckdb::string(Convert(setting)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_attach_options_destroy(duckdb_v2_attach_options_handle *options) {
	return WithErrorHandler(nullptr, [&]() {
		if (!options) {
			return;
		}
		if (*options) {
			delete Convert(*options);
			*options = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_instance_detach(duckdb_v2_instance_handle instance, duckdb_v2_str path,
                                          duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(path);
	return WithErrorHandler(err, [&]() {
		auto &wrapper = *Convert(instance);
		duckdb::lock_guard<duckdb::mutex> guard(wrapper.lock);
		wrapper.Detach(duckdb::string(Convert(path)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_instance_set_default(duckdb_v2_instance_handle instance, duckdb_v2_str path,
                                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(path);
	return WithErrorHandler(err, [&]() {
		auto &wrapper = *Convert(instance);
		duckdb::lock_guard<duckdb::mutex> guard(wrapper.lock);
		wrapper.SetDefault(duckdb::string(Convert(path)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_instance_set_option(duckdb_v2_instance_handle instance, duckdb_v2_identifier_t name,
                                              duckdb_v2_str setting, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(setting);
	return WithErrorHandler(err, [&]() {
		auto &wrapper = *Convert(instance);
		duckdb::lock_guard<duckdb::mutex> guard(wrapper.lock);
		wrapper.SetOption(duckdb::Identifier(Convert(name)), duckdb::string(Convert(setting)));
	});
}

DUCKDB_V2_ERROR duckdb_v2_instance_get_option_by_name(duckdb_v2_instance_handle instance, duckdb_v2_identifier_t name,
                                                      duckdb_v2_option_handle *out_option,
                                                      duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(out_option);
	*out_option = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &wrapper = *Convert(instance);
		duckdb::lock_guard<duckdb::mutex> guard(wrapper.lock);
		*out_option = Convert(wrapper.GetOption(Convert(name)).release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_instance_get_option_count(duckdb_v2_instance_handle instance, idx_t *out_count,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(out_count);
	return WithErrorHandler(err, [&]() {
		auto &wrapper = *Convert(instance);
		duckdb::lock_guard<duckdb::mutex> guard(wrapper.lock);
		*out_count = wrapper.GetOptionCount();
	});
}

DUCKDB_V2_ERROR duckdb_v2_instance_get_option_by_index(duckdb_v2_instance_handle instance, idx_t index,
                                                       duckdb_v2_option_handle *out_option,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(instance);
	DUCKDB_CHECK_ARG(out_option);
	*out_option = nullptr;
	return WithErrorHandler(err, [&]() {
		auto &wrapper = *Convert(instance);
		duckdb::lock_guard<duckdb::mutex> guard(wrapper.lock);
		*out_option = Convert(wrapper.GetOptionByIndex(index).release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_library_version(duckdb_v2_str *out_version, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_version);
	return WithErrorHandler(err, [&]() {
		const auto version = duckdb::DuckDB::LibraryVersion();
		*out_version = duckdb_v2_str {version, std::strlen(version)};
	});
}
