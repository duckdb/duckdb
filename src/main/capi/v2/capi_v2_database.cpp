#include "duckdb/main/capi_v2/capi_v2_internal.hpp"

#include "duckdb/common/types/value.hpp"

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_open(duckdb_v2_environment_handle env, duckdb_v2_str path, duckdb_v2_option_handle *options,
                               idx_t option_count, duckdb_v2_database_handle *out_db,
                               duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(env);
	DUCKDB_CHECK_ARG(out_db);
	DUCKDB_CHECK_ARG(path);

	return WithErrorHandler(err, [&]() {
		if (option_count > 0 && !options) {
			throw duckdb::InvalidInputException("option_count > 0 but options is null in duckdb_v2_open");
		}

		*out_db = nullptr;

		const auto config = duckdb::make_uniq<duckdb::DBConfig>();
		for (idx_t i = 0; i < option_count; i++) {
			const auto *opt = Convert(options[i]);
			if (!opt) {
				throw duckdb::InvalidInputException("null option handle in options array passed to duckdb_v2_open");
			}
			config->SetOptionByName(opt->name, duckdb::Value(opt->setting));
		}

		// Route through the env's DBInstanceCache with NEVER_CACHE so the path manager is shared across all opens.
		// File conflicts get detected, but no instance is memoized. Every open produces a fresh DatabaseInstance.

		auto *env_wrapper = Convert(env);
		const auto path_str = duckdb::string(Convert(path));
		const auto db = env_wrapper->cache->GetOrCreateInstance(path_str, *config, duckdb::CacheBehavior::NEVER_CACHE);
		auto wrapper = duckdb::make_uniq<CV2Database>(*env_wrapper, db);
		env_wrapper->open_database_count.fetch_add(1, std::memory_order_release);
		*out_db = Convert(wrapper.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_close(duckdb_v2_database_handle *db) {
	return WithErrorHandler(nullptr, [&]() {
		if (!db) {
			return;
		}
		if (*db) {
			const auto *wrapper = Convert(*db);
			auto &env = wrapper->env;
			delete wrapper;
			env.open_database_count.fetch_sub(1, std::memory_order_release);
			*db = nullptr;
		}
	});
}

DUCKDB_V2_ERROR duckdb_v2_database_option_set(duckdb_v2_database_handle db, duckdb_v2_option_handle option,
                                              duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(db);
	DUCKDB_CHECK_ARG(option);
	return WithErrorHandler(err, [&]() {
		const auto *db_wrapper = Convert(db);
		const auto *opt = Convert(option);
		// Use the database's internal ClientContext as the bridge into PhysicalSet::ApplyVariable.
		// Force GLOBAL scope: the internal context has no LOCAL settings of its own, and database-scoped settings only
		// make sense as GLOBAL anyway.
		auto &client = *db_wrapper->internal_connection->context;
		duckdb::PhysicalSet::SetVariable(client, opt->name, duckdb::SetScope::GLOBAL, duckdb::Value(opt->setting));
	});
}

DUCKDB_V2_ERROR duckdb_v2_database_option_get(duckdb_v2_database_handle db, duckdb_v2_identifier_t name,
                                              duckdb_v2_option_handle *out_option, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(db);
	DUCKDB_CHECK_ARG(name);
	DUCKDB_CHECK_ARG(out_option);
	*out_option = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto *db_wrapper = Convert(db);
		auto &client = *db_wrapper->internal_connection->context;
		auto &config = db_wrapper->database->instance->config;
		auto wrapper = CV2Option::FromName(client, config, Convert(name));
		*out_option = Convert(wrapper.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_database_option_get_count(duckdb_v2_database_handle db, idx_t *out_count,
                                                    duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(db);
	DUCKDB_CHECK_ARG(out_count);
	return WithErrorHandler(err, [&]() {
		const auto *db_wrapper = Convert(db);
		const auto &config = db_wrapper->database->instance->config;
		*out_count = duckdb::DBConfig::GetOptionCount() + config.GetExtensionSettings().size();
	});
}

DUCKDB_V2_ERROR duckdb_v2_database_option_get_by_index(duckdb_v2_database_handle db, idx_t index,
                                                       duckdb_v2_option_handle *out_option,
                                                       duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(db);
	DUCKDB_CHECK_ARG(out_option);
	*out_option = nullptr;
	return WithErrorHandler(err, [&]() {
		const auto *db_wrapper = Convert(db);
		auto &client = *db_wrapper->internal_connection->context;
		auto &config = db_wrapper->database->instance->config;

		auto wrapper = CV2Option::FromIndex(client, config, index);
		*out_option = Convert(wrapper.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_library_version(duckdb_v2_str *out_version, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(out_version);
	return WithErrorHandler(err, [&]() {
		const auto version = duckdb::DuckDB::LibraryVersion();
		*out_version = duckdb_v2_str {version, std::strlen(version)};
	});
}
