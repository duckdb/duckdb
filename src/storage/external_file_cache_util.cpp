//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache_util.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/storage/external_file_cache_util.hpp"

#include "duckdb/common/enums/cache_validation_mode.hpp"
#include "duckdb/common/open_file_info.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

CacheValidationMode ExternalFileCacheUtil::GetCacheValidationMode(const OpenFileInfo &info,
                                                                  optional_ptr<ClientContext> client_context,
                                                                  DatabaseInstance &db) {
	// First check if explicitly set in OpenFileInfo options (per-file override).
	if (info.extended_info != nullptr) {
		const auto &open_options = info.extended_info->options;
		const auto validate_entry = open_options.find("validate_external_file_cache");
		if (validate_entry != open_options.end()) {
			if (validate_entry->second.IsNull()) {
				throw InvalidInputException("Cannot use NULL as argument for validate_external_file_cache");
			}
			const bool validate_cache = BooleanValue::Get(validate_entry->second);
			return validate_cache ? CacheValidationMode::VALIDATE_ALL : CacheValidationMode::NO_VALIDATION;
		}
	}

	// If client context is available, check client-local settings first, then fall back to database config.
	if (client_context) {
		return Settings::Get<ValidateExternalFileCacheSetting>(*client_context);
	}

	// No client context, fall back to database config.
	return Settings::Get<ValidateExternalFileCacheSetting>(db);
}

} // namespace duckdb
