//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache_util.cpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/storage/external_file_cache_util.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/cache_validation_mode.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

bool GetCacheValidationMode(const OpenFileInfo &info, CacheValidationMode &mode) {
	if (info.extended_info == nullptr) {
		return false;
	}

	const auto &open_options = info.extended_info->options;
	const auto validate_entry = open_options.find("validate_external_file_cache");
	if (validate_entry == open_options.end()) {
		return false;
	}
	if (validate_entry->second.IsNull()) {
		throw InvalidInputException("Cannot use NULL as argument for validate_external_file_cache");
	}
	mode = EnumUtil::FromString<CacheValidationMode>(StringUtil::Upper(StringValue::Get(validate_entry->second)));
	return true;
}

CacheValidationMode GetCacheValidationMode(const OpenFileInfo &info, optional_ptr<ClientContext> client_context,
                                           DatabaseInstance &db) {
	// First check if explicitly set in options.
	CacheValidationMode mode;
	if (GetCacheValidationMode(info, mode)) {
		return mode;
	}

	// If client context is available, check client-local settings first, then fall back to database config.
	if (client_context) {
		return DBConfig::GetSetting<ValidateExternalFileCacheSetting>(*client_context);
	}

	// No client context, fall back to database config.
	auto &config = DBConfig::GetConfig(db);
	return config.options.validate_external_file_cache;
}

} // namespace duckdb
