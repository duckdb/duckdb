#include "duckdb/main/client_context_file_opener.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

SettingLookupResult ClientContextFileOpener::TryGetCurrentSetting(const string &key, Value &result) {
	return context.TryGetCurrentSetting(key, result);
}

// LCOV_EXCL_START
SettingLookupResult ClientContextFileOpener::TryGetCurrentSetting(const string &key, Value &result, FileOpenerInfo &) {
	return context.TryGetCurrentSetting(key, result);
}

ClientContext *FileOpener::TryGetClientContext(FileOpener *opener) {
	if (!opener) {
		return nullptr;
	}
	return opener->TryGetClientContext();
}

SettingLookupResult FileOpener::TryGetCurrentSetting(FileOpener *opener, const string &key, Value &result) {
	if (!opener) {
		return SettingLookupResult();
	}
	return opener->TryGetCurrentSetting(key, result);
}

SettingLookupResult FileOpener::TryGetCurrentSetting(FileOpener *opener, const string &key, Value &result,
                                                     FileOpenerInfo &info) {
	if (!opener) {
		return SettingLookupResult();
	}
	return opener->TryGetCurrentSetting(key, result, info);
}

SettingLookupResult FileOpener::TryGetCurrentSetting(const string &key, Value &result, FileOpenerInfo &info) {
	return this->TryGetCurrentSetting(key, result);
}
// LCOV_EXCL_STOP
} // namespace duckdb
