#include "duckdb/main/os_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/windows_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"

#include <cstdlib>

namespace duckdb {

OSUtil::OSUtil(DatabaseInstance &db) : db(db) {
}

OSUtil &OSUtil::Get(DatabaseInstance &db) {
	return db.GetOSUtil();
}

bool OSUtil::GetEnv(const string &name, string &value) {
	if (!Settings::Get<EnableExternalAccessSetting>(db)) {
		throw PermissionException(
		    "Cannot read environment variable \"%s\" - environment access is disabled by configuration", name);
	}
	return GetEnvUnrestricted(name, value);
}

string OSUtil::GetEnv(const string &name) {
	string value;
	if (!GetEnv(name, value)) {
		return string();
	}
	return value;
}

string OSUtil::GetEnvUnrestricted(const string &name) {
	string value;
	if (!GetEnvUnrestricted(name, value)) {
		return string();
	}
	return value;
}

#ifndef _WIN32

bool OSUtil::GetEnvUnrestricted(const string &name, string &value) {
	const char *env = std::getenv(name.c_str());
	if (!env) {
		return false;
	}
	value = env;
	return true;
}

#else

bool OSUtil::GetEnvUnrestricted(const string &name, string &value) {
	auto name_w = WindowsUtil::UTF8ToUnicode(name.c_str());
	auto value_w = _wgetenv(name_w.c_str());
	if (!value_w) {
		return false;
	}
	value = WindowsUtil::UnicodeToUTF8(value_w);
	return true;
}

#endif

} // namespace duckdb
