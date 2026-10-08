#include "duckdb/main/os_util.hpp"

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

bool OSUtil::IsSafeEnv(const string &name) {
	// the home directory and the resource limits of the job the database runs in
	static const char *SAFE_ENV[] = {"HOME", "USERPROFILE", "SLURM_CPUS_ON_NODE", "SLURM_MEM_PER_NODE",
	                                 "SLURM_MEM_PER_CPU"};
	for (auto safe : SAFE_ENV) {
		if (name == safe) {
			return true;
		}
	}
	return false;
}

void OSUtil::AddSafeEnv(const string &name) {
	lock_guard<mutex> guard(lock);
	safe_env.insert(name);
}

bool OSUtil::TryGetEnv(const string &name, string &value) {
	if (!IsSafeEnv(name) && !Settings::Get<EnableExternalAccessSetting>(db)) {
		lock_guard<mutex> guard(lock);
		if (safe_env.find(name) == safe_env.end()) {
			return false;
		}
	}
	return ReadEnv(name, value);
}

string OSUtil::GetEnv(const string &name) {
	string value;
	if (!TryGetEnv(name, value)) {
		return string();
	}
	return value;
}

#ifndef _WIN32

bool OSUtil::ReadEnv(const string &name, string &value) {
	const char *env = std::getenv(name.c_str());
	if (!env) {
		return false;
	}
	value = env;
	return true;
}

#else

bool OSUtil::ReadEnv(const string &name, string &value) {
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
