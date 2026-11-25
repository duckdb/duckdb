#include "duckdb/main/os_util.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/windows_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"

#include <cstdio>
#include <cstdlib>

namespace duckdb {

OSUtil::OSUtil(DatabaseInstance &db, unordered_set<string> configuration_env_p)
    : db(db), configuration_env(std::move(configuration_env_p)) {
}

OSUtil &OSUtil::Get(DatabaseInstance &db) {
	return db.GetOSUtil();
}

static constexpr idx_t MAX_SYSTEM_FILE_SIZE = 65536;

namespace {

enum class SystemFileArgument : uint8_t { NONE, PID, CGROUP_PATH };

struct SystemFileInfo {
	const char *path_format;
	SystemFileArgument argument;
};

SystemFileInfo GetSystemFileInfo(SystemFile file) {
	switch (file) {
	case SystemFile::PROCESS_CGROUPS:
		return {"/proc/self/cgroup", SystemFileArgument::NONE};
	case SystemFile::PROCESS_CMDLINE:
		return {"/proc/%s/cmdline", SystemFileArgument::PID};
	case SystemFile::PROCESS_LOGINUID:
		return {"/proc/%s/loginuid", SystemFileArgument::PID};
	case SystemFile::CGROUP_V1_CPU_QUOTA:
		return {"/sys/fs/cgroup/cpu%s/cpu.cfs_quota_us", SystemFileArgument::CGROUP_PATH};
	case SystemFile::CGROUP_V1_CPU_PERIOD:
		return {"/sys/fs/cgroup/cpu%s/cpu.cfs_period_us", SystemFileArgument::CGROUP_PATH};
	case SystemFile::CGROUP_V2_CPU_MAX:
		return {"/sys/fs/cgroup%s/cpu.max", SystemFileArgument::CGROUP_PATH};
	case SystemFile::CGROUP_V1_MEMORY_LIMIT:
		return {"/sys/fs/cgroup/memory%s/memory.limit_in_bytes", SystemFileArgument::CGROUP_PATH};
	case SystemFile::CGROUP_V2_MEMORY_MAX:
		return {"/sys/fs/cgroup%s/memory.max", SystemFileArgument::CGROUP_PATH};
	default:
		throw InternalException("Unknown system file");
	}
}

bool IsValidSystemFileArgument(SystemFileArgument type, const string &argument) {
	switch (type) {
	case SystemFileArgument::NONE:
		return argument.empty();
	case SystemFileArgument::PID:
		return !argument.empty() && argument.find_first_not_of("0123456789") == string::npos;
	case SystemFileArgument::CGROUP_PATH:
		// empty (the root) or an absolute path that cannot leave the cgroup directory
		return argument.empty() || (argument[0] == '/' && argument.find("..") == string::npos);
	default:
		throw InternalException("Unknown system file argument");
	}
}

} // namespace

bool OSUtil::TryReadSystemFile(SystemFile file, const string &argument, string &contents) {
	auto info = GetSystemFileInfo(file);
	if (!IsValidSystemFileArgument(info.argument, argument)) {
		return false;
	}
	auto path = info.argument == SystemFileArgument::NONE ? string(info.path_format)
	                                                      : StringUtil::Format(info.path_format, argument);
	auto handle = fopen(path.c_str(), "rb");
	if (!handle) {
		return false;
	}
	contents.clear();
	char buffer[4096];
	while (contents.size() < MAX_SYSTEM_FILE_SIZE) {
		auto to_read = MinValue<idx_t>(sizeof(buffer), MAX_SYSTEM_FILE_SIZE - contents.size());
		auto bytes_read = fread(buffer, 1, to_read, handle);
		contents.append(buffer, bytes_read);
		if (bytes_read < to_read) {
			break;
		}
	}
	const bool failed = ferror(handle) != 0;
	fclose(handle);
	return !failed;
}

bool OSUtil::IsConfigurationEnv(const string &name) {
	// the home directory, the resource limits of the job the database runs in, its proxy and its time zone
	static const char *CONFIGURATION_ENV[] = {
	    "HOME", "USERPROFILE", "SLURM_CPUS_ON_NODE", "SLURM_MEM_PER_NODE", "SLURM_MEM_PER_CPU", "HTTP_PROXY", "TZ"};
	for (auto known : CONFIGURATION_ENV) {
		if (name == known) {
			return true;
		}
	}
	return false;
}

bool OSUtil::GetConfigurationEnv(const string &name, string &value) {
	if (!IsConfigurationEnv(name)) {
		throw InternalException("Environment variable \"%s\" is not a configuration variable of the engine - read it "
		                        "through the database with GetEnv",
		                        name);
	}
	return ReadEnv(name, value);
}

string OSUtil::GetConfigurationEnv(const string &name) {
	string value;
	if (!GetConfigurationEnv(name, value)) {
		return string();
	}
	return value;
}

bool OSUtil::GetEnv(const string &name, string &value) {
	if (!Settings::Get<EnableExternalAccessSetting>(db)) {
		throw PermissionException(
		    "Cannot read environment variable \"%s\" - environment access is disabled by configuration", name);
	}
	return ReadEnv(name, value);
}

string OSUtil::GetEnv(const string &name) {
	string value;
	if (!GetEnv(name, value)) {
		return string();
	}
	return value;
}

bool OSUtil::GetEnvUnrestricted(const string &name, string &value) {
	if (!IsConfigurationEnv(name) && configuration_env.find(name) == configuration_env.end()) {
		throw InternalException("Environment variable \"%s\" is not a configuration variable - read it with GetEnv",
		                        name);
	}
	return ReadEnv(name, value);
}

string OSUtil::GetEnvUnrestricted(const string &name) {
	string value;
	if (!GetEnvUnrestricted(name, value)) {
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
