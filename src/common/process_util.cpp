#include "duckdb/common/process_util.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/query_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/windows.hpp"

#ifdef _WIN32
#include "duckdb/common/windows_util.hpp"
#ifdef __MINGW32__
// need to manually define this for mingw
extern "C" WINBASEAPI BOOL QueryFullProcessImageNameW(HANDLE, DWORD, LPWSTR, PDWORD);
#endif
#else
#include <cerrno>
#include <csignal>
#include <unistd.h>
#endif

// includes for describing the process holding a lock
#if defined(__linux__) || defined(__APPLE__)
#include <pwd.h>
#endif

#if defined(__linux__)
#include <climits>
#include <cstring>
#include <libgen.h>
#elif defined(__APPLE__)
// See e.g.:
// https://opensource.apple.com/source/CarbonHeaders/CarbonHeaders-18.1/TargetConditionals.h.auto.html
#include <TargetConditionals.h>
#if not(defined(TARGET_OS_IPHONE) && TARGET_OS_IPHONE == 1)
#include <libproc.h>
#endif
#endif

namespace duckdb {

int64_t ProcessUtil::CurrentProcessId() {
#ifdef _WIN32
	return static_cast<int64_t>(GetCurrentProcessId());
#else
	return static_cast<int64_t>(getpid());
#endif
}

bool ProcessUtil::ProcessIsRunning(int64_t pid) {
	if (pid <= 0) {
		// not a process id anybody handed out: kill(0) would signal our own process group
		return true;
	}
#ifdef _WIN32
	auto process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, static_cast<DWORD>(pid));
	if (!process) {
		return GetLastError() != ERROR_INVALID_PARAMETER;
	}
	DWORD exit_code = 0;
	auto running = GetExitCodeProcess(process, &exit_code) && exit_code == STILL_ACTIVE;
	CloseHandle(process);
	return running;
#else
	// EPERM means it exists and belongs to somebody else
	return kill(static_cast<pid_t>(pid), 0) == 0 || errno == EPERM;
#endif
}

namespace {

string FormatProcessDescription(const string &process_name, const string &process_owner, int64_t pid) {
	return StringUtil::Format("%s%s",
	                          !process_name.empty() ? StringUtil::Format("%s (PID %d)", process_name, pid)
	                                                : StringUtil::Format("PID %d", pid),
	                          !process_owner.empty() ? StringUtil::Format(" by user %s", process_owner) : "");
}

} // namespace

#if defined(__APPLE__) && !TARGET_OS_IPHONE

string ProcessUtil::GetProcessDescription(FileSystem &fs, int64_t pid) {
	string process_name, process_owner;
	// macOS >= 10.7 has PROC_PIDT_SHORTBSDINFO
#ifdef PROC_PIDT_SHORTBSDINFO
	struct proc_bsdshortinfo proc;
	if (proc_pidinfo(static_cast<pid_t>(pid), PROC_PIDT_SHORTBSDINFO, 0, &proc, PROC_PIDT_SHORTBSDINFO_SIZE) ==
	    PROC_PIDT_SHORTBSDINFO_SIZE) {
		process_name = proc.pbsi_comm; // only a short version however, let's take it in case proc_pidpath() below fails
		auto pw = getpwuid(proc.pbsi_uid);
		if (pw) {
			process_owner = pw->pw_name;
		}
	}
#else
	return string();
#endif
	// try to get a better process name (full path)
	char full_exec_path[PROC_PIDPATHINFO_MAXSIZE];
	if (proc_pidpath(static_cast<pid_t>(pid), full_exec_path, PROC_PIDPATHINFO_MAXSIZE) > 0) {
		process_name = full_exec_path;
	}
	return FormatProcessDescription(process_name, process_owner, pid);
}

#elif defined(__linux__)

string ProcessUtil::GetProcessDescription(FileSystem &fs, int64_t pid) {
	string process_name, process_owner;

	try {
		auto cmdline_file = fs.OpenFile(StringUtil::Format("/proc/%d/cmdline", pid), FileFlags::FILE_FLAGS_READ);
		auto cmdline = cmdline_file->ReadLine(QueryContext());
		process_name = basename(const_cast<char *>(cmdline.c_str())); // NOLINT: old C API does not take const
	} catch (std::exception &) {
		// ignore
	}

	// we would like to provide a full path to the executable if possible but we might not have rights
	{
		char exe_target[PATH_MAX];
		memset(exe_target, '\0', PATH_MAX);
		auto proc_exe_link = StringUtil::Format("/proc/%d/exe", pid);
		auto readlink_n = readlink(proc_exe_link.c_str(), exe_target, PATH_MAX);
		if (readlink_n > 0) {
			process_name = exe_target;
		}
	}

	// try to find out who created that process
	try {
		auto loginuid_file = fs.OpenFile(StringUtil::Format("/proc/%d/loginuid", pid), FileFlags::FILE_FLAGS_READ);
		auto uid = std::stoi(loginuid_file->ReadLine(QueryContext()));
		auto pw = getpwuid(uid);
		if (pw) {
			process_owner = pw->pw_name;
		}
	} catch (std::exception &) {
		// ignore
	}

	return FormatProcessDescription(process_name, process_owner, pid);
}

#elif defined(_WIN32)

string ProcessUtil::GetProcessDescription(FileSystem &fs, int64_t pid) {
	auto process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, static_cast<DWORD>(pid));
	if (!process) {
		return string();
	}
	string process_name;
	WCHAR full_path[MAX_PATH];
	DWORD full_path_size = MAX_PATH;
	if (QueryFullProcessImageNameW(process, 0, full_path, &full_path_size) && full_path_size <= MAX_PATH) {
		process_name = WindowsUtil::UnicodeToUTF8(full_path);
	}
	CloseHandle(process);
	// the owner would need the process token, which we do not ask for
	return FormatProcessDescription(process_name, string(), pid);
}

#else

string ProcessUtil::GetProcessDescription(FileSystem &fs, int64_t pid) {
	return string();
}

#endif

} // namespace duckdb
