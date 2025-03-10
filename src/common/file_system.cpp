#include "duckdb/common/file_system.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/windows_util.hpp"
#include "duckdb/common/operator/multiply.hpp"

#include <cstdint>
#include <cstdio>

#ifndef _WIN32
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef __MVS__
#define _XOPEN_SOURCE_EXTENDED 1
#include <sys/resource.h>
// enjoy - https://reviews.llvm.org/D92110
#define PATH_MAX _XOPEN_PATH_MAX
#endif

#else
#include <string>
#include <sysinfoapi.h>

#ifdef __MINGW32__
// need to manually define this for mingw
extern "C" WINBASEAPI BOOL WINAPI GetPhysicallyInstalledSystemMemory(PULONGLONG);
#endif

#undef FILE_CREATE // woo mingw
#endif

namespace duckdb {

constexpr FileOpenFlags FileFlags::FILE_FLAGS_READ;
constexpr FileOpenFlags FileFlags::FILE_FLAGS_WRITE;
constexpr FileOpenFlags FileFlags::FILE_FLAGS_DIRECT_IO;
constexpr FileOpenFlags FileFlags::FILE_FLAGS_FILE_CREATE;
constexpr FileOpenFlags FileFlags::FILE_FLAGS_FILE_CREATE_NEW;
constexpr FileOpenFlags FileFlags::FILE_FLAGS_APPEND;
constexpr FileOpenFlags FileFlags::FILE_FLAGS_PRIVATE;
constexpr FileOpenFlags FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS;
constexpr FileOpenFlags FileFlags::FILE_FLAGS_PARALLEL_ACCESS;
constexpr FileOpenFlags FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE;
constexpr FileOpenFlags FileFlags::FILE_FLAGS_NULL_IF_EXISTS;

void FileOpenFlags::Verify() {
#ifdef DEBUG
	bool is_read = flags & FileOpenFlags::FILE_FLAGS_READ;
	bool is_write = flags & FileOpenFlags::FILE_FLAGS_WRITE;
	bool is_create =
	    (flags & FileOpenFlags::FILE_FLAGS_FILE_CREATE) || (flags & FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	bool is_private = (flags & FileOpenFlags::FILE_FLAGS_PRIVATE);
	bool null_if_not_exists = flags & FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS;
	bool exclusive_create = flags & FileOpenFlags::FILE_FLAGS_EXCLUSIVE_CREATE;
	bool null_if_exists = flags & FileOpenFlags::FILE_FLAGS_NULL_IF_EXISTS;

	// require either READ or WRITE (or both)
	D_ASSERT(is_read || is_write);
	// CREATE/Append flags require writing
	D_ASSERT(is_write || !(flags & FileOpenFlags::FILE_FLAGS_APPEND));
	D_ASSERT(is_write || !(flags & FileOpenFlags::FILE_FLAGS_FILE_CREATE));
	D_ASSERT(is_write || !(flags & FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW));
	// cannot combine CREATE and CREATE_NEW flags
	D_ASSERT(!(flags & FileOpenFlags::FILE_FLAGS_FILE_CREATE && flags & FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW));

	// For is_private can only be set along with a create flag
	D_ASSERT(!is_private || is_create);
	// FILE_FLAGS_NULL_IF_NOT_EXISTS cannot be combined with CREATE/CREATE_NEW
	D_ASSERT(!(null_if_not_exists && is_create));
	// FILE_FLAGS_EXCLUSIVE_CREATE only can be combined with CREATE/CREATE_NEW
	D_ASSERT(!exclusive_create || is_create);
	// FILE_FLAGS_NULL_IF_EXISTS only can be set with EXCLUSIVE_CREATE
	D_ASSERT(!null_if_exists || exclusive_create);
#endif
}

FileSystem::~FileSystem() {
}

FileSystem &FileSystem::GetFileSystem(ClientContext &context) {
	auto &client_data = ClientData::Get(context);
	return *client_data.client_file_system;
}

bool PathMatched(const string &path, const string &sub_path) {
	return path.rfind(sub_path, 0) == 0;
}

#ifndef _WIN32

string FileSystem::GetEnvVariable(const string &name) {
	const char *env = getenv(name.c_str());
	if (!env) {
		return string();
	}
	return env;
}

bool FileSystem::IsPathAbsolute(const string &path) {
	auto path_separator = PathSeparator(path);
	return PathMatched(path, path_separator) || StringUtil::StartsWith(path, "file:/");
}

string FileSystem::PathSeparator(const string &path) {
	return "/";
}

void FileSystem::SetWorkingDirectory(const string &path) {
	if (chdir(path.c_str()) != 0) {
		throw IOException("Could not change working directory!");
	}
}

optional_idx FileSystem::GetAvailableMemory() {
	errno = 0;

#ifdef __MVS__
	struct rlimit limit;
	int rlim_rc = getrlimit(RLIMIT_AS, &limit);
	idx_t max_memory = MinValue<idx_t>(limit.rlim_max, UINTPTR_MAX);
#else
	idx_t max_memory = MinValue<idx_t>((idx_t)sysconf(_SC_PHYS_PAGES) * (idx_t)sysconf(_SC_PAGESIZE), UINTPTR_MAX);
#endif
	if (errno != 0) {
		return optional_idx();
	}
	return max_memory;
}

optional_idx FileSystem::GetAvailableDiskSpace(const string &path) {
	struct statvfs vfs;

	auto ret = statvfs(path.c_str(), &vfs);
	if (ret == -1) {
		return optional_idx();
	}
	auto block_size = vfs.f_frsize;
	// These are the blocks available for creating new files or extending existing ones
	auto available_blocks = vfs.f_bfree;
	idx_t available_disk_space = DConstants::INVALID_INDEX;
	if (!TryMultiplyOperator::Operation(static_cast<idx_t>(block_size), static_cast<idx_t>(available_blocks),
	                                    available_disk_space)) {
		return optional_idx();
	}
	return available_disk_space;
}

string FileSystem::GetWorkingDirectory() {
	auto buffer = make_unsafe_uniq_array<char>(PATH_MAX);
	char *ret = getcwd(buffer.get(), PATH_MAX);
	if (!ret) {
		throw IOException("Could not get working directory!");
	}
	return string(buffer.get());
}

string FileSystem::NormalizeAbsolutePath(const string &path) {
	D_ASSERT(IsPathAbsolute(path));
	return path;
}

#else

string FileSystem::GetEnvVariable(const string &env) {
	// first convert the environment variable name to the correct encoding
	auto env_w = WindowsUtil::UTF8ToUnicode(env.c_str());
	// use _wgetenv to get the value
	auto res_w = _wgetenv(env_w.c_str());
	if (!res_w) {
		// no environment variable of this name found
		return string();
	}
	return WindowsUtil::UnicodeToUTF8(res_w);
}

static bool StartsWithSingleBackslash(const string &path) {
	if (path.size() < 2) {
		return false;
	}
	if (path[0] != '/' && path[0] != '\\') {
		return false;
	}
	if (path[1] == '/' || path[1] == '\\') {
		return false;
	}
	return true;
}

bool FileSystem::IsPathAbsolute(const string &path) {
	// 1) A single backslash or forward-slash
	if (StartsWithSingleBackslash(path)) {
		return true;
	}
	// 2) special "long paths" on windows
	if (PathMatched(path, "\\\\?\\")) {
		return true;
	}
	// 3) a network path
	if (PathMatched(path, "\\\\")) {
		return true;
	}
	// 4) A disk designator with a backslash (e.g., C:\ or C:/)
	auto path_aux = path;
	path_aux.erase(0, 1);
	if (PathMatched(path_aux, ":\\") || PathMatched(path_aux, ":/")) {
		return true;
	}
	return false;
}

string FileSystem::NormalizeAbsolutePath(const string &path) {
	D_ASSERT(IsPathAbsolute(path));
	auto result = StringUtil::Lower(FileSystem::ConvertSeparators(path));
	if (StartsWithSingleBackslash(result)) {
		// Path starts with a single backslash or forward slash
		// prepend drive letter
		return GetWorkingDirectory().substr(0, 2) + result;
	}
	return result;
}

string FileSystem::PathSeparator(const string &path) {
	if (StringUtil::StartsWith(path, "file:")) {
		return "/";
	} else {
		return "\\";
	}
}

void FileSystem::SetWorkingDirectory(const string &path) {
	auto unicode_path = WindowsUtil::UTF8ToUnicode(path.c_str());
	if (!SetCurrentDirectoryW(unicode_path.c_str())) {
		throw IOException("Could not change working directory to \"%s\"", path);
	}
}

optional_idx FileSystem::GetAvailableMemory() {
	ULONGLONG available_memory_kb;
	if (GetPhysicallyInstalledSystemMemory(&available_memory_kb)) {
		return MinValue<idx_t>(available_memory_kb * 1000, UINTPTR_MAX);
	}
	// fallback: try GlobalMemoryStatusEx
	MEMORYSTATUSEX mem_state;
	mem_state.dwLength = sizeof(MEMORYSTATUSEX);

	if (GlobalMemoryStatusEx(&mem_state)) {
		return MinValue<idx_t>(mem_state.ullTotalPhys, UINTPTR_MAX);
	}
	return optional_idx();
}

optional_idx FileSystem::GetAvailableDiskSpace(const string &path) {
	ULARGE_INTEGER available_bytes, total_bytes, free_bytes;

	auto unicode_path = WindowsUtil::UTF8ToUnicode(path.c_str());
	if (!GetDiskFreeSpaceExW(unicode_path.c_str(), &available_bytes, &total_bytes, &free_bytes)) {
		return optional_idx();
	}
	(void)total_bytes;
	(void)free_bytes;
	return NumericCast<idx_t>(available_bytes.QuadPart);
}

string FileSystem::GetWorkingDirectory() {
	idx_t count = GetCurrentDirectoryW(0, nullptr);
	if (count == 0) {
		throw IOException("Could not get working directory!");
	}
	auto buffer = make_unsafe_uniq_array<wchar_t>(count);
	idx_t ret = GetCurrentDirectoryW(count, buffer.get());
	if (count != ret + 1) {
		throw IOException("Could not get working directory!");
	}
	return WindowsUtil::UnicodeToUTF8(buffer.get());
}

#endif

string FileSystem::JoinPath(const string &a, const string &b) {
	// FIXME: sanitize paths
	return a.empty() ? b : a + PathSeparator(a) + b;
}

string FileSystem::ConvertSeparators(const string &path) {
	auto separator_str = PathSeparator(path);
	char separator = separator_str[0];
	if (separator == '/') {
		// on unix-based systems we only accept / as a separator
		return path;
	}
	// on windows-based systems we accept both
	return StringUtil::Replace(path, "/", separator_str);
}

string FileSystem::ExtractName(const string &path) {
	if (path.empty()) {
		return string();
	}
	auto normalized_path = ConvertSeparators(path);
	auto sep = PathSeparator(path);
	auto splits = StringUtil::Split(normalized_path, sep);
	D_ASSERT(!splits.empty());
	return splits.back();
}

string FileSystem::ExtractBaseName(const string &path) {
	if (path.empty()) {
		return string();
	}
	auto vec = StringUtil::Split(ExtractName(path), ".");
	D_ASSERT(!vec.empty());
	return vec[0];
}

string FileSystem::GetHomeDirectory(optional_ptr<FileOpener> opener) {
	// read the home_directory setting first, if it is set
	if (opener) {
		Value result;
		if (opener->TryGetCurrentSetting("home_directory", result)) {
			if (!result.IsNull() && !result.ToString().empty()) {
				return result.ToString();
			}
		}
	}
	// fallback to the default home directories for the specified system
#ifdef DUCKDB_WINDOWS
	return FileSystem::GetEnvVariable("USERPROFILE");
#else
	return FileSystem::GetEnvVariable("HOME");
#endif
}

string FileSystem::GetHomeDirectory() {
	return GetHomeDirectory(nullptr);
}

string FileSystem::ExpandPath(const string &path, optional_ptr<FileOpener> opener) {
	if (path.empty()) {
		return path;
	}
	if (path[0] == '~') {
		return GetHomeDirectory(opener) + path.substr(1);
	}
	return path;
}

string FileSystem::ExpandPath(const string &path) {
	return FileSystem::ExpandPath(path, nullptr);
}

// LCOV_EXCL_START
unique_ptr<FileHandle> FileSystem::OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("%s: OpenFile is not implemented!", GetName());
}

void FileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("%s: Read (with location) is not implemented!", GetName());
}

bool FileSystem::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	// This is not a required method. Derived FileSystems may optionally override/implement.
	return false;
}

void FileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("%s: Write (with location) is not implemented!", GetName());
}

int64_t FileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	throw NotImplementedException("%s: Read is not implemented!", GetName());
}

int64_t FileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	throw NotImplementedException("%s: Write is not implemented!", GetName());
}

int64_t FileSystem::GetFileSize(FileHandle &handle) {
	throw NotImplementedException("%s: GetFileSize is not implemented!", GetName());
}

time_t FileSystem::GetLastModifiedTime(FileHandle &handle) {
	throw NotImplementedException("%s: GetLastModifiedTime is not implemented!", GetName());
}

FileType FileSystem::GetFileType(FileHandle &handle) {
	return FileType::FILE_TYPE_INVALID;
}

void FileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	throw NotImplementedException("%s: Truncate is not implemented!", GetName());
}

bool FileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("%s: DirectoryExists is not implemented!", GetName());
}

void FileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("%s: CreateDirectory is not implemented!", GetName());
}

void FileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("%s: RemoveDirectory is not implemented!", GetName());
}

bool FileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                           FileOpener *opener) {
	throw NotImplementedException("%s: ListFiles is not implemented!", GetName());
}

void FileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("%s: MoveFile is not implemented!", GetName());
}

bool FileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("%s: FileExists is not implemented!", GetName());
}

bool FileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return false;
}

void FileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("%s: RemoveFile is not implemented!", GetName());
}

void FileSystem::FileSync(FileHandle &handle) {
	throw NotImplementedException("%s: FileSync is not implemented!", GetName());
}

bool FileSystem::HasGlob(const string &str) {
	for (idx_t i = 0; i < str.size(); i++) {
		switch (str[i]) {
		case '*':
		case '?':
		case '[':
			return true;
		default:
			break;
		}
	}
	return false;
}

vector<string> FileSystem::Glob(const string &path, FileOpener *opener) {
	throw NotImplementedException("%s: Glob is not implemented!", GetName());
}

void FileSystem::RegisterSubSystem(unique_ptr<FileSystem> sub_fs) {
	throw NotImplementedException("%s: Can't register a sub system on a non-virtual file system", GetName());
}

void FileSystem::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> sub_fs) {
	throw NotImplementedException("%s: Can't register a sub system on a non-virtual file system", GetName());
}

void FileSystem::UnregisterSubSystem(const string &name) {
	throw NotImplementedException("%s: Can't unregister a sub system on a non-virtual file system", GetName());
}

unique_ptr<FileSystem> FileSystem::ExtractSubSystem(const string &name) {
	throw NotImplementedException("%s: Can't extract a sub system on a non-virtual file system", GetName());
}

void FileSystem::SetDisabledFileSystems(const vector<string> &names) {
	throw NotImplementedException("%s: Can't disable file systems on a non-virtual file system", GetName());
}

vector<string> FileSystem::ListSubSystems() {
	throw NotImplementedException("%s: Can't list sub systems on a non-virtual file system", GetName());
}

bool FileSystem::CanHandleFile(const string &fpath) {
	throw NotImplementedException("%s: CanHandleFile is not implemented!", GetName());
}

static string LookupExtensionForPattern(const string &pattern) {
	for (const auto &entry : EXTENSION_FILE_PREFIXES) {
		if (StringUtil::StartsWith(pattern, entry.name)) {
			return entry.extension;
		}
	}
	return "";
}

vector<string> FileSystem::GlobFiles(const string &pattern, ClientContext &context, FileGlobOptions options) {
	auto result = Glob(pattern);
	if (result.empty()) {
		string required_extension = LookupExtensionForPattern(pattern);
		if (!required_extension.empty() && !context.db->ExtensionIsLoaded(required_extension)) {
			auto &dbconfig = DBConfig::GetConfig(context);
			if (!ExtensionHelper::CanAutoloadExtension(required_extension) ||
			    !dbconfig.options.autoload_known_extensions) {
				auto error_message =
				    "File " + pattern + " requires the extension " + required_extension + " to be loaded";
				error_message =
				    ExtensionHelper::AddExtensionInstallHintToErrorMsg(context, error_message, required_extension);
				throw MissingExtensionException(error_message);
			}
			// an extension is required to read this file, but it is not loaded - try to load it
			ExtensionHelper::AutoLoadExtension(context, required_extension);
			// success! glob again
			// check the extension is loaded just in case to prevent an infinite loop here
			if (!context.db->ExtensionIsLoaded(required_extension)) {
				throw InternalException("Extension load \"%s\" did not throw but somehow the extension was not loaded",
				                        required_extension);
			}
			return GlobFiles(pattern, context, options);
		}
		if (options == FileGlobOptions::DISALLOW_EMPTY) {
			throw IOException("No files found that match the pattern \"%s\"", pattern);
		}
	}
	return result;
}

void FileSystem::Seek(FileHandle &handle, idx_t location) {
	throw NotImplementedException("%s: Seek is not implemented!", GetName());
}

void FileSystem::Reset(FileHandle &handle) {
	handle.Seek(0);
}

idx_t FileSystem::SeekPosition(FileHandle &handle) {
	throw NotImplementedException("%s: SeekPosition is not implemented!", GetName());
}

bool FileSystem::CanSeek() {
	throw NotImplementedException("%s: CanSeek is not implemented!", GetName());
}

bool FileSystem::IsManuallySet() {
	return false;
}

unique_ptr<FileHandle> FileSystem::OpenCompressedFile(unique_ptr<FileHandle> handle, bool write) {
	throw NotImplementedException("%s: OpenCompressedFile is not implemented!", GetName());
}

bool FileSystem::OnDiskFile(FileHandle &handle) {
	throw NotImplementedException("%s: OnDiskFile is not implemented!", GetName());
}
// LCOV_EXCL_STOP

FileHandle::FileHandle(FileSystem &file_system, string path_p, FileOpenFlags flags)
    : file_system(file_system), path(std::move(path_p)), flags(flags) {
}

FileHandle::~FileHandle() {
}

int64_t FileHandle::Read(void *buffer, idx_t nr_bytes) {
	return file_system.Read(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes));
}

bool FileHandle::Trim(idx_t offset_bytes, idx_t length_bytes) {
	return file_system.Trim(*this, offset_bytes, length_bytes);
}

int64_t FileHandle::Write(void *buffer, idx_t nr_bytes) {
	return file_system.Write(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes));
}

void FileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Read(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes), location);
}

void FileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Write(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes), location);
}

void FileHandle::Seek(idx_t location) {
	file_system.Seek(*this, location);
}

void FileHandle::Reset() {
	file_system.Reset(*this);
}

idx_t FileHandle::SeekPosition() {
	return file_system.SeekPosition(*this);
}

bool FileHandle::CanSeek() {
	return file_system.CanSeek();
}

FileCompressionType FileHandle::GetFileCompressionType() {
	return FileCompressionType::UNCOMPRESSED;
}

bool FileHandle::IsPipe() {
	return file_system.IsPipe(path);
}

string FileHandle::ReadLine() {
	string result;
	char buffer[1];
	while (true) {
		auto tuples_read = UnsafeNumericCast<idx_t>(Read(buffer, 1));
		if (tuples_read == 0 || buffer[0] == '\n') {
			return result;
		}
		if (buffer[0] != '\r') {
			result += buffer[0];
		}
	}
}

bool FileHandle::OnDiskFile() {
	return file_system.OnDiskFile(*this);
}

idx_t FileHandle::GetFileSize() {
	return NumericCast<idx_t>(file_system.GetFileSize(*this));
}

void FileHandle::Sync() {
	file_system.FileSync(*this);
}

void FileHandle::Truncate(int64_t new_size) {
	file_system.Truncate(*this, new_size);
}

FileType FileHandle::GetType() {
	return file_system.GetFileType(*this);
}

idx_t FileHandle::GetProgress() {
	throw NotImplementedException("GetProgress is not implemented for this file handle");
}

bool FileSystem::IsRemoteFile(const string &path) {
	string extension = "";
	return IsRemoteFile(path, extension);
}

bool FileSystem::IsRemoteFile(const string &path, string &extension) {
	for (const auto &entry : EXTENSION_FILE_PREFIXES) {
		if (StringUtil::StartsWith(path, entry.name)) {
			extension = entry.extension;
			return true;
		}
	}
	return false;
}

} // namespace duckdb
