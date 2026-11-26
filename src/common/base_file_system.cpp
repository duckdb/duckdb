#include "duckdb/main/context.hpp"
#include "duckdb/common/base_file_system.hpp"
#include "duckdb/common/file_system.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/time_point.hpp"
#include "duckdb/common/compressed_file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/context.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/windows_util.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/logging/log_manager.hpp"

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

#ifndef __EMSCRIPTEN__
#include <sys/resource.h>
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

const FileOpenFlags FileFlags::FILE_FLAGS_READ = FileOpenFlags(FileOpenFlags::FILE_FLAGS_READ);
const FileOpenFlags FileFlags::FILE_FLAGS_WRITE = FileOpenFlags(FileOpenFlags::FILE_FLAGS_WRITE);
const FileOpenFlags FileFlags::FILE_FLAGS_DIRECT_IO = FileOpenFlags(FileOpenFlags::FILE_FLAGS_DIRECT_IO);
const FileOpenFlags FileFlags::FILE_FLAGS_FILE_CREATE = FileOpenFlags(FileOpenFlags::FILE_FLAGS_FILE_CREATE);
const FileOpenFlags FileFlags::FILE_FLAGS_FILE_CREATE_NEW = FileOpenFlags(FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
const FileOpenFlags FileFlags::FILE_FLAGS_APPEND = FileOpenFlags(FileOpenFlags::FILE_FLAGS_APPEND);
const FileOpenFlags FileFlags::FILE_FLAGS_PRIVATE = FileOpenFlags(FileOpenFlags::FILE_FLAGS_PRIVATE);
const FileOpenFlags FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS =
    FileOpenFlags(FileOpenFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
const FileOpenFlags FileFlags::FILE_FLAGS_PARALLEL_ACCESS = FileOpenFlags(FileOpenFlags::FILE_FLAGS_PARALLEL_ACCESS);
const FileOpenFlags FileFlags::FILE_FLAGS_EXCLUSIVE_CREATE = FileOpenFlags(FileOpenFlags::FILE_FLAGS_EXCLUSIVE_CREATE);
const FileOpenFlags FileFlags::FILE_FLAGS_NULL_IF_EXISTS = FileOpenFlags(FileOpenFlags::FILE_FLAGS_NULL_IF_EXISTS);
const FileOpenFlags FileFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS =
    FileOpenFlags(FileOpenFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS);
const FileOpenFlags FileFlags::FILE_FLAGS_DISABLE_LOGGING = FileOpenFlags(FileOpenFlags::FILE_FLAGS_DISABLE_LOGGING);
const FileOpenFlags FileFlags::FILE_FLAGS_ENABLE_EXTENSION_INSTALL =
    FileOpenFlags(FileOpenFlags::FILE_FLAGS_ENABLE_EXTENSION_INSTALL);

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

BaseFileSystem::~BaseFileSystem() {
}

FileSystem &FileSystem::GetFileSystem(ClientContext &context) {
	return context.GetContext().GetFileSystem();
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

bool BaseFileSystem::IsPathAbsolute(const string &path) {
	auto path_separator = PathSeparator(path);
	return PathMatched(path, path_separator) || StringUtil::StartsWith(path, "file:/");
}

string BaseFileSystem::PathSeparator(const string &path) {
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
	if (path.empty()) {
		return false;
	}
	if (path[0] != '/' && path[0] != '\\') {
		return false;
	}
	if (path.size() == 1) {
		return true;
	}
	if (path[1] == '/' || path[1] == '\\') {
		return false;
	}
	return true;
}

bool BaseFileSystem::IsPathAbsolute(const string &path) {
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

string BaseFileSystem::PathSeparator(const string &path) {
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

optional_idx FileSystem::GetFileDescriptorLimit() {
#if !defined(_WIN32) && !defined(__EMSCRIPTEN__)
	struct rlimit limit;
	if (getrlimit(RLIMIT_NOFILE, &limit) != 0 || limit.rlim_cur == RLIM_INFINITY) {
		return optional_idx();
	}
	if (limit.rlim_cur >= NumericLimits<idx_t>::Maximum()) {
		return optional_idx();
	}
	return optional_idx(static_cast<idx_t>(limit.rlim_cur));
#else
	return optional_idx();
#endif
}

idx_t FileSystem::GetFileDescriptorShare(idx_t file_descriptor_limit) {
	return MaxValue<idx_t>(file_descriptor_limit / 8, 1);
}

string BaseFileSystem::JoinPath(const string &a, const string &b) {
	// A remote URL may carry a "?query" suffix (object version, requester-pays, an SSE-C key, ...).
	// Appending a path segment after it would bury the query mid-path and corrupt both the query and
	// the object path, so join onto the path without the query and keep the query at the very end.
	auto scheme = a.find("://");
	if (scheme != string::npos) {
		auto query = a.find('?', scheme + 3);
		if (query != string::npos) {
			return Path::FromString(a.substr(0, query)).Join(b).ToString() + a.substr(query);
		}
	}
	return Path::FromString(a).Join(b).ToString();
}

string BaseFileSystem::ConvertSeparators(const string &path) {
	auto separator_str = PathSeparator(path);
	char separator = separator_str[0];
	if (separator == '/') {
		// on unix-based systems we only accept / as a separator
		return path;
	}
	// on windows-based systems we accept both
	return StringUtil::Replace(path, "/", separator_str);
}

string BaseFileSystem::ExtractName(const string &path) {
	if (path.empty()) {
		return string();
	}
	auto normalized_path = ConvertSeparators(path);
	auto sep = PathSeparator(path);
	auto splits = StringUtil::Split(normalized_path, sep);
	D_ASSERT(!splits.empty());
	return splits.back();
}

string BaseFileSystem::ExtractBaseName(const string &path) {
	if (path.empty()) {
		return string();
	}
	auto vec = StringUtil::Split(ExtractName(path), ".");
	D_ASSERT(!vec.empty());
	return vec[0];
}

string BaseFileSystem::ExtractExtension(const string &path) {
	if (path.empty()) {
		return string();
	}
	auto vec = StringUtil::Split(ExtractName(path), ".");
	if (vec.size() < 2) {
		return string();
	}
	return vec.back();
}

string FileSystem::GetDefaultHomeDirectory() {
#ifdef DUCKDB_WINDOWS
	return FileSystem::GetEnvVariable("USERPROFILE");
#else
	return FileSystem::GetEnvVariable("HOME");
#endif
}

string FileSystem::GetHomeDirectory(Context &context) {
	// read the home_directory setting first, if it is set
	Value result;
	if (context.TryGetCurrentSetting("home_directory", result)) {
		if (!result.IsNull() && !result.ToString().empty()) {
			return result.ToString();
		}
	}
	// fallback to the default home directories for the specified system
	return GetDefaultHomeDirectory();
}

string BaseFileSystem::GetHomeDirectory() {
	return FileSystem::GetDefaultHomeDirectory();
}

// Helper function to handle file:/ URLs
static idx_t GetFileUrlOffset(const string &path) {
	if (!StringUtil::StartsWith(path, "file:/")) {
		return 0;
	}

	// Url without host: file:/some/path
	if (path[6] != '/') {
#ifdef _WIN32
		return 6;
#else
		return 5;
#endif
	}

	// Url with empty host: file:///some/path
	if (path[7] == '/') {
#ifdef _WIN32
		return 8;
#else
		return 7;
#endif
	}

	// Url with localhost: file://localhost/some/path
	if (path.compare(7, 10, "localhost/") == 0) {
#ifdef _WIN32
		return 17;
#else
		return 16;
#endif
	}

	// unknown file:/ url format
	return 0;
}

string FileSystem::ExpandPath(Context &context, const string &path) {
	if (!path.empty() && path[0] == '~') {
		return ExpandPathWithHome(path, GetHomeDirectory(context));
	}
	return ExpandPathWithHome(path, string());
}

string FileSystem::ExpandPathWithHome(const string &path, const string &home_directory) {
	if (path.empty()) {
		return path;
	}
	if (path[0] == '~') {
		return home_directory + path.substr(1);
	}
	// handle file URIs
	auto file_offset = GetFileUrlOffset(path);
	if (file_offset > 0) {
		return path.substr(file_offset);
	}
	return path;
}

string BaseFileSystem::ExpandPath(const string &path) {
	return FileSystem::ExpandPathWithHome(path, GetHomeDirectory());
}

// LCOV_EXCL_START
unique_ptr<FileHandle>
BaseFileSystem::OpenFileExtended(Context &context, const OpenFileInfo &path,
                                 FileOpenFlags flags) { // NOLINT: virtual, overrides mutate flags
	throw NotImplementedException("%s: OpenFileExtended is not implemented!", GetName());
}

bool BaseFileSystem::ListFilesExtended(Context &context, const string &directory,
                                       const std::function<void(OpenFileInfo &info)> &callback) {
	throw NotImplementedException("%s: ListFilesExtended is not implemented!", GetName());
}

unique_ptr<FileHandle> BaseFileSystem::OpenFile(Context &context, const string &path, FileOpenFlags flags) {
	if (SupportsOpenFileExtended()) {
		return OpenFileExtended(context, OpenFileInfo(path), std::move(flags));
	}
	throw NotImplementedException("%s: OpenFile is not implemented!", GetName());
}

unique_ptr<FileHandle> BaseFileSystem::OpenFile(Context &context, const OpenFileInfo &file,
                                                const FileOpenFlags &flags) {
	if (SupportsOpenFileExtended()) {
		return OpenFileExtended(context, file, flags);
	} else {
		return OpenFile(context, file.path, flags);
	}
}

bool BaseFileSystem::SupportsOpenFileExtended() const {
	return false;
}

bool BaseFileSystem::SupportsListFilesExtended() const {
	return false;
}

bool BaseFileSystem::SupportsGlobExtended() const {
	return false;
}

void BaseFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("%s: Read (with location) is not implemented!", GetName());
}

void BaseFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("%s: Write (with location) is not implemented!", GetName());
}

int64_t BaseFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	throw NotImplementedException("%s: Read is not implemented!", GetName());
}

int64_t BaseFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	throw NotImplementedException("%s: Write is not implemented!", GetName());
}

bool BaseFileSystem::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	// This is not a required method. Derived FileSystems may optionally override/implement.
	return false;
}

int64_t BaseFileSystem::GetFileSize(FileHandle &handle) {
	throw NotImplementedException("%s: GetFileSize is not implemented!", GetName());
}

timestamp_t BaseFileSystem::GetLastModifiedTime(FileHandle &handle) {
	throw NotImplementedException("%s: GetLastModifiedTime is not implemented!", GetName());
}

string BaseFileSystem::GetVersionTag(FileHandle &handle) {
	// Used to check cache invalidation for httpfs files with an ETag in CachingFileSystem
	return "";
}

optional<timestamp_t> BaseFileSystem::GetCacheValidUntil(FileHandle &handle) {
	return nullopt;
}

FileType BaseFileSystem::GetFileType(FileHandle &handle) {
	return FileType::FILE_TYPE_INVALID;
}

FileMetadata BaseFileSystem::Stats(FileHandle &handle) {
	FileMetadata metadata;
	metadata.file_size = GetFileSize(handle);
	metadata.last_modification_time = GetLastModifiedTime(handle);
	metadata.file_type = GetFileType(handle);
	metadata.version_tag = GetVersionTag(handle);
	metadata.cache_valid_until = GetCacheValidUntil(handle);
	return metadata;
}

optional<FileMetadata> BaseFileSystem::GetStatsIfExists(Context &context, const OpenFileInfo &file) {
	auto handle = OpenFile(context, file, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
	if (!handle) {
		return nullopt;
	}
	return Stats(*handle);
}

void BaseFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	throw NotImplementedException("%s: Truncate is not implemented!", GetName());
}

bool BaseFileSystem::DirectoryExists(Context &context, const string &directory) {
	throw NotImplementedException("%s: DirectoryExists is not implemented!", GetName());
}

void BaseFileSystem::CreateDirectory(Context &context, const string &directory) {
	throw NotImplementedException("%s: CreateDirectory is not implemented!", GetName());
}

bool BaseFileSystem::CreateDirectoryExtended(Context &context, const string &directory,
                                             const CreateDirectoryOptions &options) {
	switch (options.mode) {
	case CreateDirectoryMode::SINGLE:
		if (!DirectoryExists(context, directory)) {
			CreateDirectory(context, directory);
		}
		return false;
	case CreateDirectoryMode::RECURSIVE: {
		auto parsed = Path::FromString(directory);
		vector<string> directories;
		while (!parsed.GetPathSegments().empty() && !DirectoryExists(context, parsed.ToString())) {
			directories.push_back(parsed.ToString());
			parsed = parsed.Parent();
		}
		if (parsed.GetPathSegments().empty() && !DirectoryExists(context, parsed.ToString())) {
			directories.push_back(parsed.ToString());
		}

		bool target_created = false;
		for (auto riter = directories.rbegin(); riter != directories.rend(); ++riter) {
			auto created = CreateDirectoryExtended(context, *riter, {CreateDirectoryMode::SINGLE});
			if (*riter == directories.front()) {
				target_created = created;
			}
		}
		return target_created;
	}
	default:
		throw InternalException("Unknown CreateDirectoryMode");
	}
}

void BaseFileSystem::CreateDirectoriesRecursive(Context &context, const string &path) {
	CreateDirectoryExtended(context, path, {CreateDirectoryMode::RECURSIVE});
}

void BaseFileSystem::RemoveDirectory(Context &context, const string &directory) {
	throw NotImplementedException("%s: RemoveDirectory is not implemented!", GetName());
}

bool BaseFileSystem::RemoveDirectoryExtended(Context &context, const string &directory,
                                             const RemoveDirectoryOptions &options) {
	switch (options.mode) {
	case RemoveDirectoryMode::SINGLE:
		return false;
	case RemoveDirectoryMode::RECURSIVE:
		RemoveDirectory(context, directory);
		return false;
	default:
		throw InternalException("Unknown RemoveDirectoryMode");
	}
}

bool FileSystem::IsDirectory(const OpenFileInfo &info) {
	if (!info.extended_info) {
		return false;
	}
	string type;
	if (!info.extended_info->TryGetOption("type", type)) {
		return false;
	}
	return type == "directory";
}

bool BaseFileSystem::ListFiles(Context &context, const string &directory,
                               const std::function<void(const string &, bool)> &callback) {
	if (SupportsListFilesExtended()) {
		return ListFilesExtended(context, directory, [&](const OpenFileInfo &info) {
			bool is_dir = FileSystem::IsDirectory(info);
			callback(info.path, is_dir);
		});
	}
	throw NotImplementedException("%s: ListFiles is not implemented!", GetName());
}

bool BaseFileSystem::ListFiles(Context &context, const string &directory,
                               const std::function<void(OpenFileInfo &info)> &callback) {
	if (SupportsListFilesExtended()) {
		return ListFilesExtended(context, directory, callback);
	} else {
		return ListFiles(context, directory, [&](const string &path, bool is_dir) {
			OpenFileInfo info(path);
			if (is_dir) {
				info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
				info.extended_info->options["type"] = "directory";
			}
			callback(info);
		});
	}
}

void BaseFileSystem::MoveFile(Context &context, const string &source, const string &target) {
	throw NotImplementedException("%s: MoveFile is not implemented!", GetName());
}

bool BaseFileSystem::FileExists(Context &context, const string &filename) {
	throw NotImplementedException("%s: FileExists is not implemented!", GetName());
}

bool BaseFileSystem::IsPipe(Context &context, const string &filename) {
	return false;
}

void BaseFileSystem::RemoveFile(Context &context, const string &filename) {
	throw NotImplementedException("%s: RemoveFile is not implemented!", GetName());
}

bool BaseFileSystem::TryRemoveFile(Context &context, const string &filename) {
	if (FileExists(context, filename)) {
		RemoveFile(context, filename);
		return true;
	}
	return false;
}

void BaseFileSystem::RemoveFiles(Context &context, const vector<string> &filenames) {
	for (const auto &filename : filenames) {
		TryRemoveFile(context, filename);
	}
}

void BaseFileSystem::FileSync(FileHandle &handle) {
	throw NotImplementedException("%s: FileSync is not implemented!", GetName());
}

void BaseFileSystem::AbortFileWrite(FileHandle &handle) {
	handle.Close();
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

vector<OpenFileInfo> BaseFileSystem::Glob(Context &context, const string &path) {
	if (SupportsGlobExtended()) {
		auto result = GlobFilesExtended(context, path, FileGlobOptions::ALLOW_EMPTY);
		return result->GetAllFiles();
	}
	throw NotImplementedException("%s: Glob is not implemented!", GetName());
}

unique_ptr<MultiFileList> BaseFileSystem::Glob(Context &context, const string &path, const FileGlobInput &input) {
	if (!SupportsGlobExtended()) {
		auto result = Glob(context, path);
		return make_uniq<SimpleMultiFileList>(std::move(result));
	} else {
		return GlobFilesExtended(context, path, input);
	}
}

unique_ptr<MultiFileList> BaseFileSystem::GlobFilesExtended(Context &context, const string &path,
                                                            const FileGlobInput &input) {
	throw NotImplementedException("%s: GlobFilesExtended is not implemented!", GetName());
}

bool BaseFileSystem::CanHandleFile(const string &fpath) {
	throw NotImplementedException("%s: CanHandleFile is not implemented!", GetName());
}

void BaseFileSystem::Seek(FileHandle &handle, idx_t location) {
	throw NotImplementedException("%s: Seek is not implemented!", GetName());
}

void BaseFileSystem::Reset(FileHandle &handle) {
	handle.Seek(0);
}

idx_t BaseFileSystem::SeekPosition(FileHandle &handle) {
	throw NotImplementedException("%s: SeekPosition is not implemented!", GetName());
}

bool BaseFileSystem::CanSeek() {
	throw NotImplementedException("%s: CanSeek is not implemented!", GetName());
}

bool BaseFileSystem::IsManuallySet() {
	return false;
}

FileWriteMode BaseFileSystem::GetWriteMode(FileHandle &handle) {
	return FileWriteMode::SEQUENTIAL;
}

unique_ptr<FileHandle> BaseFileSystem::OpenCompressedFile(Context &context, unique_ptr<FileHandle> handle, bool write) {
	throw NotImplementedException("%s: OpenCompressedFile is not implemented!", GetName());
}

bool BaseFileSystem::IsLocalFileSystem() const {
	return false;
}

bool BaseFileSystem::OnDiskFile(FileHandle &handle) {
	throw NotImplementedException("%s: OnDiskFile is not implemented!", GetName());
}
// LCOV_EXCL_STOP

bool BaseFileSystem::TryGetNetworkThroughput(FileHandle &handle, NetworkThroughputEstimate &result) {
	return false;
}

FileHandle::FileHandle(BaseFileSystem &file_system, string path_p, FileOpenFlags flags_p)
    : file_system(file_system), path(std::move(path_p)), flags(std::move(flags_p)) {
}

FileHandle::~FileHandle() {
}

int64_t FileHandle::Read(void *buffer, idx_t nr_bytes) {
	return file_system.Read(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes));
}

static optional_ptr<QueryProfiler> GetTrackingProfiler(bool track_io, Context &context) {
	if (!track_io) {
		return nullptr;
	}
	return context.TryGetQueryProfiler();
}

int64_t FileHandle::Read(Context &context, void *buffer, idx_t nr_bytes) {
	auto profiler = GetTrackingProfiler(track_io, context);
	const bool track = profiler != nullptr;
	const auto start = track ? TimePoint::Tick() : TimePoint();
	// A sequential read can return fewer bytes than requested (e.g. at EOF), so track the bytes
	// actually read rather than the requested amount.
	auto bytes_read = file_system.Read(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes));
	if (track) {
		const auto elapsed_us = NumericCast<idx_t>(start.ElapsedMicros());
		profiler->TrackBytesRead(UnsafeNumericCast<idx_t>(bytes_read), elapsed_us);
	}

	return bytes_read;
}

bool FileHandle::Trim(idx_t offset_bytes, idx_t length_bytes) {
	return file_system.Trim(*this, offset_bytes, length_bytes);
}

int64_t FileHandle::Write(void *buffer, idx_t nr_bytes) {
	return file_system.Write(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes));
}

int64_t FileHandle::Write(Context &context, void *buffer, idx_t nr_bytes) {
	auto profiler = GetTrackingProfiler(track_io, context);
	const bool track = profiler != nullptr;
	const auto start = track ? TimePoint::Tick() : TimePoint();
	auto bytes_written = file_system.Write(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes));
	if (bytes_written > 0 && track) {
		const auto elapsed_us = NumericCast<idx_t>(start.ElapsedMicros());
		profiler->TrackBytesWritten(UnsafeNumericCast<idx_t>(bytes_written), elapsed_us);
	}

	return bytes_written;
}

void FileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Read(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes), location);
}

void FileHandle::Read(Context &context, void *buffer, idx_t nr_bytes, idx_t location) {
	auto profiler = GetTrackingProfiler(track_io, context);
	const bool track = profiler != nullptr;
	const auto start = track ? TimePoint::Tick() : TimePoint();
	file_system.Read(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes), location);
	if (track) {
		const auto elapsed_us = NumericCast<idx_t>(start.ElapsedMicros());
		profiler->TrackBytesRead(nr_bytes, elapsed_us);
	}
}

void FileHandle::Write(Context &context, void *buffer, idx_t nr_bytes, idx_t location) {
	auto profiler = GetTrackingProfiler(track_io, context);
	const bool track = profiler != nullptr;
	const auto start = track ? TimePoint::Tick() : TimePoint();
	file_system.Write(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes), location);
	if (track) {
		const auto elapsed_us = NumericCast<idx_t>(start.ElapsedMicros());
		profiler->TrackBytesWritten(nr_bytes, elapsed_us);
	}
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

FileWriteMode FileHandle::GetWriteMode() {
	return file_system.GetWriteMode(*this);
}

FileCompressionType FileHandle::GetFileCompressionType() {
	return FileCompressionType::UNCOMPRESSED;
}

bool FileHandle::IsPipe(Context &context) {
	return file_system.IsPipe(context, path);
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

string FileHandle::ReadLine(Context &context) {
	string result;
	char buffer[1];
	while (true) {
		auto tuples_read = UnsafeNumericCast<idx_t>(Read(context, buffer, 1));
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

bool FileHandle::TryGetNetworkThroughput(NetworkThroughputEstimate &result) {
	return file_system.TryGetNetworkThroughput(*this, result);
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

void FileHandle::AbortWrite() {
	file_system.AbortFileWrite(*this);
}

FileType FileHandle::GetType() {
	return file_system.GetFileType(*this);
}

FileMetadata FileHandle::Stats() {
	return file_system.Stats(*this);
}

void FileHandle::TryAddLogger(Context &context) {
	if (flags.DisableLogging()) {
		return;
	}

	if (context.IsClient() && context.GetLogger().ShouldLog(FileSystemLogType::NAME, FileSystemLogType::LEVEL)) {
		logger = context.GetLoggerPtr();
		return;
	}

	auto &database = context.GetDatabase();
	if (Logger::Get(database).ShouldLog(FileSystemLogType::NAME, FileSystemLogType::LEVEL)) {
		logger = database.GetLogManager().GlobalLoggerReference();
	}
}

idx_t FileHandle::GetProgress() {
	throw NotImplementedException("GetProgress is not implemented for this file handle");
}

bool FileSystem::IsRemoteFile(const string &path) {
	string extension = "";
	return FileSystem::IsRemoteFile(path, extension);
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

string BaseFileSystem::CanonicalizePath(Context &context, const string &path_p) {
	return GenericCanonicalizePath(path_p);
}

string BaseFileSystem::GenericCanonicalizePath(const string &path_p) {
	// TODO: @benfleis - integrate this properly with Path (needs additional work)
	if (FileSystem::IsRemoteFile(path_p) || !Path::FromString(path_p).IsLocal()) {
		// don't canonicalize remote paths
		return path_p;
	}
	auto path_sep = PathSeparator(path_p);
	auto elements = StringUtil::Split(path_p, path_sep);

	deque<string> path_stack;
	string result;
	for (idx_t i = 0; i < elements.size(); i++) {
		if (elements[i].empty() || elements[i] == ".") {
			// we ignore empty and `.`
			continue;
		}
		if (elements[i] == "..") {
			// .. pops from stack if possible, if already at root its ignored
			if (!path_stack.empty()) {
				path_stack.pop_back();
			}
		} else {
			path_stack.push_back(elements[i]);
		}
	}
	// we lost the leading / in the split/loop so lets put it back
	if (path_p[0] == '/') {
		result = "/";
	}
	while (!path_stack.empty()) {
		result += path_stack.front();
		path_stack.pop_front();
		if (!path_stack.empty()) {
			result += path_sep;
		}
	}

	return result;
}

} // namespace duckdb
