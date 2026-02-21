#include "duckdb/common/file_system.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
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

#ifdef __MVS__
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
constexpr FileOpenFlags FileFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS;
constexpr FileOpenFlags FileFlags::FILE_FLAGS_DISABLE_LOGGING;

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
	auto result = FileSystem::ConvertSeparators(path);
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

// Handle both forward & backward slashes in 1 func, hiding the _WIN32 #def
static inline bool IsPathSeparator(char c) {
	return (c == '/' || c == '\\');
}

//
// Parse path (only) proto://authority/path/to/file.txt
// - All URI paths are absolute, and URIs of the form proto://authority will be assigned path="/"
// - Not full URI parsing, only for URI paths (e.g., no query, fragment, etc.)
//
static void ParseURIScheme(const string &input, struct Path &parsed) {
	parsed.is_absolute = true;

	const size_t auth_begin = input.find("://") + 3;
	D_ASSERT(auth_begin >= 4); // non-empty protocol
	parsed.scheme = input.substr(0, auth_begin);

	const size_t path_begin = input.find('/', auth_begin);
	D_ASSERT(path_begin == string::npos || path_begin > auth_begin);
	parsed.authority = input.substr(auth_begin, (path_begin - auth_begin));
	parsed.anchor = '/';
	if (path_begin != string::npos) {
		parsed.path = input.substr(path_begin + 1);
	}
}

static void ParseFilePathTail(const string &input, struct Path &parsed) {
	size_t pos = 0;

#if defined(_WIN32)
	if (input.size() >= 2 && input[1] == ':' && StringUtil::CharacterIsAlpha(input[0])) {
		parsed.anchor.append(1, input[0]);
		parsed.anchor.append(1, ':');
		pos = 2;
	}
#endif

	if (input.size() > pos && IsPathSeparator(input[pos])) {
		parsed.anchor.append(1, parsed.separator);
		parsed.is_absolute = true;
		pos += 1;
	}
	parsed.path = input.substr(pos);
}

//
// UNC Scheme as we handle it:
// - scheme = \\
// - authority = server\share
// - anchor = \
// - path = < the rest >
//
static void ParseUNCScheme(const string &input, struct Path &parsed) {
	D_ASSERT(input.size() >= 4 && input[0] == '\\' && input[1] == '\\');

	static const char extended_prefix[] = R"(\\?\)";
	static const char extended_unc_prefix[] = R"(\\?\UNC\)";
	const bool extended = StringUtil::StartsWith(input, extended_prefix);
	const bool extended_unc = extended && StringUtil::StartsWith(input, extended_unc_prefix);

	parsed.separator = '\\';

	if (extended && !extended_unc) {
		parsed.anchor = '\\';
		ParseFilePathTail(input.substr(4), parsed);
		parsed.scheme = R"(\\?)";
		return;
	}

	const auto server_begin = extended_unc ? sizeof(extended_unc_prefix) - 1 : 2;
	const auto share_begin = input.find_first_of('\\', server_begin) + 1;
	auto pos = input.find_first_of("/\\", share_begin);
	const auto share_len = (pos == string::npos ? input.size() : pos) - share_begin;
	if (share_begin - server_begin <= 1 || share_len == 0) {
		throw InvalidInputException("Path: UNC path missing server\\share: %s", input);
	}

	parsed.scheme = extended_unc ? extended_unc_prefix : R"(\\)";
	parsed.authority = input.substr(server_begin, share_begin + share_len + -server_begin);
	parsed.anchor = '\\';
	if (pos != string::npos && (pos + 1) < input.size()) {
		parsed.path = input.substr(pos + 1);
	}
	parsed.is_absolute = true;
}

//
// Parse and normalize file:/{1,3} schemes. See
// https://en.wikipedia.org/wiki/File_URI_scheme and our docs below.
//
// DuckDB supports using the file: protocol. It currently supports the following formats:
//
//  file:/some/path (host omitted completely)
//  file:///some/path (empty host)
//  file://localhost/some/path (localhost as host)
//
// Note that the following formats are not supported because they are non-standard:
//
//   file:some/relative/path (relative path)
//   file://some/path (double-slash path)
//
// Additionally, the file: protocol currently does not support remote (non-localhost) hosts.
//
static void ParseFileSchemes(const string &input, struct Path &parsed) {
	parsed.is_absolute = true;

	size_t input_len = input.size();
	size_t path_begin = string::npos;
	D_ASSERT(input_len >= 6 && StringUtil::StartsWith(input, "file:/"));

	if (/* file:/// */ input_len >= 8 && input[6] == '/' && input[7] == '/') {
		parsed.scheme = "file://";
		parsed.anchor = "/";
		path_begin = 8;
	} else if (/* file:// */ input_len >= 7 && input[6] == '/') {
		ParseURIScheme(input, parsed);
		if (StringUtil::Lower(parsed.authority) != "localhost") {
			throw InvalidInputException("Path: file:// scheme only supports localhost authority, got: %s",
			                            parsed.authority);
		}
		path_begin = parsed.scheme.size() + parsed.authority.size() + parsed.anchor.size();
	} else /* file:/ */ {
		parsed.scheme = "file:";
		parsed.anchor = "/";
		path_begin = 6;
	}
	ParseFilePathTail(input.substr(path_begin), parsed);

	D_ASSERT(parsed.scheme == "file:" || parsed.scheme == "file://");
	D_ASSERT(parsed.scheme == "file://" || !parsed.HasAuthority());
}

static void MaybeAppendSegment(vector<string> &segments, string::const_iterator begin, string::const_iterator end) {
	if (end == begin || (end == begin + 1 && *begin == '.')) {
		return;
	}
	const string segment(begin, end);
	if (segment == ".." && !segments.empty() && segments.back() != "..") {
		segments.pop_back();
	} else {
		segments.push_back(segment);
	}
}

static void SegmentAndNormalizePath(const string &path, vector<string> &segments) {
	auto prev_pos = path.begin();
	for (auto pos = prev_pos; pos != path.end(); pos++) {
		if (IsPathSeparator(*pos)) {
			MaybeAppendSegment(segments, prev_pos, pos);
			prev_pos = pos + 1;
		}
	}
	MaybeAppendSegment(segments, prev_pos, path.end());
}

string Path::ToString() const {
	return scheme + authority + anchor + path;
}

Path Path::FromString(const string &raw) {
	Path parsed;
	const auto first_slash_pos = raw.find_first_of(R"(/\)");
	const auto scheme_pos = raw.find("://");
	const auto drive_leads = (false
#if defined(_WIN32)
	                          || (first_slash_pos == 1 && StringUtil::CharacterIsAlpha(raw[0]))
#endif
	);
	parsed.separator = first_slash_pos == string::npos ? '/' : raw[first_slash_pos];

	if (StringUtil::StartsWith(raw, "file:/")) {
		ParseFileSchemes(raw, parsed);
	}
#if defined(_WIN32)
	else if (raw.size() >= 5 && StringUtil::StartsWith(raw, R"(\\)") && raw.find_first_of('\\', 3) != string::npos) {
		ParseUNCScheme(raw, parsed);
	}
#endif
	else if (scheme_pos != string::npos && scheme_pos > 1 && scheme_pos < first_slash_pos && !drive_leads) {
		ParseURIScheme(raw, parsed);
	} else {
		ParseFilePathTail(raw, parsed);
	}
	parsed.NormalizeSegments();
	D_ASSERT(parsed.HasScheme() || !parsed.HasAuthority());
	D_ASSERT(parsed.anchor.size() <= 4);
	D_ASSERT(parsed.anchor.size() + parsed.path.size() > 0);
	return parsed;
}

bool Path::HasDrive() const {
	return GetDriveChar() != 0;
}

char Path::GetDriveChar() const {
	const auto size = anchor.size();
	D_ASSERT(size <= 4);
	if (size == 2) {
		return anchor[0];
	} else if (size == 3) {
		return anchor[anchor[1] == ':' ? 0 : 1];
	} else if (size == 4) {
		return anchor[1];
	}
	return 0;
}

vector<string> Path::GetPathSegments() const {
	vector<string> segments;
	SegmentAndNormalizePath(path, segments);
	return segments;
}

void Path::NormalizeSegments() {
	// normalize URI schemes to lowercase; UNC prefixes (\\, \\?, \\?\UNC\) are not URI schemes
	if (!scheme.empty() && !IsPathSeparator(scheme[0])) {
		scheme = StringUtil::Lower(scheme);
	}

	if (HasAnchor()) {
		string normal;
		if (IsPathSeparator(anchor[0])) {
			normal.append(1, separator);
		}
		auto drive_char = std::find_if_not(anchor.begin(), anchor.end(), IsPathSeparator);
		if (drive_char != anchor.end()) {
			normal.append(1, StringUtil::CharacterToUpper(*drive_char));
			normal.append(1, ':');
			if (IsPathSeparator(anchor.back())) {
				normal.append(1, separator);
			}
		}
		anchor = normal;
	}

	vector<string> segments;
	SegmentAndNormalizePath(path, segments);

	path.clear();
	for (const auto &segment : segments) {
		if (is_absolute && path.empty() && segment == "..") {
			continue;
		}
		if (!path.empty()) {
			path += this->separator;
		}
		path += segment;
	}
	if (scheme.empty() && anchor.empty() && path.empty()) {
		path = '.';
	}
}

void Path::Join(const Path &rhs) {
	auto &lhs = *this;
#if defined(_WIN32)
	const bool win_local = (lhs.separator == '\\') || lhs.HasDrive();
	const auto auth_is_eq =
	    win_local ? StringUtil::CIEquals(lhs.authority, rhs.authority) : (lhs.authority == rhs.authority);
	const auto path_is_prefix =
	    win_local ? StringUtil::CIStartsWith(rhs.path, lhs.path) : StringUtil::StartsWith(rhs.path, lhs.path);
#else
	const auto auth_is_eq = (lhs.authority == rhs.authority);
	const auto path_is_prefix = StringUtil::StartsWith(rhs.path, lhs.path);
#endif
	if (!rhs.is_absolute && !rhs.HasDrive()) {
		lhs.path = lhs.path + lhs.separator + rhs.path;
		lhs.NormalizeSegments();
	} else if (auth_is_eq && path_is_prefix && lhs.scheme == rhs.scheme && lhs.anchor == rhs.anchor &&
	           (lhs.path.empty() || lhs.path.size() == rhs.path.size() || IsPathSeparator(rhs.path[lhs.path.size()]))) {
		if (lhs.path.size() < rhs.path.size()) {
			lhs.path = rhs.path;
		}
	} else {
		throw InvalidInputException("Path: cannot join incompatible paths: \"%s\" onto \"%s\"", rhs.ToString(),
		                            lhs.ToString());
	}
}

string FileSystem::JoinPath(const string &a, const string &b) {
	auto lhs = Path::FromString(a);
	auto rhs = Path::FromString(b);
	lhs.Join(rhs);
	return lhs.ToString();
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

string FileSystem::ExtractExtension(const string &path) {
	if (path.empty()) {
		return string();
	}
	auto vec = StringUtil::Split(ExtractName(path), ".");
	if (vec.size() < 2) {
		return string();
	}
	return vec.back();
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
unique_ptr<FileHandle> FileSystem::OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
                                                    optional_ptr<FileOpener> opener) {
	throw NotImplementedException("%s: OpenFileExtended is not implemented!", GetName());
}

bool FileSystem::ListFilesExtended(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
                                   optional_ptr<FileOpener> opener) {
	throw NotImplementedException("%s: ListFilesExtended is not implemented!", GetName());
}

unique_ptr<FileHandle> FileSystem::OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener) {
	if (SupportsOpenFileExtended()) {
		return OpenFileExtended(OpenFileInfo(path), flags, opener);
	}
	throw NotImplementedException("%s: OpenFile is not implemented!", GetName());
}

unique_ptr<FileHandle> FileSystem::OpenFile(const OpenFileInfo &file, FileOpenFlags flags,
                                            optional_ptr<FileOpener> opener) {
	if (SupportsOpenFileExtended()) {
		return OpenFileExtended(file, flags, opener);
	} else {
		return OpenFile(file.path, flags, opener);
	}
}

bool FileSystem::SupportsOpenFileExtended() const {
	return false;
}

bool FileSystem::SupportsListFilesExtended() const {
	return false;
}

void FileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	throw NotImplementedException("%s: Read (with location) is not implemented!", GetName());
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

bool FileSystem::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	// This is not a required method. Derived FileSystems may optionally override/implement.
	return false;
}

int64_t FileSystem::GetFileSize(FileHandle &handle) {
	throw NotImplementedException("%s: GetFileSize is not implemented!", GetName());
}

timestamp_t FileSystem::GetLastModifiedTime(FileHandle &handle) {
	throw NotImplementedException("%s: GetLastModifiedTime is not implemented!", GetName());
}

string FileSystem::GetVersionTag(FileHandle &handle) {
	// Used to check cache invalidation for httpfs files with an ETag in CachingFileSystem
	return "";
}

FileType FileSystem::GetFileType(FileHandle &handle) {
	return FileType::FILE_TYPE_INVALID;
}

FileMetadata FileSystem::Stats(FileHandle &handle) {
	throw NotImplementedException("%s: Stats is not implemented!", GetName());
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

void FileSystem::CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener) {
	// To avoid hitting directories we have no permission for when using allowed_directories + enable_external_access,
	// we construct the list of directories to be created depth-first. This avoids calling DirectoryExists on a parent
	// dir that is not in the allowed_directories list

	auto sep = PathSeparator(path);
	vector<string> dirs_to_create;

	string current_prefix = path;

	StringUtil::RTrim(current_prefix, sep);

	// Strip directories from the path until we hit a directory that exists
	while (!current_prefix.empty() && !DirectoryExists(current_prefix)) {
		auto found = current_prefix.find_last_of(sep);

		// Push back the root dir
		if (found == string::npos || found == 0) {
			dirs_to_create.push_back(current_prefix);
			current_prefix = "";
			break;
		}

		// Add the directory to the directories to be created
		dirs_to_create.push_back(current_prefix.substr(found, current_prefix.size() - found));

		// Update the current prefix to remove the current dir
		current_prefix = current_prefix.substr(0, found);
	}

	// Create the directories one by one
	for (vector<string>::reverse_iterator riter = dirs_to_create.rbegin(); riter != dirs_to_create.rend(); ++riter) {
		current_prefix += *riter;
		CreateDirectory(current_prefix);
	}
}

void FileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("%s: RemoveDirectory is not implemented!", GetName());
}

bool FileSystem::IsDirectory(const OpenFileInfo &info) {
	if (!info.extended_info) {
		return false;
	}
	auto entry = info.extended_info->options.find("type");
	if (entry == info.extended_info->options.end()) {
		return false;
	}
	return StringValue::Get(entry->second) == "directory";
}

bool FileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                           FileOpener *opener) {
	if (SupportsListFilesExtended()) {
		return ListFilesExtended(
		    directory,
		    [&](const OpenFileInfo &info) {
			    bool is_dir = IsDirectory(info);
			    callback(info.path, is_dir);
		    },
		    opener);
	}
	throw NotImplementedException("%s: ListFiles is not implemented!", GetName());
}

bool FileSystem::ListFiles(const string &directory, const std::function<void(OpenFileInfo &info)> &callback,
                           optional_ptr<FileOpener> opener) {
	if (SupportsListFilesExtended()) {
		return ListFilesExtended(directory, callback, opener);
	} else {
		return ListFiles(
		    directory,
		    [&](const string &path, bool is_dir) {
			    OpenFileInfo info(path);
			    if (is_dir) {
				    info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
				    info.extended_info->options["type"] = "directory";
			    }
			    callback(info);
		    },
		    opener.get());
	}
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

bool FileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	if (FileExists(filename, opener)) {
		RemoveFile(filename, opener);
		return true;
	}
	return false;
}

void FileSystem::RemoveFiles(const vector<string> &filenames, optional_ptr<FileOpener> opener) {
	for (const auto &filename : filenames) {
		TryRemoveFile(filename, opener);
	}
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

vector<OpenFileInfo> FileSystem::Glob(const string &path, FileOpener *opener) {
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

bool FileSystem::SubSystemIsDisabled(const string &name) {
	throw NotImplementedException("%s: Non-virtual file system does not have subsystems", GetName());
}

bool FileSystem::IsDisabledForPath(const string &path) {
	throw NotImplementedException("%s: Non-virtual file system does not have subsystems", GetName());
}

vector<string> FileSystem::ListSubSystems() {
	throw NotImplementedException("%s: Can't list sub systems on a non-virtual file system", GetName());
}

bool FileSystem::CanHandleFile(const string &fpath) {
	throw NotImplementedException("%s: CanHandleFile is not implemented!", GetName());
}

vector<OpenFileInfo> FileSystem::GlobFiles(const string &pattern, ClientContext &context, const FileGlobInput &input) {
	auto result = Glob(pattern);
	if (result.empty()) {
		if (input.behavior == FileGlobOptions::FALLBACK_GLOB && !HasGlob(pattern)) {
			// if we have no glob in the pattern and we have an extension, we try to glob
			if (!HasGlob(pattern)) {
				if (input.extension.empty()) {
					throw InternalException("FALLBACK_GLOB requires an extension to be specified");
				}
				string new_pattern = JoinPath(JoinPath(pattern, "**"), "*." + input.extension);
				result = GlobFiles(new_pattern, context, FileGlobOptions::ALLOW_EMPTY);
				if (!result.empty()) {
					// we found files by globbing the target as if it was a directory - return them
					return result;
				}
			}
		}
		if (input.behavior == FileGlobOptions::FALLBACK_GLOB || input.behavior == FileGlobOptions::DISALLOW_EMPTY) {
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

unique_ptr<FileHandle> FileSystem::OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle, bool write) {
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

int64_t FileHandle::Read(QueryContext context, void *buffer, idx_t nr_bytes) {
	if (context.GetClientContext() != nullptr) {
		context.GetClientContext()->client_data->profiler->AddToCounter(MetricType::TOTAL_BYTES_READ, nr_bytes);
	}

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

void FileHandle::Read(QueryContext context, void *buffer, idx_t nr_bytes, idx_t location) {
	if (context.GetClientContext() != nullptr) {
		context.GetClientContext()->client_data->profiler->AddToCounter(MetricType::TOTAL_BYTES_READ, nr_bytes);
	}

	file_system.Read(*this, buffer, UnsafeNumericCast<int64_t>(nr_bytes), location);
}

void FileHandle::Write(QueryContext context, void *buffer, idx_t nr_bytes, idx_t location) {
	if (context.GetClientContext() != nullptr) {
		context.GetClientContext()->client_data->profiler->AddToCounter(MetricType::TOTAL_BYTES_WRITTEN, nr_bytes);
	}

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

string FileHandle::ReadLine(QueryContext context) {
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

FileMetadata FileHandle::Stats() {
	return file_system.Stats(*this);
}

void FileHandle::TryAddLogger(FileOpener &opener) {
	if (flags.DisableLogging()) {
		return;
	}

	auto context = opener.TryGetClientContext();
	if (context && Logger::Get(*context).ShouldLog(FileSystemLogType::NAME, FileSystemLogType::LEVEL)) {
		logger = context->logger;
		return;
	}

	auto database = opener.TryGetDatabase();
	if (database && Logger::Get(*database).ShouldLog(FileSystemLogType::NAME, FileSystemLogType::LEVEL)) {
		logger = database->GetLogManager().GlobalLoggerReference();
	}
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
