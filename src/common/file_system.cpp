#include "duckdb/common/file_system.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"

#include <cstdint>
#include <cstdio>

#ifndef _WIN32
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
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

FileSystem::~FileSystem() {
}

FileSystem &FileSystem::GetFileSystem(ClientContext &context) {
	return *context.db->config.file_system;
}

FileOpener *FileSystem::GetFileOpener(ClientContext &context) {
	return ClientData::Get(context).file_opener.get();
}

#ifndef _WIN32
string FileSystem::PathSeparator() {
	return "/";
}

void FileSystem::SetWorkingDirectory(const string &path) {
	if (chdir(path.c_str()) != 0) {
		throw IOException("Could not change working directory!");
	}
}

idx_t FileSystem::GetAvailableMemory() {
	errno = 0;
	idx_t max_memory = MinValue<idx_t>((idx_t)sysconf(_SC_PHYS_PAGES) * (idx_t)sysconf(_SC_PAGESIZE), UINTPTR_MAX);
	if (errno != 0) {
		return DConstants::INVALID_INDEX;
	}
	return max_memory;
}

string FileSystem::GetWorkingDirectory() {
	auto buffer = unique_ptr<char[]>(new char[PATH_MAX]);
	char *ret = getcwd(buffer.get(), PATH_MAX);
	if (!ret) {
		throw IOException("Could not get working directory!");
	}
	return string(buffer.get());
}
#else

string FileSystem::PathSeparator() {
	return "\\";
}

void FileSystem::SetWorkingDirectory(const string &path) {
	if (!SetCurrentDirectory(path.c_str())) {
		throw IOException("Could not change working directory!");
	}
}

idx_t FileSystem::GetAvailableMemory() {
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
	return DConstants::INVALID_INDEX;
}

string FileSystem::GetWorkingDirectory() {
	idx_t count = GetCurrentDirectory(0, nullptr);
	if (count == 0) {
		throw IOException("Could not get working directory!");
	}
	auto buffer = unique_ptr<char[]>(new char[count]);
	idx_t ret = GetCurrentDirectory(count, buffer.get());
	if (count != ret + 1) {
		throw IOException("Could not get working directory!");
	}
	return string(buffer.get(), ret);
}

#endif

string FileSystem::JoinPath(const string &a, const string &b) {
	// FIXME: sanitize paths
	return a + PathSeparator() + b;
}

string FileSystem::ConvertSeparators(const string &path) {
	auto separator_str = PathSeparator();
	char separator = separator_str[0];
	if (separator == '/') {
		// on unix-based systems we only accept / as a separator
		return path;
	}
	// on windows-based systems we accept both
	string result = path;
	for (idx_t i = 0; i < result.size(); i++) {
		if (result[i] == '/') {
			result[i] = separator;
		}
	}
	return result;
}

string FileSystem::ExtractBaseName(const string &path) {
	auto normalized_path = ConvertSeparators(path);
	auto sep = PathSeparator();
	auto vec = StringUtil::Split(StringUtil::Split(normalized_path, sep).back(), ".");
	return vec[0];
}

string FileSystem::GetHomeDirectory() {
#ifdef DUCKDB_WINDOWS
	const char *homedir = getenv("USERPROFILE");
#else
	const char *homedir = getenv("HOME");
#endif
	if (homedir) {
		return homedir;
	}
	return string();
}

// LCOV_EXCL_START
unique_ptr<FileHandle> FileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock,
                                            FileCompressionType compression, FileOpener *opener) {
	throw NotImplementedException("%s: OpenFile is not implemented!", GetName());
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

bool FileSystem::DirectoryExists(const string &directory) {
	throw NotImplementedException("%s: DirectoryExists is not implemented!", GetName());
}

void FileSystem::CreateDirectory(const string &directory) {
	throw NotImplementedException("%s: CreateDirectory is not implemented!", GetName());
}

void FileSystem::RemoveDirectory(const string &directory) {
	throw NotImplementedException("%s: RemoveDirectory is not implemented!", GetName());
}

bool FileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback) {
	throw NotImplementedException("%s: ListFiles is not implemented!", GetName());
}

void FileSystem::MoveFile(const string &source, const string &target) {
	throw NotImplementedException("%s: MoveFile is not implemented!", GetName());
}

bool FileSystem::FileExists(const string &filename) {
	throw NotImplementedException("%s: FileExists is not implemented!", GetName());
}

bool FileSystem::IsPipe(const string &filename) {
	throw NotImplementedException("%s: IsPipe is not implemented!", GetName());
}

void FileSystem::RemoveFile(const string &filename) {
	throw NotImplementedException("%s: RemoveFile is not implemented!", GetName());
}

void FileSystem::FileSync(FileHandle &handle) {
	throw NotImplementedException("%s: FileSync is not implemented!", GetName());
}

vector<string> FileSystem::Glob(const string &path, FileOpener *opener) {
	throw NotImplementedException("%s: Glob is not implemented!", GetName());
}

vector<string> FileSystem::Glob(const string &path, ClientContext &context) {
	return Glob(path, GetFileOpener(context));
}

void FileSystem::RegisterSubSystem(unique_ptr<FileSystem> sub_fs) {
	throw NotImplementedException("%s: Can't register a sub system on a non-virtual file system", GetName());
}

void FileSystem::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> sub_fs) {
	throw NotImplementedException("%s: Can't register a sub system on a non-virtual file system", GetName());
}

bool FileSystem::CanHandleFile(const string &fpath) {
	throw NotImplementedException("%s: CanHandleFile is not implemented!", GetName());
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

unique_ptr<FileHandle> FileSystem::OpenCompressedFile(unique_ptr<FileHandle> handle, bool write) {
	throw NotImplementedException("%s: OpenCompressedFile is not implemented!", GetName());
}

bool FileSystem::OnDiskFile(FileHandle &handle) {
	throw NotImplementedException("%s: OnDiskFile is not implemented!", GetName());
}
// LCOV_EXCL_STOP

FileHandle::FileHandle(FileSystem &file_system, string path_p) : file_system(file_system), path(move(path_p)) {
}

FileHandle::~FileHandle() {
}

int64_t FileHandle::Read(void *buffer, idx_t nr_bytes) {
	return file_system.Read(*this, buffer, nr_bytes);
}

int64_t FileHandle::Write(void *buffer, idx_t nr_bytes) {
	return file_system.Write(*this, buffer, nr_bytes);
}

void FileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Read(*this, buffer, nr_bytes, location);
}

void FileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Write(*this, buffer, nr_bytes, location);
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

string FileHandle::ReadLine() {
	string result;
	char buffer[1];
	while (true) {
		idx_t tuples_read = Read(buffer, 1);
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
	return file_system.GetFileSize(*this);
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

} // namespace duckdb
