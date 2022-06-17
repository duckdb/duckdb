#include "duckdb/common/local_file_system.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/main/client_context.hpp"
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
#include "duckdb/common/windows_util.hpp"

#include <io.h>
#include <string>

#ifdef __MINGW32__
#include <sys/stat.h>
// need to manually define this for mingw
extern "C" WINBASEAPI BOOL WINAPI GetPhysicallyInstalledSystemMemory(PULONGLONG);
#endif

#undef FILE_CREATE // woo mingw
#endif

namespace duckdb {

static void AssertValidFileFlags(uint8_t flags) {
#ifdef DEBUG
	bool is_read = flags & FileFlags::FILE_FLAGS_READ;
	bool is_write = flags & FileFlags::FILE_FLAGS_WRITE;
	// require either READ or WRITE (or both)
	D_ASSERT(is_read || is_write);
	// CREATE/Append flags require writing
	D_ASSERT(is_write || !(flags & FileFlags::FILE_FLAGS_APPEND));
	D_ASSERT(is_write || !(flags & FileFlags::FILE_FLAGS_FILE_CREATE));
	D_ASSERT(is_write || !(flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW));
	// cannot combine CREATE and CREATE_NEW flags
	D_ASSERT(!(flags & FileFlags::FILE_FLAGS_FILE_CREATE && flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW));
#endif
}

#ifdef __MINGW32__
bool LocalFileSystem::FileExists(const string &filename) {
	auto unicode_path = WindowsUtil::UTF8ToUnicode(filename.c_str());
	const wchar_t *wpath = unicode_path.c_str();
	if (_waccess(wpath, 0) == 0) {
		struct _stat64i32 status;
		_wstat64i32(wpath, &status);
		if (status.st_size > 0) {
			return true;
		}
	}
	return false;
}
bool LocalFileSystem::IsPipe(const string &filename) {
	auto unicode_path = WindowsUtil::UTF8ToUnicode(filename.c_str());
	const wchar_t *wpath = unicode_path.c_str();
	if (_waccess(wpath, 0) == 0) {
		struct _stat64i32 status;
		_wstat64i32(wpath, &status);
		if (status.st_size == 0) {
			return true;
		}
	}
	return false;
}

#else
#ifndef _WIN32
bool LocalFileSystem::FileExists(const string &filename) {
	if (!filename.empty()) {
		if (access(filename.c_str(), 0) == 0) {
			struct stat status;
			stat(filename.c_str(), &status);
			if (S_ISREG(status.st_mode)) {
				return true;
			}
		}
	}
	// if any condition fails
	return false;
}

bool LocalFileSystem::IsPipe(const string &filename) {
	if (!filename.empty()) {
		if (access(filename.c_str(), 0) == 0) {
			struct stat status;
			stat(filename.c_str(), &status);
			if (S_ISFIFO(status.st_mode)) {
				return true;
			}
		}
	}
	// if any condition fails
	return false;
}

#else
bool LocalFileSystem::FileExists(const string &filename) {
	auto unicode_path = WindowsUtil::UTF8ToUnicode(filename.c_str());
	const wchar_t *wpath = unicode_path.c_str();
	if (_waccess(wpath, 0) == 0) {
		struct _stat64i32 status;
		_wstat(wpath, &status);
		if (status.st_mode & S_IFREG) {
			return true;
		}
	}
	return false;
}
bool LocalFileSystem::IsPipe(const string &filename) {
	auto unicode_path = WindowsUtil::UTF8ToUnicode(filename.c_str());
	const wchar_t *wpath = unicode_path.c_str();
	if (_waccess(wpath, 0) == 0) {
		struct _stat64i32 status;
		_wstat(wpath, &status);
		if (status.st_mode & _S_IFCHR) {
			return true;
		}
	}
	return false;
}
#endif
#endif

#ifndef _WIN32
// somehow sometimes this is missing
#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif

// Solaris
#ifndef O_DIRECT
#define O_DIRECT 0
#endif

struct UnixFileHandle : public FileHandle {
public:
	UnixFileHandle(FileSystem &file_system, string path, int fd) : FileHandle(file_system, move(path)), fd(fd) {
	}
	~UnixFileHandle() override {
		Close();
	}

	int fd;

public:
	void Close() override {
		if (fd != -1) {
			close(fd);
		}
	};
};

static FileType GetFileTypeInternal(int fd) { // LCOV_EXCL_START
	struct stat s;
	if (fstat(fd, &s) == -1) {
		return FileType::FILE_TYPE_INVALID;
	}
	switch (s.st_mode & S_IFMT) {
	case S_IFBLK:
		return FileType::FILE_TYPE_BLOCKDEV;
	case S_IFCHR:
		return FileType::FILE_TYPE_CHARDEV;
	case S_IFIFO:
		return FileType::FILE_TYPE_FIFO;
	case S_IFDIR:
		return FileType::FILE_TYPE_DIR;
	case S_IFLNK:
		return FileType::FILE_TYPE_LINK;
	case S_IFREG:
		return FileType::FILE_TYPE_REGULAR;
	case S_IFSOCK:
		return FileType::FILE_TYPE_SOCKET;
	default:
		return FileType::FILE_TYPE_INVALID;
	}
} // LCOV_EXCL_STOP

unique_ptr<FileHandle> LocalFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock_type,
                                                 FileCompressionType compression, FileOpener *opener) {
	if (compression != FileCompressionType::UNCOMPRESSED) {
		throw NotImplementedException("Unsupported compression type for default file system");
	}

	AssertValidFileFlags(flags);

	int open_flags = 0;
	int rc;
	bool open_read = flags & FileFlags::FILE_FLAGS_READ;
	bool open_write = flags & FileFlags::FILE_FLAGS_WRITE;
	if (open_read && open_write) {
		open_flags = O_RDWR;
	} else if (open_read) {
		open_flags = O_RDONLY;
	} else if (open_write) {
		open_flags = O_WRONLY;
	} else {
		throw InternalException("READ, WRITE or both should be specified when opening a file");
	}
	if (open_write) {
		// need Read or Write
		D_ASSERT(flags & FileFlags::FILE_FLAGS_WRITE);
		open_flags |= O_CLOEXEC;
		if (flags & FileFlags::FILE_FLAGS_FILE_CREATE) {
			open_flags |= O_CREAT;
		} else if (flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW) {
			open_flags |= O_CREAT | O_TRUNC;
		}
		if (flags & FileFlags::FILE_FLAGS_APPEND) {
			open_flags |= O_APPEND;
		}
	}
	if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
#if defined(__sun) && defined(__SVR4)
		throw Exception("DIRECT_IO not supported on Solaris");
#endif
#if defined(__DARWIN__) || defined(__APPLE__) || defined(__OpenBSD__)
		// OSX does not have O_DIRECT, instead we need to use fcntl afterwards to support direct IO
		open_flags |= O_SYNC;
#else
		open_flags |= O_DIRECT | O_SYNC;
#endif
	}
	int fd = open(path.c_str(), open_flags, 0666);
	if (fd == -1) {
		throw IOException("Cannot open file \"%s\": %s", path, strerror(errno));
	}
	// #if defined(__DARWIN__) || defined(__APPLE__)
	// 	if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
	// 		// OSX requires fcntl for Direct IO
	// 		rc = fcntl(fd, F_NOCACHE, 1);
	// 		if (fd == -1) {
	// 			throw IOException("Could not enable direct IO for file \"%s\": %s", path, strerror(errno));
	// 		}
	// 	}
	// #endif
	if (lock_type != FileLockType::NO_LOCK) {
		// set lock on file
		// but only if it is not an input/output stream
		auto file_type = GetFileTypeInternal(fd);
		if (file_type != FileType::FILE_TYPE_FIFO && file_type != FileType::FILE_TYPE_SOCKET) {
			struct flock fl;
			memset(&fl, 0, sizeof fl);
			fl.l_type = lock_type == FileLockType::READ_LOCK ? F_RDLCK : F_WRLCK;
			fl.l_whence = SEEK_SET;
			fl.l_start = 0;
			fl.l_len = 0;
			rc = fcntl(fd, F_SETLK, &fl);
			if (rc == -1) {
				throw IOException("Could not set lock on file \"%s\": %s", path, strerror(errno));
			}
		}
	}
	return make_unique<UnixFileHandle>(*this, path, fd);
}

void LocalFileSystem::SetFilePointer(FileHandle &handle, idx_t location) {
	int fd = ((UnixFileHandle &)handle).fd;
	off_t offset = lseek(fd, location, SEEK_SET);
	if (offset == (off_t)-1) {
		throw IOException("Could not seek to location %lld for file \"%s\": %s", location, handle.path,
		                  strerror(errno));
	}
}

idx_t LocalFileSystem::GetFilePointer(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	off_t position = lseek(fd, 0, SEEK_CUR);
	if (position == (off_t)-1) {
		throw IOException("Could not get file position file \"%s\": %s", handle.path, strerror(errno));
	}
	return position;
}

void LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	int fd = ((UnixFileHandle &)handle).fd;
	int64_t bytes_read = pread(fd, buffer, nr_bytes, location);
	if (bytes_read == -1) {
		throw IOException("Could not read from file \"%s\": %s", handle.path, strerror(errno));
	}
	if (bytes_read != nr_bytes) {
		throw IOException("Could not read all bytes from file \"%s\": wanted=%lld read=%lld", handle.path, nr_bytes,
		                  bytes_read);
	}
}

int64_t LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	int fd = ((UnixFileHandle &)handle).fd;
	int64_t bytes_read = read(fd, buffer, nr_bytes);
	if (bytes_read == -1) {
		throw IOException("Could not read from file \"%s\": %s", handle.path, strerror(errno));
	}
	return bytes_read;
}

void LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	int fd = ((UnixFileHandle &)handle).fd;
	int64_t bytes_written = pwrite(fd, buffer, nr_bytes, location);
	if (bytes_written == -1) {
		throw IOException("Could not write file \"%s\": %s", handle.path, strerror(errno));
	}
	if (bytes_written != nr_bytes) {
		throw IOException("Could not write all bytes to file \"%s\": wanted=%lld wrote=%lld", handle.path, nr_bytes,
		                  bytes_written);
	}
}

int64_t LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	int fd = ((UnixFileHandle &)handle).fd;
	int64_t bytes_written = write(fd, buffer, nr_bytes);
	if (bytes_written == -1) {
		throw IOException("Could not write file \"%s\": %s", handle.path, strerror(errno));
	}
	return bytes_written;
}

int64_t LocalFileSystem::GetFileSize(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	struct stat s;
	if (fstat(fd, &s) == -1) {
		return -1;
	}
	return s.st_size;
}

time_t LocalFileSystem::GetLastModifiedTime(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	struct stat s;
	if (fstat(fd, &s) == -1) {
		return -1;
	}
	return s.st_mtime;
}

FileType LocalFileSystem::GetFileType(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	return GetFileTypeInternal(fd);
}

void LocalFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	int fd = ((UnixFileHandle &)handle).fd;
	if (ftruncate(fd, new_size) != 0) {
		throw IOException("Could not truncate file \"%s\": %s", handle.path, strerror(errno));
	}
}

bool LocalFileSystem::DirectoryExists(const string &directory) {
	if (!directory.empty()) {
		if (access(directory.c_str(), 0) == 0) {
			struct stat status;
			stat(directory.c_str(), &status);
			if (status.st_mode & S_IFDIR) {
				return true;
			}
		}
	}
	// if any condition fails
	return false;
}

void LocalFileSystem::CreateDirectory(const string &directory) {
	struct stat st;

	if (stat(directory.c_str(), &st) != 0) {
		/* Directory does not exist. EEXIST for race condition */
		if (mkdir(directory.c_str(), 0755) != 0 && errno != EEXIST) {
			throw IOException("Failed to create directory \"%s\"!", directory);
		}
	} else if (!S_ISDIR(st.st_mode)) {
		throw IOException("Failed to create directory \"%s\": path exists but is not a directory!", directory);
	}
}

int RemoveDirectoryRecursive(const char *path) {
	DIR *d = opendir(path);
	idx_t path_len = (idx_t)strlen(path);
	int r = -1;

	if (d) {
		struct dirent *p;
		r = 0;
		while (!r && (p = readdir(d))) {
			int r2 = -1;
			char *buf;
			idx_t len;
			/* Skip the names "." and ".." as we don't want to recurse on them. */
			if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
				continue;
			}
			len = path_len + (idx_t)strlen(p->d_name) + 2;
			buf = new char[len];
			if (buf) {
				struct stat statbuf;
				snprintf(buf, len, "%s/%s", path, p->d_name);
				if (!stat(buf, &statbuf)) {
					if (S_ISDIR(statbuf.st_mode)) {
						r2 = RemoveDirectoryRecursive(buf);
					} else {
						r2 = unlink(buf);
					}
				}
				delete[] buf;
			}
			r = r2;
		}
		closedir(d);
	}
	if (!r) {
		r = rmdir(path);
	}
	return r;
}

void LocalFileSystem::RemoveDirectory(const string &directory) {
	RemoveDirectoryRecursive(directory.c_str());
}

void LocalFileSystem::RemoveFile(const string &filename) {
	if (std::remove(filename.c_str()) != 0) {
		throw IOException("Could not remove file \"%s\": %s", filename, strerror(errno));
	}
}

bool LocalFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback) {
	if (!DirectoryExists(directory)) {
		return false;
	}
	DIR *dir = opendir(directory.c_str());
	if (!dir) {
		return false;
	}
	struct dirent *ent;
	// loop over all files in the directory
	while ((ent = readdir(dir)) != nullptr) {
		string name = string(ent->d_name);
		// skip . .. and empty files
		if (name.empty() || name == "." || name == "..") {
			continue;
		}
		// now stat the file to figure out if it is a regular file or directory
		string full_path = JoinPath(directory, name);
		if (access(full_path.c_str(), 0) != 0) {
			continue;
		}
		struct stat status;
		stat(full_path.c_str(), &status);
		if (!(status.st_mode & S_IFREG) && !(status.st_mode & S_IFDIR)) {
			// not a file or directory: skip
			continue;
		}
		// invoke callback
		callback(name, status.st_mode & S_IFDIR);
	}
	closedir(dir);
	return true;
}

void LocalFileSystem::FileSync(FileHandle &handle) {
	int fd = ((UnixFileHandle &)handle).fd;
	if (fsync(fd) != 0) {
		throw FatalException("fsync failed!");
	}
}

void LocalFileSystem::MoveFile(const string &source, const string &target) {
	//! FIXME: rename does not guarantee atomicity or overwriting target file if it exists
	if (rename(source.c_str(), target.c_str()) != 0) {
		throw IOException("Could not rename file!");
	}
}

std::string LocalFileSystem::GetLastErrorAsString() {
	return string();
}

#else

constexpr char PIPE_PREFIX[] = "\\\\.\\pipe\\";

// Returns the last Win32 error, in string format. Returns an empty string if there is no error.
std::string LocalFileSystem::GetLastErrorAsString() {
	// Get the error message, if any.
	DWORD errorMessageID = GetLastError();
	if (errorMessageID == 0)
		return std::string(); // No error message has been recorded

	LPSTR messageBuffer = nullptr;
	idx_t size =
	    FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
	                   NULL, errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);

	std::string message(messageBuffer, size);

	// Free the buffer.
	LocalFree(messageBuffer);

	return message;
}

struct WindowsFileHandle : public FileHandle {
public:
	WindowsFileHandle(FileSystem &file_system, string path, HANDLE fd)
	    : FileHandle(file_system, path), position(0), fd(fd) {
	}
	virtual ~WindowsFileHandle() {
		Close();
	}

	idx_t position;
	HANDLE fd;

public:
	void Close() override {
		CloseHandle(fd);
	};
};

unique_ptr<FileHandle> LocalFileSystem::OpenFile(const string &path, uint8_t flags, FileLockType lock_type,
                                                 FileCompressionType compression, FileOpener *opener) {
	if (compression != FileCompressionType::UNCOMPRESSED) {
		throw NotImplementedException("Unsupported compression type for default file system");
	}
	AssertValidFileFlags(flags);

	DWORD desired_access;
	DWORD share_mode;
	DWORD creation_disposition = OPEN_EXISTING;
	DWORD flags_and_attributes = FILE_ATTRIBUTE_NORMAL;
	bool open_read = flags & FileFlags::FILE_FLAGS_READ;
	bool open_write = flags & FileFlags::FILE_FLAGS_WRITE;
	if (open_read && open_write) {
		desired_access = GENERIC_READ | GENERIC_WRITE;
		share_mode = 0;
	} else if (open_read) {
		desired_access = GENERIC_READ;
		share_mode = FILE_SHARE_READ;
	} else if (open_write) {
		desired_access = GENERIC_WRITE;
		share_mode = 0;
	} else {
		throw InternalException("READ, WRITE or both should be specified when opening a file");
	}
	if (open_write) {
		if (flags & FileFlags::FILE_FLAGS_FILE_CREATE) {
			creation_disposition = OPEN_ALWAYS;
		} else if (flags & FileFlags::FILE_FLAGS_FILE_CREATE_NEW) {
			creation_disposition = CREATE_ALWAYS;
		}
	}
	if (flags & FileFlags::FILE_FLAGS_DIRECT_IO) {
		flags_and_attributes |= FILE_FLAG_NO_BUFFERING;
	}
	auto unicode_path = WindowsUtil::UTF8ToUnicode(path.c_str());
	HANDLE hFile = CreateFileW(unicode_path.c_str(), desired_access, share_mode, NULL, creation_disposition,
	                           flags_and_attributes, NULL);
	if (hFile == INVALID_HANDLE_VALUE) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Cannot open file \"%s\": %s", path.c_str(), error);
	}
	auto handle = make_unique<WindowsFileHandle>(*this, path.c_str(), hFile);
	if (flags & FileFlags::FILE_FLAGS_APPEND) {
		auto file_size = GetFileSize(*handle);
		SetFilePointer(*handle, file_size);
	}
	return move(handle);
}

void LocalFileSystem::SetFilePointer(FileHandle &handle, idx_t location) {
	((WindowsFileHandle &)handle).position = location;
}

idx_t LocalFileSystem::GetFilePointer(FileHandle &handle) {
	return ((WindowsFileHandle &)handle).position;
}

static DWORD FSInternalRead(FileHandle &handle, HANDLE hFile, void *buffer, int64_t nr_bytes, idx_t location) {
	DWORD bytes_read = 0;
	OVERLAPPED ov = {};
	ov.Internal = 0;
	ov.InternalHigh = 0;
	ov.Offset = location & 0xFFFFFFFF;
	ov.OffsetHigh = location >> 32;
	ov.hEvent = 0;
	auto rc = ReadFile(hFile, buffer, (DWORD)nr_bytes, &bytes_read, &ov);
	if (!rc) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Could not read file \"%s\" (error in ReadFile): %s", handle.path, error);
	}
	return bytes_read;
}

void LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	auto bytes_read = FSInternalRead(handle, hFile, buffer, nr_bytes, location);
	if (bytes_read != nr_bytes) {
		throw IOException("Could not read all bytes from file \"%s\": wanted=%lld read=%lld", handle.path, nr_bytes,
		                  bytes_read);
	}
}

int64_t LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	auto &pos = ((WindowsFileHandle &)handle).position;
	auto n = std::min<idx_t>(std::max<idx_t>(GetFileSize(handle), pos) - pos, nr_bytes);
	auto bytes_read = FSInternalRead(handle, hFile, buffer, n, pos);
	pos += bytes_read;
	return bytes_read;
}

static DWORD FSInternalWrite(FileHandle &handle, HANDLE hFile, void *buffer, int64_t nr_bytes, idx_t location) {
	DWORD bytes_written = 0;
	OVERLAPPED ov = {};
	ov.Internal = 0;
	ov.InternalHigh = 0;
	ov.Offset = location & 0xFFFFFFFF;
	ov.OffsetHigh = location >> 32;
	ov.hEvent = 0;
	auto rc = WriteFile(hFile, buffer, (DWORD)nr_bytes, &bytes_written, &ov);
	if (!rc) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Could not write file \"%s\" (error in WriteFile): %s", handle.path, error);
	}
	return bytes_written;
}

void LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	auto bytes_written = FSInternalWrite(handle, hFile, buffer, nr_bytes, location);
	if (bytes_written != nr_bytes) {
		throw IOException("Could not write all bytes from file \"%s\": wanted=%lld wrote=%lld", handle.path, nr_bytes,
		                  bytes_written);
	}
}

int64_t LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	auto &pos = ((WindowsFileHandle &)handle).position;
	auto bytes_written = FSInternalWrite(handle, hFile, buffer, nr_bytes, pos);
	pos += bytes_written;
	return bytes_written;
}

int64_t LocalFileSystem::GetFileSize(FileHandle &handle) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	LARGE_INTEGER result;
	if (!GetFileSizeEx(hFile, &result)) {
		return -1;
	}
	return result.QuadPart;
}

time_t LocalFileSystem::GetLastModifiedTime(FileHandle &handle) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;

	// https://docs.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfiletime
	FILETIME last_write;
	if (GetFileTime(hFile, nullptr, nullptr, &last_write) == 0) {
		return -1;
	}

	// https://stackoverflow.com/questions/29266743/what-is-dwlowdatetime-and-dwhighdatetime
	ULARGE_INTEGER ul;
	ul.LowPart = last_write.dwLowDateTime;
	ul.HighPart = last_write.dwHighDateTime;
	int64_t fileTime64 = ul.QuadPart;

	// fileTime64 contains a 64-bit value representing the number of
	// 100-nanosecond intervals since January 1, 1601 (UTC).
	// https://docs.microsoft.com/en-us/windows/win32/api/minwinbase/ns-minwinbase-filetime

	// Adapted from: https://stackoverflow.com/questions/6161776/convert-windows-filetime-to-second-in-unix-linux
	const auto WINDOWS_TICK = 10000000;
	const auto SEC_TO_UNIX_EPOCH = 11644473600LL;
	time_t result = (fileTime64 / WINDOWS_TICK - SEC_TO_UNIX_EPOCH);
	return result;
}

void LocalFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	// seek to the location
	SetFilePointer(handle, new_size);
	// now set the end of file position
	if (!SetEndOfFile(hFile)) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Failure in SetEndOfFile call on file \"%s\": %s", handle.path, error);
	}
}

static DWORD WindowsGetFileAttributes(const string &filename) {
	auto unicode_path = WindowsUtil::UTF8ToUnicode(filename.c_str());
	return GetFileAttributesW(unicode_path.c_str());
}

bool LocalFileSystem::DirectoryExists(const string &directory) {
	DWORD attrs = WindowsGetFileAttributes(directory);
	return (attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY));
}

void LocalFileSystem::CreateDirectory(const string &directory) {
	if (DirectoryExists(directory)) {
		return;
	}
	auto unicode_path = WindowsUtil::UTF8ToUnicode(directory.c_str());
	if (directory.empty() || !CreateDirectoryW(unicode_path.c_str(), NULL) || !DirectoryExists(directory)) {
		throw IOException("Could not create directory!");
	}
}

static void DeleteDirectoryRecursive(FileSystem &fs, string directory) {
	fs.ListFiles(directory, [&](const string &fname, bool is_directory) {
		if (is_directory) {
			DeleteDirectoryRecursive(fs, fs.JoinPath(directory, fname));
		} else {
			fs.RemoveFile(fs.JoinPath(directory, fname));
		}
	});
	auto unicode_path = WindowsUtil::UTF8ToUnicode(directory.c_str());
	if (!RemoveDirectoryW(unicode_path.c_str())) {
		throw IOException("Failed to delete directory");
	}
}

void LocalFileSystem::RemoveDirectory(const string &directory) {
	if (FileExists(directory)) {
		throw IOException("Attempting to delete directory \"%s\", but it is a file and not a directory!", directory);
	}
	if (!DirectoryExists(directory)) {
		return;
	}
	DeleteDirectoryRecursive(*this, directory.c_str());
}

void LocalFileSystem::RemoveFile(const string &filename) {
	auto unicode_path = WindowsUtil::UTF8ToUnicode(filename.c_str());
	DeleteFileW(unicode_path.c_str());
}

bool LocalFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback) {
	string search_dir = JoinPath(directory, "*");

	auto unicode_path = WindowsUtil::UTF8ToUnicode(search_dir.c_str());

	WIN32_FIND_DATAW ffd;
	HANDLE hFind = FindFirstFileW(unicode_path.c_str(), &ffd);
	if (hFind == INVALID_HANDLE_VALUE) {
		return false;
	}
	do {
		string cFileName = WindowsUtil::UnicodeToUTF8(ffd.cFileName);
		if (cFileName == "." || cFileName == "..") {
			continue;
		}
		callback(cFileName, ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY);
	} while (FindNextFileW(hFind, &ffd) != 0);

	DWORD dwError = GetLastError();
	if (dwError != ERROR_NO_MORE_FILES) {
		FindClose(hFind);
		return false;
	}

	FindClose(hFind);
	return true;
}

void LocalFileSystem::FileSync(FileHandle &handle) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	if (FlushFileBuffers(hFile) == 0) {
		throw IOException("Could not flush file handle to disk!");
	}
}

void LocalFileSystem::MoveFile(const string &source, const string &target) {
	auto source_unicode = WindowsUtil::UTF8ToUnicode(source.c_str());
	auto target_unicode = WindowsUtil::UTF8ToUnicode(target.c_str());
	if (!MoveFileW(source_unicode.c_str(), target_unicode.c_str())) {
		throw IOException("Could not move file");
	}
}

FileType LocalFileSystem::GetFileType(FileHandle &handle) {
	auto path = ((WindowsFileHandle &)handle).path;
	// pipes in windows are just files in '\\.\pipe\' folder
	if (strncmp(path.c_str(), PIPE_PREFIX, strlen(PIPE_PREFIX)) == 0) {
		return FileType::FILE_TYPE_FIFO;
	}
	DWORD attrs = WindowsGetFileAttributes(path.c_str());
	if (attrs != INVALID_FILE_ATTRIBUTES) {
		if (attrs & FILE_ATTRIBUTE_DIRECTORY) {
			return FileType::FILE_TYPE_DIR;
		} else {
			return FileType::FILE_TYPE_REGULAR;
		}
	}
	return FileType::FILE_TYPE_INVALID;
}
#endif

bool LocalFileSystem::CanSeek() {
	return true;
}

bool LocalFileSystem::OnDiskFile(FileHandle &handle) {
	return true;
}

void LocalFileSystem::Seek(FileHandle &handle, idx_t location) {
	if (!CanSeek()) {
		throw IOException("Cannot seek in files of this type");
	}
	SetFilePointer(handle, location);
}

idx_t LocalFileSystem::SeekPosition(FileHandle &handle) {
	if (!CanSeek()) {
		throw IOException("Cannot seek in files of this type");
	}
	return GetFilePointer(handle);
}

static bool HasGlob(const string &str) {
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

static void GlobFiles(FileSystem &fs, const string &path, const string &glob, bool match_directory,
                      vector<string> &result, bool join_path) {
	fs.ListFiles(path, [&](const string &fname, bool is_directory) {
		if (is_directory != match_directory) {
			return;
		}
		if (LikeFun::Glob(fname.c_str(), fname.size(), glob.c_str(), glob.size())) {
			if (join_path) {
				result.push_back(fs.JoinPath(path, fname));
			} else {
				result.push_back(fname);
			}
		}
	});
}

vector<string> LocalFileSystem::Glob(const string &path, FileOpener *opener) {
	if (path.empty()) {
		return vector<string>();
	}
	// split up the path into separate chunks
	vector<string> splits;
	idx_t last_pos = 0;
	for (idx_t i = 0; i < path.size(); i++) {
		if (path[i] == '\\' || path[i] == '/') {
			if (i == last_pos) {
				// empty: skip this position
				last_pos = i + 1;
				continue;
			}
			if (splits.empty()) {
				splits.push_back(path.substr(0, i));
			} else {
				splits.push_back(path.substr(last_pos, i - last_pos));
			}
			last_pos = i + 1;
		}
	}
	splits.push_back(path.substr(last_pos, path.size() - last_pos));
	// handle absolute paths
	bool absolute_path = false;
	if (path[0] == '/') {
		// first character is a slash -  unix absolute path
		absolute_path = true;
	} else if (StringUtil::Contains(splits[0], ":")) {
		// first split has a colon -  windows absolute path
		absolute_path = true;
	} else if (splits[0] == "~") {
		// starts with home directory
		auto home_directory = GetHomeDirectory();
		if (!home_directory.empty()) {
			absolute_path = true;
			splits[0] = home_directory;
		}
	}
	// Check if the path has a glob at all
	if (!HasGlob(path)) {
		// no glob: return only the file (if it exists or is a pipe)
		vector<string> result;
		if (FileExists(path) || IsPipe(path)) {
			result.push_back(path);
		} else if (!absolute_path) {
			Value value;
			if (opener->TryGetCurrentSetting("file_search_path", value)) {
				auto search_paths_str = value.ToString();
				std::vector<std::string> search_paths = StringUtil::Split(search_paths_str, ',');
				for (const auto &search_path : search_paths) {
					auto joined_path = JoinPath(search_path, path);
					if (FileExists(joined_path) || IsPipe(joined_path)) {
						result.push_back(joined_path);
					}
				}
			}
		}
		return result;
	}
	vector<string> previous_directories;
	if (absolute_path) {
		// for absolute paths, we don't start by scanning the current directory
		previous_directories.push_back(splits[0]);
	}
	for (idx_t i = absolute_path ? 1 : 0; i < splits.size(); i++) {
		bool is_last_chunk = i + 1 == splits.size();
		bool has_glob = HasGlob(splits[i]);
		// if it's the last chunk we need to find files, otherwise we find directories
		// not the last chunk: gather a list of all directories that match the glob pattern
		vector<string> result;
		if (!has_glob) {
			// no glob, just append as-is
			if (previous_directories.empty()) {
				result.push_back(splits[i]);
			} else {
				for (auto &prev_directory : previous_directories) {
					result.push_back(JoinPath(prev_directory, splits[i]));
				}
			}
		} else {
			if (previous_directories.empty()) {
				// no previous directories: list in the current path
				GlobFiles(*this, ".", splits[i], !is_last_chunk, result, false);
			} else {
				// previous directories
				// we iterate over each of the previous directories, and apply the glob of the current directory
				for (auto &prev_directory : previous_directories) {
					GlobFiles(*this, prev_directory, splits[i], !is_last_chunk, result, true);
				}
			}
		}
		if (is_last_chunk || result.empty()) {
			return result;
		}
		previous_directories = move(result);
	}
	return vector<string>();
}

unique_ptr<FileSystem> FileSystem::CreateLocal() {
	return make_unique<LocalFileSystem>();
}

} // namespace duckdb
