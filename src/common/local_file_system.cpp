#include "duckdb/common/local_file_system.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/windows.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"

#include <cstdint>
#include <cstdio>
#include <sys/stat.h>

#ifndef _WIN32
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#else
#include "duckdb/common/windows_util.hpp"

#include <io.h>
#include <string>

#ifdef __MINGW32__
// need to manually define this for mingw
extern "C" WINBASEAPI BOOL WINAPI GetPhysicallyInstalledSystemMemory(PULONGLONG);
extern "C" WINBASEAPI BOOL QueryFullProcessImageNameW(HANDLE, DWORD, LPWSTR, PDWORD);
#endif

#undef FILE_CREATE // woo mingw
#endif

// includes for giving a better error message on lock conflicts
#if defined(__linux__) || defined(__APPLE__)
#include <pwd.h>
#endif

#if defined(__linux__)
// See https://man7.org/linux/man-pages/man2/fallocate.2.html
#ifndef _GNU_SOURCE
#define _GNU_SOURCE /* See feature_test_macros(7) */
#endif
#include <fcntl.h>
#include <libgen.h>
// See e.g.:
// https://opensource.apple.com/source/CarbonHeaders/CarbonHeaders-18.1/TargetConditionals.h.auto.html
#elif defined(__APPLE__)
#include <TargetConditionals.h>
#if not(defined(TARGET_OS_IPHONE) && TARGET_OS_IPHONE == 1)
#include <libproc.h>
#endif
#elif defined(_WIN32)
#include <restartmanager.h>
#endif

namespace duckdb {
#ifndef _WIN32
bool LocalFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	if (!filename.empty()) {
		auto normalized_file = ExpandPath(filename, opener);
		if (access(normalized_file.c_str(), 0) == 0) {
			struct stat status;
			stat(normalized_file.c_str(), &status);
			if (S_ISREG(status.st_mode)) {
				return true;
			}
		}
	}
	// if any condition fails
	return false;
}

bool LocalFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	if (!filename.empty()) {
		auto normalized_file = ExpandPath(filename, opener);
		if (access(normalized_file.c_str(), 0) == 0) {
			struct stat status;
			stat(normalized_file.c_str(), &status);
			if (S_ISFIFO(status.st_mode)) {
				return true;
			}
		}
	}
	// if any condition fails
	return false;
}

#else
static std::wstring ConvertPathToUnicode(const string &path) {
	return WindowsUtil::UTF8ToUnicode(path.c_str());
}

static std::wstring NormalizePathAndConvertToUnicode(FileSystem &fs, const string &path,
                                                     optional_ptr<FileOpener> opener) {
	auto normalized_path = fs.ExpandPath(path, opener);
	return ConvertPathToUnicode(normalized_path);
}

bool LocalFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	auto unicode_path = NormalizePathAndConvertToUnicode(*this, filename, opener);
	const wchar_t *wpath = unicode_path.c_str();
	if (_waccess(wpath, 0) == 0) {
		struct _stati64 status;
		_wstati64(wpath, &status);
		if (status.st_mode & S_IFREG) {
			return true;
		}
	}
	return false;
}
bool LocalFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	auto unicode_path = NormalizePathAndConvertToUnicode(*this, filename, opener);
	const wchar_t *wpath = unicode_path.c_str();
	if (_waccess(wpath, 0) == 0) {
		struct _stati64 status;
		_wstati64(wpath, &status);
		if (status.st_mode & _S_IFCHR) {
			return true;
		}
	}
	return false;
}
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
	UnixFileHandle(FileSystem &file_system, string path, int fd, FileOpenFlags flags)
	    : FileHandle(file_system, std::move(path), flags), fd(fd) {
	}
	~UnixFileHandle() override {
		UnixFileHandle::Close();
	}

	int fd;

	// Kept for logging purposes
	idx_t current_pos = 0;

public:
	void Close() override {
		if (fd != -1) {
			close(fd);
			fd = -1;
			DUCKDB_LOG_FILE_SYSTEM_CLOSE((*this));
		}
	};
};

static FileMetadata StatsInternal(int fd, const string &path) {
	struct stat s;
	if (fstat(fd, &s) == -1) {
		throw IOException({{"errno", std::to_string(errno)}}, "Failed to get stats for file \"%s\": %s", path,
		                  strerror(errno));
	}

	FileMetadata file_metadata;
	file_metadata.file_size = s.st_size;
	file_metadata.last_modification_time = Timestamp::FromEpochSeconds(s.st_mtime);

	switch (s.st_mode & S_IFMT) {
	case S_IFBLK:
		file_metadata.file_type = FileType::FILE_TYPE_BLOCKDEV;
		break;
	case S_IFCHR:
		file_metadata.file_type = FileType::FILE_TYPE_CHARDEV;
		break;
	case S_IFIFO:
		file_metadata.file_type = FileType::FILE_TYPE_FIFO;
		break;
	case S_IFDIR:
		file_metadata.file_type = FileType::FILE_TYPE_DIR;
		break;
	case S_IFLNK:
		file_metadata.file_type = FileType::FILE_TYPE_LINK;
		break;
	case S_IFREG:
		file_metadata.file_type = FileType::FILE_TYPE_REGULAR;
		break;
	case S_IFSOCK:
		file_metadata.file_type = FileType::FILE_TYPE_SOCKET;
		break;
	default:
		file_metadata.file_type = FileType::FILE_TYPE_INVALID;
		break;
	}

	return file_metadata;
} // LCOV_EXCL_STOP

#if __APPLE__ && !TARGET_OS_IPHONE

static string AdditionalProcessInfo(FileSystem &fs, pid_t pid) {
	if (pid == getpid()) {
		return "Lock is already held in current process, likely another DuckDB instance";
	}

	string process_name, process_owner;
	// macOS >= 10.7 has PROC_PIDT_SHORTBSDINFO
#ifdef PROC_PIDT_SHORTBSDINFO
	// try to find out more about the process holding the lock
	struct proc_bsdshortinfo proc;
	if (proc_pidinfo(pid, PROC_PIDT_SHORTBSDINFO, 0, &proc, PROC_PIDT_SHORTBSDINFO_SIZE) ==
	    PROC_PIDT_SHORTBSDINFO_SIZE) {
		process_name = proc.pbsi_comm; // only a short version however, let's take it in case proc_pidpath() below fails
		// try to get actual name of conflicting process owner
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
	if (proc_pidpath(pid, full_exec_path, PROC_PIDPATHINFO_MAXSIZE) > 0) {
		// somehow could not get the path, lets use some sensible fallback
		process_name = full_exec_path;
	}
	return StringUtil::Format("Conflicting lock is held in %s%s",
	                          !process_name.empty() ? StringUtil::Format("%s (PID %d)", process_name, pid)
	                                                : StringUtil::Format("PID %d", pid),
	                          !process_owner.empty() ? StringUtil::Format(" by user %s", process_owner) : "");
}

#elif __linux__

static string AdditionalProcessInfo(FileSystem &fs, pid_t pid) {
	if (pid == getpid()) {
		return "Lock is already held in current process, likely another DuckDB instance";
	}
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

	return StringUtil::Format("Conflicting lock is held in %s%s",
	                          !process_name.empty() ? StringUtil::Format("%s (PID %d)", process_name, pid)
	                                                : StringUtil::Format("PID %d", pid),
	                          !process_owner.empty() ? StringUtil::Format(" by user %s", process_owner) : "");
}

#else
static string AdditionalProcessInfo(FileSystem &fs, pid_t pid) {
	return "";
}
#endif

bool LocalFileSystem::IsPrivateFile(const string &path_p, FileOpener *opener) {
	auto path = FileSystem::ExpandPath(path_p, opener);

	struct stat st;

	if (lstat(path.c_str(), &st) != 0) {
		throw IOException(
		    "Failed to stat '%s' when checking file permissions, file may be missing or have incorrect permissions",
		    path_p.c_str());
	}

	// If group or other have any permission, the file is not private
	if (st.st_mode & (S_IRGRP | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH)) {
		return false;
	}

	return true;
}

unique_ptr<FileHandle> LocalFileSystem::OpenFile(const string &path_p, FileOpenFlags flags,
                                                 optional_ptr<FileOpener> opener) {
	auto path = ExpandPath(path_p, opener);
	if (flags.Compression() != FileCompressionType::UNCOMPRESSED) {
		throw NotImplementedException("Unsupported compression type for default file system");
	}

	flags.Verify();

	int open_flags = 0;
	int rc;
	bool open_read = flags.OpenForReading();
	bool open_write = flags.OpenForWriting();
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
		D_ASSERT(flags.OpenForWriting());
		open_flags |= O_CLOEXEC;
		if (flags.CreateFileIfNotExists()) {
			open_flags |= O_CREAT;
		} else if (flags.OverwriteExistingFile()) {
			open_flags |= O_CREAT | O_TRUNC;
		}
		if (flags.OpenForAppending()) {
			open_flags |= O_APPEND;
		}
	}
	if (flags.DirectIO()) {
#if defined(__sun) && defined(__SVR4)
		throw InvalidInputException("DIRECT_IO not supported on Solaris");
#endif
#if defined(__DARWIN__) || defined(__APPLE__) || defined(__OpenBSD__)
		// OSX does not have O_DIRECT, instead we need to use fcntl afterwards to support direct IO
#else
		open_flags |= O_DIRECT;
#endif
	}

	// Determine permissions
	mode_t filesec;
	if (flags.CreatePrivateFile()) {
		open_flags |= O_EXCL; // Ensure we error on existing files or the permissions may not set
		filesec = 0600;
	} else {
		filesec = 0666;
	}

	if (flags.ExclusiveCreate()) {
		open_flags |= O_EXCL;
	}

	// Open the file
	int fd = open(path.c_str(), open_flags, filesec);

	if (fd == -1) {
		if (flags.ReturnNullIfNotExists() && errno == ENOENT) {
			return nullptr;
		}
		if (flags.ReturnNullIfExists() && errno == EEXIST) {
			return nullptr;
		}
		throw IOException({{"errno", std::to_string(errno)}}, "Cannot open file \"%s\": %s", path, strerror(errno));
	}

#if defined(__DARWIN__) || defined(__APPLE__)
	if (flags.DirectIO()) {
		// OSX requires fcntl for Direct IO
		rc = fcntl(fd, F_NOCACHE, 1);
		if (rc == -1) {
			throw IOException("Could not enable direct IO for file \"%s\": %s", path, strerror(errno));
		}
	}
#endif

	if (flags.Lock() != FileLockType::NO_LOCK) {
		// set lock on file
		// but only if it is not an input/output stream
		auto file_type = StatsInternal(fd, path_p).file_type;
		if (file_type != FileType::FILE_TYPE_FIFO && file_type != FileType::FILE_TYPE_SOCKET) {
			struct flock fl;
			memset(&fl, 0, sizeof fl);
			fl.l_type = flags.Lock() == FileLockType::READ_LOCK ? F_RDLCK : F_WRLCK;
			fl.l_whence = SEEK_SET;
			fl.l_start = 0;
			fl.l_len = 0;
			rc = fcntl(fd, F_SETLK, &fl);
			// Retain the original error.
			int retained_errno = errno;
			bool has_error = rc == -1;
			string extended_error;
			if (has_error) {
				if (retained_errno == ENOTSUP) {
					// file lock not supported for this file system
					if (flags.Lock() == FileLockType::READ_LOCK) {
						// for read-only, we ignore not-supported errors
						has_error = false;
						errno = 0;
					} else {
						extended_error = "File locks are not supported for this file system, cannot open the file in "
						                 "read-write mode. Try opening the file in read-only mode";
					}
				}
			}
			if (has_error) {
				if (extended_error.empty()) {
					// try to find out who is holding the lock using F_GETLK
					rc = fcntl(fd, F_GETLK, &fl);
					if (rc == -1) { // fnctl does not want to help us
						extended_error = strerror(errno);
					} else {
						extended_error = AdditionalProcessInfo(*this, fl.l_pid);
					}
					if (flags.Lock() == FileLockType::WRITE_LOCK) {
						// maybe we can get a read lock instead and tell this to the user.
						fl.l_type = F_RDLCK;
						rc = fcntl(fd, F_SETLK, &fl);
						if (rc != -1) { // success!
							extended_error +=
							    ". However, you would be able to open this database in read-only mode, e.g. by "
							    "using the -readonly parameter in the CLI";
						}
					}
				}
				rc = close(fd);
				if (rc == -1) {
					extended_error += ". Also, failed closing file";
				}
				extended_error += ". See also https://duckdb.org/docs/stable/connect/concurrency";
				throw IOException({{"errno", std::to_string(retained_errno)}}, "Could not set lock on file \"%s\": %s",
				                  path, extended_error);
			}
		}
	}

	auto file_handle = make_uniq<UnixFileHandle>(*this, path, fd, flags);
	if (opener) {
		file_handle->TryAddLogger(*opener);
		DUCKDB_LOG_FILE_SYSTEM_OPEN((*file_handle));
	}
	return std::move(file_handle);
}

void LocalFileSystem::SetFilePointer(FileHandle &handle, idx_t location) {
	int fd = handle.Cast<UnixFileHandle>().fd;
	off_t offset = lseek(fd, UnsafeNumericCast<off_t>(location), SEEK_SET);
	if (offset == (off_t)-1) {
		throw IOException({{"errno", std::to_string(errno)}}, "Could not seek to location %lld for file \"%s\": %s",
		                  location, handle.path, strerror(errno));
	}
}

idx_t LocalFileSystem::GetFilePointer(FileHandle &handle) {
	int fd = handle.Cast<UnixFileHandle>().fd;
	off_t position = lseek(fd, 0, SEEK_CUR);
	if (position == (off_t)-1) {
		throw IOException({{"errno", std::to_string(errno)}}, "Could not get file position file \"%s\": %s",
		                  handle.path, strerror(errno));
	}
	return UnsafeNumericCast<idx_t>(position);
}

void LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto bytes_to_read = nr_bytes;
	int fd = handle.Cast<UnixFileHandle>().fd;
	auto read_buffer = char_ptr_cast(buffer);
	while (nr_bytes > 0) {
		int64_t bytes_read =
		    pread(fd, read_buffer, UnsafeNumericCast<size_t>(nr_bytes), UnsafeNumericCast<off_t>(location));
		if (bytes_read == -1) {
			throw IOException({{"errno", std::to_string(errno)}}, "Could not read from file \"%s\": %s", handle.path,
			                  strerror(errno));
		}
		if (bytes_read == 0) {
			throw IOException(
			    "Could not read enough bytes from file \"%s\": attempted to read %llu bytes from location %llu",
			    handle.path, nr_bytes, location);
		}
		read_buffer += bytes_read;
		nr_bytes -= bytes_read;
		location += UnsafeNumericCast<idx_t>(bytes_read);
	}

	DUCKDB_LOG_FILE_SYSTEM_READ(handle, bytes_to_read, location - UnsafeNumericCast<idx_t>(bytes_to_read));
}

int64_t LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &unix_handle = handle.Cast<UnixFileHandle>();
	int fd = unix_handle.fd;
	int64_t bytes_read = read(fd, buffer, UnsafeNumericCast<size_t>(nr_bytes));
	if (bytes_read == -1) {
		throw IOException({{"errno", std::to_string(errno)}}, "Could not read from file \"%s\": %s", handle.path,
		                  strerror(errno));
	}

	DUCKDB_LOG_FILE_SYSTEM_READ(handle, bytes_read, unix_handle.current_pos);
	unix_handle.current_pos += UnsafeNumericCast<idx_t>(bytes_read);

	return bytes_read;
}

void LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	int fd = handle.Cast<UnixFileHandle>().fd;
	auto write_buffer = char_ptr_cast(buffer);

	auto bytes_to_write = nr_bytes;
	auto current_location = location;

	while (bytes_to_write > 0) {
		int64_t bytes_written = pwrite(fd, write_buffer, UnsafeNumericCast<size_t>(bytes_to_write),
		                               UnsafeNumericCast<off_t>(current_location));
		if (bytes_written < 0) {
			throw IOException({{"errno", std::to_string(errno)}}, "Could not write file \"%s\": %s", handle.path,
			                  strerror(errno));
		}
		if (bytes_written == 0) {
			throw IOException({{"errno", std::to_string(errno)}},
			                  "Could not write to file \"%s\" - attempted to write 0 bytes: %s", handle.path,
			                  strerror(errno));
		}
		write_buffer += bytes_written;
		bytes_to_write -= bytes_written;
		current_location += UnsafeNumericCast<idx_t>(bytes_written);
	}

	DUCKDB_LOG_FILE_SYSTEM_WRITE(handle, nr_bytes, location);
}

int64_t LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &unix_handle = handle.Cast<UnixFileHandle>();
	int fd = unix_handle.fd;

	auto bytes_to_write = nr_bytes;
	while (bytes_to_write > 0) {
		auto bytes_to_write_this_call =
		    MinValue<idx_t>(idx_t(NumericLimits<int32_t>::Maximum()), idx_t(bytes_to_write));
		int64_t current_bytes_written = write(fd, buffer, bytes_to_write_this_call);
		if (current_bytes_written <= 0) {
			throw IOException({{"errno", std::to_string(errno)}}, "Could not write file \"%s\": %s", handle.path,
			                  strerror(errno));
		}
		buffer = (void *)(data_ptr_cast(buffer) + current_bytes_written);
		bytes_to_write -= current_bytes_written;
	}

	DUCKDB_LOG_FILE_SYSTEM_WRITE(handle, nr_bytes, unix_handle.current_pos);
	unix_handle.current_pos += UnsafeNumericCast<idx_t>(nr_bytes);

	return nr_bytes;
}

bool LocalFileSystem::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
#if defined(__linux__)
	// FALLOC_FL_PUNCH_HOLE requires glibc 2.18 or up
#if __GLIBC__ < 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ < 18)
	return false;
#else
	int fd = handle.Cast<UnixFileHandle>().fd;
	int res = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, UnsafeNumericCast<int64_t>(offset_bytes),
	                    UnsafeNumericCast<int64_t>(length_bytes));
	return res == 0;
#endif
#else
	return false;
#endif
}

int64_t LocalFileSystem::GetFileSize(FileHandle &handle) {
	const auto file_metadata = Stats(handle);
	return file_metadata.file_size;
}

timestamp_t LocalFileSystem::GetLastModifiedTime(FileHandle &handle) {
	const auto file_metadata = Stats(handle);
	return file_metadata.last_modification_time;
}

FileType LocalFileSystem::GetFileType(FileHandle &handle) {
	const auto file_metadata = Stats(handle);
	return file_metadata.file_type;
}

FileMetadata LocalFileSystem::Stats(FileHandle &handle) {
	int fd = handle.Cast<UnixFileHandle>().fd;
	auto file_metadata = StatsInternal(fd, handle.GetPath());
	return file_metadata;
}

void LocalFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	int fd = handle.Cast<UnixFileHandle>().fd;
	if (ftruncate(fd, new_size) != 0) {
		throw IOException({{"errno", std::to_string(errno)}}, "Could not truncate file \"%s\": %s", handle.path,
		                  strerror(errno));
	}
}

bool LocalFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	if (!directory.empty()) {
		auto normalized_dir = ExpandPath(directory, opener);
		if (access(normalized_dir.c_str(), 0) == 0) {
			struct stat status;
			stat(normalized_dir.c_str(), &status);
			if (S_ISDIR(status.st_mode)) {
				return true;
			}
		}
	}
	// if any condition fails
	return false;
}

void LocalFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	struct stat st;

	auto normalized_dir = ExpandPath(directory, opener);
	if (stat(normalized_dir.c_str(), &st) != 0) {
		/* Directory does not exist. EEXIST for race condition */
		if (mkdir(normalized_dir.c_str(), 0755) != 0 && errno != EEXIST) {
			throw IOException({{"errno", std::to_string(errno)}}, "Failed to create directory \"%s\": %s", directory,
			                  strerror(errno));
		}
	} else if (!S_ISDIR(st.st_mode)) {
		throw IOException({{"errno", std::to_string(errno)}},
		                  "Failed to create directory \"%s\": path exists but is not a directory!", directory);
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
			buf = new (std::nothrow) char[len];
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

void LocalFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	auto normalized_dir = ExpandPath(directory, opener);
	RemoveDirectoryRecursive(normalized_dir.c_str());
}

void LocalFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto normalized_file = ExpandPath(filename, opener);
	if (std::remove(normalized_file.c_str()) != 0) {
		throw IOException({{"errno", std::to_string(errno)}}, "Could not remove file \"%s\": %s", filename,
		                  strerror(errno));
	}
}

bool LocalFileSystem::ListFilesExtended(const string &directory,
                                        const std::function<void(OpenFileInfo &info)> &callback,
                                        optional_ptr<FileOpener> opener) {
	auto normalized_dir = ExpandPath(directory, opener);
	auto dir = opendir(normalized_dir.c_str());
	if (!dir) {
		return false;
	}

	// RAII wrapper around DIR to automatically free on exceptions in callback
	duckdb::unique_ptr<DIR, std::function<void(DIR *)>> dir_unique_ptr(dir, [](DIR *d) { closedir(d); });

	struct dirent *ent;
	// loop over all files in the directory
	while ((ent = readdir(dir)) != nullptr) {
		OpenFileInfo info(ent->d_name);
		auto &name = info.path;
		// skip . .. and empty files
		if (name.empty() || name == "." || name == "..") {
			continue;
		}
		// now stat the file to figure out if it is a regular file or directory
		string full_path = JoinPath(normalized_dir, name);
		struct stat status;
		auto res = stat(full_path.c_str(), &status);
		if (res != 0) {
			continue;
		}
		if (!S_ISREG(status.st_mode) && !S_ISDIR(status.st_mode)) {
			// not a file or directory: skip
			continue;
		}
		// create extended info
		info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		auto &options = info.extended_info->options;
		// file type
		Value file_type(S_ISDIR(status.st_mode) ? "directory" : "file");
		options.emplace("type", std::move(file_type));
		// file size
		options.emplace("file_size", Value::BIGINT(UnsafeNumericCast<int64_t>(status.st_size)));
		// last modified time
		options.emplace("last_modified", Value::TIMESTAMP(Timestamp::FromTimeT(status.st_mtime)));

		// invoke callback
		callback(info);
	}
	return true;
}

void LocalFileSystem::FileSync(FileHandle &handle) {
	int fd = handle.Cast<UnixFileHandle>().fd;

#if HAVE_FULLFSYNC
	// On macOS and iOS, fsync() doesn't guarantee durability past power failures. fcntl(F_FULLFSYNC) is required for
	// that purpose. Some filesystems don't support fcntl(F_FULLFSYNC), and require a fallback to fsync().
	if (::fcntl(fd, F_FULLFSYNC) == 0) {
		return;
	}
#endif // HAVE_FULLFSYNC

#if HAVE_FDATASYNC
	bool sync_success = ::fdatasync(fd) == 0;
#else
	bool sync_success = ::fsync(fd) == 0;
#endif // HAVE_FDATASYNC

	if (sync_success) {
		return;
	}

	// Use fatal exception to handle fsyncgate issue: `fsync` only reports EIO for once, which makes it unretriable and
	// data loss unrecoverable.
	if (errno == EIO) {
		throw FatalException("fsync failed!");
	}

	// For other types of errors, throw normal IO exception.
	throw IOException("Could not fsync file \"%s\": %s", handle.GetPath(), strerror(errno));
}

void LocalFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	auto normalized_source = ExpandPath(source, opener);
	auto normalized_target = ExpandPath(target, opener);
	//! FIXME: rename does not guarantee atomicity or overwriting target file if it exists
	if (rename(normalized_source.c_str(), normalized_target.c_str()) != 0) {
		throw IOException({{"errno", to_string(errno)}}, "Could not rename file \"%s\" to \"%s\": %s", source, target,
		                  strerror(errno));
	}
}

std::string LocalFileSystem::GetLastErrorAsString() {
	return string();
}

bool LocalFileSystem::TryCanonicalizeExistingPath(string &input) {
	char resolved[PATH_MAX];
	if (!realpath(input.c_str(), resolved)) {
		return false;
	}
	input = resolved;
	return true;
}

bool LocalFileSystem::PathStartsWithDrive(const string &path) {
	return false;
}

bool LocalFileSystem::IsPathAbsolute(const string &path) {
	return FileSystem::IsPathAbsolute(path);
}

string LocalFileSystem::MakePathAbsolute(const string &path_p, optional_ptr<FileOpener> opener) {
	auto path = ExpandPath(path_p, opener);
	if (!IsPathAbsolute(path)) {
		// path is not absolute - join with working directory
		return JoinPath(GetWorkingDirectory(), path);
	} else {
		// already absolute
		return path;
	}
}
#else

constexpr char PIPE_PREFIX[] = "\\\\.\\pipe\\";

// Returns the last Win32 error, in string format. Returns an empty string if there is no error.
std::string LocalFileSystem::GetLastErrorAsString() {
	// Get the error message, if any.
	DWORD errorMessageID = GetLastError();
	if (errorMessageID == 0)
		return std::string(); // No error message has been recorded

	LPWSTR messageBuffer = nullptr;
	idx_t size = FormatMessageW(
	    FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS, NULL,
	    errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPWSTR)&messageBuffer, 0, NULL);

	if (size == 0) {
		return std::string();
	}

	// Convert wide string to UTF-8
	std::wstring wideMessage(messageBuffer, size);
	std::string message = WindowsUtil::UnicodeToUTF8(wideMessage.c_str());

	// Free the buffer.
	LocalFree(messageBuffer);

	return message;
}

static timestamp_t FiletimeToTimeStamp(FILETIME file_time) {
	// https://stackoverflow.com/questions/29266743/what-is-dwlowdatetime-and-dwhighdatetime
	ULARGE_INTEGER ul;
	ul.LowPart = file_time.dwLowDateTime;
	ul.HighPart = file_time.dwHighDateTime;
	int64_t fileTime64 = ul.QuadPart;

	// fileTime64 contains a 64-bit value representing the number of
	// 100-nanosecond intervals since January 1, 1601 (UTC).
	// https://docs.microsoft.com/en-us/windows/win32/api/minwinbase/ns-minwinbase-filetime

	// Adapted from: https://stackoverflow.com/questions/6161776/convert-windows-filetime-to-second-in-unix-linux
	const auto WINDOWS_TICK = 10000000;
	const auto SEC_TO_UNIX_EPOCH = 11644473600LL;
	return Timestamp::FromTimeT(fileTime64 / WINDOWS_TICK - SEC_TO_UNIX_EPOCH);
}

static FileMetadata StatsInternal(HANDLE hFile, const string &path) {
	FileMetadata file_metadata;

	DWORD handle_type = GetFileType(hFile);
	if (handle_type == FILE_TYPE_CHAR) {
		file_metadata.file_type = FileType::FILE_TYPE_CHARDEV;
		file_metadata.file_size = 0;
		file_metadata.last_modification_time = Timestamp::FromTimeT(0);
		return file_metadata;
	}
	if (handle_type == FILE_TYPE_PIPE) {
		file_metadata.file_type = FileType::FILE_TYPE_FIFO;
		file_metadata.file_size = 0;
		file_metadata.last_modification_time = Timestamp::FromTimeT(0);
		return file_metadata;
	}

	BY_HANDLE_FILE_INFORMATION file_info;
	if (!GetFileInformationByHandle(hFile, &file_info)) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Failed to get stats for file \"%s\": %s", path, error);
	}

	// Get file size from high and low parts.
	file_metadata.file_size =
	    (static_cast<int64_t>(file_info.nFileSizeHigh) << 32) | static_cast<int64_t>(file_info.nFileSizeLow);

	// Get last modification time
	file_metadata.last_modification_time = FiletimeToTimeStamp(file_info.ftLastWriteTime);

	// Get file type from attributes
	if (strncmp(path.c_str(), PIPE_PREFIX, strlen(PIPE_PREFIX)) == 0) {
		// pipes in windows are just files in '\\.\pipe\' folder
		file_metadata.file_type = FileType::FILE_TYPE_FIFO;
	} else if (file_info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
		file_metadata.file_type = FileType::FILE_TYPE_DIR;
	} else if (file_info.dwFileAttributes & FILE_ATTRIBUTE_DEVICE) {
		file_metadata.file_type = FileType::FILE_TYPE_CHARDEV;
	} else if (file_info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) {
		file_metadata.file_type = FileType::FILE_TYPE_LINK;
	} else if (file_info.dwFileAttributes != INVALID_FILE_ATTRIBUTES) {
		file_metadata.file_type = FileType::FILE_TYPE_REGULAR;
	} else {
		file_metadata.file_type = FileType::FILE_TYPE_INVALID;
	}

	return file_metadata;
}

struct WindowsFileHandle : public FileHandle {
public:
	WindowsFileHandle(FileSystem &file_system, string path, HANDLE fd, FileOpenFlags flags)
	    : FileHandle(file_system, path, flags), position(0), fd(fd) {
	}
	~WindowsFileHandle() override {
		Close();
	}

	idx_t position;
	HANDLE fd;

public:
	void Close() override {
		if (!fd) {
			return;
		}
		CloseHandle(fd);
		fd = nullptr;
		DUCKDB_LOG_FILE_SYSTEM_CLOSE((*this));
	};
};

static string AdditionalLockInfo(const std::wstring path) {
	// try to find out if another process is holding the lock

	// init of the somewhat obscure "Windows Restart Manager"
	// see also https://devblogs.microsoft.com/oldnewthing/20120217-00/?p=8283

	DWORD session, status, reason;
	WCHAR session_key[CCH_RM_SESSION_KEY + 1] = {0};

	status = RmStartSession(&session, 0, session_key);
	if (status != ERROR_SUCCESS) {
		return string();
	}

	PCWSTR path_ptr = path.c_str();
	status = RmRegisterResources(session, 1, &path_ptr, 0, NULL, 0, NULL);
	if (status != ERROR_SUCCESS) {
		return string();
	}
	UINT process_info_size_needed, process_info_size;

	// we first call with nProcInfo = 0 to find out how much to allocate
	process_info_size = 0;
	status = RmGetList(session, &process_info_size_needed, &process_info_size, NULL, &reason);
	if (status != ERROR_MORE_DATA || process_info_size_needed == 0) {
		return string();
	}

	// allocate
	auto process_info_buffer = duckdb::unique_ptr<RM_PROCESS_INFO[]>(new RM_PROCESS_INFO[process_info_size_needed]);
	auto process_info = process_info_buffer.get();

	// now call again to get actual data
	process_info_size = process_info_size_needed;
	status = RmGetList(session, &process_info_size_needed, &process_info_size, process_info, &reason);
	if (status != ERROR_SUCCESS || process_info_size == 0) {
		return "";
	}

	string conflict_string;
	for (UINT process_idx = 0; process_idx < process_info_size; process_idx++) {
		string process_name = WindowsUtil::UnicodeToUTF8(process_info[process_idx].strAppName);
		auto pid = process_info[process_idx].Process.dwProcessId;

		// find out full path if possible
		HANDLE process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, pid);
		if (process) {
			WCHAR full_path[MAX_PATH];
			DWORD full_path_size = MAX_PATH;
			if (QueryFullProcessImageNameW(process, 0, full_path, &full_path_size) && full_path_size <= MAX_PATH) {
				process_name = WindowsUtil::UnicodeToUTF8(full_path);
			}
			CloseHandle(process);
		}
		conflict_string += StringUtil::Format("\n%s (PID %d)", process_name, pid);
	}

	RmEndSession(session);
	if (conflict_string.empty()) {
		return string();
	}
	return "File is already open in " + conflict_string;
}

bool LocalFileSystem::IsPrivateFile(const string &path_p, FileOpener *opener) {
	// TODO: detect if file is shared in windows
	return true;
}

unique_ptr<FileHandle> LocalFileSystem::OpenFile(const string &path_p, FileOpenFlags flags,
                                                 optional_ptr<FileOpener> opener) {
	auto path = FileSystem::ExpandPath(path_p, opener);
	auto unicode_path = NormalizePathAndConvertToUnicode(*this, path, opener);
	if (flags.Compression() != FileCompressionType::UNCOMPRESSED) {
		throw NotImplementedException("Unsupported compression type for default file system");
	}
	flags.Verify();

	DWORD desired_access;
	DWORD share_mode;
	DWORD creation_disposition = OPEN_EXISTING;
	DWORD flags_and_attributes = FILE_ATTRIBUTE_NORMAL;
	bool open_read = flags.OpenForReading();
	bool open_write = flags.OpenForWriting();
	if (open_read && open_write) {
		desired_access = GENERIC_READ | GENERIC_WRITE;
	} else if (open_read) {
		desired_access = GENERIC_READ;
	} else if (open_write) {
		desired_access = GENERIC_WRITE;
	} else {
		throw InternalException("READ, WRITE or both should be specified when opening a file");
	}
	switch (flags.Lock()) {
	case FileLockType::NO_LOCK:
		share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
		break;
	case FileLockType::READ_LOCK:
		share_mode = FILE_SHARE_READ;
		break;
	case FileLockType::WRITE_LOCK:
		share_mode = 0;
		break;
	default:
		throw InternalException("Unknown FileLockType");
	}
	// For windows platform, by default deletion fails when the file is accessed by other thread/process.
	// To keep deletion behavior compatible with unix platform, which physically deletes a file when reference count
	// drops to 0 without interfering with already opened file handles, open files with [`FILE_SHARE_DELETE`].
	share_mode |= FILE_SHARE_DELETE;

	if (open_write) {
		if (flags.CreateFileIfNotExists()) {
			creation_disposition = OPEN_ALWAYS;
		} else if (flags.OverwriteExistingFile()) {
			creation_disposition = CREATE_ALWAYS;
		}
	}
	if (flags.DirectIO()) {
		flags_and_attributes |= FILE_FLAG_NO_BUFFERING;
	}
	HANDLE hFile = CreateFileW(unicode_path.c_str(), desired_access, share_mode, NULL, creation_disposition,
	                           flags_and_attributes, NULL);
	if (hFile == INVALID_HANDLE_VALUE) {
		if (flags.ReturnNullIfNotExists() && GetLastError() == ERROR_FILE_NOT_FOUND) {
			return nullptr;
		}
		auto error = LocalFileSystem::GetLastErrorAsString();

		auto extended_error = AdditionalLockInfo(unicode_path);
		if (!extended_error.empty()) {
			extended_error = "\n" + extended_error;
		}
		throw IOException("Cannot open file \"%s\": %s%s", path.c_str(), error, extended_error);
	}
	auto handle = make_uniq<WindowsFileHandle>(*this, path.c_str(), hFile, flags);
	if (flags.OpenForAppending()) {
		auto file_size = GetFileSize(*handle);
		SetFilePointer(*handle, file_size);
	}
	if (opener) {
		handle->TryAddLogger(*opener);
		DUCKDB_LOG_FILE_SYSTEM_OPEN((*handle));
	}
	return std::move(handle);
}

void LocalFileSystem::SetFilePointer(FileHandle &handle, idx_t location) {
	auto &whandle = handle.Cast<WindowsFileHandle>();
	whandle.position = location;
	LARGE_INTEGER wlocation;
	wlocation.QuadPart = location;
	SetFilePointerEx(whandle.fd, wlocation, NULL, FILE_BEGIN);
}

idx_t LocalFileSystem::GetFilePointer(FileHandle &handle) {
	return handle.Cast<WindowsFileHandle>().position;
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
		throw IOException("Could not read file \"%s\" (error in ReadFile(location: %llu, nr_bytes: %lld)): %s",
		                  handle.path, location, nr_bytes, error);
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
	DUCKDB_LOG_FILE_SYSTEM_READ(handle, bytes_read, location);
}

int64_t LocalFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	HANDLE hFile = handle.Cast<WindowsFileHandle>().fd;
	auto &pos = handle.Cast<WindowsFileHandle>().position;
	auto n = std::min<idx_t>(std::max<idx_t>(GetFileSize(handle), pos) - pos, nr_bytes);
	auto bytes_read = FSInternalRead(handle, hFile, buffer, n, pos);
	pos += bytes_read;

	DUCKDB_LOG_FILE_SYSTEM_READ(handle, bytes_read, pos - bytes_read);
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

static int64_t FSWrite(FileHandle &handle, HANDLE hFile, void *buffer, int64_t nr_bytes, idx_t location) {
	int64_t bytes_written = 0;
	while (nr_bytes > 0) {
		auto bytes_to_write = MinValue<idx_t>(idx_t(NumericLimits<int32_t>::Maximum()), idx_t(nr_bytes));
		DWORD current_bytes_written = FSInternalWrite(handle, hFile, buffer, bytes_to_write, location);
		if (current_bytes_written <= 0) {
			throw IOException({{"errno", std::to_string(errno)}}, "Could not write file \"%s\": %s", handle.path,
			                  strerror(errno));
		}
		bytes_written += current_bytes_written;
		buffer = (void *)(data_ptr_cast(buffer) + current_bytes_written);
		location += current_bytes_written;
		nr_bytes -= current_bytes_written;
	}
	return bytes_written;
}

void LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	HANDLE hFile = handle.Cast<WindowsFileHandle>().fd;
	auto bytes_written = FSWrite(handle, hFile, buffer, nr_bytes, location);
	if (bytes_written != nr_bytes) {
		throw IOException("Could not write all bytes from file \"%s\": wanted=%lld wrote=%lld", handle.path, nr_bytes,
		                  bytes_written);
	}
	DUCKDB_LOG_FILE_SYSTEM_WRITE(handle, bytes_written, location);
}

int64_t LocalFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	HANDLE hFile = handle.Cast<WindowsFileHandle>().fd;
	auto &pos = handle.Cast<WindowsFileHandle>().position;
	auto bytes_written = FSWrite(handle, hFile, buffer, nr_bytes, pos);
	pos += bytes_written;
	DUCKDB_LOG_FILE_SYSTEM_WRITE(handle, bytes_written, pos - bytes_written);
	return bytes_written;
}

bool LocalFileSystem::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	// TODO: Not yet implemented on windows.
	return false;
}

int64_t LocalFileSystem::GetFileSize(FileHandle &handle) {
	const auto file_metadata = Stats(handle);
	return file_metadata.file_size;
}

timestamp_t LocalFileSystem::GetLastModifiedTime(FileHandle &handle) {
	const auto file_metadata = Stats(handle);
	return file_metadata.last_modification_time;
}

void LocalFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	HANDLE hFile = handle.Cast<WindowsFileHandle>().fd;
	// seek to the location
	SetFilePointer(handle, new_size);
	// now set the end of file position
	if (!SetEndOfFile(hFile)) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Failure in SetEndOfFile call on file \"%s\": %s", handle.path, error);
	}
}

static DWORD WindowsGetFileAttributes(LocalFileSystem &fs, const string &filename, optional_ptr<FileOpener> opener) {
	auto unicode_path = NormalizePathAndConvertToUnicode(fs, filename, opener);
	return GetFileAttributesW(unicode_path.c_str());
}

static DWORD WindowsGetFileAttributes(const std::wstring &filename) {
	return GetFileAttributesW(filename.c_str());
}

bool LocalFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	DWORD attrs = WindowsGetFileAttributes(*this, directory, opener);
	return (attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY));
}

void LocalFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	if (DirectoryExists(directory)) {
		return;
	}
	auto unicode_path = NormalizePathAndConvertToUnicode(*this, directory, opener);
	if (directory.empty() || !CreateDirectoryW(unicode_path.c_str(), NULL) || !DirectoryExists(directory)) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Failed to create directory \"%s\": %s", directory.c_str(), error);
	}
}

static void DeleteDirectoryRecursive(FileSystem &fs, string directory, optional_ptr<FileOpener> opener) {
	fs.ListFiles(directory, [&](const string &fname, bool is_directory) {
		if (is_directory) {
			DeleteDirectoryRecursive(fs, fs.JoinPath(directory, fname), opener);
		} else {
			fs.RemoveFile(fs.JoinPath(directory, fname));
		}
	});
	auto unicode_path = NormalizePathAndConvertToUnicode(fs, directory, opener);
	if (!RemoveDirectoryW(unicode_path.c_str())) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Failed to delete directory \"%s\": %s", directory, error);
	}
}

void LocalFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	if (FileExists(directory)) {
		throw IOException("Attempting to delete directory \"%s\", but it is a file and not a directory!", directory);
	}
	if (!DirectoryExists(directory)) {
		return;
	}
	DeleteDirectoryRecursive(*this, directory, opener);
}

void LocalFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	auto unicode_path = NormalizePathAndConvertToUnicode(*this, filename, opener);
	if (!DeleteFileW(unicode_path.c_str())) {
		auto error = LocalFileSystem::GetLastErrorAsString();
		throw IOException("Failed to delete file \"%s\": %s", filename, error);
	}
}

bool LocalFileSystem::ListFilesExtended(const string &directory,
                                        const std::function<void(OpenFileInfo &info)> &callback,
                                        optional_ptr<FileOpener> opener) {
	string search_dir = JoinPath(directory, "*");
	auto unicode_path = NormalizePathAndConvertToUnicode(*this, search_dir, opener);

	WIN32_FIND_DATAW ffd;
	HANDLE hFind = FindFirstFileW(unicode_path.c_str(), &ffd);
	if (hFind == INVALID_HANDLE_VALUE) {
		return false;
	}
	do {
		OpenFileInfo info(WindowsUtil::UnicodeToUTF8(ffd.cFileName));
		auto &name = info.path;
		if (name == "." || name == "..") {
			continue;
		}
		// create extended info
		info.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		auto &options = info.extended_info->options;
		// file type
		Value file_type(ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY ? "directory" : "file");
		options.emplace("type", std::move(file_type));
		// file size
		int64_t file_size_bytes =
		    (static_cast<int64_t>(ffd.nFileSizeHigh) << 32) | static_cast<int64_t>(ffd.nFileSizeLow);
		options.emplace("file_size", Value::BIGINT(file_size_bytes));
		// last modified time
		auto last_modified_time = FiletimeToTimeStamp(ffd.ftLastWriteTime);
		options.emplace("last_modified", Value::TIMESTAMP(last_modified_time));

		// callback
		callback(info);
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
	HANDLE hFile = handle.Cast<WindowsFileHandle>().fd;
	if (FlushFileBuffers(hFile) == 0) {
		throw IOException("Could not flush file handle to disk!");
	}
}

void LocalFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	auto source_unicode = NormalizePathAndConvertToUnicode(*this, source, opener);
	auto target_unicode = NormalizePathAndConvertToUnicode(*this, target, opener);
	DWORD flags = MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH;

	if (!MoveFileExW(source_unicode.c_str(), target_unicode.c_str(), flags)) {
		throw IOException("Could not move file: %s", GetLastErrorAsString());
	}
}

FileType LocalFileSystem::GetFileType(FileHandle &handle) {
	const auto file_metadata = Stats(handle);
	return file_metadata.file_type;
}

FileMetadata LocalFileSystem::Stats(FileHandle &handle) {
	HANDLE hFile = handle.Cast<WindowsFileHandle>().fd;
	auto file_metadata = StatsInternal(hFile, handle.GetPath());
	return file_metadata;
}

bool LocalFileSystem::TryCanonicalizeExistingPath(string &input) {
	auto unicode_path = ConvertPathToUnicode(input);
	HANDLE handle = CreateFileW(unicode_path.c_str(),
	                            0, // No access needed, just query
	                            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
	                            FILE_FLAG_BACKUP_SEMANTICS, // Required for directories
	                            NULL);

	if (handle == INVALID_HANDLE_VALUE) {
		return false;
	}
	wchar_t resolved[MAX_PATH];
	DWORD len = GetFinalPathNameByHandleW(handle, resolved, MAX_PATH, FILE_NAME_NORMALIZED);
	CloseHandle(handle);

	if (len < 0 && len >= MAX_PATH) {
		return false;
	}
	input = WindowsUtil::UnicodeToUTF8(resolved);
	return true;
}

bool LocalFileSystem::PathStartsWithDrive(const string &path) {
	return path.size() >= 2 && path[0] >= 'A' && path[0] <= 'Z' && path[1] == ':';
}

bool LocalFileSystem::IsPathAbsolute(const string &path) {
	if (FileSystem::IsPathAbsolute(path)) {
		return true;
	}
	// check if this is a drive letter (e.g. C:)
	if (PathStartsWithDrive(path)) {
		return true;
	}
	return false;
}

string LocalFileSystem::MakePathAbsolute(const string &path_p, optional_ptr<FileOpener> opener) {
	auto path = ExpandPath(path_p, opener);
	if (FileSystem::IsPathAbsolute(path)) {
		// already absolute - nothing to do
		return path;
	}
	// check if this is a drive letter (e.g. C:)
	if (PathStartsWithDrive(path)) {
		// this starts with a drive letter
		// we now have two options - either this is "C:" or this is "C:\"
		// "C:" is the current working directory, C:\ is an absolute path
		if (path.size() >= 3 && (path[2] == '\\' || path[2] == '/')) {
			// C:\\ - this is already an absolute path
			return path;
		}
		// this is "C:" - expand to current working directory if this is the current drive
		auto working_directory = GetWorkingDirectory();
		if (working_directory[0] != path[0]) {
			// this is not the drive we are on right now (e.g. referencing D: while in C:)
			// default to root of drive
			working_directory = string(1, path[0]) + ":\\";
		}
		if (path.size() == 2) {
			return working_directory;
		}
		return JoinPath(working_directory, path.substr(2));
	}

	return JoinPath(GetWorkingDirectory(), path);
}

#endif

bool LocalFileSystem::CanSeek() {
	return true;
}

bool LocalFileSystem::OnDiskFile(FileHandle &handle) {
	return true;
}

string LocalFileSystem::CanonicalizePath(const string &input, optional_ptr<FileOpener> opener) {
	auto path_sep = PathSeparator(input);
	if (path_sep.size() != 1) {
		throw InternalException("path separator can only be a single byte for local file systems");
	}
	// make the path absolute
	string path = MakePathAbsolute(input, opener);

	string current = path;
	string remainder;
	idx_t dot_dot_count = 0;
	bool is_drive = false;
	while (!current.empty()) {
		if (dot_dot_count == 0 && TryCanonicalizeExistingPath(current)) {
			// successfully canonicalized "current" - add remainder if we have any
			if (remainder.empty()) {
				return current;
			}
			if (StringUtil::EndsWith(current, path_sep)) {
				return current + remainder;
			}
			return current + path_sep + remainder;
		}
		if (is_drive) {
			// this is a drive only (e.g. C:\)
			// if we reach this, this is an unknown drive letter
			// use fallback canonicalize for the remainder
			return current + FileSystem::CanonicalizePath(remainder);
		}
		// move up one directory
		optional_idx sep_idx;
		for (idx_t i = current.size(); i > 0; i--) {
			// on windows we accept both separators (\ and /, and also :)
			if (current[i - 1] == path_sep[0] || current[i - 1] == '/') {
				sep_idx = i - 1;
				break;
			}
		}
		if (!sep_idx.IsValid()) {
			// exhausted the full path and nothing exists - break out
			current = string();
			break;
		}
		auto sep = sep_idx.GetIndex();
		auto component = current.substr(sep + 1);
		if (component == "..") {
			// dot dot - we need to move up a level
			// increment the count
			dot_dot_count++;
		} else if (!component.empty() && component != ".") {
			if (dot_dot_count > 0) {
				// just clear this directory
				dot_dot_count--;
			} else {
				// add component to remainder - unless it's dot or empty
				if (remainder.empty()) {
					remainder = component;
				} else {
					remainder = component + path_sep + remainder;
				}
			}
		}
		// continue with remainder
		current = current.substr(0, sep);
		if (current.size() == 2 && PathStartsWithDrive(current)) {
			// Windows only
			// C: and C:\\ mean different things (C: is relative, C:\\ is absolute)
			// we should have already normalized to C:\\ earlier on in this function
			// so turn this base drive letter into C:\\ for the final lookup
			is_drive = true;
			current += "\\";
		}
	}
	// failed to canonicalize path - fallback to generic canonicalization
	return FileSystem::CanonicalizePath(path);
}

string LocalFileSystem::GetVersionTag(FileHandle &handle) {
	// TODO: Fix using FileSystem::Stats for v1.5, which should also fix it for Windows
#ifdef _WIN32
	return "";
#else
	int fd = handle.Cast<UnixFileHandle>().fd;
	struct stat s;
	if (fstat(fd, &s) == -1) {
		throw IOException("Failed to get file size for file \"%s\": %s", handle.path, strerror(errno));
	}

	// dev/ino should be enough, but to guard against in-place writes we also add file size and modification time
	uint64_t version_tag[4];
	Store(NumericCast<uint64_t>(s.st_dev), data_ptr_cast(&version_tag[0]));
	Store(NumericCast<uint64_t>(s.st_ino), data_ptr_cast(&version_tag[1]));
	Store(NumericCast<uint64_t>(s.st_size), data_ptr_cast(&version_tag[2]));
	Store(Timestamp::FromEpochSeconds(s.st_mtime).value, data_ptr_cast(&version_tag[3]));

	return string(char_ptr_cast(version_tag), sizeof(uint64_t) * 4);
#endif
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

static bool IsCrawl(const string &glob) {
	// glob must match exactly
	return glob == "**";
}

static bool IsSymbolicLink(const string &path) {
#ifndef _WIN32
	struct stat status;
	return (lstat(path.c_str(), &status) != -1 && S_ISLNK(status.st_mode));
#else
	auto unicode_path = ConvertPathToUnicode(path);
	auto attributes = WindowsGetFileAttributes(unicode_path);
	if (attributes == INVALID_FILE_ATTRIBUTES) {
		return false;
	}
	return attributes & FILE_ATTRIBUTE_REPARSE_POINT;
#endif
}

vector<OpenFileInfo> LocalFileSystem::FetchFileWithoutGlob(const string &path, optional_ptr<FileOpener> opener,
                                                           bool absolute_path) {
	vector<OpenFileInfo> result;
	if (FileExists(path, opener) || IsPipe(path, opener)) {
		result.emplace_back(path);
	} else if (!absolute_path) {
		Value value;
		if (opener && opener->TryGetCurrentSetting("file_search_path", value)) {
			auto search_paths_str = value.ToString();
			vector<std::string> search_paths = StringUtil::Split(search_paths_str, ',');
			for (const auto &search_path : search_paths) {
				auto joined_path = JoinPath(search_path, path);
				if (FileExists(joined_path, opener) || IsPipe(joined_path, opener)) {
					result.emplace_back(joined_path);
				}
			}
		}
	}
	return result;
}

struct PathSplit {
	PathSplit(LocalFileSystem &fs, string path_p) : path(std::move(path_p)), has_glob(fs.HasGlob(path)) {
	}

	string path;
	bool has_glob;
};

static bool HasMultipleCrawl(const vector<PathSplit> &splits) {
	idx_t crawl_count = 0;
	for (auto &split : splits) {
		if (split.path == "**") {
			crawl_count++;
		}
	}
	return crawl_count > 1;
}

struct ExpandDirectory {
	ExpandDirectory(string path_p, idx_t split_index, bool is_empty = false)
	    : path(std::move(path_p)), split_index(split_index), is_empty(is_empty) {
	}

	string path;
	idx_t split_index;
	bool is_empty = false;

	bool operator<(const ExpandDirectory &other) const {
		return path > other.path;
	}
};

static void CrawlDirectoryLevel(FileSystem &fs, const string &path, optional_ptr<vector<OpenFileInfo>> files,
                                std::priority_queue<ExpandDirectory> &directories, idx_t split_index) {
	fs.ListFiles(path, [&](OpenFileInfo &info) {
		info.path = fs.JoinPath(path, info.path);
		if (IsSymbolicLink(info.path)) {
			return;
		}
		bool is_directory = FileSystem::IsDirectory(info);
		if (is_directory) {
			directories.emplace(std::move(info.path), split_index);
		} else if (files) {
			files->push_back(std::move(info));
		}
	});
}

static void GlobFilesInternal(FileSystem &fs, const string &path, const string &glob, bool match_directory,
                              vector<OpenFileInfo> &result) {
	fs.ListFiles(path, [&](OpenFileInfo &info) {
		bool is_directory = FileSystem::IsDirectory(info);
		if (is_directory != match_directory) {
			return;
		}
		if (Glob(info.path.c_str(), info.path.size(), glob.c_str(), glob.size())) {
			info.path = fs.JoinPath(path, info.path);
			result.push_back(std::move(info));
		}
	});
}

struct LocalGlobResult : public LazyMultiFileList {
public:
	LocalGlobResult(LocalFileSystem &fs, const string &path, FileGlobOptions options, optional_ptr<FileOpener> opener);

protected:
	bool ExpandNextPath() const override;

private:
	LocalFileSystem &fs;
	string path;
	optional_ptr<FileOpener> opener;
	vector<PathSplit> splits;
	bool absolute_path = false;
	mutable std::priority_queue<ExpandDirectory> expand_directories;
	mutable bool finished = false;
};

LocalGlobResult::LocalGlobResult(LocalFileSystem &fs, const string &path_p, FileGlobOptions options_p,
                                 optional_ptr<FileOpener> opener)
    : LazyMultiFileList(FileOpener::TryGetClientContext(opener)), fs(fs), path(fs.ExpandPath(path_p, opener)),
      opener(opener) {
	if (path.empty()) {
		finished = true;
		return;
	}
	// split up the path into separate chunks
	idx_t last_pos = 0;
	for (idx_t i = 0; i < path.size(); i++) {
		if (path[i] == '\\' || path[i] == '/') {
			if (i == last_pos) {
				// empty: skip this position
				last_pos = i + 1;
				continue;
			}
			if (splits.empty()) {
				splits.emplace_back(fs, path.substr(0, i));
			} else {
				splits.emplace_back(fs, path.substr(last_pos, i - last_pos));
			}
			last_pos = i + 1;
		}
	}
	splits.emplace_back(fs, path.substr(last_pos, path.size() - last_pos));
	// handle absolute paths
	absolute_path = false;
	if (fs.IsPathAbsolute(path)) {
		// first character is a slash -  unix absolute path
		absolute_path = true;
	} else if (StringUtil::Contains(splits[0].path,
	                                ":")) { // TODO: this is weird? shouldn't IsPathAbsolute handle this?
		// first split has a colon -  windows absolute path
		absolute_path = true;
	} else if (splits[0].path == "~") {
		// starts with home directory
		auto home_directory = fs.GetHomeDirectory(opener);
		if (!home_directory.empty()) {
			absolute_path = true;
			splits[0].path = home_directory;
			D_ASSERT(path[0] == '~');
			if (!fs.HasGlob(path)) {
				expanded_files = fs.FetchFileWithoutGlob(home_directory + path.substr(1), opener, absolute_path);
				finished = true;
				return;
			}
		}
	}
	// Check if the path has a glob at all
	if (!fs.HasGlob(path)) {
		// no glob: return only the file (if it exists or is a pipe)
		expanded_files = fs.FetchFileWithoutGlob(path, opener, absolute_path);
		finished = true;
		return;
	}
	if (absolute_path) {
		// for absolute paths, we don't start by scanning the current directory
		// FIXME: we don't support /[GLOB]/.. - i.e. globs in the first level of an absolute path
		if (splits.size() > 1) {
			expand_directories.emplace(splits[0].path, 1);
		}
	} else {
		// If file_search_path is set, use those paths as the first glob elements
		Value value;
		if (opener && opener->TryGetCurrentSetting("file_search_path", value)) {
			auto search_paths_str = value.ToString();
			auto search_paths = StringUtil::Split(search_paths_str, ',');
			for (const auto &search_path : search_paths) {
				expand_directories.emplace(search_path, 0);
			}
		}
		if (expand_directories.empty()) {
			expand_directories.emplace(".", 0, true);
		}
	}

	if (HasMultipleCrawl(splits)) {
		throw IOException("Cannot use multiple \'**\' in one path");
	}
}

bool LocalGlobResult::ExpandNextPath() const {
	if (finished) {
		return false;
	}
	if (expand_directories.empty()) {
		if (expanded_files.empty()) {
			// no result found that matches the glob
			// last ditch effort: search the path as a string literal
			expanded_files = fs.FetchFileWithoutGlob(path, opener, absolute_path);
		}
		finished = true;
		return false;
	}

	auto next_dir = expand_directories.top();
	auto is_empty = next_dir.is_empty;
	auto split_index = next_dir.split_index;
	auto &current_path = next_dir.path;
	expand_directories.pop();

	auto &next_split = splits[split_index];
	bool is_last_component = split_index + 1 == splits.size();
	auto &next_component = next_split.path;
	bool has_glob = next_split.has_glob;
	// if it's the last chunk we need to find files, otherwise we find directories
	// not the last chunk: gather a list of all directories that match the glob pattern
	if (!has_glob) {
		// no glob, just append as-is
		if (is_empty) {
			if (is_last_component) {
				throw InternalException("No glob in only component - but entire split has globs?");
			}
			// no path yet - just append
			expand_directories.emplace(next_component, split_index + 1);
		} else {
			if (is_last_component) {
				// last component - we are emitting a result here
				auto filename = fs.JoinPath(current_path, next_component);
				if (fs.FileExists(filename, opener) || fs.DirectoryExists(filename, opener)) {
					expanded_files.emplace_back(std::move(filename));
				}
			} else {
				// not the last component - add the next directory as "to-be-expanded"
				expand_directories.emplace(fs.JoinPath(current_path, next_component), split_index + 1);
			}
		}
	} else {
		// glob - need to resolve the glob
		if (IsCrawl(next_component)) {
			if (is_last_component) {
				// the crawl is the last component - we are looking for files in this directory
				// any directories we encounter are added to the expand directories
				CrawlDirectoryLevel(fs, current_path, expanded_files, expand_directories, split_index);
			} else {
				// not the last crawl
				// ** also matches the current directory (i.e. dir/**/file.parquet also matches dir/file.parquet)
				expand_directories.emplace(current_path, split_index + 1);
				// now crawl the contents of this directory - but don't add any files we find
				CrawlDirectoryLevel(fs, current_path, nullptr, expand_directories, split_index);
			}
		} else {
			// glob this directory according to the next component
			if (is_last_component) {
				// last component - match files and place them in the result
				GlobFilesInternal(fs, current_path, next_component, false, expanded_files);
			} else {
				// not the last component - match directories and add to expansion list
				vector<OpenFileInfo> child_directories;
				GlobFilesInternal(fs, current_path, next_component, true, child_directories);
				for (auto &file : child_directories) {
					expand_directories.emplace(std::move(file.path), split_index + 1);
				}
			}
		}
	}
	return true;
}

unique_ptr<MultiFileList> LocalFileSystem::GlobFilesExtended(const string &path, const FileGlobInput &input,
                                                             optional_ptr<FileOpener> opener) {
	return make_uniq<LocalGlobResult>(*this, path, FileGlobOptions::ALLOW_EMPTY, opener);
}

unique_ptr<FileSystem> FileSystem::CreateLocal() {
	return make_uniq<LocalFileSystem>();
}

} // namespace duckdb
