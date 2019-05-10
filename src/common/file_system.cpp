#include "common/file_system.hpp"

#include "common/exception.hpp"
#include "common/helper.hpp"
#include "common/string_util.hpp"

using namespace duckdb;
using namespace std;

#ifndef _WIN32
#include <cstdio>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

struct UnixFileHandle : public FileHandle {
public:
	UnixFileHandle(FileSystem &file_system, string path, int fd) : FileHandle(file_system, path), fd(fd) {
	}
	virtual ~UnixFileHandle() {
		Close();
	}

protected:
	void Close() override {
		if (fd != -1) {
			close(fd);
		}
	};

public:
	int fd;
};

unique_ptr<FileHandle> FileSystem::OpenFile(const char *path, uint8_t flags, FileLockType lock_type) {
	int open_flags = 0;
	int rc;
	// cannot combine Read and Write flags
	assert(!(flags & FileFlags::READ && flags & FileFlags::WRITE));
	// cannot combine Read and DirectIO/CREATE flags
	assert(!(flags & FileFlags::READ && flags & FileFlags::DIRECT_IO));
	assert(!(flags & FileFlags::READ && flags & FileFlags::CREATE));
	if (flags & FileFlags::READ) {
		open_flags = O_RDONLY;
	} else {
		// need Read or Write
		assert(flags & FileFlags::WRITE);
		open_flags = O_RDWR | O_CLOEXEC;
		if (flags & FileFlags::CREATE) {
			open_flags |= O_CREAT;
		}
		if (flags & FileFlags::DIRECT_IO) {
#if defined(__DARWIN__) || defined(__APPLE__)
			// OSX does not have O_DIRECT, instead we need to use fcntl afterwards to support direct IO
			open_flags |= O_SYNC;
#else
			open_flags |= O_DIRECT | O_SYNC;
#endif
		}
	}
	int fd = open(path, open_flags, 0666);
	if (fd == -1) {
		throw IOException("Cannot open file \"%s\": %s", path, strerror(errno));
	}
#if defined(__DARWIN__) || defined(__APPLE__)
	if (flags & FileFlags::DIRECT_IO) {
		// OSX requires fcntl for Direct IO
		rc = fcntl(fd, F_NOCACHE, 1);
		if (fd == -1) {
			throw IOException("Could not enable direct IO for file \"%s\": %s", path, strerror(errno));
		}
	}
#endif
	if (lock_type != FileLockType::NO_LOCK) {
		// set lock on file
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
	return make_unique<UnixFileHandle>(*this, path, fd);
}

static void seek_in_file(FileHandle &handle, uint64_t location) {
	int fd = ((UnixFileHandle &)handle).fd;
	off_t offset = lseek(fd, location, SEEK_SET);
	if (offset == (off_t)-1) {
		throw IOException("Could not seek to location %lld for file \"%s\": %s", location, handle.path.c_str(),
		                  strerror(errno));
	}
}

void FileSystem::Read(FileHandle &handle, void *buffer, uint64_t nr_bytes, uint64_t location) {
	int fd = ((UnixFileHandle &)handle).fd;
	// lseek to the location
	seek_in_file(handle, location);
	// now read from the location
	errno = 0;
	int64_t bytes_read = read(fd, buffer, nr_bytes);
	if (bytes_read == -1) {
		throw IOException("Could not read from file \"%s\": %s", handle.path.c_str(), strerror(errno));
	}
	if ((uint64_t)bytes_read != nr_bytes) {
		throw IOException("Could not read sufficient bytes from file \"%s\"", handle.path.c_str());
	}
}

void FileSystem::Write(FileHandle &handle, void *buffer, uint64_t nr_bytes, uint64_t location) {
	int fd = ((UnixFileHandle &)handle).fd;
	// lseek to the location
	seek_in_file(handle, location);
	// now write to the location
	errno = 0;
	int64_t bytes_written = write(fd, buffer, nr_bytes);
	if (bytes_written == -1) {
		throw IOException("Could not write file \"%s\": %s", handle.path.c_str(), strerror(errno));
	}
	if ((uint64_t)bytes_written != nr_bytes) {
		throw IOException("Could not write sufficient bytes from file \"%s\"", handle.path.c_str());
	}
}

bool FileSystem::DirectoryExists(const string &directory) {
	if (!directory.empty()) {
		if (access(directory.c_str(), 0) == 0) {
			struct stat status;
			stat(directory.c_str(), &status);
			if (status.st_mode & S_IFDIR)
				return true;
		}
	}
	// if any condition fails
	return false;
}

bool FileSystem::FileExists(const string &filename) {
	if (!filename.empty()) {
		if (access(filename.c_str(), 0) == 0) {
			struct stat status;
			stat(filename.c_str(), &status);
			if (!(status.st_mode & S_IFDIR))
				return true;
		}
	}
	// if any condition fails
	return false;
}

void FileSystem::CreateDirectory(const string &directory) {
	struct stat st;

	if (stat(directory.c_str(), &st) != 0) {
		/* Directory does not exist. EEXIST for race condition */
		if (mkdir(directory.c_str(), 0755) != 0 && errno != EEXIST) {
			throw IOException("Failed create directory!");
		}
	} else if (!S_ISDIR(st.st_mode)) {
		throw IOException("Could not create directory!");
	}
}

int remove_directory_recursively(const char *path) {
	DIR *d = opendir(path);
	uint64_t path_len = strlen(path);
	int r = -1;

	if (d) {
		struct dirent *p;
		r = 0;
		while (!r && (p = readdir(d))) {
			int r2 = -1;
			char *buf;
			uint64_t len;
			/* Skip the names "." and ".." as we don't want to recurse on them. */
			if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
				continue;
			}
			len = path_len + strlen(p->d_name) + 2;
			buf = new char[len];
			if (buf) {
				struct stat statbuf;
				snprintf(buf, len, "%s/%s", path, p->d_name);
				if (!stat(buf, &statbuf)) {
					if (S_ISDIR(statbuf.st_mode)) {
						r2 = remove_directory_recursively(buf);
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

void FileSystem::RemoveDirectory(const string &directory) {
	remove_directory_recursively(directory.c_str());
}

void FileSystem::RemoveFile(const string &filename) {
	if (std::remove(filename.c_str()) != 0) {
		throw IOException("Could not remove file \"%s\": %s", filename.c_str(), strerror(errno));
	}
}

bool FileSystem::ListFiles(const string &directory, function<void(string)> callback) {
	if (!DirectoryExists(directory)) {
		return false;
	}
	DIR *dir;
	struct dirent *ent;
	if ((dir = opendir(directory.c_str())) != NULL) {
		/* print all the files and directories within directory */
		while ((ent = readdir(dir)) != NULL) {
			string name = string(ent->d_name);
			if (!name.empty() && name[0] != '.') {
				callback(name);
			}
		}
		closedir(dir);
	} else {
		return false;
	}
	return true;
}

string FileSystem::PathSeparator() {
	return "/";
}

void FileSystem::FileSync(FILE *file) {
	fsync(fileno(file));
}

void FileSystem::MoveFile(const string &source, const string &target) {
	//! FIXME: rename does not guarantee atomicity or overwriting target file if it exists
	if (rename(source.c_str(), target.c_str()) != 0) {
		throw IOException("Could not rename file!");
	}
}

#else

#include <string>
#include <windows.h>

#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory

// Returns the last Win32 error, in string format. Returns an empty string if there is no error.
std::string GetLastErrorAsString() {
	// Get the error message, if any.
	DWORD errorMessageID = ::GetLastError();
	if (errorMessageID == 0)
		return std::string(); // No error message has been recorded

	LPSTR messageBuffer = nullptr;
	uint64_t size =
	    FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
	                   NULL, errorMessageID, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&messageBuffer, 0, NULL);

	std::string message(messageBuffer, size);

	// Free the buffer.
	LocalFree(messageBuffer);

	return message;
}

struct WindowsFileHandle : public FileHandle {
public:
	WindowsFileHandle(FileSystem &file_system, string path, HANDLE fd) : FileHandle(file_system, path), fd(fd) {
	}
	virtual ~WindowsFileHandle() {
		Close();
	}

protected:
	void Close() override {
		CloseHandle(fd);
	};

public:
	HANDLE fd;
};

unique_ptr<FileHandle> FileSystem::OpenFile(const char *path, uint8_t flags, FileLockType lock_type) {
	// cannot combine Read and Write flags
	assert(!(flags & FileFlags::READ && flags & FileFlags::WRITE));
	// cannot combine Read and DirectIO/CREATE flags
	assert(!(flags & FileFlags::READ && flags & FileFlags::DIRECT_IO));
	assert(!(flags & FileFlags::READ && flags & FileFlags::CREATE));
	DWORD desired_access;
	DWORD share_mode;
	DWORD creation_disposition = OPEN_EXISTING;
	DWORD flags_and_attributes = FILE_ATTRIBUTE_NORMAL;
	if (flags & FileFlags::READ) {
		desired_access = GENERIC_READ;
		share_mode = FILE_SHARE_READ;

	} else {
		// need Read or Write
		assert(flags & FileFlags::WRITE);
		desired_access = GENERIC_READ | GENERIC_WRITE;
		share_mode = 0;
		if (flags & FileFlags::CREATE) {
			creation_disposition = CREATE_NEW;
		}
		if (flags & FileFlags::DIRECT_IO) {
			flags_and_attributes |= FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;
		}
	}
	HANDLE hFile =
	    CreateFileA(path, desired_access, share_mode, NULL, creation_disposition, flags_and_attributes, NULL);
	if (hFile == INVALID_HANDLE_VALUE) {
		auto error = GetLastErrorAsString();
		throw IOException("Cannot open file \"%s\": %s", path, error.c_str());
	}
	return make_unique<WindowsFileHandle>(*this, path, hFile);
}

static void seek_in_file(FileHandle &handle, uint64_t location) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	LARGE_INTEGER loc;
	loc.QuadPart = location;
	auto rc = SetFilePointerEx(hFile, loc, NULL, FILE_BEGIN);
	if (rc == 0) {
		auto error = GetLastErrorAsString();
		throw IOException("Could not seek to location %lld for file \"%s\": %s", location, handle.path.c_str(),
		                  error.c_str());
	}
}

void FileSystem::Read(FileHandle &handle, void *buffer, uint64_t nr_bytes, uint64_t location) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	seek_in_file(handle, location);

	auto rc = ReadFile(hFile, buffer, (DWORD)nr_bytes, NULL, NULL);
	if (rc == 0) {
		auto error = GetLastErrorAsString();
		throw IOException("Could not write file \"%s\": %s", handle.path.c_str(), error.c_str());
	}
}

void FileSystem::Write(FileHandle &handle, void *buffer, uint64_t nr_bytes, uint64_t location) {
	HANDLE hFile = ((WindowsFileHandle &)handle).fd;
	seek_in_file(handle, location);

	auto rc = WriteFile(hFile, buffer, (DWORD)nr_bytes, NULL, NULL);
	if (rc == 0) {
		auto error = GetLastErrorAsString();
		throw IOException("Could not write file \"%s\": %s", handle.path.c_str(), error.c_str());
	}
}

bool FileSystem::DirectoryExists(const string &directory) {
	DWORD attrs = GetFileAttributesA(directory.c_str());
	return (attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY));
}

bool FileSystem::FileExists(const string &filename) {
	DWORD attrs = GetFileAttributesA(filename.c_str());
	return (attrs != INVALID_FILE_ATTRIBUTES && !(attrs & FILE_ATTRIBUTE_DIRECTORY));
}

void FileSystem::CreateDirectory(const string &directory) {
	if (DirectoryExists(directory)) {
		return;
	}
	if (directory.empty() || !CreateDirectoryA(directory.c_str(), NULL) || !DirectoryExists(directory)) {
		throw IOException("Could not create directory!");
	}
}

static void delete_dir_special_snowflake_windows(string directory) {
	if (directory.size() + 3 > MAX_PATH) {
		throw IOException("Pathname too long");
	}
	// create search pattern
	TCHAR szDir[MAX_PATH];
	snprintf(szDir, MAX_PATH, "%s\\*", directory.c_str());

	WIN32_FIND_DATA ffd;
	HANDLE hFind = FindFirstFile(szDir, &ffd);
	if (hFind == INVALID_HANDLE_VALUE) {
		throw IOException("Could not find directory");
	}

	do {
		if (string(ffd.cFileName) == "." || string(ffd.cFileName) == "..") {
			continue;
		}
		if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
			// recurse to zap directory contents
			FileSystem fs;
			delete_dir_special_snowflake_windows(fs.JoinPath(directory, ffd.cFileName));
		} else {
			if (strlen(ffd.cFileName) + directory.size() + 1 > MAX_PATH) {
				throw IOException("Pathname too long");
			}
			// create search pattern
			TCHAR del_path[MAX_PATH];
			snprintf(del_path, MAX_PATH, "%s\\%s", directory.c_str(), ffd.cFileName);
			if (!DeleteFileA(del_path)) {
				throw IOException("Failed to delete directory entry");
			}
		}
	} while (FindNextFile(hFind, &ffd) != 0);

	DWORD dwError = GetLastError();
	if (dwError != ERROR_NO_MORE_FILES) {
		throw IOException("Something went wrong");
	}
	FindClose(hFind);

	if (!RemoveDirectoryA(directory.c_str())) {
		throw IOException("Failed to delete directory");
	}
}

void FileSystem::RemoveDirectory(const string &directory) {
	delete_dir_special_snowflake_windows(directory.c_str());
}

void FileSystem::RemoveFile(const string &filename) {
	DeleteFileA(filename.c_str());
}

bool FileSystem::ListFiles(const string &directory, function<void(string)> callback) {
	string search_dir = JoinPath(directory, "*");

	WIN32_FIND_DATA ffd;
	HANDLE hFind = FindFirstFile(search_dir.c_str(), &ffd);
	if (hFind == INVALID_HANDLE_VALUE) {
		return false;
	}
	do {
		if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
			continue;
		}

		callback(string(ffd.cFileName));
	} while (FindNextFile(hFind, &ffd) != 0);

	DWORD dwError = GetLastError();
	if (dwError != ERROR_NO_MORE_FILES) {
		FindClose(hFind);
		return false;
	}

	FindClose(hFind);
	return true;
}

string FileSystem::PathSeparator() {
	return "\\";
}

void FileSystem::FileSync(FILE *file) {
	throw NotImplementedException("Can't sync FILE* on Windows");

	/* // this is the correct way but we need a file name or Windows HANDLE
	HANDLE hdl = CreateFileA(lpFileName, GENERIC_READ, 0, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
	if (hdl == INVALID_HANDLE_VALUE) {
	    //error
	}

	if (FlushFileBuffers(hdl) == 0) {
	    //error
	}
	CloseHandle(hdl);
	*/
}

void FileSystem::MoveFile(const string &source, const string &target) {
	if (!MoveFileA(source.c_str(), target.c_str())) {
		throw IOException("Could not move file");
	}
}
#endif

void FileHandle::Read(void *buffer, uint64_t nr_bytes, uint64_t location) {
	file_system.Read(*this, buffer, nr_bytes, location);
}

void FileHandle::Write(void *buffer, uint64_t nr_bytes, uint64_t location) {
	file_system.Write(*this, buffer, nr_bytes, location);
}

string FileSystem::JoinPath(const string &a, const string &b) {
	// FIXME: sanitize paths
	return a + PathSeparator() + b;
}

Buffer::Buffer(void *internal_buffer, void *buffer, uint64_t size)
    : buffer(buffer), size(size), internal_buffer(internal_buffer) {
}

Buffer::~Buffer() {
	free(internal_buffer);
}

unique_ptr<Buffer> Buffer::AllocateAlignedBuffer(uint64_t bufsiz) {
	assert(bufsiz % 4096 == 0);
	// we add 4095 to ensure that we can align the buffer to 4096
	void *internal_buffer = malloc(bufsiz + 4095);
	// round to multiple of 4096
	uint64_t num = (uint64_t)internal_buffer;
	uint64_t remainder = num % 4096;
	if (remainder != 0) {
		num = num + 4096 - remainder;
	}
	assert(num % 4096 == 0);
	assert(num + bufsiz <= ((uint64_t)internal_buffer + bufsiz + 4095));
	assert(num >= (uint64_t)internal_buffer);
	return unique_ptr<Buffer>(new Buffer(internal_buffer, (void *)num, bufsiz));
}
