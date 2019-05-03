#include "common/file_system.hpp"

#include "common/exception.hpp"
#include "common/string_util.hpp"
#include "common/helper.hpp"

using namespace duckdb;
using namespace std;

#ifndef _WIN32
#include <cstdio>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

struct UnixFileHandle : public FileHandle {
public:
	UnixFileHandle(string path, int fd) : FileHandle(path), fd(fd){}
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
		open_flags = O_RDWR  | O_CLOEXEC;
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
		fprintf(stderr, "Cannot open file \"%s\": %s", path, strerror(errno));
		throw IOException("Cannot open file \"%s\": %s", path, strerror(errno));
	}
#if defined(__DARWIN__) || defined(__APPLE__)
	if (flags & FileFlags::DIRECT_IO) {
		// OSX requires fcntl for Direct IO
		rc = fcntl(fd, F_NOCACHE, 1);
		if (fd == -1) {
			fprintf(stderr, "Could not enable direct IO for file \"%s\": %s", path, strerror(errno));
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
	return make_unique<UnixFileHandle>(path, fd);
}

static void seek_in_file(FileHandle &handle, uint64_t location) {
	int fd = ((UnixFileHandle&) handle).fd;
	off_t offset = lseek(fd, location, SEEK_SET);
	if (offset == (off_t) -1) {
		throw IOException("Could not seek to location %lld for file \"%s\": %s", location, handle.path.c_str(), strerror(errno));
	}
}

void FileSystem::Read(FileHandle &handle, void *buffer, uint64_t nr_bytes, uint64_t location) {
	int fd = ((UnixFileHandle&) handle).fd;
	// lseek to the location
	seek_in_file(handle, location);
	// now read from the location
	errno = 0;
	ssize_t bytes_read = read(fd, buffer, nr_bytes);
	if (bytes_read == -1) {
		throw IOException("Could not read from file \"%s\": %s", handle.path.c_str(), strerror(errno));
	}
	if (bytes_read != nr_bytes) {
		throw IOException("Could not read sufficient bytes from file \"%s\"", handle.path.c_str());
	}
}

void FileSystem::Write(FileHandle &handle, void *buffer, uint64_t nr_bytes, uint64_t location) {
	int fd = ((UnixFileHandle&) handle).fd;
	// lseek to the location
	seek_in_file(handle, location);
	// now write to the location
	errno = 0;
	ssize_t bytes_written = write(fd, buffer, nr_bytes);
	if (bytes_written == -1) {
		throw IOException("Could not write file \"%s\": %s", handle.path.c_str(), strerror(errno));
	}
	if (bytes_written != nr_bytes) {
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

void FileSystem::RemoveDirectory(const string &directory) {
	rmdir(directory.c_str());
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


unique_ptr<FileHandle> FileSystem::OpenFile(const char *path, FileFlags flags, FileLockType lock_type) {
	throw NotImplementedException("OpenFile on Windows");
}

void FileSystem::Read(FileHandle &handle, void *buffer, uint64_t nr_bytes, uint64_t location) {
	throw NotImplementedException("Read/Write on Windows");
}

void FileSystem::Write(FileHandle &handle, void *buffer, uint64_t nr_bytes, uint64_t location) {
	throw NotImplementedException("Read/Write on Windows");
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

static void delete_dir_special_snowflake_windows(const char *dirname) {
	if (strlen(dirname) + 3 > MAX_PATH) {
		throw IOException("Pathname too long");
	}
	// create search pattern
	TCHAR szDir[MAX_PATH];
	snprintf(szDir, MAX_PATH, "%s\\*", dirname);

	WIN32_FIND_DATA ffd;
	HANDLE hFind = FindFirstFile(szDir, &ffd);
	if (INVALID_HANDLE_VALUE == hFind) {
		throw IOException("Could not find directory");
	}

	do {
		if (strcmp(ffd.cFileName, ".") == 0 || strcmp(ffd.cFileName, "..") == 0) {
			continue;
		}
		if (ffd.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
			// recurse to zap directory contents
			delete_dir_special_snowflake_windows(ffd.cFileName);
		} else {
			if (strlen(ffd.cFileName) + strlen(dirname) + 1 > MAX_PATH) {
				throw IOException("Pathname too long");
			}
			// create search pattern
			TCHAR del_path[MAX_PATH];
			snprintf(del_path, MAX_PATH, "%s\\%s", dirname, ffd.cFileName);
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

	if (!RemoveDirectoryA(dirname)) {
		throw IOException("Failed to delete directory");
	}
}

void FileSystem::RemoveDirectory(const string &directory) {
	delete_dir_special_snowflake_windows(directory.c_str());
}

void FileSystem::RemoveFile(const string &filename) {
	throw NotImplementedException("Remove file on windows");
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
	FileSystem::Read(*this, buffer, nr_bytes, location);
}

void FileHandle::Write(void *buffer, uint64_t nr_bytes, uint64_t location) {
	FileSystem::Write(*this, buffer, nr_bytes, location);
}

string FileSystem::JoinPath(const string &a, const string &b) {
	// FIXME: sanitize paths
	return a + PathSeparator() + b;
}