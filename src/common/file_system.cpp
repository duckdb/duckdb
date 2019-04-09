#include "common/file_system.hpp"

#include "common/exception.hpp"
#include "common/string_util.hpp"

using namespace std;

#ifndef _WIN32
#include <cstdio>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace duckdb {
bool DirectoryExists(const string &directory) {
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

bool FileExists(const string &filename) {
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

void CreateDirectory(const string &directory) {
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

void RemoveDirectory(const string &directory) {
	auto command = "rm -r " + StringUtil::Replace(directory, " ", "\\ ");
	system(command.c_str());
}

bool ListFiles(const string &directory, function<void(string)> callback) {
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

string PathSeparator() {
	return "/";
}

void FileSync(FILE *file) {
	fsync(fileno(file));
}

void MoveFile(const string &source, const string &target) {
	//! FIXME: rename does not guarantee atomicity or overwriting target file if it exists
	if (rename(source.c_str(), target.c_str()) != 0) {
		throw IOException("Could not rename file!");
	}
}
} // namespace duckdb

#else

#include <string>
#include <windows.h>

#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory

namespace duckdb {

bool DirectoryExists(const string &directory) {
	DWORD attrs = GetFileAttributesA(directory.c_str());
	return (attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_DIRECTORY));
}

bool FileExists(const string &filename) {
	DWORD attrs = GetFileAttributesA(filename.c_str());
	return (attrs != INVALID_FILE_ATTRIBUTES && !(attrs & FILE_ATTRIBUTE_DIRECTORY));
}

void CreateDirectory(const string &directory) {
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

void RemoveDirectory(const string &directory) {
	delete_dir_special_snowflake_windows(directory.c_str());
}

bool ListFiles(const string &directory, function<void(string)> callback) {
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

string PathSeparator() {
	return "\\";
}

void FileSync(FILE *file) {
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

void MoveFile(const string &source, const string &target) {
	if (!MoveFileA(source.c_str(), target.c_str())) {
		throw IOException("Could not move file");
	}
}
} // namespace duckdb
#endif

namespace duckdb {
string JoinPath(const string &a, const string &b) {
	// FIXME: sanitize paths
	return a + PathSeparator() + b;
}
} // namespace duckdb
