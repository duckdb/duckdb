#include "common/file_system.hpp"

#include "common/exception.hpp"
#include "common/string_util.hpp"

using namespace std;

#ifndef _MSC_VER
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

void SetWorkingDirectory(const string &directory) {
	chdir(directory.c_str());
}

string PathSeparator() {
	return "/";
}

void FileSync(FILE *file) {
	fsync(fileno(file));
}

string GetWorkingDirectory() {
	char current_path[FILENAME_MAX];

	if (!getcwd(current_path, sizeof(current_path))) {
		return string();
	}
	return string(current_path);
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

static bool path_has_attr(const char *pathname, DWORD attr) {
	DWORD attrs = GetFileAttributesA(pathname);
	if (attrs == INVALID_FILE_ATTRIBUTES) {
		return false;
	}
	if (attrs & attr) {
		return true;
	}
	return false;
}

bool DirectoryExists(const string &directory) {
	if (!directory.empty()) {
		return path_has_attr(directory.c_str(), FILE_ATTRIBUTE_DIRECTORY);
	}
	return false;
}

bool FileExists(const string &filename) {
	if (!filename.empty()) {
		return path_has_attr(filename.c_str(), FILE_ATTRIBUTE_NORMAL);
	}
	return false;
}

void CreateDirectory(const string &directory) {
	if (directory.empty() || !CreateDirectoryA(directory.c_str(), NULL) || !DirectoryExists(directory)) {
		throw IOException("Could not create directory!");
	}
}

void RemoveDirectory(const string &directory) {
	// SHFileOperation needs a double-NULL-terminated string as input
	string path(directory);
	path.resize(path.size() + 2);
	path[path.size() - 1] = 0;
	path[path.size() - 2] = 0;

	SHFILEOPSTRUCT shfo = {NULL,  FO_DELETE, path.c_str(), NULL, FOF_SILENT | FOF_NOERRORUI | FOF_NOCONFIRMATION,
	                       FALSE, NULL,      NULL};

	if (!SHFileOperation(&shfo) == 0) {
		throw IOException("Could not delete directory!");
	}
}

bool ListFiles(const string &directory, function<void(string)> callback) {
	string search_dir = JoinPath(directory, "*");

	WIN32_FIND_DATA ffd;
	HANDLE hFind = FindFirstFile(search_dir.c_str(), &ffd);
	if (hFind == INVALID_HANDLE_VALUE) {
		return false;
	}
	do {
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

void SetWorkingDirectory(const string &directory) {
	SetCurrentDirectory(directory.c_str());
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

string GetWorkingDirectory() {
	string s;
	s.resize(MAX_PATH);
	GetCurrentDirectory(MAX_PATH, (LPSTR)s.c_str());
	return s;
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
