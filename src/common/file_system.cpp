#include "common/file_system.hpp"

#include "common/exception.hpp"
#include "common/string_util.hpp"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;

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

string JoinPath(const string &a, const string &b) {
	// FIXME: sanitize paths
	return a + PathSeparator() + b;
}

void FileSync(FILE *file) {
	fsync(fileno(file));
}

#include <stdio.h> /* defines FILENAME_MAX */
#ifdef WINDOWS
#include <direct.h>
#define GetCurrentDir _getcwd
#else
#include <unistd.h>
#define GetCurrentDir getcwd
#endif

string GetWorkingDirectory() {
	char current_path[FILENAME_MAX];

	if (!GetCurrentDir(current_path, sizeof(current_path))) {
		return string();
	}
	return string(current_path);
}

} // namespace duckdb
