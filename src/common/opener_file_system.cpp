#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

void OpenerFileSystem::VerifyNoOpener(optional_ptr<FileOpener> opener) {
	if (opener) {
		throw InternalException("OpenerFileSystem cannot take an opener - the opener is pushed automatically");
	}
}
void OpenerFileSystem::VerifyCanAccessFileInternal(const string &path, FileType type) {
	auto opener = GetOpener();
	if (!opener) {
		return;
	}
	auto db = opener->TryGetDatabase();
	if (!db) {
		return;
	}
	auto &config = db->config;
	if (!config.CanAccessFile(path, type)) {
		throw PermissionException("Cannot access %s \"%s\" - file system operations are disabled by configuration",
		                          type == FileType::FILE_TYPE_DIR ? "directory" : "file", path);
	}
}

void OpenerFileSystem::VerifyCanWriteFile(const string &path) {

	auto opener = GetOpener();
	if (!opener) {
		return;
	}

	auto db = opener->TryGetDatabase();
	if (!db) {
		return;
	}
	auto &config = db->config;

	const auto home_directory = GetHomeDirectory();

	string default_extension_folder = home_directory;
	default_extension_folder = JoinPath(default_extension_folder, ".duckdb");
	default_extension_folder = JoinPath(default_extension_folder, "extensions");

	string extension_folder =
	    !config.options.extension_directory.empty() ? config.options.extension_directory : default_extension_folder;

	extension_folder = FileSystem::ExpandPath(extension_folder, nullptr);
	if (extension_folder[0] != '/') {
		extension_folder = config.SanitizeAllowedPath(GetWorkingDirectory() + '/' + extension_folder);
	}

	// Now extension folder is absolute

	string absolute_path = "";

	string sanitized_path = config.SanitizeAllowedPath(path);

	if (sanitized_path[0] == '~') {
		absolute_path = FileSystem::GetHomeDirectory(opener) + sanitized_path.substr(1);
	} else if (sanitized_path[0] == '/') {
		absolute_path = sanitized_path;
	} else {
		absolute_path = config.SanitizeAllowedPath(GetWorkingDirectory() + '/' + sanitized_path);
	}

	if (StringUtil::StartsWith(absolute_path, extension_folder)) {
		throw PermissionException("Cannot access \"%s\" - writing to extension directory is disabled by configuration",
		                          path);
	}
}

void OpenerFileSystem::VerifyCanAccessFile(const string &path) {
	VerifyCanAccessFileInternal(path, FileType::FILE_TYPE_REGULAR);
}

void OpenerFileSystem::VerifyCanAccessDirectory(const string &path) {
	VerifyCanAccessFileInternal(path, FileType::FILE_TYPE_DIR);
}

} // namespace duckdb
