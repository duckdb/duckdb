#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/string_util.hpp"

#ifndef DISABLE_DUCKDB_REMOTE_INSTALL
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
#include "httplib.hpp"
#endif
#endif
#include "duckdb/common/windows_undefs.hpp"

#include <fstream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Install Extension
//===--------------------------------------------------------------------===//
const string ExtensionHelper::NormalizeVersionTag(const string &version_tag) {
	if (version_tag.length() > 0 && version_tag[0] != 'v') {
		return "v" + version_tag;
	}
	return version_tag;
}

bool ExtensionHelper::IsRelease(const string &version_tag) {
	return !StringUtil::Contains(version_tag, "-dev");
}

const string ExtensionHelper::GetVersionDirectoryName() {
#ifdef DUCKDB_WASM_VERSION
	return DUCKDB_QUOTE_DEFINE(DUCKDB_WASM_VERSION);
#endif
	if (IsRelease(DuckDB::LibraryVersion())) {
		return NormalizeVersionTag(DuckDB::LibraryVersion());
	} else {
		return DuckDB::SourceID();
	}
}

const vector<string> ExtensionHelper::PathComponents() {
	return vector<string> {".duckdb", "extensions", GetVersionDirectoryName(), DuckDB::Platform()};
}

string ExtensionHelper::ExtensionDirectory(DBConfig &config, FileSystem &fs) {
#ifdef WASM_LOADABLE_EXTENSIONS
	throw PermissionException("ExtensionDirectory functionality is not supported in duckdb-wasm");
#endif
	string extension_directory;
	if (!config.options.extension_directory.empty()) { // create the extension directory if not present
		extension_directory = config.options.extension_directory;
		// TODO this should probably live in the FileSystem
		// convert random separators to platform-canonic
		extension_directory = fs.ConvertSeparators(extension_directory);
		// expand ~ in extension directory
		extension_directory = fs.ExpandPath(extension_directory);
		if (!fs.DirectoryExists(extension_directory)) {
			auto sep = fs.PathSeparator(extension_directory);
			auto splits = StringUtil::Split(extension_directory, sep);
			D_ASSERT(!splits.empty());
			string extension_directory_prefix;
			if (StringUtil::StartsWith(extension_directory, sep)) {
				extension_directory_prefix = sep; // this is swallowed by Split otherwise
			}
			for (auto &split : splits) {
				extension_directory_prefix = extension_directory_prefix + split + sep;
				if (!fs.DirectoryExists(extension_directory_prefix)) {
					fs.CreateDirectory(extension_directory_prefix);
				}
			}
		}
	} else { // otherwise default to home
		string home_directory = fs.GetHomeDirectory();
		// exception if the home directory does not exist, don't create whatever we think is home
		if (!fs.DirectoryExists(home_directory)) {
			throw IOException("Can't find the home directory at '%s'\nSpecify a home directory using the SET "
			                  "home_directory='/path/to/dir' option.",
			                  home_directory);
		}
		extension_directory = home_directory;
	}
	D_ASSERT(fs.DirectoryExists(extension_directory));

	auto path_components = PathComponents();
	for (auto &path_ele : path_components) {
		extension_directory = fs.JoinPath(extension_directory, path_ele);
		if (!fs.DirectoryExists(extension_directory)) {
			fs.CreateDirectory(extension_directory);
		}
	}
	return extension_directory;
}

string ExtensionHelper::ExtensionDirectory(ClientContext &context) {
	auto &config = DBConfig::GetConfig(context);
	auto &fs = FileSystem::GetFileSystem(context);
	return ExtensionDirectory(config, fs);
}

bool ExtensionHelper::CreateSuggestions(const string &extension_name, string &message) {
	vector<string> candidates;
	for (idx_t ext_count = ExtensionHelper::DefaultExtensionCount(), i = 0; i < ext_count; i++) {
		candidates.emplace_back(ExtensionHelper::GetDefaultExtension(i).name);
	}
	for (idx_t ext_count = ExtensionHelper::ExtensionAliasCount(), i = 0; i < ext_count; i++) {
		candidates.emplace_back(ExtensionHelper::GetExtensionAlias(i).alias);
	}
	auto closest_extensions = StringUtil::TopNLevenshtein(candidates, extension_name);
	message = StringUtil::CandidatesMessage(closest_extensions, "Candidate extensions");
	for (auto &closest : closest_extensions) {
		if (closest == extension_name) {
			message = "Extension \"" + extension_name + "\" is an existing extension.\n";
			return true;
		}
	}
	return false;
}

void ExtensionHelper::InstallExtension(DBConfig &config, FileSystem &fs, const string &extension, bool force_install,
                                       const string &repository) {
#ifdef WASM_LOADABLE_EXTENSIONS
	// Install is currently a no-op
	return;
#endif
	string local_path = ExtensionDirectory(config, fs);
	InstallExtensionInternal(config, nullptr, fs, local_path, extension, force_install, repository);
}

void ExtensionHelper::InstallExtension(ClientContext &context, const string &extension, bool force_install,
                                       const string &repository) {
#ifdef WASM_LOADABLE_EXTENSIONS
	// Install is currently a no-op
	return;
#endif
	auto &config = DBConfig::GetConfig(context);
	auto &fs = FileSystem::GetFileSystem(context);
	string local_path = ExtensionDirectory(context);
	auto &client_config = ClientConfig::GetConfig(context);
	InstallExtensionInternal(config, &client_config, fs, local_path, extension, force_install, repository);
}

unsafe_unique_array<data_t> ReadExtensionFileFromDisk(FileSystem &fs, const string &path, idx_t &file_size) {
	auto source_file = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	file_size = source_file->GetFileSize();
	auto in_buffer = make_unsafe_uniq_array<data_t>(file_size);
	source_file->Read(in_buffer.get(), file_size);
	source_file->Close();
	return in_buffer;
}

void WriteExtensionFileToDisk(FileSystem &fs, const string &path, void *data, idx_t data_size) {
	auto target_file = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_APPEND |
	                                         FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	target_file->Write(data, data_size);
	target_file->Close();
	target_file.reset();
}

string ExtensionHelper::ExtensionUrlTemplate(optional_ptr<const ClientConfig> client_config, const string &repository) {
	string default_endpoint = "http://extensions.duckdb.org";
	string versioned_path = "/${REVISION}/${PLATFORM}/${NAME}.duckdb_extension";
#ifdef WASM_LOADABLE_EXTENSIONS
	versioned_path = "/duckdb-wasm" + versioned_path + ".wasm";
#else
	versioned_path = versioned_path + ".gz";
#endif
	string custom_endpoint = client_config ? client_config->custom_extension_repo : string();
	string endpoint;
	if (!repository.empty()) {
		endpoint = repository;
	} else if (!custom_endpoint.empty()) {
		endpoint = custom_endpoint;
	} else {
		endpoint = default_endpoint;
	}
	string url_template = endpoint + versioned_path;
	return url_template;
}

string ExtensionHelper::ExtensionFinalizeUrlTemplate(const string &url_template, const string &extension_name) {
	auto url = StringUtil::Replace(url_template, "${REVISION}", GetVersionDirectoryName());
	url = StringUtil::Replace(url, "${PLATFORM}", DuckDB::Platform());
	url = StringUtil::Replace(url, "${NAME}", extension_name);
	return url;
}

void ExtensionHelper::InstallExtensionInternal(DBConfig &config, ClientConfig *client_config, FileSystem &fs,
                                               const string &local_path, const string &extension, bool force_install,
                                               const string &repository) {
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Installing external extensions is disabled through a compile time flag");
#else
	if (!config.options.enable_external_access) {
		throw PermissionException("Installing extensions is disabled through configuration");
	}
	auto extension_name = ApplyExtensionAlias(fs.ExtractBaseName(extension));

	string local_extension_path = fs.JoinPath(local_path, extension_name + ".duckdb_extension");
	if (fs.FileExists(local_extension_path) && !force_install) {
		return;
	}

	auto uuid = UUID::ToString(UUID::GenerateRandomUUID());
	string temp_path = local_extension_path + ".tmp-" + uuid;
	if (fs.FileExists(temp_path)) {
		fs.RemoveFile(temp_path);
	}
	auto is_http_url = StringUtil::Contains(extension, "http://");
	if (fs.FileExists(extension)) {
		idx_t file_size;
		auto in_buffer = ReadExtensionFileFromDisk(fs, extension, file_size);
		WriteExtensionFileToDisk(fs, temp_path, in_buffer.get(), file_size);

		if (fs.FileExists(local_extension_path) && force_install) {
			fs.RemoveFile(local_extension_path);
		}
		fs.MoveFile(temp_path, local_extension_path);
		return;
	} else if (StringUtil::Contains(extension, "/") && !is_http_url) {
		throw IOException("Failed to read extension from \"%s\": no such file", extension);
	}

#ifdef DISABLE_DUCKDB_REMOTE_INSTALL
	throw BinderException("Remote extension installation is disabled through configuration");
#else

	string url_template = ExtensionUrlTemplate(client_config, repository);

	if (is_http_url) {
		url_template = extension;
		extension_name = "";
	}

	string url = ExtensionFinalizeUrlTemplate(url_template, extension_name);

	string no_http = StringUtil::Replace(url, "http://", "");

	idx_t next = no_http.find('/', 0);
	if (next == string::npos) {
		throw IOException("No slash in URL template");
	}

	// Special case to install extension from a local file, useful for testing
	if (!StringUtil::Contains(url_template, "http://")) {
		string file = fs.ConvertSeparators(url);
		if (!fs.FileExists(file)) {
			// check for non-gzipped variant
			file = file.substr(0, file.size() - 3);
			if (!fs.FileExists(file)) {
				throw IOException("Failed to copy local extension \"%s\" at PATH \"%s\"\n", extension_name, file);
			}
		}
		auto read_handle = fs.OpenFile(file, FileFlags::FILE_FLAGS_READ);
		auto test_data = std::unique_ptr<unsigned char[]> {new unsigned char[read_handle->GetFileSize()]};
		read_handle->Read(test_data.get(), read_handle->GetFileSize());
		WriteExtensionFileToDisk(fs, temp_path, (void *)test_data.get(), read_handle->GetFileSize());

		if (fs.FileExists(local_extension_path) && force_install) {
			fs.RemoveFile(local_extension_path);
		}
		fs.MoveFile(temp_path, local_extension_path);
		return;
	}

	// Push the substring [last, next) on to splits
	auto hostname_without_http = no_http.substr(0, next);
	auto url_local_part = no_http.substr(next);

	auto url_base = "http://" + hostname_without_http;
	duckdb_httplib::Client cli(url_base.c_str());

	duckdb_httplib::Headers headers = {{"User-Agent", StringUtil::Format("DuckDB %s %s %s", DuckDB::LibraryVersion(),
	                                                                     DuckDB::SourceID(), DuckDB::Platform())}};

	auto res = cli.Get(url_local_part.c_str(), headers);

	if (!res || res->status != 200) {
		// create suggestions
		string message;
		auto exact_match = ExtensionHelper::CreateSuggestions(extension_name, message);
		if (exact_match) {
			message += "\nAre you using a development build? In this case, extensions might not (yet) be uploaded.";
		}
		if (res.error() == duckdb_httplib::Error::Success) {
			throw HTTPException(res.value(), "Failed to download extension \"%s\" at URL \"%s%s\"\n%s", extension_name,
			                    url_base, url_local_part, message);
		} else {
			throw IOException("Failed to download extension \"%s\" at URL \"%s%s\"\n%s (ERROR %s)", extension_name,
			                  url_base, url_local_part, message, to_string(res.error()));
		}
	}
	auto decompressed_body = GZipFileSystem::UncompressGZIPString(res->body);

	WriteExtensionFileToDisk(fs, temp_path, (void *)decompressed_body.data(), decompressed_body.size());

	if (fs.FileExists(local_extension_path) && force_install) {
		fs.RemoveFile(local_extension_path);
	}
	fs.MoveFile(temp_path, local_extension_path);
#endif
#endif
}

} // namespace duckdb
