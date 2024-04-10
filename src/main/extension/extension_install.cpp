#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/main/extension_install_info.hpp"

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
	if (!version_tag.empty() && version_tag[0] != 'v') {
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
	return vector<string> {GetVersionDirectoryName(), DuckDB::Platform()};
}

duckdb::string ExtensionHelper::DefaultExtensionFolder(FileSystem &fs) {
	string home_directory = fs.GetHomeDirectory();
	// exception if the home directory does not exist, don't create whatever we think is home
	if (!fs.DirectoryExists(home_directory)) {
		throw IOException("Can't find the home directory at '%s'\nSpecify a home directory using the SET "
		                  "home_directory='/path/to/dir' option.",
		                  home_directory);
	}
	string res = home_directory;
	res = fs.JoinPath(res, ".duckdb");
	res = fs.JoinPath(res, "extensions");
	return res;
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
	} else { // otherwise default to home
		extension_directory = DefaultExtensionFolder(fs);
	}
	{
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
	auto lowercase_extension_name = StringUtil::Lower(extension_name);
	vector<string> candidates;
	for (idx_t ext_count = ExtensionHelper::DefaultExtensionCount(), i = 0; i < ext_count; i++) {
		candidates.emplace_back(ExtensionHelper::GetDefaultExtension(i).name);
	}
	for (idx_t ext_count = ExtensionHelper::ExtensionAliasCount(), i = 0; i < ext_count; i++) {
		candidates.emplace_back(ExtensionHelper::GetExtensionAlias(i).alias);
	}
	auto closest_extensions = StringUtil::TopNLevenshtein(candidates, lowercase_extension_name);
	message = StringUtil::CandidatesMessage(closest_extensions, "Candidate extensions");
	for (auto &closest : closest_extensions) {
		if (closest == lowercase_extension_name) {
			message = "Extension \"" + extension_name + "\" is an existing extension.\n";
			return true;
		}
	}
	return false;
}

void ExtensionHelper::InstallExtension(DBConfig &config, FileSystem &fs, const string &extension, bool force_install,
                                       const string &repository, const string &version) {
#ifdef WASM_LOADABLE_EXTENSIONS
	// Install is currently a no-op
	return;
#endif
	string local_path = ExtensionDirectory(config, fs);
	InstallExtensionInternal(config, fs, local_path, extension, force_install, repository, version);
}

void ExtensionHelper::InstallExtension(ClientContext &context, const string &extension, bool force_install,
                                       const string &repository, const string &version) {
#ifdef WASM_LOADABLE_EXTENSIONS
	// Install is currently a no-op
	return;
#endif
	auto &config = DBConfig::GetConfig(context);
	auto &fs = FileSystem::GetFileSystem(context);
	string local_path = ExtensionDirectory(context);
	InstallExtensionInternal(config, fs, local_path, extension, force_install, repository, version);
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

void WriteExtensionMetadataFileToDisk(FileSystem &fs, const string &path, ExtensionInstallInfo &metadata) {
	auto file_writer = BufferedFileWriter(fs, path);

	auto serializer = BinarySerializer(file_writer);
	serializer.Begin();
	metadata.Serialize(serializer);
	serializer.End();

	file_writer.Flush();
}

static string ResolveRepository(optional_ptr<const DBConfig> db_config, const string &repository) {
	string custom_endpoint = db_config ? db_config->options.custom_extension_repo : string();
	if (!repository.empty()) {
		auto known_repository_url = ExtensionRepository::TryGetRepositoryUrl(repository);
		if (!known_repository_url.empty()) {
			return known_repository_url;
		}
		return repository;
	} else if (!custom_endpoint.empty()) {
		return custom_endpoint;
	}
	return ExtensionHelper::DEFAULT_REPOSITORY;
}

string ExtensionHelper::ExtensionUrlTemplate(optional_ptr<const DBConfig> db_config, const string &repository,
                                             const string &version) {
	string versioned_path;
	if (!version.empty()) {
		versioned_path = "/${NAME}/" + version + "/${REVISION}/${PLATFORM}/${NAME}.duckdb_extension";
	} else {
		versioned_path = "/${REVISION}/${PLATFORM}/${NAME}.duckdb_extension";
	}
#ifdef WASM_LOADABLE_EXTENSIONS
	string default_endpoint = DEFAULT_REPOSITORY;
	versioned_path = versioned_path + ".wasm";
#else
	string default_endpoint = DEFAULT_REPOSITORY;
	versioned_path = versioned_path + ".gz";
#endif
	string custom_endpoint = db_config ? db_config->options.custom_extension_repo : string();
	string endpoint = ResolveRepository(db_config, repository);
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

static void WriteExtensionFiles(FileSystem &fs, const string &temp_path, const string &local_extension_path,
                                void *in_buffer, idx_t file_size, bool force_install, ExtensionInstallInfo &info) {
	// Write files
	WriteExtensionFileToDisk(fs, temp_path, in_buffer, file_size);

	if (fs.FileExists(local_extension_path) && force_install) {
		fs.RemoveFile(local_extension_path);
	}
	fs.MoveFile(temp_path, local_extension_path);

	// Write metadata
	auto metadata_tmp_path = temp_path + ".info";
	auto metadata_file_path = local_extension_path + ".info";

	// Metadata is written as a very simple file containing the origin of the installed file
	WriteExtensionMetadataFileToDisk(fs, metadata_tmp_path, info);

	if (fs.FileExists(metadata_file_path) && force_install) {
		fs.RemoveFile(metadata_file_path);
	}

	fs.MoveFile(metadata_tmp_path, metadata_file_path);
}

// Install an extension using a filesystem
template <bool IS_LOCAL_FILESYSTEM>
static void DirectInstallExtension(DBConfig &config, FileSystem &fs, const string &path, const string &temp_path,
                                   const string &extension_name, const string &local_extension_path,
                                   bool force_install) {
	string file = fs.ConvertSeparators(path);
	if (!fs.FileExists(file)) {
		// check for non-gzipped variant
		file = file.substr(0, file.size() - 3);
		if (!fs.FileExists(file)) {
			throw IOException("Failed to copy local extension \"%s\" at PATH \"%s\"\n", extension_name, file);
		}
	}

	idx_t file_size;
	auto in_buffer = ReadExtensionFileFromDisk(fs, file, file_size);

	ExtensionInstallInfo info;
	info.mode = IS_LOCAL_FILESYSTEM ? ExtensionInstallMode::LOCAL_FILE : ExtensionInstallMode::CUSTOM_PATH;
	info.full_path = file;
	WriteExtensionFiles(fs, temp_path, local_extension_path, (void *)in_buffer.get(), file_size, force_install, info);
}

static void InstallFromHttpUrl(DBConfig &config, const string &url, const string &extension_name,
                               const string &repository, const string &temp_path, const string &local_extension_path,
                               bool force_install) {
	string no_http = StringUtil::Replace(url, "http://", "");

	idx_t next = no_http.find('/', 0);
	if (next == string::npos) {
		throw IOException("No slash in URL template");
	}

	// Push the substring [last, next) on to splits
	auto hostname_without_http = no_http.substr(0, next);
	auto url_local_part = no_http.substr(next);

	auto url_base = "http://" + hostname_without_http;
	duckdb_httplib::Client cli(url_base.c_str());

	duckdb_httplib::Headers headers = {
	    {"User-Agent", StringUtil::Format("%s %s", config.UserAgent(), DuckDB::SourceID())}};

	auto res = cli.Get(url_local_part.c_str(), headers);

	if (!res || res->status != 200) {
		// create suggestions
		string message;
		auto exact_match = ExtensionHelper::CreateSuggestions(extension_name, message);
		if (exact_match && !ExtensionHelper::IsRelease(DuckDB::LibraryVersion())) {
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

	ExtensionInstallInfo info;
	info.mode = ExtensionInstallMode::REPOSITORY;
	info.full_path = url;
	info.repository = repository;

	auto fs = FileSystem::CreateLocal();
	WriteExtensionFiles(*fs, temp_path, local_extension_path, (void *)decompressed_body.data(),
	                    decompressed_body.size(), force_install, info);
}

static void InstallFromRepository(DBConfig &config, const string &url, const string &extension_name,
                                  const string &repository, const string &temp_path, const string &local_extension_path,
                                  const string &version, bool force_install) {
	string url_template = ExtensionHelper::ExtensionUrlTemplate(&config, repository, version);
	string generated_url = ExtensionHelper::ExtensionFinalizeUrlTemplate(url_template, extension_name);
	return InstallFromHttpUrl(config, generated_url, extension_name, repository, temp_path, local_extension_path,
	                          force_install);
}

void ExtensionHelper::InstallExtensionInternal(DBConfig &config, FileSystem &fs, const string &local_path,
                                               const string &extension, bool force_install, const string &repository,
                                               const string &version) {
#ifdef DUCKDB_DISABLE_EXTENSION_LOAD
	throw PermissionException("Installing external extensions is disabled through a compile time flag");
#else
	if (!config.options.enable_external_access) {
		throw PermissionException("Installing extensions is disabled through configuration");
	}

	auto extension_name = ApplyExtensionAlias(fs.ExtractBaseName(extension));
	string local_extension_path = fs.JoinPath(local_path, extension_name + ".duckdb_extension");
	string temp_path = local_extension_path + ".tmp-" + UUID::ToString(UUID::GenerateRandomUUID());

	if (fs.FileExists(local_extension_path) && !force_install) {
		return;
	}

	if (fs.FileExists(temp_path)) {
		fs.RemoveFile(temp_path);
	}

	// Locally install the extension
	if (ExtensionHelper::IsFullPath(extension) &&
	    fs.GetSubsystem(extension).GetName() == "LocalFileSystem") { // TODO NAME not hardcoded
		DirectInstallExtension<true>(config, fs, extension, temp_path, extension, local_extension_path, force_install);
		return;
	}

#ifdef DISABLE_DUCKDB_REMOTE_INSTALL
	throw BinderException("Remote extension installation is disabled through configuration");
#else

	// For files not supported by the LocalFileSystem; we can use any of DuckDB's filesystems to install it, with the
	// exception of http files: these have custom handling to avoid requiring an extension for installation
	if (IsFullPath(extension) && !StringUtil::StartsWith(extension, "http://")) {
		DirectInstallExtension<false>(config, fs, extension, temp_path, extension, local_extension_path, force_install);
		return;
	}

	// Default case -> install extension from repository
	auto resolved_repository = ResolveRepository(&config, repository);

	if (StringUtil::StartsWith(extension, "http://")) {
		// Extension is a full http:// path
		InstallFromHttpUrl(config, extension, extension_name, "", temp_path, local_extension_path, force_install);
	} else {
		// Extension is a regular repository
		InstallFromRepository(config, extension, extension_name, resolved_repository, temp_path, local_extension_path,
		                      version, force_install);
	}

#endif
#endif
}

} // namespace duckdb
