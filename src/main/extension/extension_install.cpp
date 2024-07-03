#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/logging/http_logger.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/extension_helper.hpp"
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

unique_ptr<ExtensionInstallInfo> ExtensionHelper::InstallExtension(DBConfig &config, FileSystem &fs,
                                                                   const string &extension, bool force_install,
                                                                   optional_ptr<ExtensionRepository> repository,
                                                                   bool throw_on_origin_mismatch,
                                                                   const string &version) {
#ifdef WASM_LOADABLE_EXTENSIONS
	// Install is currently a no-op
	return nullptr;
#endif
	string local_path = ExtensionDirectory(config, fs);
	return InstallExtensionInternal(config, fs, local_path, extension, force_install, throw_on_origin_mismatch, version,
	                                repository);
}

unique_ptr<ExtensionInstallInfo> ExtensionHelper::InstallExtension(ClientContext &context, const string &extension,
                                                                   bool force_install,
                                                                   optional_ptr<ExtensionRepository> repository,
                                                                   bool throw_on_origin_mismatch,
                                                                   const string &version) {
#ifdef WASM_LOADABLE_EXTENSIONS
	// Install is currently a no-op
	return nullptr;
#endif
	auto &db_config = DBConfig::GetConfig(context);
	auto &fs = FileSystem::GetFileSystem(context);
	string local_path = ExtensionDirectory(context);
	optional_ptr<HTTPLogger> http_logger =
	    ClientConfig::GetConfig(context).enable_http_logging ? context.client_data->http_logger.get() : nullptr;
	return InstallExtensionInternal(db_config, fs, local_path, extension, force_install, throw_on_origin_mismatch,
	                                version, repository, http_logger, context);
}

unsafe_unique_array<data_t> ReadExtensionFileFromDisk(FileSystem &fs, const string &path, idx_t &file_size) {
	auto source_file = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	file_size = source_file->GetFileSize();
	auto in_buffer = make_unsafe_uniq_array<data_t>(file_size);
	source_file->Read(in_buffer.get(), file_size);
	source_file->Close();
	return in_buffer;
}

static void WriteExtensionFileToDisk(FileSystem &fs, const string &path, void *data, idx_t data_size) {
	auto target_file = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_APPEND |
	                                         FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	target_file->Write(data, data_size);
	target_file->Close();
	target_file.reset();
}

static void WriteExtensionMetadataFileToDisk(FileSystem &fs, const string &path, ExtensionInstallInfo &metadata) {
	auto file_writer = BufferedFileWriter(fs, path);
	BinarySerializer::Serialize(metadata, file_writer);
	file_writer.Sync();
}

string ExtensionHelper::ExtensionUrlTemplate(optional_ptr<const DBConfig> db_config,
                                             const ExtensionRepository &repository, const string &version) {
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
	string default_endpoint = ExtensionRepository::DEFAULT_REPOSITORY_URL;
	versioned_path = versioned_path + ".gz";
#endif
	string url_template = repository.path + versioned_path;
	return url_template;
}

string ExtensionHelper::ExtensionFinalizeUrlTemplate(const string &url_template, const string &extension_name) {
	auto url = StringUtil::Replace(url_template, "${REVISION}", GetVersionDirectoryName());
	url = StringUtil::Replace(url, "${PLATFORM}", DuckDB::Platform());
	url = StringUtil::Replace(url, "${NAME}", extension_name);
	return url;
}

static void CheckExtensionMetadataOnInstall(DBConfig &config, void *in_buffer, idx_t file_size,
                                            ExtensionInstallInfo &info, const string &extension_name) {
	if (file_size < ParsedExtensionMetaData::FOOTER_SIZE) {
		throw IOException("Failed to install '%s', file too small to be a valid DuckDB extension!", extension_name);
	}

	auto parsed_metadata = ExtensionHelper::ParseExtensionMetaData(static_cast<char *>(in_buffer) +
	                                                               (file_size - ParsedExtensionMetaData::FOOTER_SIZE));

	auto metadata_mismatch_error = parsed_metadata.GetInvalidMetadataError();

	if (!metadata_mismatch_error.empty() && !config.options.allow_extensions_metadata_mismatch) {
		throw IOException("Failed to install '%s'\n%s", extension_name, metadata_mismatch_error);
	}

	info.version = parsed_metadata.extension_version;
}

// Note: since this method is not atomic, this can fail in different ways, that should all be handled properly by
// DuckDB:
//   1. Crash after extension removal: extension is now uninstalled, metadata file still present
//   2. Crash after metadata removal: extension is now uninstalled, extension dir is clean
//   3. Crash after extension move: extension is now uninstalled, new metadata file present
static void WriteExtensionFiles(FileSystem &fs, const string &temp_path, const string &local_extension_path,
                                void *in_buffer, idx_t file_size, ExtensionInstallInfo &info) {
	// Write extension to tmp file
	WriteExtensionFileToDisk(fs, temp_path, in_buffer, file_size);

	// Write metadata to tmp file
	auto metadata_tmp_path = temp_path + ".info";
	auto metadata_file_path = local_extension_path + ".info";
	WriteExtensionMetadataFileToDisk(fs, metadata_tmp_path, info);

	// First remove the local extension we are about to replace
	if (fs.FileExists(local_extension_path)) {
		fs.RemoveFile(local_extension_path);
	}

	// Then remove the old metadata file
	if (fs.FileExists(metadata_file_path)) {
		fs.RemoveFile(metadata_file_path);
	}

	fs.MoveFile(metadata_tmp_path, metadata_file_path);
	fs.MoveFile(temp_path, local_extension_path);
}

// Install an extension using a filesystem
static unique_ptr<ExtensionInstallInfo> DirectInstallExtension(DBConfig &config, FileSystem &fs, const string &path,
                                                               const string &temp_path, const string &extension_name,
                                                               const string &local_extension_path, bool force_install,
                                                               optional_ptr<ExtensionRepository> repository,
                                                               optional_ptr<ClientContext> context) {
	string file = fs.ConvertSeparators(path);

	// Try autoloading httpfs for loading extensions over https
	if (context) {
		auto &db = DatabaseInstance::GetDatabase(*context);
		if (StringUtil::StartsWith(path, "https://") && !db.ExtensionIsLoaded("httpfs") &&
		    db.config.options.autoload_known_extensions) {
			ExtensionHelper::AutoLoadExtension(*context, "httpfs");
		}
	}

	// Check if file exists
	bool exists = fs.FileExists(file);

	// Recheck without .gz
	if (!exists && StringUtil::EndsWith(file, ".gz")) {
		file = file.substr(0, file.size() - 3);
		exists = fs.FileExists(file);
	}

	// Throw error on failure
	if (!exists) {
		if (!fs.IsRemoteFile(file)) {
			throw IOException("Failed to copy local extension \"%s\" at PATH \"%s\"\n", extension_name, file);
		}
		if (StringUtil::StartsWith(file, "https://")) {
			throw IOException("Failed to install remote extension \"%s\" from url \"%s\"", extension_name, file);
		}
	}

	idx_t file_size;
	auto in_buffer = ReadExtensionFileFromDisk(fs, file, file_size);

	ExtensionInstallInfo info;

	string decompressed_data;
	void *extension_decompressed;
	idx_t extension_decompressed_size;

	if (GZipFileSystem::CheckIsZip(const_char_ptr_cast(in_buffer.get()), file_size)) {
		decompressed_data = GZipFileSystem::UncompressGZIPString(const_char_ptr_cast(in_buffer.get()), file_size);
		extension_decompressed = (void *)decompressed_data.data();
		extension_decompressed_size = decompressed_data.size();
	} else {
		extension_decompressed = (void *)in_buffer.get();
		extension_decompressed_size = file_size;
	}

	CheckExtensionMetadataOnInstall(config, extension_decompressed, extension_decompressed_size, info, extension_name);

	if (!repository) {
		info.mode = ExtensionInstallMode::CUSTOM_PATH;
		info.full_path = file;
	} else {
		info.mode = ExtensionInstallMode::REPOSITORY;
		info.full_path = file;
		info.repository_url = repository->path;
	}

	WriteExtensionFiles(fs, temp_path, local_extension_path, extension_decompressed, extension_decompressed_size, info);

	return make_uniq<ExtensionInstallInfo>(info);
}

static unique_ptr<ExtensionInstallInfo> InstallFromHttpUrl(DBConfig &config, const string &url,
                                                           const string &extension_name, const string &temp_path,
                                                           const string &local_extension_path, bool force_install,
                                                           optional_ptr<ExtensionRepository> repository,
                                                           optional_ptr<HTTPLogger> http_logger) {
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
	if (http_logger) {
		cli.set_logger(http_logger->GetLogger<duckdb_httplib::Request, duckdb_httplib::Response>());
	}

	duckdb_httplib::Headers headers = {
	    {"User-Agent", StringUtil::Format("%s %s", config.UserAgent(), DuckDB::SourceID())}};

	unique_ptr<ExtensionInstallInfo> install_info;
	{
		auto fs = FileSystem::CreateLocal();
		if (fs->FileExists(local_extension_path + ".info")) {
			install_info = ExtensionInstallInfo::TryReadInfoFile(*fs, local_extension_path + ".info", extension_name);
		}
		if (install_info && !install_info->etag.empty()) {
			headers.insert({"If-None-Match", StringUtil::Format("%s", install_info->etag)});
		}
	}

	auto res = cli.Get(url_local_part.c_str(), headers);

	if (install_info && res && res->status == 304) {
		return install_info;
	}

	if (!res || res->status != 200) {
		// create suggestions
		string message;
		auto exact_match = ExtensionHelper::CreateSuggestions(extension_name, message);
		if (exact_match && !ExtensionHelper::IsRelease(DuckDB::LibraryVersion())) {
			message += "\nAre you using a development build? In this case, extensions might not (yet) be uploaded.";
		}
		if (res.error() == duckdb_httplib::Error::Success) {
			throw HTTPException(res.value(), "Failed to download extension \"%s\" at URL \"%s%s\" (HTTP %n)\n%s",
			                    extension_name, url_base, url_local_part, res->status, message);
		} else {
			throw IOException("Failed to download extension \"%s\" at URL \"%s%s\"\n%s (ERROR %s)", extension_name,
			                  url_base, url_local_part, message, to_string(res.error()));
		}
	}
	auto decompressed_body = GZipFileSystem::UncompressGZIPString(res->body);

	ExtensionInstallInfo info;
	CheckExtensionMetadataOnInstall(config, (void *)decompressed_body.data(), decompressed_body.size(), info,
	                                extension_name);
	if (res->has_header("ETag")) {
		info.etag = res->get_header_value("ETag");
	}

	if (repository) {
		info.mode = ExtensionInstallMode::REPOSITORY;
		info.full_path = url;
		info.repository_url = repository->path;
	} else {
		info.mode = ExtensionInstallMode::CUSTOM_PATH;
		info.full_path = url;
	}

	auto fs = FileSystem::CreateLocal();
	WriteExtensionFiles(*fs, temp_path, local_extension_path, (void *)decompressed_body.data(),
	                    decompressed_body.size(), info);

	return make_uniq<ExtensionInstallInfo>(info);
}

// Install an extension using a hand-rolled http request
static unique_ptr<ExtensionInstallInfo> InstallFromRepository(DBConfig &config, FileSystem &fs, const string &url,
                                                              const string &extension_name,
                                                              ExtensionRepository &repository, const string &temp_path,
                                                              const string &local_extension_path, const string &version,
                                                              bool force_install, optional_ptr<HTTPLogger> http_logger,
                                                              optional_ptr<ClientContext> context) {
	string url_template = ExtensionHelper::ExtensionUrlTemplate(&config, repository, version);
	string generated_url = ExtensionHelper::ExtensionFinalizeUrlTemplate(url_template, extension_name);

	// Special handling for http repository: avoid using regular filesystem (note: the filesystem is not used here)
	if (StringUtil::StartsWith(repository.path, "http://")) {
		return InstallFromHttpUrl(config, generated_url, extension_name, temp_path, local_extension_path, force_install,
		                          repository, http_logger);
	}

	// Default case, let the FileSystem figure it out
	return DirectInstallExtension(config, fs, generated_url, temp_path, extension_name, local_extension_path,
	                              force_install, repository, context);
}

static bool IsHTTP(const string &path) {
	return StringUtil::StartsWith(path, "http://") || !StringUtil::StartsWith(path, "https://");
}

static void ThrowErrorOnMismatchingExtensionOrigin(FileSystem &fs, const string &local_extension_path,
                                                   const string &extension_name, const string &extension,
                                                   optional_ptr<ExtensionRepository> repository) {
	auto install_info = ExtensionInstallInfo::TryReadInfoFile(fs, local_extension_path + ".info", extension_name);

	string format_string = "Installing extension '%s' failed. The extension is already installed "
	                       "but the origin is different.\n"
	                       "Currently installed extension is from %s '%s', while the extension to be "
	                       "installed is from %s '%s'.\n"
	                       "To solve this rerun this command with `FORCE INSTALL`";
	string repo = "repository";
	string custom_path = "custom_path";

	if (install_info) {
		if (install_info->mode == ExtensionInstallMode::REPOSITORY && repository &&
		    install_info->repository_url != repository->path) {
			throw InvalidInputException(format_string, extension_name, repo, install_info->repository_url, repo,
			                            repository->path);
		}
		if (install_info->mode == ExtensionInstallMode::REPOSITORY && ExtensionHelper::IsFullPath(extension)) {
			throw InvalidInputException(format_string, extension_name, repo, install_info->repository_url, custom_path,
			                            extension);
		}
	}
}

unique_ptr<ExtensionInstallInfo>
ExtensionHelper::InstallExtensionInternal(DBConfig &config, FileSystem &fs, const string &local_path,
                                          const string &extension, bool force_install, bool throw_on_origin_mismatch,
                                          const string &version, optional_ptr<ExtensionRepository> repository,
                                          optional_ptr<HTTPLogger> http_logger, optional_ptr<ClientContext> context) {
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
		// File exists: throw error if origin mismatches
		if (throw_on_origin_mismatch && !config.options.allow_extensions_metadata_mismatch &&
		    fs.FileExists(local_extension_path + ".info")) {
			ThrowErrorOnMismatchingExtensionOrigin(fs, local_extension_path, extension_name, extension, repository);
		}

		// File exists, but that's okay, install is now a NOP
		return nullptr;
	}

	if (fs.FileExists(temp_path)) {
		fs.RemoveFile(temp_path);
	}

	if (ExtensionHelper::IsFullPath(extension) && repository) {
		throw InvalidInputException("Cannot pass both a repository and a full path url");
	}

	// Resolve default repository if there is none set
	ExtensionRepository resolved_repository;
	if (!ExtensionHelper::IsFullPath(extension) && !repository) {
		resolved_repository = ExtensionRepository::GetDefaultRepository(config);
		repository = resolved_repository;
	}

	// Install extension from local, direct url
	if (ExtensionHelper::IsFullPath(extension) && !IsHTTP(extension)) {
		LocalFileSystem local_fs;
		return DirectInstallExtension(config, local_fs, extension, temp_path, extension, local_extension_path,
		                              force_install, nullptr, context);
	}

	// Install extension from local url based on a repository (Note that this will install it as a local file)
	if (repository && !IsHTTP(repository->path)) {
		LocalFileSystem local_fs;
		return InstallFromRepository(config, fs, extension, extension_name, *repository, temp_path,
		                             local_extension_path, version, force_install, http_logger, context);
	}

#ifdef DISABLE_DUCKDB_REMOTE_INSTALL
	throw BinderException("Remote extension installation is disabled through configuration");
#else

	// Full path direct installation
	if (IsFullPath(extension)) {
		if (StringUtil::StartsWith(extension, "http://")) {
			// HTTP takes separate path to avoid dependency on httpfs extension
			return InstallFromHttpUrl(config, extension, extension_name, temp_path, local_extension_path, force_install,
			                          nullptr, http_logger);
		}

		// Direct installation from local or remote path
		return DirectInstallExtension(config, fs, extension, temp_path, extension, local_extension_path, force_install,
		                              nullptr, context);
	}

	// Repository installation
	return InstallFromRepository(config, fs, extension, extension_name, *repository, temp_path, local_extension_path,
	                             version, force_install, http_logger, context);
#endif
#endif
}

} // namespace duckdb
