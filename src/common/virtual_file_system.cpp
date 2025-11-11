#include "duckdb/common/virtual_file_system.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/pipe_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

VirtualFileSystem::VirtualFileSystem() : VirtualFileSystem(FileSystem::CreateLocal()) {
}

VirtualFileSystem::VirtualFileSystem(unique_ptr<FileSystem> &&inner) : default_fs(std::move(inner)) {
	VirtualFileSystem::RegisterSubSystem(FileCompressionType::GZIP, make_uniq<GZipFileSystem>());
}

unique_ptr<FileHandle> VirtualFileSystem::OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
                                                           optional_ptr<FileOpener> opener) {
	auto compression = flags.Compression();
	if (compression == FileCompressionType::AUTO_DETECT) {
		// auto-detect compression settings based on file name
		auto lower_path = StringUtil::Lower(file.path);
		if (StringUtil::EndsWith(lower_path, ".tmp")) {
			// strip .tmp
			lower_path = lower_path.substr(0, lower_path.length() - 4);
		}
		if (IsFileCompressed(file.path, FileCompressionType::GZIP)) {
			compression = FileCompressionType::GZIP;
		} else if (IsFileCompressed(file.path, FileCompressionType::ZSTD)) {
			compression = FileCompressionType::ZSTD;
		} else {
			compression = FileCompressionType::UNCOMPRESSED;
		}
	}
	// open the base file handle in UNCOMPRESSED mode

	flags.SetCompression(FileCompressionType::UNCOMPRESSED);
	auto file_handle = FindFileSystem(file.path, opener).OpenFile(file, flags, opener);
	if (!file_handle) {
		return nullptr;
	}

	const auto context = !flags.MultiClientAccess() ? FileOpener::TryGetClientContext(opener) : QueryContext();
	if (file_handle->GetType() == FileType::FILE_TYPE_FIFO) {
		file_handle = PipeFileSystem::OpenPipe(context, std::move(file_handle));
	} else if (compression != FileCompressionType::UNCOMPRESSED) {
		auto entry = compressed_fs.find(compression);
		if (entry == compressed_fs.end()) {
			if (compression == FileCompressionType::ZSTD) {
				throw NotImplementedException(
				    "Attempting to open a compressed file, but the compression type is not supported.\nConsider "
				    "explicitly \"INSTALL parquet; LOAD parquet;\" to support this compression scheme");
			}
			throw NotImplementedException(
			    "Attempting to open a compressed file, but the compression type is not supported");
		}
		file_handle = entry->second->OpenCompressedFile(context, std::move(file_handle), flags.OpenForWriting());
	}
	return file_handle;
}

void VirtualFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	handle.file_system.Read(handle, buffer, nr_bytes, location);
}

void VirtualFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	handle.file_system.Write(handle, buffer, nr_bytes, location);
}

int64_t VirtualFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	return handle.file_system.Read(handle, buffer, nr_bytes);
}

int64_t VirtualFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	return handle.file_system.Write(handle, buffer, nr_bytes);
}

int64_t VirtualFileSystem::GetFileSize(FileHandle &handle) {
	return handle.file_system.GetFileSize(handle);
}
timestamp_t VirtualFileSystem::GetLastModifiedTime(FileHandle &handle) {
	return handle.file_system.GetLastModifiedTime(handle);
}
string VirtualFileSystem::GetVersionTag(FileHandle &handle) {
	return handle.file_system.GetVersionTag(handle);
}
FileType VirtualFileSystem::GetFileType(FileHandle &handle) {
	return handle.file_system.GetFileType(handle);
}
FileMetadata VirtualFileSystem::Stats(FileHandle &handle) {
	return handle.file_system.Stats(handle);
}

void VirtualFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	handle.file_system.Truncate(handle, new_size);
}

void VirtualFileSystem::FileSync(FileHandle &handle) {
	handle.file_system.FileSync(handle);
}

// need to look up correct fs for this
bool VirtualFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return FindFileSystem(directory).DirectoryExists(directory, opener);
}
void VirtualFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	FindFileSystem(directory).CreateDirectory(directory, opener);
}

void VirtualFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	FindFileSystem(directory).RemoveDirectory(directory, opener);
}

bool VirtualFileSystem::ListFilesExtended(const string &directory,
                                          const std::function<void(OpenFileInfo &info)> &callback,
                                          optional_ptr<FileOpener> opener) {
	return FindFileSystem(directory, opener).ListFiles(directory, callback, opener);
}

void VirtualFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	FindFileSystem(source).MoveFile(source, target, opener);
}

bool VirtualFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return FindFileSystem(filename, opener).FileExists(filename, opener);
}

bool VirtualFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return FindFileSystem(filename).IsPipe(filename, opener);
}

void VirtualFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	FindFileSystem(filename).RemoveFile(filename, opener);
}

bool VirtualFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	return FindFileSystem(filename).TryRemoveFile(filename, opener);
}

string VirtualFileSystem::PathSeparator(const string &path) {
	return FindFileSystem(path).PathSeparator(path);
}

vector<OpenFileInfo> VirtualFileSystem::Glob(const string &path, FileOpener *opener) {
	return FindFileSystem(path, opener).Glob(path, opener);
}

void VirtualFileSystem::RegisterSubSystem(unique_ptr<FileSystem> fs) {
	// Sub-filesystem number is not expected to be huge, also filesystem registration should be called infrequently.
	const auto &name = fs->GetName();
	for (auto sub_system = sub_systems.begin(); sub_system != sub_systems.end(); sub_system++) {
		if (sub_system->get()->GetName() == name) {
			throw InvalidInputException("Filesystem with name %s has already been registered, cannot re-register!",
			                            name);
		}
	}
	sub_systems.push_back(std::move(fs));
}

void VirtualFileSystem::UnregisterSubSystem(const string &name) {
	for (auto sub_system = sub_systems.begin(); sub_system != sub_systems.end(); sub_system++) {
		if (sub_system->get()->GetName() == name) {
			sub_systems.erase(sub_system);
			return;
		}
	}
	throw InvalidInputException("Could not find filesystem with name %s", name);
}

void VirtualFileSystem::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) {
	compressed_fs[compression_type] = std::move(fs);
}

unique_ptr<FileSystem> VirtualFileSystem::ExtractSubSystem(const string &name) {
	// If the subsystem has been disabled, we don't allow extraction and return nullptr here.
	if (disabled_file_systems.find(name) != disabled_file_systems.end()) {
		return nullptr;
	}

	unique_ptr<FileSystem> extracted_filesystem;
	for (auto iter = sub_systems.begin(); iter != sub_systems.end(); ++iter) {
		auto &cur_filesystem = *iter;
		if (cur_filesystem->GetName() == name) {
			extracted_filesystem = std::move(cur_filesystem);
			sub_systems.erase(iter);
			return extracted_filesystem;
		}
	}

	// Requested subfilesystem is not registered.
	return nullptr;
}

vector<string> VirtualFileSystem::ListSubSystems() {
	vector<string> names(sub_systems.size());
	for (idx_t i = 0; i < sub_systems.size(); i++) {
		names[i] = sub_systems[i]->GetName();
	}
	return names;
}

std::string VirtualFileSystem::GetName() const {
	return "VirtualFileSystem";
}

void VirtualFileSystem::SetDisabledFileSystems(const vector<string> &names) {
	unordered_set<string> new_disabled_file_systems;
	for (auto &name : names) {
		if (name.empty()) {
			continue;
		}
		if (new_disabled_file_systems.find(name) != new_disabled_file_systems.end()) {
			throw InvalidInputException("Duplicate disabled file system \"%s\"", name);
		}
		new_disabled_file_systems.insert(name);
	}
	for (auto &disabled_fs : disabled_file_systems) {
		if (new_disabled_file_systems.find(disabled_fs) == new_disabled_file_systems.end()) {
			throw InvalidInputException("File system \"%s\" has been disabled previously, it cannot be re-enabled",
			                            disabled_fs);
		}
	}
	disabled_file_systems = std::move(new_disabled_file_systems);
}

bool VirtualFileSystem::SubSystemIsDisabled(const string &name) {
	return disabled_file_systems.find(name) != disabled_file_systems.end();
}

FileSystem &VirtualFileSystem::FindFileSystem(const string &path, optional_ptr<FileOpener> opener) {
	return FindFileSystem(path, FileOpener::TryGetDatabase(opener));
}

FileSystem &VirtualFileSystem::FindFileSystem(const string &path, optional_ptr<DatabaseInstance> db_instance) {
	auto fs = FindFileSystemInternal(path);

	if (!fs && db_instance) {
		string required_extension;

		for (const auto &entry : EXTENSION_FILE_PREFIXES) {
			if (StringUtil::StartsWith(path, entry.name)) {
				required_extension = entry.extension;
			}
		}
		if (!required_extension.empty() && db_instance && !db_instance->ExtensionIsLoaded(required_extension)) {
			auto &dbconfig = DBConfig::GetConfig(*db_instance);
			if (!ExtensionHelper::CanAutoloadExtension(required_extension) ||
			    !dbconfig.options.autoload_known_extensions) {
				auto error_message = "File " + path + " requires the extension " + required_extension + " to be loaded";
				error_message =
				    ExtensionHelper::AddExtensionInstallHintToErrorMsg(*db_instance, error_message, required_extension);
				throw MissingExtensionException(error_message);
			}
			// an extension is required to read this file, but it is not loaded - try to load it
			ExtensionHelper::AutoLoadExtension(*db_instance, required_extension);
		}

		// Retry after having autoloaded
		fs = FindFileSystem(path);
	}

	if (!fs) {
		fs = default_fs;
	}
	if (!disabled_file_systems.empty() && disabled_file_systems.find(fs->GetName()) != disabled_file_systems.end()) {
		throw PermissionException("File system %s has been disabled by configuration", fs->GetName());
	}
	return *fs;
}

FileSystem &VirtualFileSystem::FindFileSystem(const string &path) {
	auto fs = FindFileSystemInternal(path);
	if (!fs) {
		fs = default_fs;
	}
	if (!disabled_file_systems.empty() && disabled_file_systems.find(fs->GetName()) != disabled_file_systems.end()) {
		throw PermissionException("File system %s has been disabled by configuration", fs->GetName());
	}
	return *fs;
}

optional_ptr<FileSystem> VirtualFileSystem::FindFileSystemInternal(const string &path) {
	FileSystem *fs = nullptr;

	for (auto &sub_system : sub_systems) {
		if (sub_system->CanHandleFile(path)) {
			if (sub_system->IsManuallySet()) {
				return *sub_system;
			}
			fs = sub_system.get();
		}
	}
	if (fs) {
		return *fs;
	}

	// We could use default_fs, that's on the caller
	return nullptr;
}

} // namespace duckdb
