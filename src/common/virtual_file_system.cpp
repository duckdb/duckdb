#include "duckdb/common/virtual_file_system.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/pipe_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/caching_file_system_wrapper.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"

namespace duckdb {

struct FileSystemHandle {
	explicit FileSystemHandle(unique_ptr<FileSystem> fs) : file_system(std::move(fs)) {
	}

	unique_ptr<FileSystem> file_system;
};

//! The FileSystemRegistry holds the set of file systems that are registered.
//! Note that it should be treated as read-only.
//! When a change is made (e.g. by registering new file system) a copy of the registry is made
struct FileSystemRegistry {
	explicit FileSystemRegistry(unique_ptr<FileSystem> fs)
	    : default_fs(make_shared_ptr<FileSystemHandle>(std::move(fs))) {
	}

	vector<shared_ptr<FileSystemHandle>> sub_systems;
	map<FileCompressionType, shared_ptr<FileSystemHandle>> compressed_fs;
	const shared_ptr<FileSystemHandle> default_fs;
	unordered_set<string> disabled_file_systems;

public:
	shared_ptr<FileSystemRegistry> RegisterSubSystem(unique_ptr<FileSystem> fs) const;
	shared_ptr<FileSystemRegistry> RegisterSubSystem(FileCompressionType compression_type,
	                                                 unique_ptr<FileSystem> fs) const;
	shared_ptr<FileSystemRegistry> SetDisabledFileSystems(const vector<string> &names) const;
	shared_ptr<FileSystemRegistry> ExtractSubSystem(const string &name, unique_ptr<FileSystem> &result) const;
};

shared_ptr<FileSystemRegistry> FileSystemRegistry::RegisterSubSystem(unique_ptr<FileSystem> fs) const {
	auto new_registry = make_shared_ptr<FileSystemRegistry>(*this);
	const auto &name = fs->GetName();
	for (auto &sub_system : new_registry->sub_systems) {
		if (sub_system->file_system->GetName() == name) {
			throw InvalidInputException("Filesystem with name %s has already been registered, cannot re-register!",
			                            name);
		}
	}
	new_registry->sub_systems.push_back(make_shared_ptr<FileSystemHandle>(std::move(fs)));
	return new_registry;
}

shared_ptr<FileSystemRegistry> FileSystemRegistry::RegisterSubSystem(FileCompressionType compression_type,
                                                                     unique_ptr<FileSystem> fs) const {
	auto new_registry = make_shared_ptr<FileSystemRegistry>(*this);
	new_registry->compressed_fs[compression_type] = make_shared_ptr<FileSystemHandle>(std::move(fs));
	return new_registry;
}

shared_ptr<FileSystemRegistry> FileSystemRegistry::SetDisabledFileSystems(const vector<string> &names) const {
	auto new_registry = make_shared_ptr<FileSystemRegistry>(*this);
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
	new_registry->disabled_file_systems = std::move(new_disabled_file_systems);
	return new_registry;
}

shared_ptr<FileSystemRegistry> FileSystemRegistry::ExtractSubSystem(const string &name,
                                                                    unique_ptr<FileSystem> &result) const {
	auto new_registry = make_shared_ptr<FileSystemRegistry>(*this);
	// If the subsystem has been disabled, we don't allow extraction and return nullptr here.
	if (disabled_file_systems.find(name) != disabled_file_systems.end()) {
		return nullptr;
	}

	for (auto iter = new_registry->sub_systems.begin(); iter != new_registry->sub_systems.end(); ++iter) {
		auto &cur_filesystem = (*iter)->file_system;
		if (cur_filesystem->GetName() == name) {
			result = std::move(cur_filesystem);
			new_registry->sub_systems.erase(iter);
			return new_registry;
		}
	}

	// Requested subfilesystem is not registered.
	return nullptr;
}

VirtualFileSystem::VirtualFileSystem() : VirtualFileSystem(FileSystem::CreateLocal()) {
}

VirtualFileSystem::VirtualFileSystem(unique_ptr<FileSystem> &&inner)
    : file_system_registry(make_shared_ptr<FileSystemRegistry>(std::move(inner))) {
	VirtualFileSystem::RegisterSubSystem(FileCompressionType::GZIP, make_uniq<GZipFileSystem>());
}

VirtualFileSystem::~VirtualFileSystem() {
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

	auto registry = file_system_registry.atomic_load();
	auto &internal_filesystem = FindFileSystem(registry, file.path, opener);

	// File handle gets created.
	unique_ptr<FileHandle> file_handle = nullptr;

	// Handle caching logic.
	if (flags.GetCachingMode() != CachingMode::NO_CACHING) {
		auto caching_filesystem =
		    make_shared_ptr<CachingFileSystemWrapper>(internal_filesystem, opener, flags.GetCachingMode());
		// caching filesystem's lifecycle is extended inside of caching file handle.
		file_handle = caching_filesystem->OpenFile(file, flags, opener);
	} else {
		file_handle = internal_filesystem.OpenFile(file, flags, opener);
	}
	if (!file_handle) {
		return nullptr;
	}

	// Evaluate and apply compression option then.
	const auto context = !flags.MultiClientAccess() ? FileOpener::TryGetClientContext(opener) : QueryContext();
	if (file_handle->GetType() == FileType::FILE_TYPE_FIFO) {
		file_handle = PipeFileSystem::OpenPipe(context, std::move(file_handle));
	} else if (compression != FileCompressionType::UNCOMPRESSED) {
		auto entry = registry->compressed_fs.find(compression);
		if (entry == registry->compressed_fs.end()) {
			if (compression == FileCompressionType::ZSTD) {
				throw NotImplementedException(
				    "Attempting to open a compressed file, but the compression type is not supported.\nConsider "
				    "explicitly \"INSTALL parquet; LOAD parquet;\" to support this compression scheme");
			}
			throw NotImplementedException(
			    "Attempting to open a compressed file, but the compression type is not supported");
		}
		auto &compressed_fs = *entry->second->file_system;
		file_handle = compressed_fs.OpenCompressedFile(context, std::move(file_handle), flags.OpenForWriting());
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
	return FindFileSystem(directory, opener).DirectoryExists(directory, opener);
}
void VirtualFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	FindFileSystem(directory, opener).CreateDirectory(directory, opener);
}

void VirtualFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	FindFileSystem(directory, opener).RemoveDirectory(directory, opener);
}

bool VirtualFileSystem::ListFilesExtended(const string &directory,
                                          const std::function<void(OpenFileInfo &info)> &callback,
                                          optional_ptr<FileOpener> opener) {
	return FindFileSystem(directory, opener).ListFiles(directory, callback, opener);
}

void VirtualFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	FindFileSystem(source, opener).MoveFile(source, target, opener);
}

bool VirtualFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return FindFileSystem(filename, opener).FileExists(filename, opener);
}

bool VirtualFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return FindFileSystem(filename, opener).IsPipe(filename, opener);
}

void VirtualFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	FindFileSystem(filename, opener).RemoveFile(filename, opener);
}

bool VirtualFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	return FindFileSystem(filename, opener).TryRemoveFile(filename, opener);
}

void VirtualFileSystem::RemoveFiles(const vector<string> &filenames, optional_ptr<FileOpener> opener) {
	reference_map_t<FileSystem, vector<string>> files_by_fs;
	for (const auto &filename : filenames) {
		auto &fs = FindFileSystem(filename, opener);
		files_by_fs[fs].push_back(filename);
	}
	for (auto &entry : files_by_fs) {
		entry.first.get().RemoveFiles(entry.second, opener);
	}
}

string VirtualFileSystem::PathSeparator(const string &path) {
	return FindFileSystem(path, nullptr).PathSeparator(path);
}

string VirtualFileSystem::CanonicalizePath(const string &path_p, optional_ptr<FileOpener> opener) {
	return FindFileSystem(path_p, opener).CanonicalizePath(path_p, opener);
}

unique_ptr<MultiFileList> VirtualFileSystem::GlobFilesExtended(const string &path, const FileGlobInput &input,
                                                               optional_ptr<FileOpener> opener) {
	return FindFileSystem(path, opener).Glob(path, input, opener);
}

void VirtualFileSystem::RegisterSubSystem(unique_ptr<FileSystem> fs) {
	lock_guard<mutex> guard(registry_lock);
	auto new_registry = file_system_registry->RegisterSubSystem(std::move(fs));
	file_system_registry.atomic_store(new_registry);
}

void VirtualFileSystem::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) {
	lock_guard<mutex> guard(registry_lock);
	auto new_registry = file_system_registry->RegisterSubSystem(compression_type, std::move(fs));
	file_system_registry.atomic_store(new_registry);
}

void VirtualFileSystem::UnregisterSubSystem(const string &name) {
	auto sub_system = ExtractSubSystem(name);

	lock_guard<mutex> guard(registry_lock);
	unregistered_file_systems.push_back(std::move(sub_system));
}

void VirtualFileSystem::SetDisabledFileSystems(const vector<string> &names) {
	lock_guard<mutex> guard(registry_lock);
	auto new_registry = file_system_registry->SetDisabledFileSystems(names);
	file_system_registry.atomic_store(new_registry);
}
unique_ptr<FileSystem> VirtualFileSystem::ExtractSubSystem(const string &name) {
	lock_guard<mutex> guard(registry_lock);
	unique_ptr<FileSystem> result;
	auto new_registry = file_system_registry->ExtractSubSystem(name, result);
	if (new_registry) {
		file_system_registry.atomic_store(new_registry);
	}
	return result;
}

vector<string> VirtualFileSystem::ListSubSystems() {
	auto registry = file_system_registry.atomic_load();
	auto &sub_systems = registry->sub_systems;
	vector<string> names;
	for (auto &sub_system : sub_systems) {
		names.push_back(sub_system->file_system->GetName());
	}
	return names;
}

std::string VirtualFileSystem::GetName() const {
	return "VirtualFileSystem";
}

bool VirtualFileSystem::SubSystemIsDisabled(const string &name) {
	auto registry = file_system_registry.atomic_load();
	auto &disabled_file_systems = registry->disabled_file_systems;
	return disabled_file_systems.find(name) != disabled_file_systems.end();
}

bool VirtualFileSystem::IsDisabledForPath(const string &path) {
	auto registry = file_system_registry.atomic_load();
	auto &disabled_file_systems = registry->disabled_file_systems;
	if (disabled_file_systems.empty()) {
		return false;
	}
	auto fs = FindFileSystemInternal(*registry, path);
	if (!fs) {
		fs = registry->default_fs->file_system;
	}
	return disabled_file_systems.find(fs->GetName()) != disabled_file_systems.end();
}

FileSystem &VirtualFileSystem::FindFileSystem(const string &path, optional_ptr<FileOpener> opener) {
	auto registry = file_system_registry.atomic_load();
	return FindFileSystem(registry, path, opener);
}

FileSystem &VirtualFileSystem::FindFileSystem(shared_ptr<FileSystemRegistry> &registry, const string &path,
                                              optional_ptr<FileOpener> opener) {
	auto db_instance = FileOpener::TryGetDatabase(opener);
	auto fs = FindFileSystemInternal(*registry, path);

	if (!fs && db_instance) {
		string required_extension;

		for (const auto &entry : EXTENSION_FILE_PREFIXES) {
			if (StringUtil::StartsWith(path, entry.name)) {
				required_extension = entry.extension;
			}
		}
		if (!required_extension.empty() && db_instance && !db_instance->ExtensionIsLoaded(required_extension)) {
			if (!ExtensionHelper::CanAutoloadExtension(required_extension) ||
			    !Settings::Get<AutoloadKnownExtensionsSetting>(*db_instance)) {
				auto error_message = "File " + path + " requires the extension " + required_extension + " to be loaded";
				error_message =
				    ExtensionHelper::AddExtensionInstallHintToErrorMsg(*db_instance, error_message, required_extension);
				throw MissingExtensionException(error_message);
			}
			// an extension is required to read this file, but it is not loaded - try to load it
			ExtensionHelper::AutoLoadExtension(*db_instance, required_extension);
		}
		// refresh the registry after loading extensions
		registry = file_system_registry.atomic_load();

		// Retry after having autoloaded
		fs = FindFileSystemInternal(*registry, path);
	}

	if (!fs) {
		fs = registry->default_fs->file_system;
	}
	auto &disabled_file_systems = registry->disabled_file_systems;
	if (!disabled_file_systems.empty() && disabled_file_systems.find(fs->GetName()) != disabled_file_systems.end()) {
		throw PermissionException("File system %s has been disabled by configuration", fs->GetName());
	}
	return *fs;
}

optional_ptr<FileSystem> VirtualFileSystem::FindFileSystemInternal(FileSystemRegistry &registry, const string &path) {
	optional_ptr<FileSystem> fs;
	for (auto &sub_system : registry.sub_systems) {
		auto &sub_fs = *sub_system->file_system;
		if (sub_fs.CanHandleFile(path)) {
			if (sub_fs.IsManuallySet()) {
				return sub_fs;
			}
			fs = sub_fs;
		}
	}
	return fs;
}

} // namespace duckdb
