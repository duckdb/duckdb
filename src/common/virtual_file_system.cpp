#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/common/gzip_file_system.hpp"
#include "duckdb/common/pipe_file_system.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

VirtualFileSystem::VirtualFileSystem() : default_fs(FileSystem::CreateLocal()) {
	VirtualFileSystem::RegisterSubSystem(FileCompressionType::GZIP, make_uniq<GZipFileSystem>());
}

unique_ptr<FileHandle> VirtualFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                   optional_ptr<FileOpener> opener) {
	auto compression = flags.Compression();
	if (compression == FileCompressionType::AUTO_DETECT) {
		// auto-detect compression settings based on file name
		auto lower_path = StringUtil::Lower(path);
		if (StringUtil::EndsWith(lower_path, ".tmp")) {
			// strip .tmp
			lower_path = lower_path.substr(0, lower_path.length() - 4);
		}
		if (IsFileCompressed(path, FileCompressionType::GZIP)) {
			compression = FileCompressionType::GZIP;
		} else if (IsFileCompressed(path, FileCompressionType::ZSTD)) {
			compression = FileCompressionType::ZSTD;
		} else {
			compression = FileCompressionType::UNCOMPRESSED;
		}
	}
	// open the base file handle in UNCOMPRESSED mode
	flags.SetCompression(FileCompressionType::UNCOMPRESSED);
	auto file_handle = FindFileSystem(path).OpenFile(path, flags, opener);
	if (!file_handle) {
		return nullptr;
	}
	if (file_handle->GetType() == FileType::FILE_TYPE_FIFO) {
		file_handle = PipeFileSystem::OpenPipe(std::move(file_handle));
	} else if (compression != FileCompressionType::UNCOMPRESSED) {
		auto entry = compressed_fs.find(compression);
		if (entry == compressed_fs.end()) {
			throw NotImplementedException(
			    "Attempting to open a compressed file, but the compression type is not supported");
		}
		file_handle = entry->second->OpenCompressedFile(std::move(file_handle), flags.OpenForWriting());
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
time_t VirtualFileSystem::GetLastModifiedTime(FileHandle &handle) {
	return handle.file_system.GetLastModifiedTime(handle);
}
FileType VirtualFileSystem::GetFileType(FileHandle &handle) {
	return handle.file_system.GetFileType(handle);
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

bool VirtualFileSystem::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                                  FileOpener *opener) {
	return FindFileSystem(directory).ListFiles(directory, callback, opener);
}

void VirtualFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	FindFileSystem(source).MoveFile(source, target, opener);
}

bool VirtualFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return FindFileSystem(filename).FileExists(filename, opener);
}

bool VirtualFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return FindFileSystem(filename).IsPipe(filename, opener);
}

void VirtualFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	FindFileSystem(filename).RemoveFile(filename, opener);
}

string VirtualFileSystem::PathSeparator(const string &path) {
	return FindFileSystem(path).PathSeparator(path);
}

vector<string> VirtualFileSystem::Glob(const string &path, FileOpener *opener) {
	return FindFileSystem(path).Glob(path, opener);
}

void VirtualFileSystem::RegisterSubSystem(unique_ptr<FileSystem> fs) {
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

FileSystem &VirtualFileSystem::FindFileSystem(const string &path) {
	auto &fs = FindFileSystemInternal(path);
	if (!disabled_file_systems.empty() && disabled_file_systems.find(fs.GetName()) != disabled_file_systems.end()) {
		throw PermissionException("File system %s has been disabled by configuration", fs.GetName());
	}
	return fs;
}

FileSystem &VirtualFileSystem::FindFileSystemInternal(const string &path) {
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
	return *default_fs;
}

} // namespace duckdb
