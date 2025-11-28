#include "duckdb/storage/caching_file_system_wrapper.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

// CachingFileHandleWrapper implementation
CachingFileHandleWrapper::CachingFileHandleWrapper(CachingFileSystemWrapper &file_system,
                                                     unique_ptr<CachingFileHandle> handle)
    : FileHandle(file_system, handle->GetPath(), FileOpenFlags::FILE_FLAGS_READ), caching_handle(std::move(handle)),
      position(0) {
	// CachingFileSystem is read-only, so we use FILE_FLAGS_READ
}

CachingFileHandleWrapper::~CachingFileHandleWrapper() {
}

void CachingFileHandleWrapper::Close() {
	current_buffer_handle = BufferHandle(); // Release buffer handle
	if (caching_handle) {
		caching_handle.reset();
	}
}

CachingFileHandle &CachingFileHandleWrapper::GetCachingFileHandle() {
	return *caching_handle;
}

// CachingFileSystemWrapper implementation
CachingFileSystemWrapper::CachingFileSystemWrapper(FileSystem &file_system, DatabaseInstance &db)
    : caching_file_system(file_system, db), underlying_file_system(file_system) {
}

CachingFileSystemWrapper::~CachingFileSystemWrapper() {
}

std::string CachingFileSystemWrapper::GetName() const {
	return "CachingFileSystemWrapper";
}

unique_ptr<FileHandle> CachingFileSystemWrapper::OpenFile(const string &path, FileOpenFlags flags,
                                                          optional_ptr<FileOpener> opener) {
	return OpenFile(OpenFileInfo(path), flags, opener);
}

unique_ptr<FileHandle> CachingFileSystemWrapper::OpenFile(const OpenFileInfo &path, FileOpenFlags flags,
                                                          optional_ptr<FileOpener> opener) {
	if (SupportsOpenFileExtended()) {
		return OpenFileExtended(path, flags, opener);
	}
	throw NotImplementedException("CachingFileSystemWrapper: OpenFile is not implemented!");
}

unique_ptr<FileHandle> CachingFileSystemWrapper::OpenFileExtended(const OpenFileInfo &path, FileOpenFlags flags,
                                                                  optional_ptr<FileOpener> opener) {
	auto caching_handle = caching_file_system.OpenFile(path, flags);
	if (!caching_handle) {
		return nullptr;
	}
	return make_uniq<CachingFileHandleWrapper>(*this, std::move(caching_handle));
}

bool CachingFileSystemWrapper::SupportsOpenFileExtended() const {
	return true;
}

void CachingFileSystemWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &caching_handle = wrapper.GetCachingFileHandle();

	data_ptr_t cached_buffer = nullptr;
	auto buffer_handle = caching_handle.Read(cached_buffer, UnsafeNumericCast<idx_t>(nr_bytes), location);
	if (!buffer_handle.IsValid()) {
		throw IOException("Failed to read from caching file handle");
	}

	// Copy data from cached buffer to user's buffer
	memcpy(buffer, cached_buffer, UnsafeNumericCast<size_t>(nr_bytes));

	// Keep the buffer handle alive in the wrapper
	wrapper.current_buffer_handle = std::move(buffer_handle);
}

int64_t CachingFileSystemWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &caching_handle = wrapper.GetCachingFileHandle();

	idx_t bytes_to_read = UnsafeNumericCast<idx_t>(nr_bytes);
	data_ptr_t cached_buffer = nullptr;
	auto buffer_handle = caching_handle.Read(cached_buffer, bytes_to_read);

	if (!buffer_handle.IsValid()) {
		throw IOException("Failed to read from caching file handle");
	}

	// Copy data from cached buffer to user's buffer
	memcpy(buffer, cached_buffer, UnsafeNumericCast<size_t>(bytes_to_read));

	// Keep the buffer handle alive in the wrapper
	wrapper.current_buffer_handle = std::move(buffer_handle);

	// Update position
	wrapper.position += bytes_to_read;

	return UnsafeNumericCast<int64_t>(bytes_to_read);
}

void CachingFileSystemWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	// CachingFileSystem is read-only, delegate to underlying file system
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &file_handle = wrapper.GetCachingFileHandle().GetFileHandle();
	underlying_file_system.Write(file_handle, buffer, nr_bytes, location);
}

int64_t CachingFileSystemWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	// CachingFileSystem is read-only, delegate to underlying file system
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &file_handle = wrapper.GetCachingFileHandle().GetFileHandle();
	return underlying_file_system.Write(file_handle, buffer, nr_bytes);
}

bool CachingFileSystemWrapper::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &file_handle = wrapper.GetCachingFileHandle().GetFileHandle();
	return underlying_file_system.Trim(file_handle, offset_bytes, length_bytes);
}

int64_t CachingFileSystemWrapper::GetFileSize(FileHandle &handle) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &caching_handle = wrapper.GetCachingFileHandle();
	return UnsafeNumericCast<int64_t>(caching_handle.GetFileSize());
}

timestamp_t CachingFileSystemWrapper::GetLastModifiedTime(FileHandle &handle) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &caching_handle = wrapper.GetCachingFileHandle();
	return caching_handle.GetLastModifiedTime();
}

string CachingFileSystemWrapper::GetVersionTag(FileHandle &handle) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &caching_handle = wrapper.GetCachingFileHandle();
	return caching_handle.GetVersionTag();
}

FileType CachingFileSystemWrapper::GetFileType(FileHandle &handle) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &file_handle = wrapper.GetCachingFileHandle().GetFileHandle();
	return underlying_file_system.GetFileType(file_handle);
}

FileMetadata CachingFileSystemWrapper::Stats(FileHandle &handle) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &file_handle = wrapper.GetCachingFileHandle().GetFileHandle();
	return underlying_file_system.Stats(file_handle);
}

void CachingFileSystemWrapper::Truncate(FileHandle &handle, int64_t new_size) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &file_handle = wrapper.GetCachingFileHandle().GetFileHandle();
	underlying_file_system.Truncate(file_handle, new_size);
}

void CachingFileSystemWrapper::FileSync(FileHandle &handle) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &file_handle = wrapper.GetCachingFileHandle().GetFileHandle();
	underlying_file_system.FileSync(file_handle);
}

bool CachingFileSystemWrapper::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return underlying_file_system.DirectoryExists(directory, opener);
}

void CachingFileSystemWrapper::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	underlying_file_system.CreateDirectory(directory, opener);
}

void CachingFileSystemWrapper::CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener) {
	underlying_file_system.CreateDirectoriesRecursive(path, opener);
}

void CachingFileSystemWrapper::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	underlying_file_system.RemoveDirectory(directory, opener);
}

bool CachingFileSystemWrapper::ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
                                         FileOpener *opener) {
	return underlying_file_system.ListFiles(directory, callback, opener);
}

bool CachingFileSystemWrapper::ListFilesExtended(const string &directory,
                                                 const std::function<void(OpenFileInfo &info)> &callback,
                                                 optional_ptr<FileOpener> opener) {
	// Use the public ListFiles API which will internally call ListFilesExtended if supported
	return underlying_file_system.ListFiles(directory, callback, opener);
}

bool CachingFileSystemWrapper::SupportsListFilesExtended() const {
	// Check if the underlying file system supports it by checking if it has the method
	// Since we can't call the protected method directly, we assume it does if it's a VirtualFileSystem
	// or we can just return true and let the public API handle the fallback
	return true;
}

void CachingFileSystemWrapper::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	underlying_file_system.MoveFile(source, target, opener);
}

bool CachingFileSystemWrapper::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return underlying_file_system.FileExists(filename, opener);
}

bool CachingFileSystemWrapper::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return underlying_file_system.IsPipe(filename, opener);
}

void CachingFileSystemWrapper::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	underlying_file_system.RemoveFile(filename, opener);
}

bool CachingFileSystemWrapper::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	return underlying_file_system.TryRemoveFile(filename, opener);
}

string CachingFileSystemWrapper::GetHomeDirectory() {
	return underlying_file_system.GetHomeDirectory();
}

string CachingFileSystemWrapper::ExpandPath(const string &path) {
	return underlying_file_system.ExpandPath(path);
}

string CachingFileSystemWrapper::PathSeparator(const string &path) {
	return underlying_file_system.PathSeparator(path);
}

vector<OpenFileInfo> CachingFileSystemWrapper::Glob(const string &path, FileOpener *opener) {
	return underlying_file_system.Glob(path, opener);
}

void CachingFileSystemWrapper::RegisterSubSystem(unique_ptr<FileSystem> sub_fs) {
	underlying_file_system.RegisterSubSystem(std::move(sub_fs));
}

void CachingFileSystemWrapper::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) {
	underlying_file_system.RegisterSubSystem(compression_type, std::move(fs));
}

void CachingFileSystemWrapper::UnregisterSubSystem(const string &name) {
	underlying_file_system.UnregisterSubSystem(name);
}

unique_ptr<FileSystem> CachingFileSystemWrapper::ExtractSubSystem(const string &name) {
	return underlying_file_system.ExtractSubSystem(name);
}

vector<string> CachingFileSystemWrapper::ListSubSystems() {
	return underlying_file_system.ListSubSystems();
}

bool CachingFileSystemWrapper::CanHandleFile(const string &fpath) {
	return underlying_file_system.CanHandleFile(fpath);
}

void CachingFileSystemWrapper::Seek(FileHandle &handle, idx_t location) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	wrapper.position = location;
	auto &file_handle = wrapper.GetCachingFileHandle().GetFileHandle();
	underlying_file_system.Seek(file_handle, location);
}

void CachingFileSystemWrapper::Reset(FileHandle &handle) {
	Seek(handle, 0);
}

idx_t CachingFileSystemWrapper::SeekPosition(FileHandle &handle) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &file_handle = wrapper.GetCachingFileHandle().GetFileHandle();
	return underlying_file_system.SeekPosition(file_handle);
}

bool CachingFileSystemWrapper::IsManuallySet() {
	return underlying_file_system.IsManuallySet();
}

bool CachingFileSystemWrapper::CanSeek() {
	return underlying_file_system.CanSeek();
}

bool CachingFileSystemWrapper::OnDiskFile(FileHandle &handle) {
	auto &wrapper = handle.Cast<CachingFileHandleWrapper>();
	auto &caching_handle = wrapper.GetCachingFileHandle();
	return caching_handle.OnDiskFile();
}

unique_ptr<FileHandle> CachingFileSystemWrapper::OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle,
                                                                    bool write) {
	return underlying_file_system.OpenCompressedFile(context, std::move(handle), write);
}

void CachingFileSystemWrapper::SetDisabledFileSystems(const vector<string> &names) {
	underlying_file_system.SetDisabledFileSystems(names);
}

bool CachingFileSystemWrapper::SubSystemIsDisabled(const string &name) {
	return underlying_file_system.SubSystemIsDisabled(name);
}

} // namespace duckdb

