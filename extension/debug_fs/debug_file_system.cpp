#include "duckdb/common/memory_mapped_file.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "debug_file_system.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/thread.hpp"
#include "io_latency_model.hpp"

namespace duckdb {

DebugFileSystem::DebugFileSystem(unique_ptr<FileSystem> inner_fs) : inner_fs(std::move(inner_fs)) {
}

FileSystem &DebugFileSystem::GetInnerFileSystem() {
	return *inner_fs;
}

void DebugFileSystem::SetDelayMeanMs(double v) {
	const annotated_lock_guard<annotated_mutex> guard(random_engine_lock);
	delay_mean_ms = v;
}

void DebugFileSystem::SetDelayStddevMs(double v) {
	const annotated_lock_guard<annotated_mutex> guard(random_engine_lock);
	delay_stddev_ms = v;
}

void DebugFileSystem::ApplyDelay() {
#ifndef DUCKDB_NO_THREADS
	double mean_ms;
	double stddev_ms;
	{
		const annotated_lock_guard<annotated_mutex> guard(random_engine_lock);
		mean_ms = delay_mean_ms;
		stddev_ms = delay_stddev_ms;
	}
	if (mean_ms <= 0.0 && stddev_ms <= 0.0) {
		return;
	}

	double delay_ms = 0;
	if (stddev_ms <= 0.0) {
		delay_ms = mean_ms;
	} else {
		const annotated_lock_guard<annotated_mutex> guard(random_engine_lock);
		delay_ms = IoLatencyModel(mean_ms, stddev_ms).SampleLatency(random_engine);
	}
	if (delay_ms > 0.0) {
		ThreadUtil::SleepMs(LossyNumericCast<idx_t>(delay_ms));
	}
#endif
}

unique_ptr<FileHandle> DebugFileSystem::OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
                                                         optional_ptr<FileOpener> opener) {
	ApplyDelay();
	return inner_fs->OpenFile(file, flags, opener);
}

void DebugFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	ApplyDelay();
	inner_fs->Read(handle, buffer, nr_bytes, location);
}

int64_t DebugFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	ApplyDelay();
	return inner_fs->Read(handle, buffer, nr_bytes);
}

void DebugFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	ApplyDelay();
	inner_fs->Write(handle, buffer, nr_bytes, location);
}

int64_t DebugFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	ApplyDelay();
	return inner_fs->Write(handle, buffer, nr_bytes);
}

string DebugFileSystem::GetName() const {
	return inner_fs->GetName();
}

bool DebugFileSystem::IsLocalFileSystem() const {
	return inner_fs->IsLocalFileSystem();
}

int64_t DebugFileSystem::GetFileSize(FileHandle &handle) {
	return inner_fs->GetFileSize(handle);
}

timestamp_t DebugFileSystem::GetLastModifiedTime(FileHandle &handle) {
	return inner_fs->GetLastModifiedTime(handle);
}

string DebugFileSystem::GetVersionTag(FileHandle &handle) {
	return inner_fs->GetVersionTag(handle);
}

FileType DebugFileSystem::GetFileType(FileHandle &handle) {
	return inner_fs->GetFileType(handle);
}

FileMetadata DebugFileSystem::Stats(FileHandle &handle) {
	return inner_fs->Stats(handle);
}

void DebugFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	inner_fs->Truncate(handle, new_size);
}

void DebugFileSystem::FileSync(FileHandle &handle) {
	inner_fs->FileSync(handle);
}

bool DebugFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return inner_fs->DirectoryExists(directory, opener);
}

void DebugFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	inner_fs->CreateDirectory(directory, opener);
}

void DebugFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	inner_fs->RemoveDirectory(directory, opener);
}

void DebugFileSystem::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	inner_fs->MoveFile(source, target, opener);
}

bool DebugFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	return inner_fs->FileExists(filename, opener);
}

bool DebugFileSystem::IsPipe(const string &filename, optional_ptr<FileOpener> opener) {
	return inner_fs->IsPipe(filename, opener);
}

void DebugFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	inner_fs->RemoveFile(filename, opener);
}

bool DebugFileSystem::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	return inner_fs->TryRemoveFile(filename, opener);
}

void DebugFileSystem::RemoveFiles(const vector<string> &filenames, optional_ptr<FileOpener> opener) {
	inner_fs->RemoveFiles(filenames, opener);
}

string DebugFileSystem::PathSeparator(const string &path) {
	return inner_fs->PathSeparator(path);
}

vector<OpenFileInfo> DebugFileSystem::Glob(const string &path, FileOpener *opener) {
	return inner_fs->Glob(path, opener);
}

void DebugFileSystem::RegisterSubSystem(unique_ptr<FileSystem> sub_fs) {
	inner_fs->RegisterSubSystem(std::move(sub_fs));
}

void DebugFileSystem::RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) {
	inner_fs->RegisterSubSystem(compression_type, std::move(fs));
}

void DebugFileSystem::UnregisterSubSystem(const string &name) {
	inner_fs->UnregisterSubSystem(name);
}

unique_ptr<FileSystem> DebugFileSystem::ExtractSubSystem(const string &name) {
	return inner_fs->ExtractSubSystem(name);
}

void DebugFileSystem::SetDisabledFileSystems(const vector<string> &names) {
	inner_fs->SetDisabledFileSystems(names);
}

bool DebugFileSystem::SubSystemIsDisabled(const string &name) {
	return inner_fs->SubSystemIsDisabled(name);
}

bool DebugFileSystem::IsDisabledForPath(const string &path) {
	return inner_fs->IsDisabledForPath(path);
}

vector<string> DebugFileSystem::ListSubSystems() {
	return inner_fs->ListSubSystems();
}

string DebugFileSystem::GetHomeDirectory() {
	return inner_fs->GetHomeDirectory();
}

string DebugFileSystem::ExpandPath(const string &path) {
	return inner_fs->ExpandPath(path);
}

unique_ptr<MemoryMappedFile> DebugFileSystem::MemoryMapFile(const OpenFileInfo &path, FileOpenFlags flags,
                                                            const MMapOptions &options,
                                                            optional_ptr<FileOpener> opener) {
	return inner_fs->MemoryMapFile(path, flags, options, opener);
}

bool DebugFileSystem::SupportsOpenFileExtended() const {
	return true;
}

bool DebugFileSystem::ListFilesExtended(const string &directory,
                                        const std::function<void(OpenFileInfo &info)> &callback,
                                        optional_ptr<FileOpener> opener) {
	return inner_fs->ListFiles(directory, callback, opener);
}

bool DebugFileSystem::SupportsListFilesExtended() const {
	return true;
}

unique_ptr<MultiFileList> DebugFileSystem::GlobFilesExtended(const string &path, const FileGlobInput &input,
                                                             optional_ptr<FileOpener> opener) {
	return inner_fs->Glob(path, input, opener);
}

bool DebugFileSystem::SupportsGlobExtended() const {
	return true;
}

string DebugFileSystem::CanonicalizePath(const string &path_p, optional_ptr<FileOpener> opener) {
	return inner_fs->CanonicalizePath(path_p, opener);
}

} // namespace duckdb
