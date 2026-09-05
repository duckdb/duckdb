#include "duckdb/common/compressed_file_system.hpp"
#include "duckdb/common/memory_mapped_file.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "debug_file_system.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/time_point.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/database.hpp"
#include "io_latency_model.hpp"

namespace duckdb {

DebugFileHandle::DebugFileHandle(DebugFileSystem &fs, unique_ptr<FileHandle> inner_p)
    : FileHandle(fs, inner_p->path, inner_p->flags), inner(std::move(inner_p)) {
}

void DebugFileHandle::Close() {
	inner->Close();
}

bool DebugFileHandle::CanSeek() {
	return inner->CanSeek();
}

idx_t DebugFileHandle::GetProgress() {
	return inner->GetProgress();
}

FileCompressionType DebugFileHandle::GetFileCompressionType() {
	return inner->GetFileCompressionType();
}

#ifndef DUCKDB_NO_THREADS
//! One read that was started asynchronously and has not been completed yet
struct PendingDebugRead {
	FileHandle *handle;
	void *buffer;
	int64_t nr_bytes;
	idx_t location;
	AsyncIOCallback callback;
	//! When this read should be completed, in milliseconds since the queue was created
	double due_ms;
};

//! Completes asynchronously started reads on a dedicated thread, standing in for a platform whose I/O genuinely
//! completes out-of-band (e.g. a browser handing a fetch response back to the event loop).
class DebugAsyncReadQueue {
public:
	DebugAsyncReadQueue() : start(TimePoint::Tick()), thread([this] { Run(); }) {
	}
	~DebugAsyncReadQueue() {
		{
			unique_lock<std::mutex> lock(queue_lock);
			shutdown = true;
		}
		queue_cv.notify_all();
		thread.join();
	}

	void Push(PendingDebugRead read) {
		{
			unique_lock<std::mutex> lock(queue_lock);
			pending.push_back(std::move(read));
		}
		queue_cv.notify_one();
	}

	double ElapsedMs() const {
		return static_cast<double>(TimePoint::ElapsedMicros(start, TimePoint::Tick())) / 1000.0;
	}

private:
	void Run() {
		while (true) {
			PendingDebugRead read;
			{
				unique_lock<std::mutex> lock(queue_lock);
				queue_cv.wait(lock, [this] { return shutdown || !pending.empty(); });
				if (pending.empty()) {
					// only wake up for shutdown once everything that was started has been completed
					return;
				}
				// complete whichever read is due first, so injected latencies overlap instead of serializing
				auto earliest = pending.begin();
				for (auto it = pending.begin(); it != pending.end(); it++) {
					if (it->due_ms < earliest->due_ms) {
						earliest = it;
					}
				}
				read = std::move(*earliest);
				pending.erase(earliest);
			}
			auto remaining_ms = read.due_ms - ElapsedMs();
			if (remaining_ms > 0) {
				ThreadUtil::SleepMs(LossyNumericCast<idx_t>(remaining_ms));
			}
			try {
				read.handle->file_system.Read(*read.handle, read.buffer, read.nr_bytes, read.location);
			} catch (std::exception &ex) {
				ErrorData error(ex);
				read.callback(&error);
				continue;
			}
			read.callback(nullptr);
		}
	}

	TimePoint start;
	std::mutex queue_lock;
	std::condition_variable queue_cv;
	vector<PendingDebugRead> pending;
	bool shutdown = false;
	//! Declared last so the thread only starts once every member it touches is initialized
	std::thread thread;
};
#else
class DebugAsyncReadQueue {};
#endif

DebugFileSystem::DebugFileSystem(unique_ptr<FileSystem> inner_fs, DatabaseInstance &db)
    : inner_fs(std::move(inner_fs)), db(db) {
}

DebugFileSystem::~DebugFileSystem() = default;

void DebugFileSystem::SetDelayMeanMs(double v) {
	const annotated_lock_guard<annotated_mutex> guard(random_engine_lock);
	delay_mean_ms = v;
	ALWAYS_ASSERT(delay_mean_ms >= 0.0);
}

void DebugFileSystem::SetDelayStddevMs(double v) {
	const annotated_lock_guard<annotated_mutex> guard(random_engine_lock);
	delay_stddev_ms = v;
	ALWAYS_ASSERT(delay_stddev_ms >= 0.0);
}

void DebugFileSystem::SetRandomSeed(optional_idx seed) {
	const annotated_lock_guard<annotated_mutex> guard(random_engine_lock);
	if (random_engine && seed.IsValid()) {
		throw InvalidInputException("Cannot change debug_fs_random_seed after the random engine has been initialized");
	}
	random_seed = seed;
}

void DebugFileSystem::EnsureRandomEngineInitialized() {
	const annotated_lock_guard<annotated_mutex> guard(random_engine_lock);
	if (random_engine) {
		return;
	}
	uint64_t seed = 0;
	if (random_seed.IsValid()) {
		seed = random_seed.GetIndex();
	} else {
		seed = NumericCast<uint64_t>(Timestamp::GetCurrentTimestamp().value);
	}
	random_engine = make_uniq<RandomEngine>();
	random_engine->SetSeed(seed);
	// Log the random seed for reproduction.
	DUCKDB_LOG_INFO(db, "DebugFileSystem initialized with random seed: %llu", seed);
}

double DebugFileSystem::SampleDelayMs() {
	double mean_ms = 0;
	double stddev_ms = 0;
	{
		const annotated_lock_guard<annotated_mutex> guard(random_engine_lock);
		mean_ms = delay_mean_ms;
		stddev_ms = delay_stddev_ms;
	}

	if (mean_ms == 0.0) {
		return 0.0;
	}

	// Lazy initialize the random engine on first IO operation, so user-set random seed could be applied.
	EnsureRandomEngineInitialized();

	if (stddev_ms == 0.0) {
		return mean_ms;
	}
	const annotated_lock_guard<annotated_mutex> guard(random_engine_lock);
	return IoLatencyModel(mean_ms, stddev_ms).SampleLatency(*random_engine);
}

void DebugFileSystem::ApplyDelay() {
#ifndef DUCKDB_NO_THREADS
	auto delay_ms = SampleDelayMs();
	if (delay_ms > 0.0) {
		ThreadUtil::SleepMs(LossyNumericCast<idx_t>(delay_ms));
	}
#endif
}

void DebugFileSystem::SetAsyncReads(bool async_reads_p) {
	async_reads = async_reads_p;
}

idx_t DebugFileSystem::GetMaxConcurrentReads() const {
	return max_concurrent_reads.load();
}

idx_t DebugFileSystem::GetAsyncReadCount() const {
	return async_read_count.load();
}

void DebugFileSystem::ResetReadStats() {
	max_concurrent_reads = 0;
	async_read_count = 0;
}

void DebugFileSystem::ReadStarted() {
	const auto in_flight = ++reads_in_flight;
	auto observed = max_concurrent_reads.load();
	while (in_flight > observed && !max_concurrent_reads.compare_exchange_weak(observed, in_flight)) {
	}
}

void DebugFileSystem::ReadFinished() {
	--reads_in_flight;
}

unique_ptr<FileHandle> DebugFileSystem::OpenFileExtended(const OpenFileInfo &file, FileOpenFlags flags,
                                                         optional_ptr<FileOpener> opener) {
	ApplyDelay();
	auto inner_handle = inner_fs->OpenFile(file, flags, opener);
	if (!inner_handle) {
		return nullptr;
	}
	return make_uniq<DebugFileHandle>(*this, std::move(inner_handle));
}

void DebugFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	ReadStarted();
	ApplyDelay();
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	inner.file_system.Read(inner, buffer, nr_bytes, location);
	ReadFinished();
}

bool DebugFileSystem::TryStartRead(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location,
                                   AsyncIOCallback callback) {
#ifdef DUCKDB_NO_THREADS
	return false;
#else
	if (!async_reads) {
		return false;
	}
	if (!async_queue) {
		const annotated_lock_guard<annotated_mutex> guard(random_engine_lock);
		if (!async_queue) {
			async_queue = make_uniq<DebugAsyncReadQueue>();
		}
	}
	auto delay_ms = SampleDelayMs();
	ReadStarted();
	async_read_count++;
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	// the injected latency is served by the completion thread, so the caller is free while it elapses
	async_queue->Push(PendingDebugRead {&inner, buffer, nr_bytes, location,
	                                    [this, callback](optional_ptr<ErrorData> error) {
		                                    ReadFinished();
		                                    callback(error);
	                                    },
	                                    async_queue->ElapsedMs() + delay_ms});
	return true;
#endif
}

int64_t DebugFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	ApplyDelay();
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.Read(inner, buffer, nr_bytes);
}

void DebugFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	ApplyDelay();
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	inner.file_system.Write(inner, buffer, nr_bytes, location);
}

int64_t DebugFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	ApplyDelay();
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.Write(inner, buffer, nr_bytes);
}

string DebugFileSystem::GetName() const {
	return inner_fs->GetName();
}

bool DebugFileSystem::IsLocalFileSystem() const {
	return inner_fs->IsLocalFileSystem();
}

int64_t DebugFileSystem::GetFileSize(FileHandle &handle) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.GetFileSize(inner);
}

timestamp_t DebugFileSystem::GetLastModifiedTime(FileHandle &handle) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.GetLastModifiedTime(inner);
}

string DebugFileSystem::GetVersionTag(FileHandle &handle) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.GetVersionTag(inner);
}

FileType DebugFileSystem::GetFileType(FileHandle &handle) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.GetFileType(inner);
}

FileMetadata DebugFileSystem::Stats(FileHandle &handle) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.Stats(inner);
}

void DebugFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	inner.file_system.Truncate(inner, new_size);
}

void DebugFileSystem::FileSync(FileHandle &handle) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	inner.file_system.FileSync(inner);
}

void DebugFileSystem::AbortFileWrite(FileHandle &handle) {
	handle.Cast<DebugFileHandle>().inner->AbortWrite();
}

void DebugFileSystem::Seek(FileHandle &handle, idx_t location) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	inner.file_system.Seek(inner, location);
}

void DebugFileSystem::Reset(FileHandle &handle) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	inner.file_system.Reset(inner);
}

idx_t DebugFileSystem::SeekPosition(FileHandle &handle) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.SeekPosition(inner);
}

FileWriteMode DebugFileSystem::GetWriteMode(FileHandle &handle) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.GetWriteMode(inner);
}

bool DebugFileSystem::OnDiskFile(FileHandle &handle) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.OnDiskFile(inner);
}

bool DebugFileSystem::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.Trim(inner, offset_bytes, length_bytes);
}

bool DebugFileSystem::TryGetNetworkThroughput(FileHandle &handle, NetworkThroughputEstimate &result) {
	auto &inner = *handle.Cast<DebugFileHandle>().inner;
	return inner.file_system.TryGetNetworkThroughput(inner, result);
}

unique_ptr<FileHandle> DebugFileSystem::OpenCompressedFile(QueryContext context, unique_ptr<FileHandle> handle,
                                                           bool write) {
	auto &debug_handle = handle->Cast<DebugFileHandle>();
	auto &inner_fs_ref = debug_handle.inner->file_system;
	auto compressed = inner_fs_ref.OpenCompressedFile(context, std::move(debug_handle.inner), write);
	return make_uniq<DebugFileHandle>(*this, std::move(compressed));
}

bool DebugFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	return inner_fs->DirectoryExists(directory, opener);
}

void DebugFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	CreateDirectoryExtended(directory, {CreateDirectoryMode::SINGLE}, opener);
}

bool DebugFileSystem::CreateDirectoryExtended(const string &directory, const CreateDirectoryOptions &options,
                                              optional_ptr<FileOpener> opener) {
	return inner_fs->CreateDirectoryExtended(directory, options, opener);
}

void DebugFileSystem::CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener) {
	CreateDirectoryExtended(path, {CreateDirectoryMode::RECURSIVE}, opener);
}

void DebugFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	RemoveDirectoryExtended(directory, {RemoveDirectoryMode::RECURSIVE}, opener);
}

bool DebugFileSystem::RemoveDirectoryExtended(const string &directory, const RemoveDirectoryOptions &options,
                                              optional_ptr<FileOpener> opener) {
	return inner_fs->RemoveDirectoryExtended(directory, options, opener);
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

void DebugFileSystem::RegisterCompressionFilesystem(unique_ptr<CompressedFileSystem> fs) {
	inner_fs->RegisterCompressionFilesystem(std::move(fs));
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
