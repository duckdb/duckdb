#include "duckdb/storage/external_file_cache/caching_file_system.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/enums/cache_validation_mode.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/external_file_cache/external_file_cache.hpp"
#include "duckdb/storage/external_file_cache/external_file_cache_util.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;

//===----------------------------------------------------------------------===//
// FetchBlockTask
//===----------------------------------------------------------------------===//

class FetchBlockTask : public BaseExecutorTask {
public:
	FetchBlockTask(CachingFileHandle &caching_file_handle_p, TaskExecutor &executor, QueryContext context_p,
	               BufferManager &buffer_manager_p, shared_ptr<CacheBlock> block_p, idx_t block_idx_p,
	               idx_t block_size_p, BufferHandle &result_pin_p)
	    : BaseExecutorTask(executor), caching_file_handle(caching_file_handle_p), context(context_p),
	      buffer_manager(buffer_manager_p), block(std::move(block_p)), block_idx(block_idx_p), block_size(block_size_p),
	      result_pin(result_pin_p) {
	}

	void ExecuteTask() override {
		annotated_unique_lock<annotated_mutex> lk(block->mtx);

		while (true) {
			switch (block->state) {
			case CacheBlockState::LOADED: {
				auto pin = buffer_manager.Pin(block->block_handle);
				if (pin.IsValid()) {
#ifdef DEBUG
					D_ASSERT(Checksum(pin.Ptr(), block->nr_bytes) == block->checksum);
#endif
					result_pin = std::move(pin);
					return;
				}
				// Evicted by buffer manager, need to re-fetch
				block->state = CacheBlockState::EMPTY;
				continue;
			}
			case CacheBlockState::EMPTY: {
				block->state = CacheBlockState::LOADING;
				lk.unlock();

				try {
					auto &file_handle = caching_file_handle.GetFileHandle();
					const idx_t file_size = file_handle.GetFileSize();
					const idx_t offset = block_idx * block_size;
					if (offset >= file_size) {
						lk.lock();
						// If there're other workers waiting for this block, we need to reset the block to empty state
						// for another attempt.
						block->state = CacheBlockState::EMPTY;
						block->cv.notify_all();
						return;
					}
					const idx_t to_read = MinValue(block_size, file_size - offset);
					auto buf = buffer_manager.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, to_read);
					caching_file_handle.ReadAndRecord(context, buf.GetDataMutable(), to_read, offset);

					lk.lock();
					block->block_handle = buf.GetBlockHandle();
					block->nr_bytes = to_read;
					block->state = CacheBlockState::LOADED;
#ifdef DEBUG
					block->checksum = Checksum(buf.Ptr(), to_read);
#endif
					result_pin = std::move(buf);
					block->cv.notify_all();
				} catch (std::exception &e) {
					lk.lock();
					block->state = CacheBlockState::IO_ERROR;
					block->cv.notify_all();
					throw;
				}
				return;
			}
			case CacheBlockState::LOADING: {
				block->cv.wait(lk,
				               [&]() DUCKDB_REQUIRES(block->mtx) { return block->state != CacheBlockState::LOADING; });
				continue;
			}
			case CacheBlockState::IO_ERROR: {
				// IO operation failed, reset the block to empty state for another attempt.
				block->state = CacheBlockState::EMPTY;
				continue;
			}
			}
		}
	}

private:
	CachingFileHandle &caching_file_handle;
	QueryContext context;
	BufferManager &buffer_manager;
	shared_ptr<CacheBlock> block;
	idx_t block_idx;
	idx_t block_size;
	BufferHandle &result_pin;
};

//===----------------------------------------------------------------------===//
// CachingFileSystem
//===----------------------------------------------------------------------===//

CachingFileSystem::CachingFileSystem(FileSystem &file_system_p, DatabaseInstance &db_p)
    : file_system(file_system_p), db(db_p), external_file_cache(ExternalFileCache::Get(db)) {
}

CachingFileSystem::~CachingFileSystem() {
}

CachingFileSystem CachingFileSystem::Get(ClientContext &context) {
	return CachingFileSystem(FileSystem::GetFileSystem(context), *context.db);
}

unique_ptr<CachingFileHandle> CachingFileSystem::OpenFile(const OpenFileInfo &path, FileOpenFlags flags,
                                                          optional_ptr<FileOpener> opener) {
	return make_uniq<CachingFileHandle>(QueryContext(), *this, path, flags, opener);
}

unique_ptr<CachingFileHandle> CachingFileSystem::OpenFile(QueryContext context, const OpenFileInfo &path,
                                                          FileOpenFlags flags, optional_ptr<FileOpener> opener) {
	return make_uniq<CachingFileHandle>(context, *this, path, flags, opener);
}

//===----------------------------------------------------------------------===//
// CachingFileHandle
//===----------------------------------------------------------------------===//

bool CachingFileHandle::StripForceFullDownloadIfPresent() {
	auto &extended_info_p = path.extended_info;
	if (!extended_info_p) {
		return false;
	}

	auto &extended_info = *extended_info_p;
	const bool contains_force_full_download = extended_info.options.count("force_full_download");
	if (!contains_force_full_download) {
		return false;
	}

	//! We do have 'force_full_download' - strip it
	auto new_extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	*new_extended_info = extended_info;
	new_extended_info->options.erase("force_full_download");
	if (!new_extended_info->options.count("file_size")) {
		new_extended_info->options["file_size"] = Value::UBIGINT(GetFileSize());
	}
	path.extended_info = new_extended_info;
	return true;
}

shared_ptr<CachingFileHandle::CachedFile> CachingFileHandle::EnsureCachedFileCurrent() {
	bool needs_reopen = false;
	{
		annotated_lock_guard<annotated_mutex> guard(file_handle_mutex);
		if (cached_file && cached_file->generation == external_file_cache.GetGeneration()) {
			return cached_file;
		}
		needs_reopen = file_handle != nullptr;
		if (needs_reopen) {
			file_handle.reset();
		}
		cached_file = external_file_cache.GetOrCreateCachedFile(path.path);
	}

	if (needs_reopen) {
		GetFileHandle();
	}
	return cached_file;
}

CachingFileHandle::CachingFileHandle(QueryContext context, CachingFileSystem &caching_file_system_p,
                                     const OpenFileInfo &path_p, FileOpenFlags flags_p,
                                     optional_ptr<FileOpener> opener_p)
    : context(context), caching_file_system(caching_file_system_p),
      external_file_cache(caching_file_system.external_file_cache), path(path_p), flags(flags_p), opener(opener_p),
      validate(
          ExternalFileCacheUtil::GetCacheValidationMode(path_p, context.GetClientContext(), caching_file_system_p.db)),
      cached_file(nullptr), position(0) {
	cached_file = external_file_cache.GetOrCreateCachedFile(path_p.path);
	if (!external_file_cache.IsEnabled() || Validate()) {
		// If caching is disabled, or if we must validate cache entries, we always have to open the file
		GetFileHandle();
		return;
	}
	// If we don't have any cached blocks, we must also open the file.
	bool needs_open = false;
	{
		annotated_lock_guard<annotated_mutex> guard(cached_file->map_lock);
		needs_open = cached_file->blocks.empty();
	}
	if (needs_open) {
		GetFileHandle();
	}
	auto needs_full_download = StripForceFullDownloadIfPresent();
	if (needs_full_download) {
		Read(GetFileSize(), 0);
	}
}

CachingFileHandle::~CachingFileHandle() {
}

FileHandle &CachingFileHandle::GetFileHandle() {
	annotated_lock_guard<annotated_mutex> guard(file_handle_mutex);
	if (file_handle) {
		return *file_handle;
	}

	// The caching file handle can service parallel block fetch tasks against a single shared FileHandle.
	// Request parallel access on the underlying filesystem handle to avoid races in implementations that
	// require explicit opt-in for concurrent pread-style access (e.g., HTTPFS).
	auto internal_flags = flags | FileFlags::FILE_FLAGS_PARALLEL_ACCESS;
	file_handle = caching_file_system.file_system.OpenFile(path, internal_flags, opener);
	last_modified = caching_file_system.file_system.GetLastModifiedTime(*file_handle);
	version_tag = caching_file_system.file_system.GetVersionTag(*file_handle);

	{
		annotated_lock_guard<annotated_mutex> meta_guard(cached_file->meta_lock);
		const bool first_access = (cached_file->file_size == 0);
		if (first_access || Validate()) {
			if (!ExternalFileCache::IsValid(Validate(), cached_file->version_tag, cached_file->last_modified,
			                                version_tag, last_modified)) {
				annotated_lock_guard<annotated_mutex> map_guard(cached_file->map_lock);
				cached_file->blocks.clear();
				cached_file->cached_block_size.SetInvalid();
			}
			cached_file->file_size = file_handle->GetFileSize();
			cached_file->last_modified = last_modified;
			cached_file->version_tag = version_tag;
			cached_file->can_seek = file_handle->CanSeek();
			cached_file->on_disk_file = file_handle->OnDiskFile();
		}
	}
	return *file_handle;
}

Allocator &CachingFileHandle::GetBufferAllocator() const {
	return external_file_cache.GetBufferManager().GetBufferAllocator();
}

FileBufferHandleGroup CachingFileHandle::Read(const idx_t nr_bytes, const idx_t location) {
	if (nr_bytes == 0) {
		return FileBufferHandleGroup();
	}

	// Only cache when file metadata is available.
	const bool no_validation_metadata =
	    Validate() && version_tag.empty() && (!last_modified.IsFinite() || last_modified == timestamp_t(0));

	if (!external_file_cache.IsEnabled() || !external_file_cache.ShouldCacheFile(path.path) || no_validation_metadata) {
		auto buf = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
		ReadAndRecord(context, buf.GetDataMutable(), nr_bytes, location);
		vector<FileBufferHandleGroup::MemoryHandle> mem_handles;
		mem_handles.push_back({std::move(buf), 0, nr_bytes});
		return FileBufferHandleGroup(std::move(mem_handles));
	}

	auto current_cached_file = EnsureCachedFileCurrent();
	const idx_t block_size = external_file_cache.GetCacheBlockSize(current_cached_file->path);
	const idx_t first_block = location / block_size;
	const idx_t last_block = (location + nr_bytes - 1) / block_size;
	const idx_t num_blocks = last_block - first_block + 1;

	// Atomically reindex (if needed) and acquire the block range.
	auto blocks =
	    external_file_cache.ReindexAndAcquireBlocks(*current_cached_file, block_size, first_block, num_blocks);

	// Schedule block fetch tasks for all blocks.
	vector<BufferHandle> pins(num_blocks);
	auto &scheduler = TaskScheduler::GetScheduler(caching_file_system.db);
	TaskExecutor executor(scheduler, TaskSchedulerType::ASYNC);

	for (idx_t idx = 0; idx < num_blocks; idx++) {
		executor.ScheduleTask(make_uniq<FetchBlockTask>(*this, executor, context,
		                                                external_file_cache.GetBufferManager(), blocks[idx],
		                                                first_block + idx, block_size, pins[idx]));
	}
	executor.WorkOnTasks();

	// Build the handle group.
	vector<FileBufferHandleGroup::MemoryHandle> mem_handles;
	mem_handles.reserve(num_blocks);
	idx_t remaining = nr_bytes;
	for (idx_t idx = 0; idx < num_blocks; idx++) {
		const idx_t block_start = (first_block + idx) * block_size;
		const idx_t offset_in_block = (idx == 0) ? (location - block_start) : 0;
		idx_t block_valid_bytes = 0;
		{
			auto &block = *blocks[idx];
			annotated_lock_guard<annotated_mutex> block_guard(block.mtx);
			block_valid_bytes = block.nr_bytes;
		}
		const idx_t available_in_block =
		    (block_valid_bytes > offset_in_block) ? (block_valid_bytes - offset_in_block) : 0;
		const idx_t length = MinValue(available_in_block, remaining);
		mem_handles.push_back({std::move(pins[idx]), offset_in_block, length});
		remaining -= length;
	}

	// After all tasks complete, check if the cache was invalidated by another thread.
	if (Validate()) {
		const annotated_lock_guard<annotated_mutex> meta_guard(current_cached_file->meta_lock);
		if (!ExternalFileCache::IsValid(true, current_cached_file->version_tag, current_cached_file->last_modified,
		                                version_tag, last_modified)) {
			for (auto &block : blocks) {
				block->Reinit();
			}
		}
	}

	return FileBufferHandleGroup(std::move(mem_handles));
}

FileBufferHandleGroup CachingFileHandle::Read(idx_t &nr_bytes) {
	if (!external_file_cache.IsEnabled() || !CanSeek()) {
		auto buf = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
		nr_bytes = NumericCast<idx_t>(GetFileHandle().Read(context, buf.GetDataMutable(), nr_bytes));
		vector<FileBufferHandleGroup::MemoryHandle> mem_handles;
		mem_handles.push_back({std::move(buf), 0, nr_bytes});
		position += nr_bytes;
		return FileBufferHandleGroup(std::move(mem_handles));
	}

	const idx_t file_size = GetFileSize();
	if (position >= file_size) {
		nr_bytes = 0;
		return {};
	}

	nr_bytes = MinValue(nr_bytes, file_size - position);
	auto group = Read(nr_bytes, position);
	position += nr_bytes;
	return group;
}

string CachingFileHandle::GetPath() const {
	return path.path;
}

idx_t CachingFileHandle::GetFileSize() {
	if (!Validate()) {
		auto current_cached_file = EnsureCachedFileCurrent();
		annotated_lock_guard<annotated_mutex> guard(current_cached_file->meta_lock);
		return current_cached_file->file_size;
	}
	return GetFileHandle().GetFileSize();
}

timestamp_t CachingFileHandle::GetLastModifiedTime() {
	if (!Validate()) {
		auto current_cached_file = EnsureCachedFileCurrent();
		annotated_lock_guard<annotated_mutex> guard(current_cached_file->meta_lock);
		return current_cached_file->last_modified;
	}
	GetFileHandle();
	return last_modified;
}

string CachingFileHandle::GetVersionTag() {
	if (!Validate()) {
		auto current_cached_file = EnsureCachedFileCurrent();
		annotated_lock_guard<annotated_mutex> guard(current_cached_file->meta_lock);
		return current_cached_file->version_tag;
	}
	GetFileHandle();
	return version_tag;
}

bool CachingFileHandle::Validate() const {
	if (!external_file_cache.ShouldCacheFile(path.path)) {
		// uncached files have no stale state to serve, so their metadata must always be read fresh
		return true;
	}
	switch (validate) {
	case CacheValidationMode::VALIDATE_ALL:
		return true;
	case CacheValidationMode::VALIDATE_REMOTE:
		return FileSystem::IsRemoteFile(path.path);
	case CacheValidationMode::NO_VALIDATION:
		return false;
	default:
		return true;
	}
}

bool CachingFileHandle::CanSeek() {
	if (!Validate()) {
		auto current_cached_file = EnsureCachedFileCurrent();
		annotated_lock_guard<annotated_mutex> guard(current_cached_file->meta_lock);
		return current_cached_file->can_seek;
	}
	return GetFileHandle().CanSeek();
}

bool CachingFileHandle::IsRemoteFile() const {
	return FileSystem::IsRemoteFile(path.path);
}

bool CachingFileHandle::OnDiskFile() {
	if (!Validate()) {
		auto current_cached_file = EnsureCachedFileCurrent();
		annotated_lock_guard<annotated_mutex> guard(current_cached_file->meta_lock);
		return current_cached_file->on_disk_file;
	}
	return GetFileHandle().OnDiskFile();
}

void ReadThroughputEstimator::AddSample(double seconds, idx_t bytes) {
	const double b = static_cast<double>(bytes);
	lock_guard<mutex> guard(lock);
	sample_count++;
	sum_bytes += b;
	sum_seconds += seconds;
	sum_bytes_sq += b * b;
	sum_bytes_seconds += b * seconds;
}

bool ReadThroughputEstimator::TryEstimate(NetworkThroughputEstimate &result) const {
	lock_guard<mutex> guard(lock);
	if (sample_count < 2) {
		return false;
	}
	const double n = static_cast<double>(sample_count);
	const double variance = sum_bytes_sq - sum_bytes * sum_bytes / n;
	if (!(variance > 0)) {
		// every read was the same size, so we cant really get the latency
		return false;
	}
	// least squares estimate of the line "total_time = intercept + slope * bytes".
	const double slope = (sum_bytes_seconds - sum_bytes * sum_seconds / n) / variance;
	const double intercept = (sum_seconds - slope * sum_bytes) / n;
	if (!(slope > 0) || !(intercept > 0)) {
		return false;
	}
	// intercept is the fixed start-up cost of a read, slope is seconds per byte so bandwidth is 1 / slope
	result.latency_seconds = intercept;
	result.bandwidth_bytes_per_s = 1.0 / slope;
	return true;
}

bool CachingFileHandle::TryGetNetworkThroughput(NetworkThroughputEstimate &result) {
	// Remote files measure throughput in their own file system; local files fit it from this handle's own reads.
	if (GetFileHandle().TryGetNetworkThroughput(result)) {
		return true;
	}
	return throughput_estimator.TryEstimate(result);
}

void CachingFileHandle::ReadAndRecord(QueryContext context, data_ptr_t buffer, idx_t nr_bytes, idx_t location) {
	auto &handle = GetFileHandle();
	const auto read_start = steady_clock::now();
	handle.Read(context, buffer, nr_bytes, location);
	RecordReadThroughput(duration<double>(steady_clock::now() - read_start).count(), nr_bytes);
}

void CachingFileHandle::RecordReadThroughput(double total_seconds, idx_t bytes) {
	if (IsRemoteFile() || !(total_seconds > 0)) {
		return;
	}
	throughput_estimator.AddSample(total_seconds, bytes);
}

idx_t CachingFileHandle::SeekPosition() {
	return position;
}

void CachingFileHandle::Seek(idx_t location) {
	position = location;
	if (file_handle != nullptr) {
		file_handle->Seek(location);
	}
}

} // namespace duckdb
