#include "duckdb/storage/external_file_cache/caching_file_system.hpp"

#include "duckdb/common/checksum.hpp"
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
					file_handle.Read(context, buf.GetDataMutable(), to_read, offset);

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
	return make_uniq<CachingFileHandle>(QueryContext(), *this, path, flags, opener,
	                                    external_file_cache.GetOrCreateCachedFile(path.path));
}

unique_ptr<CachingFileHandle> CachingFileSystem::OpenFile(QueryContext context, const OpenFileInfo &path,
                                                          FileOpenFlags flags, optional_ptr<FileOpener> opener) {
	return make_uniq<CachingFileHandle>(context, *this, path, flags, opener,
	                                    external_file_cache.GetOrCreateCachedFile(path.path));
}

//===----------------------------------------------------------------------===//
// CachingFileHandle
//===----------------------------------------------------------------------===//

CachingFileHandle::CachingFileHandle(QueryContext context, CachingFileSystem &caching_file_system_p,
                                     const OpenFileInfo &path_p, FileOpenFlags flags_p,
                                     optional_ptr<FileOpener> opener_p, CachedFile &cached_file_p)
    : context(context), caching_file_system(caching_file_system_p),
      external_file_cache(caching_file_system.external_file_cache), path(path_p), flags(flags_p), opener(opener_p),
      validate(
          ExternalFileCacheUtil::GetCacheValidationMode(path_p, context.GetClientContext(), caching_file_system_p.db)),
      cached_file(cached_file_p), position(0) {
	if (!external_file_cache.IsEnabled() || Validate()) {
		// If caching is disabled, or if we must validate cache entries, we always have to open the file
		GetFileHandle();
		return;
	}
	// If we don't have any cached blocks, we must also open the file.
	bool needs_open = false;
	{
		annotated_lock_guard<annotated_mutex> guard(cached_file.map_lock);
		needs_open = cached_file.blocks.empty();
	}
	if (needs_open) {
		GetFileHandle();
	}
}

CachingFileHandle::~CachingFileHandle() {
}

FileHandle &CachingFileHandle::GetFileHandle() {
	annotated_lock_guard<annotated_mutex> guard(file_handle_mutex);
	if (file_handle) {
		return *file_handle;
	}

	file_handle = caching_file_system.file_system.OpenFile(path, flags, opener);
	last_modified = caching_file_system.file_system.GetLastModifiedTime(*file_handle);
	version_tag = caching_file_system.file_system.GetVersionTag(*file_handle);

	{
		annotated_lock_guard<annotated_mutex> meta_guard(cached_file.meta_lock);
		const bool first_access = (cached_file.file_size == 0);
		if (first_access || Validate()) {
			if (!ExternalFileCache::IsValid(Validate(), cached_file.version_tag, cached_file.last_modified, version_tag,
			                                last_modified)) {
				annotated_lock_guard<annotated_mutex> map_guard(cached_file.map_lock);
				cached_file.blocks.clear();
				cached_file.cached_block_size.SetInvalid();
			}
			cached_file.file_size = file_handle->GetFileSize();
			cached_file.last_modified = last_modified;
			cached_file.version_tag = version_tag;
			cached_file.can_seek = file_handle->CanSeek();
			cached_file.on_disk_file = file_handle->OnDiskFile();
		}
	}
	return *file_handle;
}

FileBufferHandleGroup CachingFileHandle::Read(const idx_t nr_bytes, const idx_t location) {
	if (nr_bytes == 0) {
		return FileBufferHandleGroup();
	}

	if (!external_file_cache.IsEnabled()) {
		auto buf = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
		GetFileHandle().Read(context, buf.GetDataMutable(), nr_bytes, location);
		vector<FileBufferHandleGroup::MemoryHandle> mem_handles;
		mem_handles.push_back({std::move(buf), 0, nr_bytes});
		return FileBufferHandleGroup(std::move(mem_handles));
	}

	const idx_t block_size = external_file_cache.GetCacheBlockSize(cached_file.path);
	const idx_t first_block = location / block_size;
	const idx_t last_block = (location + nr_bytes - 1) / block_size;
	const idx_t num_blocks = last_block - first_block + 1;

	// Atomically reindex (if needed) and acquire the block range.
	auto blocks = external_file_cache.ReindexAndAcquireBlocks(cached_file, block_size, first_block, num_blocks);

	// Schedule block fetch tasks for all blocks.
	vector<BufferHandle> pins(num_blocks);
	auto &scheduler = TaskScheduler::GetScheduler(caching_file_system.db);
	TaskExecutor executor(scheduler);

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
		const annotated_lock_guard<annotated_mutex> meta_guard(cached_file.meta_lock);
		if (!ExternalFileCache::IsValid(true, cached_file.version_tag, cached_file.last_modified, version_tag,
		                                last_modified)) {
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
	return cached_file.path;
}

idx_t CachingFileHandle::GetFileSize() {
	if (!Validate()) {
		annotated_lock_guard<annotated_mutex> guard(cached_file.meta_lock);
		return cached_file.file_size;
	}
	return GetFileHandle().GetFileSize();
}

timestamp_t CachingFileHandle::GetLastModifiedTime() {
	if (!Validate()) {
		annotated_lock_guard<annotated_mutex> guard(cached_file.meta_lock);
		return cached_file.last_modified;
	}
	GetFileHandle();
	return last_modified;
}

string CachingFileHandle::GetVersionTag() {
	if (!Validate()) {
		annotated_lock_guard<annotated_mutex> guard(cached_file.meta_lock);
		return cached_file.version_tag;
	}
	GetFileHandle();
	return version_tag;
}

bool CachingFileHandle::Validate() const {
	switch (validate) {
	case CacheValidationMode::VALIDATE_ALL:
		return true;
	case CacheValidationMode::VALIDATE_REMOTE:
		return FileSystem::IsRemoteFile(cached_file.path);
	case CacheValidationMode::NO_VALIDATION:
		return false;
	default:
		return true;
	}
}

bool CachingFileHandle::CanSeek() {
	if (!Validate()) {
		annotated_lock_guard<annotated_mutex> guard(cached_file.meta_lock);
		return cached_file.can_seek;
	}
	return GetFileHandle().CanSeek();
}

bool CachingFileHandle::IsRemoteFile() const {
	return FileSystem::IsRemoteFile(cached_file.path);
}

bool CachingFileHandle::OnDiskFile() {
	if (!Validate()) {
		annotated_lock_guard<annotated_mutex> guard(cached_file.meta_lock);
		return cached_file.on_disk_file;
	}
	return GetFileHandle().OnDiskFile();
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
