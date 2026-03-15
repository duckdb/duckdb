#include "duckdb/storage/caching_file_system.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/enums/cache_validation_mode.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include "duckdb/storage/external_file_cache_util.hpp"

namespace duckdb {

// Forward declaration.
class DatabaseInstance;

namespace {

// Return whether validation should occur for a specific file
bool ShouldValidate(const OpenFileInfo &info, optional_ptr<ClientContext> client_context, DatabaseInstance &db,
                    const string &filepath) {
	const CacheValidationMode mode = ExternalFileCacheUtil::GetCacheValidationMode(info, client_context, db);
	switch (mode) {
	case CacheValidationMode::VALIDATE_ALL:
		return true;
	case CacheValidationMode::VALIDATE_REMOTE:
		return FileSystem::IsRemoteFile(filepath);
	case CacheValidationMode::NO_VALIDATION:
		return false;
	default:
		return true;
	}
}

} // namespace

//===----------------------------------------------------------------------===//
// FetchBlockTask
//===----------------------------------------------------------------------===//

class FetchBlockTask : public BaseExecutorTask {
public:
	FetchBlockTask(TaskExecutor &executor, FileHandle &file_handle_p, QueryContext context_p,
	               BufferManager &buffer_manager_p, shared_ptr<CacheBlock> block_p, idx_t block_idx_p,
	               idx_t file_size_p, BufferHandle &result_pin_p)
	    : BaseExecutorTask(executor), file_handle(file_handle_p), context(context_p), buffer_manager(buffer_manager_p),
	      block(std::move(block_p)), block_idx(block_idx_p), file_size(file_size_p), result_pin(result_pin_p) {
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
					const idx_t offset = block_idx * ExternalFileCache::CACHE_BLOCK_SIZE;
					const idx_t to_read = MinValue(ExternalFileCache::CACHE_BLOCK_SIZE, file_size - offset);
					auto buf = buffer_manager.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, to_read);
					file_handle.Read(context, buf.Ptr(), to_read, offset);

				lk.lock();
				block->block_handle = buf.GetBlockHandle();
				block->state = CacheBlockState::LOADED;
#ifdef DEBUG
				block->nr_bytes = to_read;
				block->checksum = Checksum(buf.Ptr(), to_read);
#endif
				result_pin = std::move(buf);
				block->cv.notify_all();
				} catch (std::exception &e) {
					lk.lock();
					block->state = CacheBlockState::ERROR;
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
			case CacheBlockState::ERROR: {
				// IO operation failed, reset the block to empty state for another attempt.
				block->state = CacheBlockState::EMPTY;
				continue;
			}
			}
		}
	}

private:
	FileHandle &file_handle;
	QueryContext context;
	BufferManager &buffer_manager;
	shared_ptr<CacheBlock> block;
	idx_t block_idx;
	idx_t file_size;
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
		GetFileHandle().Read(context, buf.Ptr(), nr_bytes, location);
		vector<FileBufferHandleGroup::MemoryHandle> mem_handles;
		mem_handles.push_back({std::move(buf), 0, nr_bytes});
		return FileBufferHandleGroup(std::move(mem_handles));
	}

	// Ensure the file is open so metadata is set before blocks are visible to other threads
	auto &fh = GetFileHandle();
	const idx_t file_size = fh.GetFileSize();

	const idx_t block_size = ExternalFileCache::CACHE_BLOCK_SIZE;
	const idx_t first_block = location / block_size;
	const idx_t last_block = (location + nr_bytes - 1) / block_size;
	const idx_t num_blocks = last_block - first_block + 1;

	vector<shared_ptr<CacheBlock>> blocks(num_blocks);
	{
		annotated_lock_guard<annotated_mutex> guard(cached_file.map_lock);
		for (idx_t idx = 0; idx < num_blocks; idx++) {
			const idx_t block_idx = first_block + idx;
			auto &entry = cached_file.blocks[block_idx];
			if (!entry) {
				entry = make_shared_ptr<CacheBlock>();
			}
			blocks[idx] = entry;
		}
	}

	// Schedule one FetchBlockTask per block
	vector<BufferHandle> pins(num_blocks);
	auto &scheduler = TaskScheduler::GetScheduler(caching_file_system.db);
	TaskExecutor executor(scheduler);

	for (idx_t idx = 0; idx < num_blocks; idx++) {
		executor.ScheduleTask(make_uniq<FetchBlockTask>(executor, fh, context, external_file_cache.GetBufferManager(),
		                                                blocks[idx], first_block + idx, file_size, pins[idx]));
	}
	executor.WorkOnTasks();

	// Build the handle group.
	vector<FileBufferHandleGroup::MemoryHandle> mem_handles;
	mem_handles.reserve(num_blocks);
	idx_t remaining = nr_bytes;
	for (idx_t idx = 0; idx < num_blocks; idx++) {
		const idx_t block_start = (first_block + idx) * block_size;
		const idx_t offset_in_block = (idx == 0) ? (location - block_start) : 0;
		const idx_t length = MinValue(block_size - offset_in_block, remaining);
		mem_handles.push_back({std::move(pins[idx]), offset_in_block, length});
		remaining -= length;
	}

	return FileBufferHandleGroup(std::move(mem_handles));
}

FileBufferHandleGroup CachingFileHandle::Read(idx_t &nr_bytes) {
	if (!external_file_cache.IsEnabled() || !CanSeek()) {
		auto buf = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
		nr_bytes = NumericCast<idx_t>(GetFileHandle().Read(context, buf.Ptr(), nr_bytes));
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
	if (file_handle || Validate()) {
		return GetFileHandle().GetFileSize();
	}
	annotated_lock_guard<annotated_mutex> guard(cached_file.meta_lock);
	return cached_file.file_size;
}

timestamp_t CachingFileHandle::GetLastModifiedTime() {
	if (file_handle || Validate()) {
		GetFileHandle();
		return last_modified;
	}
	annotated_lock_guard<annotated_mutex> guard(cached_file.meta_lock);
	return cached_file.last_modified;
}

string CachingFileHandle::GetVersionTag() {
	if (file_handle || Validate()) {
		GetFileHandle();
		return version_tag;
	}
	annotated_lock_guard<annotated_mutex> guard(cached_file.meta_lock);
	return cached_file.version_tag;
}

bool CachingFileHandle::Validate() const {
	return ShouldValidate(path, context.GetClientContext(), caching_file_system.db, cached_file.path);
}

bool CachingFileHandle::CanSeek() {
	if (file_handle || Validate()) {
		return GetFileHandle().CanSeek();
	}
	annotated_lock_guard<annotated_mutex> guard(cached_file.meta_lock);
	return cached_file.can_seek;
}

bool CachingFileHandle::IsRemoteFile() const {
	return FileSystem::IsRemoteFile(cached_file.path);
}

bool CachingFileHandle::OnDiskFile() {
	if (file_handle || Validate()) {
		return GetFileHandle().OnDiskFile();
	}
	annotated_lock_guard<annotated_mutex> guard(cached_file.meta_lock);
	return cached_file.on_disk_file;
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
