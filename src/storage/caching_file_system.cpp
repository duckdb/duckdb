#include "duckdb/storage/caching_file_system.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/external_file_cache.hpp"

namespace duckdb {

CachingFileSystem::CachingFileSystem(FileSystem &file_system_p, DatabaseInstance &db)
    : file_system(file_system_p), external_file_cache(ExternalFileCache::Get(db)) {
}

CachingFileSystem::~CachingFileSystem() {
}

CachingFileSystem CachingFileSystem::Get(ClientContext &context) {
	return CachingFileSystem(FileSystem::GetFileSystem(context), *context.db);
}

unique_ptr<CachingFileHandle> CachingFileSystem::OpenFile(const OpenFileInfo &path, FileOpenFlags flags) {
	return make_uniq<CachingFileHandle>(QueryContext(), *this, path, flags,
	                                    external_file_cache.GetOrCreateCachedFile(path.path));
}

unique_ptr<CachingFileHandle> CachingFileSystem::OpenFile(QueryContext context, const OpenFileInfo &path,
                                                          FileOpenFlags flags) {
	return make_uniq<CachingFileHandle>(context, *this, path, flags,
	                                    external_file_cache.GetOrCreateCachedFile(path.path));
}

CachingFileHandle::CachingFileHandle(QueryContext context, CachingFileSystem &caching_file_system_p,
                                     const OpenFileInfo &path_p, FileOpenFlags flags_p, CachedFile &cached_file_p)
    : context(context), caching_file_system(caching_file_system_p),
      external_file_cache(caching_file_system.external_file_cache), path(path_p), flags(flags_p), validate(true),
      cached_file(cached_file_p), position(0) {
	if (path.extended_info) {
		const auto &open_options = path.extended_info->options;
		const auto validate_entry = open_options.find("validate_external_file_cache");
		if (validate_entry != open_options.end()) {
			if (validate_entry->second.IsNull()) {
				throw InvalidInputException("Cannot use NULL as argument for validate_external_file_cache");
			}
			validate = BooleanValue::Get(validate_entry->second);
		}
	}
	if (!external_file_cache.IsEnabled() || validate) {
		// If caching is disabled, or if we must validate cache entries, we always have to open the file
		GetFileHandle();
		return;
	}
	// If we don't have any cached file ranges, we must also open the file
	auto guard = cached_file.lock.GetSharedLock();
	if (cached_file.Ranges(guard).empty()) {
		guard.reset();
		GetFileHandle();
	}
}

CachingFileHandle::~CachingFileHandle() {
}

FileHandle &CachingFileHandle::GetFileHandle() {
	if (!file_handle) {
		file_handle = caching_file_system.file_system.OpenFile(path, flags);
		last_modified = caching_file_system.file_system.GetLastModifiedTime(*file_handle);
		version_tag = caching_file_system.file_system.GetVersionTag(*file_handle);

		auto guard = cached_file.lock.GetExclusiveLock();
		if (!cached_file.IsValid(guard, validate, version_tag, last_modified)) {
			cached_file.Ranges(guard).clear(); // Invalidate entire cache
		}
		cached_file.FileSize(guard) = file_handle->GetFileSize();
		cached_file.LastModified(guard) = last_modified;
		cached_file.VersionTag(guard) = version_tag;
		cached_file.CanSeek(guard) = file_handle->CanSeek();
		cached_file.OnDiskFile(guard) = file_handle->OnDiskFile();
	}
	return *file_handle;
}

static bool ShouldExpandToFillGap(const idx_t current_length, const idx_t added_length) {
	const idx_t MAX_BOUND_TO_BE_ADDED_LENGTH = 1048576;

	if (added_length > MAX_BOUND_TO_BE_ADDED_LENGTH) {
		// Absolute value of what would be needed to added is too high
		return false;
	}
	if (added_length > current_length) {
		// Relative value of what would be needed to added is too high
		return false;
	}

	return true;
}

BufferHandle CachingFileHandle::Read(data_ptr_t &buffer, const idx_t nr_bytes, const idx_t location) {
	BufferHandle result;
	if (!external_file_cache.IsEnabled()) {
		result = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
		buffer = result.Ptr();
		GetFileHandle().Read(context, buffer, nr_bytes, location);
		return result;
	}

	// Try to read from the cache, filling overlapping_ranges in the process
	vector<shared_ptr<CachedFileRange>> overlapping_ranges;
	optional_idx start_location_of_next_range;
	result = TryReadFromCache(buffer, nr_bytes, location, overlapping_ranges, start_location_of_next_range);
	if (result.IsValid()) {
		return result; // Success
	}

	idx_t new_nr_bytes = nr_bytes;
	if (start_location_of_next_range.IsValid()) {
		const idx_t nr_bytes_to_be_added = start_location_of_next_range.GetIndex() - location - nr_bytes;
		if (ShouldExpandToFillGap(nr_bytes, nr_bytes_to_be_added)) {
			// Grow the range from location to start_location_of_next_range, so that to fill gaps in the cached ranges
			new_nr_bytes = nr_bytes + nr_bytes_to_be_added;
		}
	}

	// Finally, if we weren't able to find the file range in the cache, we have to create a new file range
	result = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, new_nr_bytes);
	auto new_file_range =
	    make_shared_ptr<CachedFileRange>(result.GetBlockHandle(), new_nr_bytes, location, version_tag);
	buffer = result.Ptr();

	// Interleave reading and copying from cached buffers
	if (OnDiskFile()) {
		// On-disk file: prefer interleaving reading and copying from cached buffers
		ReadAndCopyInterleaved(overlapping_ranges, new_file_range, buffer, new_nr_bytes, location, true);
	} else {
		// Remote file: prefer interleaving reading and copying from cached buffers only if reduces number of real
		// reads
		if (ReadAndCopyInterleaved(overlapping_ranges, new_file_range, buffer, new_nr_bytes, location, false) <= 1) {
			ReadAndCopyInterleaved(overlapping_ranges, new_file_range, buffer, new_nr_bytes, location, true);
		} else {
			GetFileHandle().Read(context, buffer, new_nr_bytes, location);
		}
	}

	return TryInsertFileRange(result, buffer, new_nr_bytes, location, new_file_range);
}

BufferHandle CachingFileHandle::Read(data_ptr_t &buffer, idx_t &nr_bytes) {
	BufferHandle result;

	// If we can't seek, we can't use the cache for these calls,
	// because we won't be able to seek over any parts we skipped by reading from the cache
	if (!external_file_cache.IsEnabled() || !CanSeek()) {
		result = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
		buffer = result.Ptr();
		nr_bytes = NumericCast<idx_t>(GetFileHandle().Read(context, buffer, nr_bytes));
		position += NumericCast<idx_t>(nr_bytes);
		return result;
	}

	// Try to read from the cache first
	vector<shared_ptr<CachedFileRange>> overlapping_ranges;
	{
		optional_idx start_location_of_next_range;
		result = TryReadFromCache(buffer, nr_bytes, position, overlapping_ranges, start_location_of_next_range);
		// start_location_of_next_range is in this case discarded
	}

	if (result.IsValid()) {
		position += nr_bytes;
		return result; // Success
	}

	// Finally, if we weren't able to find the file range in the cache, we have to create a new file range
	result = external_file_cache.GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
	buffer = result.Ptr();

	GetFileHandle().Seek(position);
	nr_bytes = NumericCast<idx_t>(GetFileHandle().Read(context, buffer, nr_bytes));
	auto new_file_range = make_shared_ptr<CachedFileRange>(result.GetBlockHandle(), nr_bytes, position, version_tag);

	result = TryInsertFileRange(result, buffer, nr_bytes, position, new_file_range);
	position += NumericCast<idx_t>(nr_bytes);

	return result;
}

string CachingFileHandle::GetPath() const {
	return cached_file.path;
}

idx_t CachingFileHandle::GetFileSize() {
	if (file_handle || validate) {
		return GetFileHandle().GetFileSize();
	}
	auto guard = cached_file.lock.GetSharedLock();
	return cached_file.FileSize(guard);
}

timestamp_t CachingFileHandle::GetLastModifiedTime() {
	if (file_handle || validate) {
		GetFileHandle();
		return last_modified;
	}
	auto guard = cached_file.lock.GetSharedLock();
	return cached_file.LastModified(guard);
}

string CachingFileHandle::GetVersionTag() {
	if (file_handle || validate) {
		GetFileHandle();
		return version_tag;
	}
	auto guard = cached_file.lock.GetSharedLock();
	return cached_file.VersionTag(guard);
}

bool CachingFileHandle::Validate() const {
	return validate;
}

bool CachingFileHandle::CanSeek() {
	if (file_handle || validate) {
		return GetFileHandle().CanSeek();
	}
	auto guard = cached_file.lock.GetSharedLock();
	return cached_file.CanSeek(guard);
}

bool CachingFileHandle::IsRemoteFile() const {
	return FileSystem::IsRemoteFile(cached_file.path);
}

bool CachingFileHandle::OnDiskFile() {
	if (file_handle || validate) {
		return GetFileHandle().OnDiskFile();
	}
	auto guard = cached_file.lock.GetSharedLock();
	return cached_file.OnDiskFile(guard);
}

const string &CachingFileHandle::GetVersionTag(const unique_ptr<StorageLockKey> &guard) {
	if (file_handle || validate) {
		GetFileHandle();
		return version_tag;
	}
	return cached_file.VersionTag(guard);
}

BufferHandle CachingFileHandle::TryReadFromCache(data_ptr_t &buffer, idx_t nr_bytes, idx_t location,
                                                 vector<shared_ptr<CachedFileRange>> &overlapping_ranges,
                                                 optional_idx &start_location_of_next_range) {
	BufferHandle result;

	// Get read lock for cached ranges
	auto guard = cached_file.lock.GetSharedLock();
	auto &ranges = cached_file.Ranges(guard);

	// First, try to see if we've read from the exact same location before
	auto it = ranges.find(location);
	if (it != ranges.end()) {
		// We have read from the exact same location before
		if (it->second->GetOverlap(nr_bytes, location) == CachedFileRangeOverlap::FULL) {
			// The file range contains the requested file range
			// FIXME: if we ever start persisting this stuff, this read needs to happen outside of the lock
			result = TryReadFromFileRange(guard, *it->second, buffer, nr_bytes, location);
			if (result.IsValid()) {
				return result;
			}
		}
	}

	// Second, loop through file ranges (ordered by location) to see if any contain the requested file range
	const auto this_end = location + nr_bytes;

	// Start at lower_bound (first range with location not less than location of requested range) minus one
	// This works because we don't allow fully overlapping ranges in the files
	it = ranges.lower_bound(location);
	if (it != ranges.begin()) {
		--it;
	}
	while (it != ranges.end()) {
		if (it->second->location >= this_end) {
			// We're past the requested location, we are going to bail out, save start_location_of_next_range
			start_location_of_next_range = it->second->location;
			break;
		}
		// Check if the cached range overlaps the requested one
		switch (it->second->GetOverlap(nr_bytes, location)) {
		case CachedFileRangeOverlap::NONE:
			// No overlap at all
			break;
		case CachedFileRangeOverlap::PARTIAL:
			// Partial overlap, store for potential use later
			overlapping_ranges.push_back(it->second);
			break;
		case CachedFileRangeOverlap::FULL:
			// The file range fully contains the requested file range, if the buffer is still valid we're done
			// FIXME: if we ever start persisting this stuff, this read needs to happen outside of the lock
			result = TryReadFromFileRange(guard, *it->second, buffer, nr_bytes, location);
			if (result.IsValid()) {
				return result;
			}
			break;
		default:
			throw InternalException("Unknown CachedFileRangeOverlap");
		}
		++it;
	}

	return result;
}

BufferHandle CachingFileHandle::TryReadFromFileRange(const unique_ptr<StorageLockKey> &guard,
                                                     CachedFileRange &file_range, data_ptr_t &buffer, idx_t nr_bytes,
                                                     idx_t location) {
	D_ASSERT(file_range.GetOverlap(nr_bytes, location) == CachedFileRangeOverlap::FULL);
	auto result = external_file_cache.GetBufferManager().Pin(file_range.block_handle);
	if (result.IsValid()) {
		buffer = result.Ptr() + (location - file_range.location);
	}
	return result;
}

BufferHandle CachingFileHandle::TryInsertFileRange(BufferHandle &pin, data_ptr_t &buffer, idx_t nr_bytes,
                                                   idx_t location, shared_ptr<CachedFileRange> &new_file_range) {
	// Grab the lock again (write lock this time) to insert the newly created buffer into the ranges
	auto guard = cached_file.lock.GetExclusiveLock();
	auto &ranges = cached_file.Ranges(guard);

	// Start at lower_bound (first range with location not less than location of newly created range)
	const auto this_end = location + nr_bytes;
	auto it = ranges.lower_bound(location);
	if (it != ranges.begin()) {
		--it;
	}
	while (it != ranges.end()) {
		if (it->second->location >= this_end) {
			// We're past the requested location
			break;
		}
		if (it->second->GetOverlap(*new_file_range) == CachedFileRangeOverlap::FULL) {
			// Another thread has read a range that fully contains the requested range in the meantime
			auto other_pin = TryReadFromFileRange(guard, *it->second, buffer, nr_bytes, location);
			if (other_pin.IsValid()) {
				return other_pin;
			}
			it = ranges.erase(it);
			continue;
		}
		// Check if the new range overlaps with a cached one
		switch (new_file_range->GetOverlap(*it->second)) {
		case CachedFileRangeOverlap::NONE:
			break; // No overlap, still useful
		case CachedFileRangeOverlap::PARTIAL:
			break; // The newly created range does not fully contain this range, so it is still useful
		case CachedFileRangeOverlap::FULL:
			// Full overlap, this range will be obsolete when we insert the current one
			// Since we have the write lock here, we can do some cleanup
			it = ranges.erase(it);
			continue;
		default:
			throw InternalException("Unknown CachedFileRangeOverlap");
		}

		++it;
	}
	D_ASSERT(pin.IsValid());

	// Finally, insert newly created buffer into the map
	new_file_range->AddCheckSum();
	ranges[location] = std::move(new_file_range);
	cached_file.Verify(guard);

	return std::move(pin);
}

idx_t CachingFileHandle::ReadAndCopyInterleaved(const vector<shared_ptr<CachedFileRange>> &overlapping_ranges,
                                                const shared_ptr<CachedFileRange> &new_file_range, data_ptr_t buffer,
                                                const idx_t nr_bytes, const idx_t location, const bool actually_read) {
	idx_t non_cached_read_count = 0;

	idx_t current_location = location;
	idx_t remaining_bytes = nr_bytes;
	for (auto &overlapping_range : overlapping_ranges) {
		D_ASSERT(new_file_range->GetOverlap(*overlapping_range) != CachedFileRangeOverlap::NONE);

		if (remaining_bytes == 0) {
			break; // All requested bytes were read
		}

		if (overlapping_range->location > current_location) {
			// We need to read from the file until we're at the location of the current overlapping file range
			const auto buffer_offset = nr_bytes - remaining_bytes;
			const auto bytes_to_read = overlapping_range->location - current_location;
			D_ASSERT(bytes_to_read < remaining_bytes);
			if (actually_read) {
				GetFileHandle().Read(context, buffer + buffer_offset, bytes_to_read, current_location);
			}
			current_location += bytes_to_read;
			remaining_bytes -= bytes_to_read;
			non_cached_read_count++;
		}

		if (overlapping_range->GetOverlap(remaining_bytes, current_location) == CachedFileRangeOverlap::NONE) {
			continue; // Remainder does not overlap with the current overlapping file range
		}

		// Try to pin the current overlapping file range
		auto overlapping_file_range_pin = external_file_cache.GetBufferManager().Pin(overlapping_range->block_handle);
		if (!overlapping_file_range_pin.IsValid()) {
			continue; // No longer valid
		}

		// Finally, we can copy the data over
		D_ASSERT(current_location >= overlapping_range->location);
		const auto buffer_offset = nr_bytes - remaining_bytes;
		const auto overlapping_range_offset = current_location - overlapping_range->location;
		D_ASSERT(overlapping_range->nr_bytes > overlapping_range_offset);
		const auto bytes_to_read = MinValue(overlapping_range->nr_bytes - overlapping_range_offset, remaining_bytes);
		if (actually_read) {
			memcpy(buffer + buffer_offset, overlapping_file_range_pin.Ptr() + overlapping_range_offset, bytes_to_read);
		}
		current_location += bytes_to_read;
		remaining_bytes -= bytes_to_read;
	}

	// Read the remaining bytes (if any)
	if (remaining_bytes != 0) {
		const auto buffer_offset = nr_bytes - remaining_bytes;
		if (actually_read) {
			GetFileHandle().Read(context, buffer + buffer_offset, remaining_bytes, current_location);
		}
		non_cached_read_count++;
	}

	return non_cached_read_count;
}

} // namespace duckdb
