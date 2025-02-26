#include "duckdb/storage/caching_file_system.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

CachingFileSystem::CachedFileRange::CachedFileRange(shared_ptr<BlockHandle> block_handle_p, idx_t nr_bytes_p,
                                                    idx_t location_p, time_t last_modified_p)
    : block_handle(std::move(block_handle_p)), nr_bytes(nr_bytes_p), location(location_p),
      last_modified(last_modified_p) {
}

CachingFileSystem::CachedFileRange::~CachedFileRange() {
	VerifyCheckSum();
}

CachingFileSystem::CachedFileRangeOverlap
CachingFileSystem::CachedFileRange::GetOverlap(const idx_t other_nr_bytes, const idx_t other_location) const {
	const auto this_end = this->location + this->nr_bytes;
	const auto other_end = other_nr_bytes + other_location;
	if (this->location <= other_location && this_end >= other_end) {
		return CachedFileRangeOverlap::FULL;
	}
	if (this->location < other_end && other_location < this_end) {
		return CachedFileRangeOverlap::PARTIAL;
	}
	return CachedFileRangeOverlap::NONE;
}

CachingFileSystem::CachedFileRangeOverlap
CachingFileSystem::CachedFileRange::GetOverlap(const CachedFileRange &other) const {
	return GetOverlap(other.nr_bytes, other.location);
}

void CachingFileSystem::CachedFileRange::AddCheckSum() {
#ifdef DEBUG
	D_ASSERT(checksum == 0);
	auto buffer_handle = block_handle->block_manager.buffer_manager.Pin(block_handle);
	checksum = Hash(buffer_handle.Ptr(), nr_bytes);
#endif
}

void CachingFileSystem::CachedFileRange::VerifyCheckSum() {
#ifdef DEBUG
	if (checksum == 0) {
		return;
	}
	auto buffer_handle = block_handle->block_manager.buffer_manager.Pin(block_handle);
	D_ASSERT(checksum == Hash(buffer_handle.Ptr(), nr_bytes));
#endif
}

CachingFileSystem::CachedFile::CachedFile(string path_p) : path(std::move(path_p)) {
}

void CachingFileSystem::CachedFile::Verify() const {
#ifdef DEBUG
	for (const auto &range1 : ranges) {
		for (const auto &range2 : ranges) {
			if (range1 == range2) {
				continue;
			}
			D_ASSERT(range1.second->GetOverlap(*range2.second) != CachedFileRangeOverlap::FULL);
		}
	}
#endif
}

CachingFileSystem::CachingFileSystem(DatabaseInstance &db, bool enable_p)
    : file_system(FileSystem::GetFileSystem(db)), buffer_manager(BufferManager::GetBufferManager(db)), enable(enable_p),
      check_cached_file_invalidation(true) { // TODO: this defaults to "true" for now
}

void CachingFileSystem::SetEnabled(bool enable_p) {
	lock_guard<mutex> guard(lock);
	enable = enable_p;
	if (!enable) {
		cached_files.clear();
	}
}

vector<CachedFileInformation> CachingFileSystem::GetCachedFileInformation() const {
	lock_guard<mutex> guard(lock);
	vector<CachedFileInformation> result;
	for (const auto &file : cached_files) {
		for (const auto &range_entry : file.second->ranges) {
			const auto &range = *range_entry.second;
			result.push_back(
			    {file.first, range.nr_bytes, range.location, range.last_modified, !range.block_handle->IsUnloaded()});
		}
	}
	return result;
}

CachingFileSystem &CachingFileSystem::Get(DatabaseInstance &db) {
	return db.GetCachingFileSystem();
}

CachingFileSystem &CachingFileSystem::Get(ClientContext &context) {
	return Get(*context.db);
}

CachingFileSystem::CachedFile &CachingFileSystem::GetOrCreateCachedFile(const string &path) {
	lock_guard<mutex> guard(lock);
	auto &entry = cached_files[path];
	if (!entry) {
		entry = make_uniq<CachedFile>(path);
	}
	return *entry;
}

unique_ptr<CachingFileHandle> CachingFileSystem::OpenFile(const string &path, FileOpenFlags flags) {
	return make_uniq<CachingFileHandle>(*this, GetOrCreateCachedFile(file_system.ExpandPath(path)), flags);
}

CachingFileHandle::CachingFileHandle(CachingFileSystem &caching_file_system_p, CachedFile &cached_file_p,
                                     FileOpenFlags flags_p)
    : caching_file_system(caching_file_system_p), cached_file(cached_file_p), flags(flags_p) {
	if (!caching_file_system.enable || caching_file_system.check_cached_file_invalidation) {
		// If caching is disabled, or if we must check cache invalidation, we always have to open the file
		GetFileHandle();
		return;
	}
	// If we don't have any cached file ranges, we must also open the file
	lock_guard<mutex> guard(cached_file.lock);
	if (cached_file.ranges.empty()) {
		GetFileHandle();
	}
}

FileHandle &CachingFileHandle::GetFileHandle() {
	if (!file_handle) {
		file_handle = caching_file_system.file_system.OpenFile(cached_file.path, flags);
		last_modified = caching_file_system.file_system.GetLastModifiedTime(*file_handle);

		cached_file.file_size = file_handle->GetFileSize();
		cached_file.last_modified = last_modified;
		cached_file.can_seek = file_handle->CanSeek();
		cached_file.on_disk_file = file_handle->OnDiskFile();
	}
	return *file_handle;
}

BufferHandle CachingFileHandle::Read(data_ptr_t &buffer, const idx_t nr_bytes, const idx_t location) {
	BufferHandle result;
	if (!caching_file_system.enable) {
		result = caching_file_system.buffer_manager.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
		buffer = result.Ptr();
		GetFileHandle().Read(buffer, nr_bytes, location);
		return result;
	}

	// Get lock for cached ranges
	unique_lock<mutex> lock(cached_file.lock);

	// First, try to see if we've read from the exact same location before
	auto it = cached_file.ranges.find(location);
	if (it != cached_file.ranges.end()) {
		// We have read from the exact same location before
		if (!RangeIsValid(*it->second)) {
			// The range is no longer valid: erase it
			it = cached_file.ranges.erase(it);
		} else if (it->second->GetOverlap(nr_bytes, location) == CachedFileRangeOverlap::FULL) {
			// The file range contains the requested file range
			result = TryReadFromFileRange(*it->second, buffer, nr_bytes, location);
			if (result.IsValid()) {
				return result;
			}
		}
	}

	// Second, loop through file ranges (ordered by location) to see if any contain the requested file range
	const auto this_end = location + nr_bytes;
	vector<shared_ptr<CachedFileRange>> overlapping_ranges;
	for (it = cached_file.ranges.begin(); it != cached_file.ranges.end();) {
		if (it->second->location >= this_end) {
			// We're past the requested location
			break;
		}
		if (!RangeIsValid(*it->second)) {
			// The range is no longer valid: erase it
			it = cached_file.ranges.erase(it);
			continue;
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
			result = TryReadFromFileRange(*it->second, buffer, nr_bytes, location);
			if (result.IsValid()) {
				return result;
			}
			break;
		}
		it++;
	}

	// We can unlock now because we copied over the ranges to local
	lock.unlock();

	// Finally, if we weren't able to find the file range in the cache, we have to create a new file range
	result = caching_file_system.buffer_manager.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, nr_bytes);
	auto new_file_range = make_shared_ptr<CachedFileRange>(result.GetBlockHandle(), nr_bytes, location, last_modified);
	buffer = result.Ptr();

	// Interleave reading and copying from cached buffers
	idx_t current_location = location;
	idx_t remaining_bytes = nr_bytes;
	for (auto &overlapping_range : overlapping_ranges) {
		D_ASSERT(new_file_range->GetOverlap(*overlapping_range) == CachedFileRangeOverlap::PARTIAL);

		if (remaining_bytes == 0) {
			break; // All requested bytes were read
		}

		if (overlapping_range->location > current_location) {
			// We need to read from the file until we're at the location of the current overlapping file range
			const auto buffer_offset = nr_bytes - remaining_bytes;
			const auto bytes_to_read = overlapping_range->location - current_location;
			D_ASSERT(bytes_to_read < remaining_bytes);
			GetFileHandle().Read(buffer + buffer_offset, bytes_to_read, current_location);
			current_location += bytes_to_read;
			remaining_bytes -= bytes_to_read;
		}

		if (overlapping_range->GetOverlap(remaining_bytes, current_location) == CachedFileRangeOverlap::NONE) {
			continue; // Remainder does not overlap with the current overlapping file range
		}
		D_ASSERT(overlapping_range->GetOverlap(remaining_bytes, current_location) == CachedFileRangeOverlap::PARTIAL);

		// Try to pin the current overlapping file range
		auto overlapping_file_range_pin = caching_file_system.buffer_manager.Pin(overlapping_range->block_handle);
		if (!overlapping_file_range_pin.IsValid()) {
			continue; // No longer valid
		}

		// Finally, we can copy the data over
		D_ASSERT(current_location >= overlapping_range->location);
		const auto buffer_offset = nr_bytes - remaining_bytes;
		const auto overlapping_range_offset = current_location - overlapping_range->location;
		D_ASSERT(overlapping_range->nr_bytes > overlapping_range_offset);
		const auto bytes_to_read = MinValue(overlapping_range->nr_bytes - overlapping_range_offset, remaining_bytes);
		memcpy(buffer + buffer_offset, overlapping_file_range_pin.Ptr() + overlapping_range_offset, bytes_to_read);
		current_location += bytes_to_read;
		remaining_bytes -= bytes_to_read;
	}

	// Read the remaining bytes (if any)
	if (remaining_bytes != 0) {
		const auto buffer_offset = nr_bytes - remaining_bytes;
		GetFileHandle().Read(buffer + buffer_offset, remaining_bytes, current_location);
	}

	// Grab the lock again to insert the newly created buffer into the ranges
	lock.lock();

	// Start at lower_bound (first range with a location not less than the location of the newly create range)
	for (it = cached_file.ranges.lower_bound(location); it != cached_file.ranges.end();) {
		if (!RangeIsValid(*it->second)) {
			// The range is no longer valid: erase it
			it = cached_file.ranges.erase(it);
			continue;
		}
		if (it->second->GetOverlap(*new_file_range) == CachedFileRangeOverlap::FULL) {
			// Another thread has read a range that fully contains the requested range in the meantime
			result = TryReadFromFileRange(*it->second, buffer, nr_bytes, location);
			if (result.IsValid()) {
				return result;
			}
		}
		// Check if the new range overlaps with a cached one
		bool break_loop = false;
		switch (new_file_range->GetOverlap(*it->second)) {
		case CachedFileRangeOverlap::NONE:
			break_loop = true; // We iterated past potential overlaps
			break;
		case CachedFileRangeOverlap::PARTIAL:
			break; // The newly created range does not fully contain this range, so it is still useful
		case CachedFileRangeOverlap::FULL:
			// Full overlap, this range will be obsolete when we insert the current one
			it = cached_file.ranges.erase(it);
			continue;
		}
		if (break_loop) {
			break;
		}
		it++;
	}

	// Finally, insert newly created buffer into the map
	new_file_range->AddCheckSum();
	cached_file.ranges[location] = std::move(new_file_range);
	cached_file.Verify();

	return result;
}

string CachingFileHandle::GetPath() const {
	return cached_file.path;
}

idx_t CachingFileHandle::GetFileSize() {
	if (file_handle || caching_file_system.check_cached_file_invalidation) {
		return GetFileHandle().GetFileSize();
	}
	return cached_file.file_size;
}

time_t CachingFileHandle::GetLastModifiedTime() {
	if (file_handle || caching_file_system.check_cached_file_invalidation) {
		GetFileHandle();
		return last_modified;
	}
	return cached_file.last_modified;
}

bool CachingFileHandle::CanSeek() {
	if (file_handle || caching_file_system.check_cached_file_invalidation) {
		return GetFileHandle().CanSeek();
	}
	return cached_file.can_seek;
}

bool CachingFileHandle::OnDiskFile() {
	if (file_handle || caching_file_system.check_cached_file_invalidation) {
		return GetFileHandle().OnDiskFile();
	}
	return cached_file.on_disk_file.load();
}

bool CachingFileHandle::RangeIsValid(const CachedFileRange &range) {
	if (!caching_file_system.check_cached_file_invalidation) {
		return true;
	}
	return GetLastModifiedTime() == range.last_modified;
}

BufferHandle CachingFileHandle::TryReadFromFileRange(CachedFileRange &file_range, data_ptr_t &buffer, idx_t nr_bytes,
                                                     idx_t location) {
	D_ASSERT(file_range.GetOverlap(nr_bytes, location) == CachedFileRangeOverlap::FULL);
	auto result = caching_file_system.buffer_manager.Pin(file_range.block_handle);
	if (result.IsValid()) {
		buffer = result.Ptr() + (location - file_range.location);
	}
	return result;
}

} // namespace duckdb
