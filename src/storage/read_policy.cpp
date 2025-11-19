#include "duckdb/storage/read_policy.hpp"

namespace duckdb {

namespace {

// Default block size for aligned read, which is made for object storage access.
constexpr idx_t ALIGNED_READ_BLOCK_SIZE = 2 * 1024 * 1024; // 2MiB

// Align a value down to the nearest multiple of ALIGNED_READ_BLOCK_SIZE.
idx_t AlignDown(idx_t value) {
	return (value / ALIGNED_READ_BLOCK_SIZE) * ALIGNED_READ_BLOCK_SIZE;
}

// Align a value up to the nearest multiple of ALIGNED_READ_BLOCK_SIZE.
idx_t AlignUp(idx_t value) {
	return ((value + ALIGNED_READ_BLOCK_SIZE - 1) / ALIGNED_READ_BLOCK_SIZE) * ALIGNED_READ_BLOCK_SIZE;
}

// Util function for default read policy.
bool ShouldExpandToFillGap(const idx_t current_length, const idx_t added_length) {
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

} // namespace

ReadPolicyResult DefaultReadPolicy::CalculateBytesToRead(idx_t nr_bytes, idx_t location, idx_t file_size,
                                                         optional_idx start_location_of_next_range) {
	idx_t new_nr_bytes = nr_bytes;
	if (start_location_of_next_range.IsValid()) {
		const idx_t nr_bytes_to_be_added = start_location_of_next_range.GetIndex() - location - nr_bytes;
		if (ShouldExpandToFillGap(nr_bytes, nr_bytes_to_be_added)) {
			// Grow the range from location to start_location_of_next_range, so that to fill gaps in the cached ranges
			new_nr_bytes = nr_bytes + nr_bytes_to_be_added;
		}
	}
	// Make sure we don't read past the end of the file
	if (location + new_nr_bytes > file_size) {
		new_nr_bytes = file_size - location;
	}
	return {location, new_nr_bytes};
}

ReadPolicyResult AlignedReadPolicy::CalculateBytesToRead(idx_t nr_bytes, idx_t location, idx_t file_size,
                                                         optional_idx start_location_of_next_range) {
	const idx_t aligned_start = AlignDown(location);
	const idx_t requested_end = location + nr_bytes;
	idx_t aligned_end = AlignUp(requested_end);

	// Adjust aligned_end if we have a known next range location.
	if (start_location_of_next_range.IsValid()) {
		D_ASSERT(start_location_of_next_range.GetIndex() % ALIGNED_READ_BLOCK_SIZE == 0);
		const idx_t next_range_start = start_location_of_next_range.GetIndex();
		if (aligned_end > next_range_start) {
			aligned_end = next_range_start;
		}
	}

	// Ensure we don't read past the end of the file.
	if (aligned_end > file_size) {
		aligned_end = file_size;
	}

	const idx_t aligned_nr_bytes = aligned_end - aligned_start;
	return {aligned_start, aligned_nr_bytes};
}

} // namespace duckdb
