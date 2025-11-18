#include "duckdb/storage/read_policy.hpp"

namespace duckdb {

namespace {

// Hardcoded block size for aligned reads: 2MiB
constexpr idx_t ALIGNED_READ_BLOCK_SIZE = 2 * 1024 * 1024; // 2MiB

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

ReadPolicyResult DefaultReadPolicy::CalculateBytesToRead(idx_t nr_bytes, idx_t location,
                                                         optional_idx start_location_of_next_range) {
	idx_t new_nr_bytes = nr_bytes;
	if (start_location_of_next_range.IsValid()) {
		const idx_t nr_bytes_to_be_added = start_location_of_next_range.GetIndex() - location - nr_bytes;
		if (ShouldExpandToFillGap(nr_bytes, nr_bytes_to_be_added)) {
			// Grow the range from location to start_location_of_next_range, so that to fill gaps in the cached ranges
			new_nr_bytes = nr_bytes + nr_bytes_to_be_added;
		}
	}
	return {location, new_nr_bytes};
}

ReadPolicyResult AlignedReadPolicy::CalculateBytesToRead(idx_t nr_bytes, idx_t location,
                                                         optional_idx start_location_of_next_range) {
	// Use the hardcoded 2MiB block size
	const idx_t block_size_to_use = ALIGNED_READ_BLOCK_SIZE;

	// Align the start location down to block boundary
	const idx_t aligned_start = (location / block_size_to_use) * block_size_to_use;

	// Calculate the end location (requested end)
	const idx_t requested_end = location + nr_bytes;

	// Align the end location up to block boundary
	const idx_t aligned_end = ((requested_end + block_size_to_use - 1) / block_size_to_use) * block_size_to_use;

	// Calculate aligned bytes to read
	idx_t aligned_nr_bytes = aligned_end - aligned_start;

	// If we have a next range location, we might want to limit the read
	if (start_location_of_next_range.IsValid()) {
		const idx_t next_range_start = start_location_of_next_range.GetIndex();
		// Don't read beyond the next range
		if (aligned_end > next_range_start) {
			// Use the default policy logic to potentially fill the gap
			DefaultReadPolicy default_policy;
			ReadPolicyResult default_result =
			    default_policy.CalculateBytesToRead(nr_bytes, location, start_location_of_next_range);
			// But still align it
			const idx_t default_end = default_result.read_location + default_result.read_bytes;
			const idx_t aligned_default_end =
			    ((default_end + block_size_to_use - 1) / block_size_to_use) * block_size_to_use;
			aligned_nr_bytes = aligned_default_end - aligned_start;
			// Make sure we don't exceed the next range
			if (aligned_start + aligned_nr_bytes > next_range_start) {
				aligned_nr_bytes = next_range_start - aligned_start;
			}
		}
	}

	return {aligned_start, aligned_nr_bytes};
}

} // namespace duckdb
