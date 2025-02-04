#include "duckdb/storage/compression/roaring/roaring.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/bitpacking.hpp"

namespace duckdb {

namespace roaring {

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//

ContainerSegmentScan::ContainerSegmentScan(data_ptr_t data)
    : segments(reinterpret_cast<uint8_t *>(data)), index(0), count(0) {
}

// Returns the base of the current segment, forwarding the index if the segment is depleted of values
uint16_t ContainerSegmentScan::operator++(int) {
	while (index < COMPRESSED_SEGMENT_COUNT && count >= segments[index]) {
		count = 0;
		index++;
	}
	count++;

	// index == COMPRESSED_SEGMENT_COUNT is allowed for runs, as the last run could end at ROARING_CONTAINER_SIZE
	D_ASSERT(index <= COMPRESSED_SEGMENT_COUNT);
	if (index < COMPRESSED_SEGMENT_COUNT) {
		D_ASSERT(segments[index] != 0);
	}
	uint16_t base = static_cast<uint16_t>(index) * COMPRESSED_SEGMENT_SIZE;
	return base;
}

//===--------------------------------------------------------------------===//
// ContainerScanState
//===--------------------------------------------------------------------===//

//! RunContainer

RunContainerScanState::RunContainerScanState(idx_t container_index, idx_t container_size, idx_t count,
                                             data_ptr_t data_p)
    : ContainerScanState(container_index, container_size), count(count), data(data_p) {
}

void RunContainerScanState::ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) {
	auto &result_mask = FlatVector::Validity(result);

	// This method assumes that the validity mask starts off as having all bits set for the entries that are being
	// scanned.

	idx_t result_idx = 0;
	if (!run_index) {
		LoadNextRun();
	}
	while (!finished && result_idx < to_scan) {
		// Either we are already inside a run, then 'start_of_run' will be scanned_count
		// or we're skipping values until the run begins
		auto start_of_run =
		    MaxValue<idx_t>(MinValue<idx_t>(run.start, scanned_count + to_scan), scanned_count + result_idx);
		result_idx = start_of_run - scanned_count;

		// How much of the run are we covering?
		idx_t run_end = run.start + 1 + run.length;
		auto run_or_scan_end = MinValue<idx_t>(run_end, scanned_count + to_scan);

		// Process the run
		D_ASSERT(run_or_scan_end >= start_of_run);
		if (run_or_scan_end > start_of_run) {
			idx_t amount = run_or_scan_end - start_of_run;
			idx_t start = result_offset + result_idx;
			idx_t end = start + amount;
			SetInvalidRange(result_mask, start, end);
		}

		result_idx += run_or_scan_end - start_of_run;
		if (scanned_count + result_idx == run_end) {
			// Fully processed the current run
			LoadNextRun();
		}
	}
	scanned_count += to_scan;
}

void RunContainerScanState::Skip(idx_t to_skip) {
	idx_t end = scanned_count + to_skip;
	if (!run_index) {
		LoadNextRun();
	}
	while (scanned_count < end && !finished) {
		idx_t run_end = run.start + 1 + run.length;
		scanned_count = MinValue<idx_t>(run_end, end);
		if (scanned_count == run_end) {
			LoadNextRun();
		}
	}
	// In case run_index has already reached count
	scanned_count = end;
}

void RunContainerScanState::Verify() const {
#ifdef DEBUG
	uint16_t index = 0;
	for (idx_t i = 0; i < count; i++) {
		auto run = reinterpret_cast<RunContainerRLEPair *>(data)[i];
		D_ASSERT(run.start >= index);
		index = run.start + 1 + run.length;
	}
#endif
}

void RunContainerScanState::LoadNextRun() {
	if (run_index >= count) {
		finished = true;
		return;
	}
	run = reinterpret_cast<RunContainerRLEPair *>(data)[run_index];
	run_index++;
}

CompressedRunContainerScanState::CompressedRunContainerScanState(idx_t container_index, idx_t container_size,
                                                                 idx_t count, data_ptr_t segments, data_ptr_t data)
    : RunContainerScanState(container_index, container_size, count, data), segments(segments), segment(segments) {
	D_ASSERT(count >= COMPRESSED_RUN_THRESHOLD);
	//! Used by Verify, have to use it to avoid a compiler warning/error
	(void)this->segments;
}

void CompressedRunContainerScanState::LoadNextRun() {
	if (run_index >= count) {
		finished = true;
		return;
	}
	uint16_t start = segment++;
	start += reinterpret_cast<uint8_t *>(data)[(run_index * 2) + 0];

	uint16_t end = segment++;
	end += reinterpret_cast<uint8_t *>(data)[(run_index * 2) + 1];

	D_ASSERT(end > start);
	run = RunContainerRLEPair {start, static_cast<uint16_t>(end - 1 - start)};
	run_index++;
}

void CompressedRunContainerScanState::Verify() const {
#ifdef DEBUG
	uint16_t index = 0;
	ContainerSegmentScan verify_segment(segments);
	for (idx_t i = 0; i < count; i++) {
		// Get the start index of the run
		uint16_t start = verify_segment++;
		start += reinterpret_cast<uint8_t *>(data)[(i * 2) + 0];

		// Get the end index of the run
		uint16_t end = verify_segment++;
		end += reinterpret_cast<uint8_t *>(data)[(i * 2) + 1];

		D_ASSERT(!i || start >= index);
		D_ASSERT(end > start);
		index = end;
	}
#endif
}

//! BitsetContainer

BitsetContainerScanState::BitsetContainerScanState(idx_t container_index, idx_t count, validity_t *bitset)
    : ContainerScanState(container_index, count), bitset(bitset) {
}

void BitsetContainerScanState::ScanPartial(Vector &result, idx_t result_offset, idx_t to_scan) {
	if (!result_offset && (to_scan % ValidityMask::BITS_PER_VALUE) == 0 &&
	    (scanned_count % ValidityMask::BITS_PER_VALUE) == 0) {
		ValidityUncompressed::AlignedScan(reinterpret_cast<data_ptr_t>(bitset), scanned_count, result, to_scan);
	} else {
		ValidityUncompressed::UnalignedScan(reinterpret_cast<data_ptr_t>(bitset), container_size, scanned_count, result,
		                                    result_offset, to_scan);
	}
	scanned_count += to_scan;
}

void BitsetContainerScanState::Skip(idx_t to_skip) {
	// NO OP: we only need to forward scanned_count
	scanned_count += to_skip;
}

void BitsetContainerScanState::Verify() const {
	// uncompressed, nothing to verify
	return;
}

RoaringScanState::RoaringScanState(ColumnSegment &segment) : segment(segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	handle = buffer_manager.Pin(segment.block);
	auto base_ptr = handle.Ptr() + segment.GetBlockOffset();
	data_ptr = base_ptr + sizeof(idx_t);

	// Deserialize the container metadata for this segment
	auto metadata_offset = Load<idx_t>(base_ptr);
	auto metadata_ptr = data_ptr + metadata_offset;

	auto segment_count = segment.count.load();
	auto container_count = segment_count / ROARING_CONTAINER_SIZE;
	if (segment_count % ROARING_CONTAINER_SIZE != 0) {
		container_count++;
	}
	metadata_collection.Deserialize(metadata_ptr, container_count);
	ContainerMetadataCollectionScanner scanner(metadata_collection);
	data_start_position.reserve(container_count);
	idx_t position = 0;
	for (idx_t i = 0; i < container_count; i++) {
		auto metadata = scanner.GetNext();
		container_metadata.push_back(metadata);
		if (metadata.IsUncompressed()) {
			position = AlignValue<idx_t>(position);
		} else if (metadata.IsArray() && metadata.Cardinality() < COMPRESSED_ARRAY_THRESHOLD) {
			position = AlignValue<idx_t, sizeof(uint16_t)>(position);
		} else if (metadata.IsRun() && metadata.NumberOfRuns() < COMPRESSED_RUN_THRESHOLD) {
			position = AlignValue<idx_t, sizeof(RunContainerRLEPair)>(position);
		}
		data_start_position.push_back(position);
		position += SkipVector(metadata);
	}
}

idx_t RoaringScanState::SkipVector(const ContainerMetadata &metadata) {
	// NOTE: this doesn't care about smaller containers, since only the last container can be smaller
	return metadata.GetDataSizeInBytes(ROARING_CONTAINER_SIZE);
}

bool RoaringScanState::UseContainerStateCache(idx_t container_index, idx_t internal_offset) {
	if (!current_container) {
		// No container loaded yet
		return false;
	}
	if (current_container->container_index != container_index) {
		// Not the same container
		return false;
	}
	if (current_container->scanned_count != internal_offset) {
		// Not the same scan offset
		return false;
	}
	return true;
}

ContainerMetadata RoaringScanState::GetContainerMetadata(idx_t container_index) {
	return container_metadata[container_index];
}

data_ptr_t RoaringScanState::GetStartOfContainerData(idx_t container_index) {
	return data_ptr + data_start_position[container_index];
}

ContainerScanState &RoaringScanState::LoadContainer(idx_t container_index, idx_t internal_offset) {
	if (UseContainerStateCache(container_index, internal_offset)) {
		return *current_container;
	}
	auto metadata = GetContainerMetadata(container_index);
	auto data_ptr = GetStartOfContainerData(container_index);

	auto segment_count = segment.count.load();
	auto start_of_container = container_index * ROARING_CONTAINER_SIZE;
	auto container_size = MinValue<idx_t>(segment_count - start_of_container, ROARING_CONTAINER_SIZE);
	if (metadata.IsUncompressed()) {
		current_container = make_uniq<BitsetContainerScanState>(container_index, container_size,
		                                                        reinterpret_cast<validity_t *>(data_ptr));
	} else if (metadata.IsRun()) {
		D_ASSERT(metadata.IsInverted());
		auto number_of_runs = metadata.NumberOfRuns();
		if (number_of_runs >= COMPRESSED_RUN_THRESHOLD) {
			auto segments = data_ptr;
			data_ptr = segments + COMPRESSED_SEGMENT_COUNT;
			current_container = make_uniq<CompressedRunContainerScanState>(container_index, container_size,
			                                                               number_of_runs, segments, data_ptr);
		} else {
			D_ASSERT(AlignValue<sizeof(RunContainerRLEPair)>(data_ptr) == data_ptr);
			current_container =
			    make_uniq<RunContainerScanState>(container_index, container_size, number_of_runs, data_ptr);
		}
	} else {
		auto cardinality = metadata.Cardinality();
		if (cardinality >= COMPRESSED_ARRAY_THRESHOLD) {
			auto segments = data_ptr;
			data_ptr = segments + COMPRESSED_SEGMENT_COUNT;
			if (metadata.IsInverted()) {
				current_container = make_uniq<CompressedArrayContainerScanState<NULLS>>(
				    container_index, container_size, cardinality, segments, data_ptr);
			} else {
				current_container = make_uniq<CompressedArrayContainerScanState<NON_NULLS>>(
				    container_index, container_size, cardinality, segments, data_ptr);
			}
		} else {
			D_ASSERT(AlignValue<sizeof(uint16_t)>(data_ptr) == data_ptr);
			if (metadata.IsInverted()) {
				current_container =
				    make_uniq<ArrayContainerScanState<NULLS>>(container_index, container_size, cardinality, data_ptr);
			} else {
				current_container = make_uniq<ArrayContainerScanState<NON_NULLS>>(container_index, container_size,
				                                                                  cardinality, data_ptr);
			}
		}
	}

	current_container->Verify();

	auto &scan_state = *current_container;
	if (internal_offset) {
		Skip(scan_state, internal_offset);
	}
	return *current_container;
}

void RoaringScanState::ScanInternal(ContainerScanState &scan_state, idx_t to_scan, Vector &result, idx_t offset) {
	scan_state.ScanPartial(result, offset, to_scan);
}

idx_t RoaringScanState::GetContainerIndex(idx_t start_index, idx_t &offset) {
	idx_t container_index = start_index / ROARING_CONTAINER_SIZE;
	offset = start_index % ROARING_CONTAINER_SIZE;
	return container_index;
}

void RoaringScanState::ScanPartial(idx_t start_idx, Vector &result, idx_t offset, idx_t count) {
	result.Flatten(count);
	idx_t remaining = count;
	idx_t scanned = 0;
	while (remaining) {
		idx_t internal_offset;
		idx_t container_idx = GetContainerIndex(start_idx + scanned, internal_offset);
		auto &scan_state = LoadContainer(container_idx, internal_offset);
		idx_t remaining_in_container = scan_state.container_size - scan_state.scanned_count;
		idx_t to_scan = MinValue<idx_t>(remaining, remaining_in_container);
		ScanInternal(scan_state, to_scan, result, offset + scanned);
		remaining -= to_scan;
		scanned += to_scan;
	}
	D_ASSERT(scanned == count);
}

void RoaringScanState::Skip(ContainerScanState &scan_state, idx_t skip_count) {
	D_ASSERT(scan_state.scanned_count + skip_count <= scan_state.container_size);
	if (scan_state.scanned_count + skip_count == scan_state.container_size) {
		scan_state.scanned_count = scan_state.container_size;
		// This skips all remaining values covered by this container
		return;
	}
	scan_state.Skip(skip_count);
}

} // namespace roaring

} // namespace duckdb
