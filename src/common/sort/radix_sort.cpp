#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/sort/comparators.hpp"
#include "duckdb/common/sort/duckdb_pdqsort.hpp"
#include "duckdb/common/sort/sort.hpp"

namespace duckdb {

//! Calls std::sort on strings that are tied by their prefix after the radix sort
static void SortTiedBlobs(BufferManager &buffer_manager, const data_ptr_t dataptr, const idx_t &start, const idx_t &end,
                          const idx_t &tie_col, bool *ties, const data_ptr_t blob_ptr, const SortLayout &sort_layout) {
	const auto row_width = sort_layout.blob_layout.GetRowWidth();
	// Locate the first blob row in question
	data_ptr_t row_ptr = dataptr + start * sort_layout.entry_size;
	data_ptr_t blob_row_ptr = blob_ptr + Load<uint32_t>(row_ptr + sort_layout.comparison_size) * row_width;
	if (!Comparators::TieIsBreakable(tie_col, blob_row_ptr, sort_layout)) {
		// Quick check to see if ties can be broken
		return;
	}
	// Fill pointer array for sorting
	auto ptr_block = make_unsafe_uniq_array_uninitialized<data_ptr_t>(end - start);
	auto entry_ptrs = (data_ptr_t *)ptr_block.get();
	for (idx_t i = start; i < end; i++) {
		entry_ptrs[i - start] = row_ptr;
		row_ptr += sort_layout.entry_size;
	}
	// Slow pointer-based sorting
	const int order = sort_layout.order_types[tie_col] == OrderType::DESCENDING ? -1 : 1;
	const idx_t &col_idx = sort_layout.sorting_to_blob_col.at(tie_col);
	const auto &tie_col_offset = sort_layout.blob_layout.GetOffsets()[col_idx];
	auto logical_type = sort_layout.blob_layout.GetTypes()[col_idx];
	std::sort(entry_ptrs, entry_ptrs + end - start,
	          [&blob_ptr, &order, &sort_layout, &tie_col_offset, &row_width, &logical_type](const data_ptr_t l,
	                                                                                        const data_ptr_t r) {
		          idx_t left_idx = Load<uint32_t>(l + sort_layout.comparison_size);
		          idx_t right_idx = Load<uint32_t>(r + sort_layout.comparison_size);
		          data_ptr_t left_ptr = blob_ptr + left_idx * row_width + tie_col_offset;
		          data_ptr_t right_ptr = blob_ptr + right_idx * row_width + tie_col_offset;
		          return order * Comparators::CompareVal(left_ptr, right_ptr, logical_type) < 0;
	          });
	// Re-order
	auto temp_block = buffer_manager.GetBufferAllocator().Allocate((end - start) * sort_layout.entry_size);
	data_ptr_t temp_ptr = temp_block.get();
	for (idx_t i = 0; i < end - start; i++) {
		FastMemcpy(temp_ptr, entry_ptrs[i], sort_layout.entry_size);
		temp_ptr += sort_layout.entry_size;
	}
	memcpy(dataptr + start * sort_layout.entry_size, temp_block.get(), (end - start) * sort_layout.entry_size);
	// Determine if there are still ties (if this is not the last column)
	if (tie_col < sort_layout.column_count - 1) {
		data_ptr_t idx_ptr = dataptr + start * sort_layout.entry_size + sort_layout.comparison_size;
		// Load current entry
		data_ptr_t current_ptr = blob_ptr + Load<uint32_t>(idx_ptr) * row_width + tie_col_offset;
		for (idx_t i = 0; i < end - start - 1; i++) {
			// Load next entry and compare
			idx_ptr += sort_layout.entry_size;
			data_ptr_t next_ptr = blob_ptr + Load<uint32_t>(idx_ptr) * row_width + tie_col_offset;
			ties[start + i] = Comparators::CompareVal(current_ptr, next_ptr, logical_type) == 0;
			current_ptr = next_ptr;
		}
	}
}

//! Identifies sequences of rows that are tied by the prefix of a blob column, and sorts them
static void SortTiedBlobs(BufferManager &buffer_manager, SortedBlock &sb, bool *ties, data_ptr_t dataptr,
                          const idx_t &count, const idx_t &tie_col, const SortLayout &sort_layout) {
	D_ASSERT(!ties[count - 1]);
	auto &blob_block = *sb.blob_sorting_data->data_blocks.back();
	auto blob_handle = buffer_manager.Pin(blob_block.block);
	const data_ptr_t blob_ptr = blob_handle.Ptr();

	for (idx_t i = 0; i < count; i++) {
		if (!ties[i]) {
			continue;
		}
		idx_t j;
		for (j = i + 1; j < count; j++) {
			if (!ties[j]) {
				break;
			}
		}
		SortTiedBlobs(buffer_manager, dataptr, i, j + 1, tie_col, ties, blob_ptr, sort_layout);
		i = j;
	}
}

//! Returns whether there are any 'true' values in the ties[] array
static bool AnyTies(bool ties[], const idx_t &count) {
	D_ASSERT(!ties[count - 1]);
	bool any_ties = false;
	for (idx_t i = 0; i < count - 1; i++) {
		any_ties = any_ties || ties[i];
	}
	return any_ties;
}

//! Compares subsequent rows to check for ties
static void ComputeTies(data_ptr_t dataptr, const idx_t &count, const idx_t &col_offset, const idx_t &tie_size,
                        bool ties[], const SortLayout &sort_layout) {
	D_ASSERT(!ties[count - 1]);
	D_ASSERT(col_offset + tie_size <= sort_layout.comparison_size);
	// Align dataptr
	dataptr += col_offset;
	for (idx_t i = 0; i < count - 1; i++) {
		ties[i] = ties[i] && FastMemcmp(dataptr, dataptr + sort_layout.entry_size, tie_size) == 0;
		dataptr += sort_layout.entry_size;
	}
}

//! Textbook LSD radix sort
void RadixSortLSD(BufferManager &buffer_manager, const data_ptr_t &dataptr, const idx_t &count, const idx_t &col_offset,
                  const idx_t &row_width, const idx_t &sorting_size) {
	auto temp_block = buffer_manager.GetBufferAllocator().Allocate(count * row_width);
	bool swap = false;

	idx_t counts[SortConstants::VALUES_PER_RADIX];
	for (idx_t r = 1; r <= sorting_size; r++) {
		// Init counts to 0
		memset(counts, 0, sizeof(counts));
		// Const some values for convenience
		const data_ptr_t source_ptr = swap ? temp_block.get() : dataptr;
		const data_ptr_t target_ptr = swap ? dataptr : temp_block.get();
		const idx_t offset = col_offset + sorting_size - r;
		// Collect counts
		data_ptr_t offset_ptr = source_ptr + offset;
		for (idx_t i = 0; i < count; i++) {
			counts[*offset_ptr]++;
			offset_ptr += row_width;
		}
		// Compute offsets from counts
		idx_t max_count = counts[0];
		for (idx_t val = 1; val < SortConstants::VALUES_PER_RADIX; val++) {
			max_count = MaxValue<idx_t>(max_count, counts[val]);
			counts[val] = counts[val] + counts[val - 1];
		}
		if (max_count == count) {
			continue;
		}
		// Re-order the data in temporary array
		data_ptr_t row_ptr = source_ptr + (count - 1) * row_width;
		for (idx_t i = 0; i < count; i++) {
			idx_t &radix_offset = --counts[*(row_ptr + offset)];
			FastMemcpy(target_ptr + radix_offset * row_width, row_ptr, row_width);
			row_ptr -= row_width;
		}
		swap = !swap;
	}
	// Move data back to original buffer (if it was swapped)
	if (swap) {
		memcpy(dataptr, temp_block.get(), count * row_width);
	}
}

//! Insertion sort, used when count of values is low
inline void InsertionSort(const data_ptr_t orig_ptr, const data_ptr_t temp_ptr, const idx_t &count,
                          const idx_t &col_offset, const idx_t &row_width, const idx_t &total_comp_width,
                          const idx_t &offset, bool swap) {
	const data_ptr_t source_ptr = swap ? temp_ptr : orig_ptr;
	const data_ptr_t target_ptr = swap ? orig_ptr : temp_ptr;
	if (count > 1) {
		const idx_t total_offset = col_offset + offset;
		auto temp_val = make_unsafe_uniq_array_uninitialized<data_t>(row_width);
		const data_ptr_t val = temp_val.get();
		const auto comp_width = total_comp_width - offset;
		for (idx_t i = 1; i < count; i++) {
			FastMemcpy(val, source_ptr + i * row_width, row_width);
			idx_t j = i;
			while (j > 0 &&
			       FastMemcmp(source_ptr + (j - 1) * row_width + total_offset, val + total_offset, comp_width) > 0) {
				FastMemcpy(source_ptr + j * row_width, source_ptr + (j - 1) * row_width, row_width);
				j--;
			}
			FastMemcpy(source_ptr + j * row_width, val, row_width);
		}
	}
	if (swap) {
		memcpy(target_ptr, source_ptr, count * row_width);
	}
}

//! MSD radix sort that switches to insertion sort with low bucket sizes
void RadixSortMSD(const data_ptr_t orig_ptr, const data_ptr_t temp_ptr, const idx_t &count, const idx_t &col_offset,
                  const idx_t &row_width, const idx_t &comp_width, const idx_t &offset, idx_t locations[], bool swap) {
	const data_ptr_t source_ptr = swap ? temp_ptr : orig_ptr;
	const data_ptr_t target_ptr = swap ? orig_ptr : temp_ptr;
	// Init counts to 0
	memset(locations, 0, SortConstants::MSD_RADIX_LOCATIONS * sizeof(idx_t));
	idx_t *counts = locations + 1;
	// Collect counts
	const idx_t total_offset = col_offset + offset;
	data_ptr_t offset_ptr = source_ptr + total_offset;
	for (idx_t i = 0; i < count; i++) {
		counts[*offset_ptr]++;
		offset_ptr += row_width;
	}
	// Compute locations from counts
	idx_t max_count = 0;
	for (idx_t radix = 0; radix < SortConstants::VALUES_PER_RADIX; radix++) {
		max_count = MaxValue<idx_t>(max_count, counts[radix]);
		counts[radix] += locations[radix];
	}
	if (max_count != count) {
		// Re-order the data in temporary array
		data_ptr_t row_ptr = source_ptr;
		for (idx_t i = 0; i < count; i++) {
			const idx_t &radix_offset = locations[*(row_ptr + total_offset)]++;
			FastMemcpy(target_ptr + radix_offset * row_width, row_ptr, row_width);
			row_ptr += row_width;
		}
		swap = !swap;
	}
	// Check if done
	if (offset == comp_width - 1) {
		if (swap) {
			memcpy(orig_ptr, temp_ptr, count * row_width);
		}
		return;
	}
	if (max_count == count) {
		RadixSortMSD(orig_ptr, temp_ptr, count, col_offset, row_width, comp_width, offset + 1,
		             locations + SortConstants::MSD_RADIX_LOCATIONS, swap);
		return;
	}
	// Recurse
	idx_t radix_count = locations[0];
	for (idx_t radix = 0; radix < SortConstants::VALUES_PER_RADIX; radix++) {
		const idx_t loc = (locations[radix] - radix_count) * row_width;
		if (radix_count > SortConstants::INSERTION_SORT_THRESHOLD) {
			RadixSortMSD(orig_ptr + loc, temp_ptr + loc, radix_count, col_offset, row_width, comp_width, offset + 1,
			             locations + SortConstants::MSD_RADIX_LOCATIONS, swap);
		} else if (radix_count != 0) {
			InsertionSort(orig_ptr + loc, temp_ptr + loc, radix_count, col_offset, row_width, comp_width, offset + 1,
			              swap);
		}
		radix_count = locations[radix + 1] - locations[radix];
	}
}

//! Calls different sort functions, depending on the count and sorting sizes
void RadixSort(BufferManager &buffer_manager, const data_ptr_t &dataptr, const idx_t &count, const idx_t &col_offset,
               const idx_t &sorting_size, const SortLayout &sort_layout, bool contains_string) {

	if (contains_string) {
		auto begin = duckdb_pdqsort::PDQIterator(dataptr, sort_layout.entry_size);
		auto end = begin + count;
		duckdb_pdqsort::PDQConstants constants(sort_layout.entry_size, col_offset, sorting_size, *end);
		return duckdb_pdqsort::pdqsort_branchless(begin, begin + count, constants);
	}

	if (count <= SortConstants::INSERTION_SORT_THRESHOLD) {
		return InsertionSort(dataptr, nullptr, count, col_offset, sort_layout.entry_size, sorting_size, 0, false);
	}

	if (sorting_size <= SortConstants::MSD_RADIX_SORT_SIZE_THRESHOLD) {
		return RadixSortLSD(buffer_manager, dataptr, count, col_offset, sort_layout.entry_size, sorting_size);
	}

	const auto block_size = buffer_manager.GetBlockSize();
	auto temp_block =
	    buffer_manager.Allocate(MemoryTag::ORDER_BY, MaxValue(count * sort_layout.entry_size, block_size));
	auto pre_allocated_array =
	    make_unsafe_uniq_array_uninitialized<idx_t>(sorting_size * SortConstants::MSD_RADIX_LOCATIONS);
	RadixSortMSD(dataptr, temp_block.Ptr(), count, col_offset, sort_layout.entry_size, sorting_size, 0,
	             pre_allocated_array.get(), false);
}

//! Identifies sequences of rows that are tied, and calls radix sort on these
static void SubSortTiedTuples(BufferManager &buffer_manager, const data_ptr_t dataptr, const idx_t &count,
                              const idx_t &col_offset, const idx_t &sorting_size, bool ties[],
                              const SortLayout &sort_layout, bool contains_string) {
	D_ASSERT(!ties[count - 1]);
	for (idx_t i = 0; i < count; i++) {
		if (!ties[i]) {
			continue;
		}
		idx_t j;
		for (j = i + 1; j < count; j++) {
			if (!ties[j]) {
				break;
			}
		}
		RadixSort(buffer_manager, dataptr + i * sort_layout.entry_size, j - i + 1, col_offset, sorting_size,
		          sort_layout, contains_string);
		i = j;
	}
}

void LocalSortState::SortInMemory() {
	auto &sb = *sorted_blocks.back();
	auto &block = *sb.radix_sorting_data.back();
	const auto &count = block.count;
	auto handle = buffer_manager->Pin(block.block);
	const auto dataptr = handle.Ptr();
	// Assign an index to each row
	data_ptr_t idx_dataptr = dataptr + sort_layout->comparison_size;
	for (uint32_t i = 0; i < count; i++) {
		Store<uint32_t>(i, idx_dataptr);
		idx_dataptr += sort_layout->entry_size;
	}
	// Radix sort and break ties until no more ties, or until all columns are sorted
	idx_t sorting_size = 0;
	idx_t col_offset = 0;
	unsafe_unique_array<bool> ties_ptr;
	bool *ties = nullptr;
	bool contains_string = false;
	for (idx_t i = 0; i < sort_layout->column_count; i++) {
		sorting_size += sort_layout->column_sizes[i];
		contains_string = contains_string || sort_layout->logical_types[i].InternalType() == PhysicalType::VARCHAR;
		if (sort_layout->constant_size[i] && i < sort_layout->column_count - 1) {
			// Add columns to the sorting size until we reach a variable size column, or the last column
			continue;
		}

		if (!ties) {
			// This is the first sort
			RadixSort(*buffer_manager, dataptr, count, col_offset, sorting_size, *sort_layout, contains_string);
			ties_ptr = make_unsafe_uniq_array_uninitialized<bool>(count);
			ties = ties_ptr.get();
			std::fill_n(ties, count - 1, true);
			ties[count - 1] = false;
		} else {
			// For subsequent sorts, we only have to subsort the tied tuples
			SubSortTiedTuples(*buffer_manager, dataptr, count, col_offset, sorting_size, ties, *sort_layout,
			                  contains_string);
		}

		contains_string = false;

		if (sort_layout->constant_size[i] && i == sort_layout->column_count - 1) {
			// All columns are sorted, no ties to break because last column is constant size
			break;
		}

		ComputeTies(dataptr, count, col_offset, sorting_size, ties, *sort_layout);
		if (!AnyTies(ties, count)) {
			// No ties, stop sorting
			break;
		}

		if (!sort_layout->constant_size[i]) {
			SortTiedBlobs(*buffer_manager, sb, ties, dataptr, count, i, *sort_layout);
			if (!AnyTies(ties, count)) {
				// No more ties after tie-breaking, stop
				break;
			}
		}

		col_offset += sorting_size;
		sorting_size = 0;
	}
}

} // namespace duckdb
