#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/sort/comparators.hpp"
#include "duckdb/common/sort/sort.hpp"

namespace duckdb {

MergeSorter::MergeSorter(GlobalSortState &state, BufferManager &buffer_manager)
    : state(state), buffer_manager(buffer_manager), sort_layout(state.sort_layout) {
}

void MergeSorter::PerformInMergeRound() {
	while (true) {
		{
			lock_guard<mutex> pair_guard(state.lock);
			if (state.pair_idx == state.num_pairs) {
				break;
			}
			GetNextPartition();
		}
		MergePartition();
	}
}

void MergeSorter::MergePartition() {
	auto &left_block = *left->sb;
	auto &right_block = *right->sb;
#ifdef DEBUG
	D_ASSERT(left_block.radix_sorting_data.size() == left_block.payload_data->data_blocks.size());
	D_ASSERT(right_block.radix_sorting_data.size() == right_block.payload_data->data_blocks.size());
	if (!state.payload_layout.AllConstant() && state.external) {
		D_ASSERT(left_block.payload_data->data_blocks.size() == left_block.payload_data->heap_blocks.size());
		D_ASSERT(right_block.payload_data->data_blocks.size() == right_block.payload_data->heap_blocks.size());
	}
	if (!sort_layout.all_constant) {
		D_ASSERT(left_block.radix_sorting_data.size() == left_block.blob_sorting_data->data_blocks.size());
		D_ASSERT(right_block.radix_sorting_data.size() == right_block.blob_sorting_data->data_blocks.size());
		if (state.external) {
			D_ASSERT(left_block.blob_sorting_data->data_blocks.size() ==
			         left_block.blob_sorting_data->heap_blocks.size());
			D_ASSERT(right_block.blob_sorting_data->data_blocks.size() ==
			         right_block.blob_sorting_data->heap_blocks.size());
		}
	}
#endif
	// Set up the write block
	// Each merge task produces a SortedBlock with exactly state.block_capacity rows or less
	result->InitializeWrite();
	// Initialize arrays to store merge data
	bool left_smaller[STANDARD_VECTOR_SIZE];
	idx_t next_entry_sizes[STANDARD_VECTOR_SIZE];
	// Merge loop
#ifdef DEBUG
	auto l_count = left->Remaining();
	auto r_count = right->Remaining();
#endif
	while (true) {
		auto l_remaining = left->Remaining();
		auto r_remaining = right->Remaining();
		if (l_remaining + r_remaining == 0) {
			// Done
			break;
		}
		const idx_t next = MinValue(l_remaining + r_remaining, (idx_t)STANDARD_VECTOR_SIZE);
		if (l_remaining != 0 && r_remaining != 0) {
			// Compute the merge (not needed if one side is exhausted)
			ComputeMerge(next, left_smaller);
		}
		// Actually merge the data (radix, blob, and payload)
		MergeRadix(next, left_smaller);
		if (!sort_layout.all_constant) {
			MergeData(*result->blob_sorting_data, *left_block.blob_sorting_data, *right_block.blob_sorting_data, next,
			          left_smaller, next_entry_sizes, true);
			D_ASSERT(result->radix_sorting_data.size() == result->blob_sorting_data->data_blocks.size());
		}
		MergeData(*result->payload_data, *left_block.payload_data, *right_block.payload_data, next, left_smaller,
		          next_entry_sizes, false);
		D_ASSERT(result->radix_sorting_data.size() == result->payload_data->data_blocks.size());
	}
#ifdef DEBUG
	D_ASSERT(result->Count() == l_count + r_count);
#endif
}

void MergeSorter::GetNextPartition() {
	// Create result block
	state.sorted_blocks_temp[state.pair_idx].push_back(make_unique<SortedBlock>(buffer_manager, state));
	result = state.sorted_blocks_temp[state.pair_idx].back().get();
	// Determine which blocks must be merged
	auto &left_block = *state.sorted_blocks[state.pair_idx * 2];
	auto &right_block = *state.sorted_blocks[state.pair_idx * 2 + 1];
	const idx_t l_count = left_block.Count();
	const idx_t r_count = right_block.Count();
	// Initialize left and right reader
	left = make_unique<SBScanState>(buffer_manager, state);
	right = make_unique<SBScanState>(buffer_manager, state);
	// Compute the work that this thread must do using Merge Path
	idx_t l_end;
	idx_t r_end;
	if (state.l_start + state.r_start + state.block_capacity < l_count + r_count) {
		left->sb = state.sorted_blocks[state.pair_idx * 2].get();
		right->sb = state.sorted_blocks[state.pair_idx * 2 + 1].get();
		const idx_t intersection = state.l_start + state.r_start + state.block_capacity;
		GetIntersection(intersection, l_end, r_end);
		D_ASSERT(l_end <= l_count);
		D_ASSERT(r_end <= r_count);
		D_ASSERT(intersection == l_end + r_end);
	} else {
		l_end = l_count;
		r_end = r_count;
	}
	// Create slices of the data that this thread must merge
	left->SetIndices(0, 0);
	right->SetIndices(0, 0);
	left_input = left_block.CreateSlice(state.l_start, l_end, left->entry_idx);
	right_input = right_block.CreateSlice(state.r_start, r_end, right->entry_idx);
	left->sb = left_input.get();
	right->sb = right_input.get();
	state.l_start = l_end;
	state.r_start = r_end;
	D_ASSERT(left->Remaining() + right->Remaining() == state.block_capacity || (l_end == l_count && r_end == r_count));
	// Update global state
	if (state.l_start == l_count && state.r_start == r_count) {
		// Delete references to previous pair
		state.sorted_blocks[state.pair_idx * 2] = nullptr;
		state.sorted_blocks[state.pair_idx * 2 + 1] = nullptr;
		// Advance pair
		state.pair_idx++;
		state.l_start = 0;
		state.r_start = 0;
	}
}

int MergeSorter::CompareUsingGlobalIndex(SBScanState &l, SBScanState &r, const idx_t l_idx, const idx_t r_idx) {
	D_ASSERT(l_idx < l.sb->Count());
	D_ASSERT(r_idx < r.sb->Count());

	// Easy comparison using the previous result (intersections must increase monotonically)
	if (l_idx < state.l_start) {
		return -1;
	}
	if (r_idx < state.r_start) {
		return 1;
	}

	l.sb->GlobalToLocalIndex(l_idx, l.block_idx, l.entry_idx);
	r.sb->GlobalToLocalIndex(r_idx, r.block_idx, r.entry_idx);

	l.PinRadix(l.block_idx);
	r.PinRadix(r.block_idx);
	data_ptr_t l_ptr = l.radix_handle.Ptr() + l.entry_idx * sort_layout.entry_size;
	data_ptr_t r_ptr = r.radix_handle.Ptr() + r.entry_idx * sort_layout.entry_size;

	int comp_res;
	if (sort_layout.all_constant) {
		comp_res = FastMemcmp(l_ptr, r_ptr, sort_layout.comparison_size);
	} else {
		l.PinData(*l.sb->blob_sorting_data);
		r.PinData(*r.sb->blob_sorting_data);
		comp_res = Comparators::CompareTuple(l, r, l_ptr, r_ptr, sort_layout, state.external);
	}
	return comp_res;
}

void MergeSorter::GetIntersection(const idx_t diagonal, idx_t &l_idx, idx_t &r_idx) {
	const idx_t l_count = left->sb->Count();
	const idx_t r_count = right->sb->Count();
	// Cover some edge cases
	// Code coverage off because these edge cases cannot happen unless other code changes
	// Edge cases have been tested extensively while developing Merge Path in a script
	// LCOV_EXCL_START
	if (diagonal >= l_count + r_count) {
		l_idx = l_count;
		r_idx = r_count;
		return;
	} else if (diagonal == 0) {
		l_idx = 0;
		r_idx = 0;
		return;
	} else if (l_count == 0) {
		l_idx = 0;
		r_idx = diagonal;
		return;
	} else if (r_count == 0) {
		r_idx = 0;
		l_idx = diagonal;
		return;
	}
	// LCOV_EXCL_STOP
	// Determine offsets for the binary search
	const idx_t l_offset = MinValue(l_count, diagonal);
	const idx_t r_offset = diagonal > l_count ? diagonal - l_count : 0;
	D_ASSERT(l_offset + r_offset == diagonal);
	const idx_t search_space = diagonal > MaxValue(l_count, r_count) ? l_count + r_count - diagonal
	                                                                 : MinValue(diagonal, MinValue(l_count, r_count));
	// Double binary search
	idx_t li = 0;
	idx_t ri = search_space - 1;
	idx_t middle;
	int comp_res;
	while (li <= ri) {
		middle = (li + ri) / 2;
		l_idx = l_offset - middle;
		r_idx = r_offset + middle;
		if (l_idx == l_count || r_idx == 0) {
			comp_res = CompareUsingGlobalIndex(*left, *right, l_idx - 1, r_idx);
			if (comp_res > 0) {
				l_idx--;
				r_idx++;
			} else {
				return;
			}
			if (l_idx == 0 || r_idx == r_count) {
				// This case is incredibly difficult to cover as it is dependent on parallelism randomness
				// But it has been tested extensively during development in a script
				// LCOV_EXCL_START
				return;
				// LCOV_EXCL_STOP
			} else {
				break;
			}
		}
		comp_res = CompareUsingGlobalIndex(*left, *right, l_idx, r_idx);
		if (comp_res > 0) {
			li = middle + 1;
		} else {
			ri = middle - 1;
		}
	}
	int l_r_min1 = CompareUsingGlobalIndex(*left, *right, l_idx, r_idx - 1);
	int l_min1_r = CompareUsingGlobalIndex(*left, *right, l_idx - 1, r_idx);
	if (l_r_min1 > 0 && l_min1_r < 0) {
		return;
	} else if (l_r_min1 > 0) {
		l_idx--;
		r_idx++;
	} else if (l_min1_r < 0) {
		l_idx++;
		r_idx--;
	}
}

void MergeSorter::ComputeMerge(const idx_t &count, bool left_smaller[]) {
	auto &l = *left;
	auto &r = *right;
	auto &l_sorted_block = *l.sb;
	auto &r_sorted_block = *r.sb;
	// Save indices to restore afterwards
	idx_t l_block_idx_before = l.block_idx;
	idx_t l_entry_idx_before = l.entry_idx;
	idx_t r_block_idx_before = r.block_idx;
	idx_t r_entry_idx_before = r.entry_idx;
	// Data pointers for both sides
	data_ptr_t l_radix_ptr;
	data_ptr_t r_radix_ptr;
	// Compute the merge of the next 'count' tuples
	idx_t compared = 0;
	while (compared < count) {
		// Move to the next block (if needed)
		if (l.block_idx < l_sorted_block.radix_sorting_data.size() &&
		    l.entry_idx == l_sorted_block.radix_sorting_data[l.block_idx].count) {
			l.block_idx++;
			l.entry_idx = 0;
		}
		if (r.block_idx < r_sorted_block.radix_sorting_data.size() &&
		    r.entry_idx == r_sorted_block.radix_sorting_data[r.block_idx].count) {
			r.block_idx++;
			r.entry_idx = 0;
		}
		const bool l_done = l.block_idx == l_sorted_block.radix_sorting_data.size();
		const bool r_done = r.block_idx == r_sorted_block.radix_sorting_data.size();
		if (l_done || r_done) {
			// One of the sides is exhausted, no need to compare
			break;
		}
		// Pin the radix sorting data
		if (!l_done) {
			left->PinRadix(l.block_idx);
			l_radix_ptr = left->RadixPtr();
		}
		if (!r_done) {
			right->PinRadix(r.block_idx);
			r_radix_ptr = right->RadixPtr();
		}
		const idx_t &l_count = !l_done ? l_sorted_block.radix_sorting_data[l.block_idx].count : 0;
		const idx_t &r_count = !r_done ? r_sorted_block.radix_sorting_data[r.block_idx].count : 0;
		// Compute the merge
		if (sort_layout.all_constant) {
			// All sorting columns are constant size
			for (; compared < count && l.entry_idx < l_count && r.entry_idx < r_count; compared++) {
				left_smaller[compared] = FastMemcmp(l_radix_ptr, r_radix_ptr, sort_layout.comparison_size) < 0;
				const bool &l_smaller = left_smaller[compared];
				const bool r_smaller = !l_smaller;
				// Use comparison bool (0 or 1) to increment entries and pointers
				l.entry_idx += l_smaller;
				r.entry_idx += r_smaller;
				l_radix_ptr += l_smaller * sort_layout.entry_size;
				r_radix_ptr += r_smaller * sort_layout.entry_size;
			}
		} else {
			// Pin the blob data
			if (!l_done) {
				left->PinData(*l_sorted_block.blob_sorting_data);
			}
			if (!r_done) {
				right->PinData(*r_sorted_block.blob_sorting_data);
			}
			// Merge with variable size sorting columns
			for (; compared < count && l.entry_idx < l_count && r.entry_idx < r_count; compared++) {
				left_smaller[compared] =
				    Comparators::CompareTuple(*left, *right, l_radix_ptr, r_radix_ptr, sort_layout, state.external) < 0;
				const bool &l_smaller = left_smaller[compared];
				const bool r_smaller = !l_smaller;
				// Use comparison bool (0 or 1) to increment entries and pointers
				l.entry_idx += l_smaller;
				r.entry_idx += r_smaller;
				l_radix_ptr += l_smaller * sort_layout.entry_size;
				r_radix_ptr += r_smaller * sort_layout.entry_size;
			}
		}
	}
	// Reset block indices
	left->SetIndices(l_block_idx_before, l_entry_idx_before);
	right->SetIndices(r_block_idx_before, r_entry_idx_before);
}

void MergeSorter::MergeRadix(const idx_t &count, const bool left_smaller[]) {
	auto &l = *left;
	auto &r = *right;
	// Save indices to restore afterwards
	idx_t l_block_idx_before = l.block_idx;
	idx_t l_entry_idx_before = l.entry_idx;
	idx_t r_block_idx_before = r.block_idx;
	idx_t r_entry_idx_before = r.entry_idx;

	auto &l_blocks = l.sb->radix_sorting_data;
	auto &r_blocks = r.sb->radix_sorting_data;
	RowDataBlock *l_block;
	RowDataBlock *r_block;

	data_ptr_t l_ptr;
	data_ptr_t r_ptr;

	RowDataBlock *result_block = &result->radix_sorting_data.back();
	auto result_handle = buffer_manager.Pin(result_block->block);
	data_ptr_t result_ptr = result_handle.Ptr() + result_block->count * sort_layout.entry_size;

	idx_t copied = 0;
	while (copied < count) {
		// Move to the next block (if needed)
		if (l.block_idx < l_blocks.size() && l.entry_idx == l_blocks[l.block_idx].count) {
			// Delete reference to previous block
			l_blocks[l.block_idx].block = nullptr;
			// Advance block
			l.block_idx++;
			l.entry_idx = 0;
		}
		if (r.block_idx < r_blocks.size() && r.entry_idx == r_blocks[r.block_idx].count) {
			// Delete reference to previous block
			r_blocks[r.block_idx].block = nullptr;
			// Advance block
			r.block_idx++;
			r.entry_idx = 0;
		}
		const bool l_done = l.block_idx == l_blocks.size();
		const bool r_done = r.block_idx == r_blocks.size();
		// Pin the radix sortable blocks
		if (!l_done) {
			l_block = &l_blocks[l.block_idx];
			left->PinRadix(l.block_idx);
			l_ptr = l.RadixPtr();
		}
		if (!r_done) {
			r_block = &r_blocks[r.block_idx];
			r.PinRadix(r.block_idx);
			r_ptr = r.RadixPtr();
		}
		const idx_t &l_count = !l_done ? l_block->count : 0;
		const idx_t &r_count = !r_done ? r_block->count : 0;
		// Copy using computed merge
		if (!l_done && !r_done) {
			// Both sides have data - merge
			MergeRows(l_ptr, l.entry_idx, l_count, r_ptr, r.entry_idx, r_count, result_block, result_ptr,
			          sort_layout.entry_size, left_smaller, copied, count);
		} else if (r_done) {
			// Right side is exhausted
			FlushRows(l_ptr, l.entry_idx, l_count, result_block, result_ptr, sort_layout.entry_size, copied, count);
		} else {
			// Left side is exhausted
			FlushRows(r_ptr, r.entry_idx, r_count, result_block, result_ptr, sort_layout.entry_size, copied, count);
		}
	}
	// Reset block indices
	left->SetIndices(l_block_idx_before, l_entry_idx_before);
	right->SetIndices(r_block_idx_before, r_entry_idx_before);
}

void MergeSorter::MergeData(SortedData &result_data, SortedData &l_data, SortedData &r_data, const idx_t &count,
                            const bool left_smaller[], idx_t next_entry_sizes[], bool reset_indices) {
	auto &l = *left;
	auto &r = *right;
	// Save indices to restore afterwards
	idx_t l_block_idx_before = l.block_idx;
	idx_t l_entry_idx_before = l.entry_idx;
	idx_t r_block_idx_before = r.block_idx;
	idx_t r_entry_idx_before = r.entry_idx;

	const auto &layout = result_data.layout;
	const idx_t row_width = layout.GetRowWidth();
	const idx_t heap_pointer_offset = layout.GetHeapPointerOffset();

	// Left and right row data to merge
	data_ptr_t l_ptr;
	data_ptr_t r_ptr;
	// Accompanying left and right heap data (if needed)
	data_ptr_t l_heap_ptr;
	data_ptr_t r_heap_ptr;

	// Result rows to write to
	RowDataBlock *result_data_block = &result_data.data_blocks.back();
	auto result_data_handle = buffer_manager.Pin(result_data_block->block);
	data_ptr_t result_data_ptr = result_data_handle.Ptr() + result_data_block->count * row_width;
	// Result heap to write to (if needed)
	RowDataBlock *result_heap_block;
	BufferHandle result_heap_handle;
	data_ptr_t result_heap_ptr;
	if (!layout.AllConstant() && state.external) {
		result_heap_block = &result_data.heap_blocks.back();
		result_heap_handle = buffer_manager.Pin(result_heap_block->block);
		result_heap_ptr = result_heap_handle.Ptr() + result_heap_block->byte_offset;
	}

	idx_t copied = 0;
	while (copied < count) {
		// Move to new data blocks (if needed)
		if (l.block_idx < l_data.data_blocks.size() && l.entry_idx == l_data.data_blocks[l.block_idx].count) {
			// Delete reference to previous block
			l_data.data_blocks[l.block_idx].block = nullptr;
			if (!layout.AllConstant() && state.external) {
				l_data.heap_blocks[l.block_idx].block = nullptr;
			}
			// Advance block
			l.block_idx++;
			l.entry_idx = 0;
		}
		if (r.block_idx < r_data.data_blocks.size() && r.entry_idx == r_data.data_blocks[r.block_idx].count) {
			// Delete reference to previous block
			r_data.data_blocks[r.block_idx].block = nullptr;
			if (!layout.AllConstant() && state.external) {
				r_data.heap_blocks[r.block_idx].block = nullptr;
			}
			// Advance block
			r.block_idx++;
			r.entry_idx = 0;
		}
		const bool l_done = l.block_idx == l_data.data_blocks.size();
		const bool r_done = r.block_idx == r_data.data_blocks.size();
		// Pin the row data blocks
		if (!l_done) {
			l.PinData(l_data);
			l_ptr = l.DataPtr(l_data);
		}
		if (!r_done) {
			r.PinData(r_data);
			r_ptr = r.DataPtr(r_data);
		}
		const idx_t &l_count = !l_done ? l_data.data_blocks[l.block_idx].count : 0;
		const idx_t &r_count = !r_done ? r_data.data_blocks[r.block_idx].count : 0;
		// Perform the merge
		if (layout.AllConstant() || !state.external) {
			// If all constant size, or if we are doing an in-memory sort, we do not need to touch the heap
			if (!l_done && !r_done) {
				// Both sides have data - merge
				MergeRows(l_ptr, l.entry_idx, l_count, r_ptr, r.entry_idx, r_count, result_data_block, result_data_ptr,
				          row_width, left_smaller, copied, count);
			} else if (r_done) {
				// Right side is exhausted
				FlushRows(l_ptr, l.entry_idx, l_count, result_data_block, result_data_ptr, row_width, copied, count);
			} else {
				// Left side is exhausted
				FlushRows(r_ptr, r.entry_idx, r_count, result_data_block, result_data_ptr, row_width, copied, count);
			}
		} else {
			// External sorting with variable size data. Pin the heap blocks too
			if (!l_done) {
				l_heap_ptr = l.BaseHeapPtr(l_data) + Load<idx_t>(l_ptr + heap_pointer_offset);
				D_ASSERT(l_heap_ptr - l.BaseHeapPtr(l_data) >= 0);
				D_ASSERT((idx_t)(l_heap_ptr - l.BaseHeapPtr(l_data)) < l_data.heap_blocks[l.block_idx].byte_offset);
			}
			if (!r_done) {
				r_heap_ptr = r.BaseHeapPtr(r_data) + Load<idx_t>(r_ptr + heap_pointer_offset);
				D_ASSERT(r_heap_ptr - r.BaseHeapPtr(r_data) >= 0);
				D_ASSERT((idx_t)(r_heap_ptr - r.BaseHeapPtr(r_data)) < r_data.heap_blocks[r.block_idx].byte_offset);
			}
			// Both the row and heap data need to be dealt with
			if (!l_done && !r_done) {
				// Both sides have data - merge
				idx_t l_idx_copy = l.entry_idx;
				idx_t r_idx_copy = r.entry_idx;
				data_ptr_t result_data_ptr_copy = result_data_ptr;
				idx_t copied_copy = copied;
				// Merge row data
				MergeRows(l_ptr, l_idx_copy, l_count, r_ptr, r_idx_copy, r_count, result_data_block,
				          result_data_ptr_copy, row_width, left_smaller, copied_copy, count);
				const idx_t merged = copied_copy - copied;
				// Compute the entry sizes and number of heap bytes that will be copied
				idx_t copy_bytes = 0;
				data_ptr_t l_heap_ptr_copy = l_heap_ptr;
				data_ptr_t r_heap_ptr_copy = r_heap_ptr;
				for (idx_t i = 0; i < merged; i++) {
					// Store base heap offset in the row data
					Store<idx_t>(result_heap_block->byte_offset + copy_bytes, result_data_ptr + heap_pointer_offset);
					result_data_ptr += row_width;
					// Compute entry size and add to total
					const bool &l_smaller = left_smaller[copied + i];
					const bool r_smaller = !l_smaller;
					auto &entry_size = next_entry_sizes[copied + i];
					entry_size =
					    l_smaller * Load<uint32_t>(l_heap_ptr_copy) + r_smaller * Load<uint32_t>(r_heap_ptr_copy);
					D_ASSERT(entry_size >= sizeof(uint32_t));
					D_ASSERT(l_heap_ptr_copy - l.BaseHeapPtr(l_data) + l_smaller * entry_size <=
					         l_data.heap_blocks[l.block_idx].byte_offset);
					D_ASSERT(r_heap_ptr_copy - r.BaseHeapPtr(r_data) + r_smaller * entry_size <=
					         r_data.heap_blocks[r.block_idx].byte_offset);
					l_heap_ptr_copy += l_smaller * entry_size;
					r_heap_ptr_copy += r_smaller * entry_size;
					copy_bytes += entry_size;
				}
				// Reallocate result heap block size (if needed)
				if (result_heap_block->byte_offset + copy_bytes > result_heap_block->capacity) {
					idx_t new_capacity = result_heap_block->byte_offset + copy_bytes;
					buffer_manager.ReAllocate(result_heap_block->block, new_capacity);
					result_heap_block->capacity = new_capacity;
					result_heap_ptr = result_heap_handle.Ptr() + result_heap_block->byte_offset;
				}
				D_ASSERT(result_heap_block->byte_offset + copy_bytes <= result_heap_block->capacity);
				// Now copy the heap data
				for (idx_t i = 0; i < merged; i++) {
					const bool &l_smaller = left_smaller[copied + i];
					const bool r_smaller = !l_smaller;
					const auto &entry_size = next_entry_sizes[copied + i];
					memcpy(result_heap_ptr, (data_ptr_t)(l_smaller * (idx_t)l_heap_ptr + r_smaller * (idx_t)r_heap_ptr),
					       entry_size);
					D_ASSERT(Load<uint32_t>(result_heap_ptr) == entry_size);
					result_heap_ptr += entry_size;
					l_heap_ptr += l_smaller * entry_size;
					r_heap_ptr += r_smaller * entry_size;
					l.entry_idx += l_smaller;
					r.entry_idx += r_smaller;
				}
				// Update result indices and pointers
				result_heap_block->count += merged;
				result_heap_block->byte_offset += copy_bytes;
				copied += merged;
			} else if (r_done) {
				// Right side is exhausted - flush left
				FlushBlobs(layout, l_count, l_ptr, l.entry_idx, l_heap_ptr, result_data_block, result_data_ptr,
				           result_heap_block, result_heap_handle, result_heap_ptr, copied, count);
			} else {
				// Left side is exhausted - flush right
				FlushBlobs(layout, r_count, r_ptr, r.entry_idx, r_heap_ptr, result_data_block, result_data_ptr,
				           result_heap_block, result_heap_handle, result_heap_ptr, copied, count);
			}
			D_ASSERT(result_data_block->count == result_heap_block->count);
		}
	}
	if (reset_indices) {
		left->SetIndices(l_block_idx_before, l_entry_idx_before);
		right->SetIndices(r_block_idx_before, r_entry_idx_before);
	}
}

void MergeSorter::MergeRows(data_ptr_t &l_ptr, idx_t &l_entry_idx, const idx_t &l_count, data_ptr_t &r_ptr,
                            idx_t &r_entry_idx, const idx_t &r_count, RowDataBlock *target_block,
                            data_ptr_t &target_ptr, const idx_t &entry_size, const bool left_smaller[], idx_t &copied,
                            const idx_t &count) {
	const idx_t next = MinValue(count - copied, target_block->capacity - target_block->count);
	idx_t i;
	for (i = 0; i < next && l_entry_idx < l_count && r_entry_idx < r_count; i++) {
		const bool &l_smaller = left_smaller[copied + i];
		const bool r_smaller = !l_smaller;
		// Use comparison bool (0 or 1) to copy an entry from either side
		FastMemcpy(target_ptr, (data_ptr_t)(l_smaller * (idx_t)l_ptr + r_smaller * (idx_t)r_ptr), entry_size);
		target_ptr += entry_size;
		// Use the comparison bool to increment entries and pointers
		l_entry_idx += l_smaller;
		r_entry_idx += r_smaller;
		l_ptr += l_smaller * entry_size;
		r_ptr += r_smaller * entry_size;
	}
	// Update counts
	target_block->count += i;
	copied += i;
}

void MergeSorter::FlushRows(data_ptr_t &source_ptr, idx_t &source_entry_idx, const idx_t &source_count,
                            RowDataBlock *target_block, data_ptr_t &target_ptr, const idx_t &entry_size, idx_t &copied,
                            const idx_t &count) {
	// Compute how many entries we can fit
	idx_t next = MinValue(count - copied, target_block->capacity - target_block->count);
	next = MinValue(next, source_count - source_entry_idx);
	// Copy them all in a single memcpy
	const idx_t copy_bytes = next * entry_size;
	memcpy(target_ptr, source_ptr, copy_bytes);
	target_ptr += copy_bytes;
	source_ptr += copy_bytes;
	// Update counts
	source_entry_idx += next;
	target_block->count += next;
	copied += next;
}

void MergeSorter::FlushBlobs(const RowLayout &layout, const idx_t &source_count, data_ptr_t &source_data_ptr,
                             idx_t &source_entry_idx, data_ptr_t &source_heap_ptr, RowDataBlock *target_data_block,
                             data_ptr_t &target_data_ptr, RowDataBlock *target_heap_block,
                             BufferHandle &target_heap_handle, data_ptr_t &target_heap_ptr, idx_t &copied,
                             const idx_t &count) {
	const idx_t row_width = layout.GetRowWidth();
	const idx_t heap_pointer_offset = layout.GetHeapPointerOffset();
	idx_t source_entry_idx_copy = source_entry_idx;
	data_ptr_t target_data_ptr_copy = target_data_ptr;
	idx_t copied_copy = copied;
	// Flush row data
	FlushRows(source_data_ptr, source_entry_idx_copy, source_count, target_data_block, target_data_ptr_copy, row_width,
	          copied_copy, count);
	const idx_t flushed = copied_copy - copied;
	// Compute the entry sizes and number of heap bytes that will be copied
	idx_t copy_bytes = 0;
	data_ptr_t source_heap_ptr_copy = source_heap_ptr;
	for (idx_t i = 0; i < flushed; i++) {
		// Store base heap offset in the row data
		Store<idx_t>(target_heap_block->byte_offset + copy_bytes, target_data_ptr + heap_pointer_offset);
		target_data_ptr += row_width;
		// Compute entry size and add to total
		auto entry_size = Load<uint32_t>(source_heap_ptr_copy);
		D_ASSERT(entry_size >= sizeof(uint32_t));
		source_heap_ptr_copy += entry_size;
		copy_bytes += entry_size;
	}
	// Reallocate result heap block size (if needed)
	if (target_heap_block->byte_offset + copy_bytes > target_heap_block->capacity) {
		idx_t new_capacity = target_heap_block->byte_offset + copy_bytes;
		buffer_manager.ReAllocate(target_heap_block->block, new_capacity);
		target_heap_block->capacity = new_capacity;
		target_heap_ptr = target_heap_handle.Ptr() + target_heap_block->byte_offset;
	}
	D_ASSERT(target_heap_block->byte_offset + copy_bytes <= target_heap_block->capacity);
	// Copy the heap data in one go
	memcpy(target_heap_ptr, source_heap_ptr, copy_bytes);
	target_heap_ptr += copy_bytes;
	source_heap_ptr += copy_bytes;
	source_entry_idx += flushed;
	copied += flushed;
	// Update result indices and pointers
	target_heap_block->count += flushed;
	target_heap_block->byte_offset += copy_bytes;
	D_ASSERT(target_heap_block->byte_offset <= target_heap_block->capacity);
}

} // namespace duckdb
