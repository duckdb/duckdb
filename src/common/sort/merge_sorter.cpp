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
	auto &left = *left_block;
	auto &right = *right_block;
#ifdef DEBUG
	D_ASSERT(left.radix_sorting_data.size() == left.payload_data->data_blocks.size());
	D_ASSERT(right.radix_sorting_data.size() == right.payload_data->data_blocks.size());
	if (!state.payload_layout.AllConstant() && state.external) {
		D_ASSERT(left.payload_data->data_blocks.size() == left.payload_data->heap_blocks.size());
		D_ASSERT(right.payload_data->data_blocks.size() == right.payload_data->heap_blocks.size());
	}
	if (!sort_layout.all_constant) {
		D_ASSERT(left.radix_sorting_data.size() == left.blob_sorting_data->data_blocks.size());
		D_ASSERT(right.radix_sorting_data.size() == right.blob_sorting_data->data_blocks.size());
		if (state.external) {
			D_ASSERT(left.blob_sorting_data->data_blocks.size() == left.blob_sorting_data->heap_blocks.size());
			D_ASSERT(right.blob_sorting_data->data_blocks.size() == right.blob_sorting_data->heap_blocks.size());
		}
	}
#endif
	// Set up the write block
	// Each merge task produces a SortedBlock with exactly state.rows_per_merge_partition rows or less
	result->InitializeWrite();
	// Initialize arrays to store merge data
	bool left_smaller[STANDARD_VECTOR_SIZE];
	idx_t next_entry_sizes[STANDARD_VECTOR_SIZE];
	// Merge loop
#ifdef DEBUG
	auto l_count = left.Remaining();
	auto r_count = right.Remaining();
#endif
	while (true) {
		auto l_remaining = left.Remaining();
		auto r_remaining = right.Remaining();
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
			MergeData(*result->blob_sorting_data, *left.blob_sorting_data, *right.blob_sorting_data, next, left_smaller,
			          next_entry_sizes);
			D_ASSERT(left.block_idx == left.blob_sorting_data->block_idx &&
			         left.entry_idx == left.blob_sorting_data->entry_idx);
			D_ASSERT(right.block_idx == right.blob_sorting_data->block_idx &&
			         right.entry_idx == right.blob_sorting_data->entry_idx);
			D_ASSERT(result->radix_sorting_data.size() == result->blob_sorting_data->data_blocks.size());
		}
		MergeData(*result->payload_data, *left.payload_data, *right.payload_data, next, left_smaller, next_entry_sizes);
		D_ASSERT(left.block_idx == left.payload_data->block_idx && left.entry_idx == left.payload_data->entry_idx);
		D_ASSERT(right.block_idx == right.payload_data->block_idx && right.entry_idx == right.payload_data->entry_idx);
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
	auto &left = *state.sorted_blocks[state.pair_idx * 2];
	auto &right = *state.sorted_blocks[state.pair_idx * 2 + 1];
	const idx_t l_count = left.Count();
	const idx_t r_count = right.Count();
	// Compute the work that this thread must do using Merge Path
	idx_t l_end;
	idx_t r_end;
	if (state.l_start + state.r_start + state.rows_per_merge_partition < l_count + r_count) {
		const idx_t intersection = state.l_start + state.r_start + state.rows_per_merge_partition;
		GetIntersection(left, right, intersection, l_end, r_end);
		D_ASSERT(l_end <= l_count);
		D_ASSERT(r_end <= r_count);
		D_ASSERT(intersection == l_end + r_end);
		// Unpin after finding the intersection
		if (!sort_layout.blob_layout.AllConstant()) {
			left.blob_sorting_data->ResetIndices(0, 0);
			right.blob_sorting_data->ResetIndices(0, 0);
		}
	} else {
		l_end = l_count;
		r_end = r_count;
	}
	// Create slices of the data that this thread must merge
	left_block = left.CreateSlice(state.l_start, l_end);
	right_block = right.CreateSlice(state.r_start, r_end);
	// Update global state
	state.l_start = l_end;
	state.r_start = r_end;
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

int MergeSorter::CompareUsingGlobalIndex(SortedBlock &l, SortedBlock &r, const idx_t l_idx, const idx_t r_idx) {
	D_ASSERT(l_idx < l.Count());
	D_ASSERT(r_idx < r.Count());

	// Easy comparison using the previous result (intersections must increase monotonically)
	if (l_idx < state.l_start) {
		return -1;
	}
	if (r_idx < state.r_start) {
		return 1;
	}

	idx_t l_block_idx;
	idx_t l_entry_idx;
	l.GlobalToLocalIndex(l_idx, l_block_idx, l_entry_idx);

	idx_t r_block_idx;
	idx_t r_entry_idx;
	r.GlobalToLocalIndex(r_idx, r_block_idx, r_entry_idx);

	l.PinRadix(l_block_idx);
	r.PinRadix(r_block_idx);
	data_ptr_t l_ptr = l.radix_handle->Ptr() + l_entry_idx * sort_layout.entry_size;
	data_ptr_t r_ptr = r.radix_handle->Ptr() + r_entry_idx * sort_layout.entry_size;

	int comp_res;
	if (sort_layout.all_constant) {
		comp_res = memcmp(l_ptr, r_ptr, sort_layout.comparison_size);
	} else {
		l.blob_sorting_data->block_idx = l_block_idx;
		l.blob_sorting_data->entry_idx = l_entry_idx;
		l.blob_sorting_data->Pin();
		r.blob_sorting_data->block_idx = r_block_idx;
		r.blob_sorting_data->entry_idx = r_entry_idx;
		r.blob_sorting_data->Pin();
		comp_res = Comparators::CompareTuple(l, r, l_ptr, r_ptr, sort_layout, state.external);
	}
	return comp_res;
}

void MergeSorter::GetIntersection(SortedBlock &l, SortedBlock &r, const idx_t diagonal, idx_t &l_idx, idx_t &r_idx) {
	const idx_t l_count = l.Count();
	const idx_t r_count = r.Count();
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
	idx_t left = 0;
	idx_t right = search_space - 1;
	idx_t middle;
	int comp_res;
	while (left <= right) {
		middle = (left + right) / 2;
		l_idx = l_offset - middle;
		r_idx = r_offset + middle;
		if (l_idx == l_count || r_idx == 0) {
			comp_res = CompareUsingGlobalIndex(l, r, l_idx - 1, r_idx);
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
		comp_res = CompareUsingGlobalIndex(l, r, l_idx, r_idx);
		if (comp_res > 0) {
			left = middle + 1;
		} else {
			right = middle - 1;
		}
	}
	int l_r_min1 = CompareUsingGlobalIndex(l, r, l_idx, r_idx - 1);
	int l_min1_r = CompareUsingGlobalIndex(l, r, l_idx - 1, r_idx);
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
	auto &left = *left_block;
	auto &right = *right_block;
	// Store indices to restore after computing the merge
	idx_t l_block_idx = left.block_idx;
	idx_t r_block_idx = right.block_idx;
	idx_t l_entry_idx = left.entry_idx;
	idx_t r_entry_idx = right.entry_idx;
	// Data pointers for both sides
	data_ptr_t l_radix_ptr;
	data_ptr_t r_radix_ptr;
	// Compute the merge of the next 'count' tuples
	idx_t compared = 0;
	while (compared < count) {
		// Move to the next block (if needed)
		if (l_block_idx < left.radix_sorting_data.size() && l_entry_idx == left.radix_sorting_data[l_block_idx].count) {
			l_block_idx++;
			l_entry_idx = 0;
			if (!sort_layout.all_constant) {
				left.blob_sorting_data->block_idx = l_block_idx;
				left.blob_sorting_data->entry_idx = l_entry_idx;
			}
		}
		if (r_block_idx < right.radix_sorting_data.size() &&
		    r_entry_idx == right.radix_sorting_data[r_block_idx].count) {
			r_block_idx++;
			r_entry_idx = 0;
			if (!sort_layout.all_constant) {
				right.blob_sorting_data->block_idx = r_block_idx;
				right.blob_sorting_data->entry_idx = r_entry_idx;
			}
		}
		const bool l_done = l_block_idx == left.radix_sorting_data.size();
		const bool r_done = r_block_idx == right.radix_sorting_data.size();
		if (l_done || r_done) {
			// One of the sides is exhausted, no need to compare
			break;
		}
		// Pin the radix sorting data
		if (!l_done) {
			left.PinRadix(l_block_idx);
			l_radix_ptr = left.radix_handle->Ptr() + l_entry_idx * sort_layout.entry_size;
		}
		if (!r_done) {
			right.PinRadix(r_block_idx);
			r_radix_ptr = right.radix_handle->Ptr() + r_entry_idx * sort_layout.entry_size;
		}
		const idx_t &l_count = !l_done ? left.radix_sorting_data[l_block_idx].count : 0;
		const idx_t &r_count = !r_done ? right.radix_sorting_data[r_block_idx].count : 0;
		// Compute the merge
		if (sort_layout.all_constant) {
			// All sorting columns are constant size
			for (; compared < count && l_entry_idx < l_count && r_entry_idx < r_count; compared++) {
				left_smaller[compared] = memcmp(l_radix_ptr, r_radix_ptr, sort_layout.comparison_size) < 0;
				const bool &l_smaller = left_smaller[compared];
				const bool r_smaller = !l_smaller;
				// Use comparison bool (0 or 1) to increment entries and pointers
				l_entry_idx += l_smaller;
				r_entry_idx += r_smaller;
				l_radix_ptr += l_smaller * sort_layout.entry_size;
				r_radix_ptr += r_smaller * sort_layout.entry_size;
			}
		} else {
			// Pin the blob data
			if (!l_done) {
				left.blob_sorting_data->Pin();
			}
			if (!r_done) {
				right.blob_sorting_data->Pin();
			}
			// Merge with variable size sorting columns
			for (; compared < count && l_entry_idx < l_count && r_entry_idx < r_count; compared++) {
				D_ASSERT(l_block_idx == left.blob_sorting_data->block_idx &&
				         l_entry_idx == left.blob_sorting_data->entry_idx);
				D_ASSERT(r_block_idx == right.blob_sorting_data->block_idx &&
				         r_entry_idx == right.blob_sorting_data->entry_idx);
				left_smaller[compared] =
				    Comparators::CompareTuple(left, right, l_radix_ptr, r_radix_ptr, sort_layout, state.external) < 0;
				const bool &l_smaller = left_smaller[compared];
				const bool r_smaller = !l_smaller;
				// Use comparison bool (0 or 1) to increment entries and pointers
				l_entry_idx += l_smaller;
				r_entry_idx += r_smaller;
				l_radix_ptr += l_smaller * sort_layout.entry_size;
				r_radix_ptr += r_smaller * sort_layout.entry_size;
				left.blob_sorting_data->Advance(l_smaller);
				right.blob_sorting_data->Advance(r_smaller);
			}
		}
	}
	// Reset block indices before the actual merge
	if (!sort_layout.all_constant) {
		left.blob_sorting_data->ResetIndices(left.block_idx, left.entry_idx);
		right.blob_sorting_data->ResetIndices(right.block_idx, right.entry_idx);
	}
}

void MergeSorter::MergeRadix(const idx_t &count, const bool left_smaller[]) {
	auto &left = *left_block;
	auto &right = *right_block;
	RowDataBlock *l_block;
	RowDataBlock *r_block;

	data_ptr_t l_ptr;
	data_ptr_t r_ptr;

	RowDataBlock *result_block = &result->radix_sorting_data.back();
	auto result_handle = buffer_manager.Pin(result_block->block);
	data_ptr_t result_ptr = result_handle->Ptr() + result_block->count * sort_layout.entry_size;

	idx_t copied = 0;
	while (copied < count) {
		// Move to the next block (if needed)
		if (left.block_idx < left.radix_sorting_data.size() &&
		    left.entry_idx == left.radix_sorting_data[left.block_idx].count) {
			// Delete reference to previous block
			left.radix_sorting_data[left.block_idx].block = nullptr;
			// Advance block
			left.block_idx++;
			left.entry_idx = 0;
		}
		if (right.block_idx < right.radix_sorting_data.size() &&
		    right.entry_idx == right.radix_sorting_data[right.block_idx].count) {
			// Delete reference to previous block
			right.radix_sorting_data[right.block_idx].block = nullptr;
			// Advance block
			right.block_idx++;
			right.entry_idx = 0;
		}
		const bool l_done = left.block_idx == left.radix_sorting_data.size();
		const bool r_done = right.block_idx == right.radix_sorting_data.size();
		// Pin the radix sortable blocks
		if (!l_done) {
			l_block = &left.radix_sorting_data[left.block_idx];
			left.PinRadix(left.block_idx);
			l_ptr = left.radix_handle->Ptr() + left.entry_idx * sort_layout.entry_size;
		}
		if (!r_done) {
			r_block = &right.radix_sorting_data[right.block_idx];
			right.PinRadix(right.block_idx);
			r_ptr = right.radix_handle->Ptr() + right.entry_idx * sort_layout.entry_size;
		}
		const idx_t &l_count = !l_done ? l_block->count : 0;
		const idx_t &r_count = !r_done ? r_block->count : 0;
		// Create new result block (if needed)
		if (result_block->count == state.block_capacity) {
			result->CreateBlock();
			result_block = &result->radix_sorting_data.back();
			result_handle = buffer_manager.Pin(result_block->block);
			result_ptr = result_handle->Ptr();
		}
		// Copy using computed merge
		if (!l_done && !r_done) {
			// Both sides have data - merge
			MergeRows(l_ptr, left.entry_idx, l_count, r_ptr, right.entry_idx, r_count, result_block, result_ptr,
			          sort_layout.entry_size, left_smaller, copied, count);
		} else if (r_done) {
			// Right side is exhausted
			FlushRows(l_ptr, left.entry_idx, l_count, result_block, result_ptr, sort_layout.entry_size, copied, count);
		} else {
			// Left side is exhausted
			FlushRows(r_ptr, right.entry_idx, r_count, result_block, result_ptr, sort_layout.entry_size, copied, count);
		}
	}
}

void MergeSorter::MergeData(SortedData &result_data, SortedData &l_data, SortedData &r_data, const idx_t &count,
                            const bool left_smaller[], idx_t next_entry_sizes[]) {
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
	data_ptr_t result_data_ptr = result_data_handle->Ptr() + result_data_block->count * row_width;
	// Result heap to write to (if needed)
	RowDataBlock *result_heap_block;
	unique_ptr<BufferHandle> result_heap_handle;
	data_ptr_t result_heap_ptr;
	if (!layout.AllConstant() && state.external) {
		result_heap_block = &result_data.heap_blocks.back();
		result_heap_handle = buffer_manager.Pin(result_heap_block->block);
		result_heap_ptr = result_heap_handle->Ptr() + result_heap_block->byte_offset;
	}

	idx_t copied = 0;
	while (copied < count) {
		// Move to new data blocks (if needed)
		if (l_data.block_idx < l_data.data_blocks.size() &&
		    l_data.entry_idx == l_data.data_blocks[l_data.block_idx].count) {
			// Delete reference to previous block
			l_data.data_blocks[l_data.block_idx].block = nullptr;
			if (!layout.AllConstant() && state.external) {
				l_data.heap_blocks[l_data.block_idx].block = nullptr;
			}
			// Advance block
			l_data.block_idx++;
			l_data.entry_idx = 0;
		}
		if (r_data.block_idx < r_data.data_blocks.size() &&
		    r_data.entry_idx == r_data.data_blocks[r_data.block_idx].count) {
			// Delete reference to previous block
			r_data.data_blocks[r_data.block_idx].block = nullptr;
			if (!layout.AllConstant() && state.external) {
				r_data.heap_blocks[r_data.block_idx].block = nullptr;
			}
			// Advance block
			r_data.block_idx++;
			r_data.entry_idx = 0;
		}
		const bool l_done = l_data.block_idx == l_data.data_blocks.size();
		const bool r_done = r_data.block_idx == r_data.data_blocks.size();
		// Pin the row data blocks
		if (!l_done) {
			l_data.Pin();
			l_ptr = l_data.data_handle->Ptr() + l_data.entry_idx * row_width;
		}
		if (!r_done) {
			r_data.Pin();
			r_ptr = r_data.data_handle->Ptr() + r_data.entry_idx * row_width;
		}
		const idx_t &l_count = !l_done ? l_data.data_blocks[l_data.block_idx].count : 0;
		const idx_t &r_count = !r_done ? r_data.data_blocks[r_data.block_idx].count : 0;
		// Create new result block (if needed)
		if (result_data_block->count == state.block_capacity) {
			result_data.CreateBlock();
			result_data_block = &result_data.data_blocks.back();
			result_data_handle = buffer_manager.Pin(result_data_block->block);
			result_data_ptr = result_data_handle->Ptr();
			if (!layout.AllConstant() && state.external) {
				result_heap_block = &result_data.heap_blocks.back();
				result_heap_handle = buffer_manager.Pin(result_heap_block->block);
				result_heap_ptr = result_heap_handle->Ptr();
			}
		}
		// Perform the merge
		if (layout.AllConstant() || !state.external) {
			// If all constant size, or if we are doing an in-memory sort, we do not need to touch the heap
			if (!l_done && !r_done) {
				// Both sides have data - merge
				MergeRows(l_ptr, l_data.entry_idx, l_count, r_ptr, r_data.entry_idx, r_count, result_data_block,
				          result_data_ptr, row_width, left_smaller, copied, count);
			} else if (r_done) {
				// Right side is exhausted
				FlushRows(l_ptr, l_data.entry_idx, l_count, result_data_block, result_data_ptr, row_width, copied,
				          count);
			} else {
				// Left side is exhausted
				FlushRows(r_ptr, r_data.entry_idx, r_count, result_data_block, result_data_ptr, row_width, copied,
				          count);
			}
		} else {
			// External sorting with variable size data. Pin the heap blocks too
			if (!l_done) {
				l_heap_ptr = l_data.heap_handle->Ptr() + Load<idx_t>(l_ptr + heap_pointer_offset);
				D_ASSERT(l_heap_ptr - l_data.heap_handle->Ptr() >= 0);
				D_ASSERT((idx_t)(l_heap_ptr - l_data.heap_handle->Ptr()) <
				         l_data.heap_blocks[l_data.block_idx].byte_offset);
			}
			if (!r_done) {
				r_heap_ptr = r_data.heap_handle->Ptr() + Load<idx_t>(r_ptr + heap_pointer_offset);
				D_ASSERT(r_heap_ptr - r_data.heap_handle->Ptr() >= 0);
				D_ASSERT((idx_t)(r_heap_ptr - r_data.heap_handle->Ptr()) <
				         r_data.heap_blocks[r_data.block_idx].byte_offset);
			}
			// Both the row and heap data need to be dealt with
			if (!l_done && !r_done) {
				// Both sides have data - merge
				idx_t l_idx_copy = l_data.entry_idx;
				idx_t r_idx_copy = r_data.entry_idx;
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
					entry_size = l_smaller * Load<idx_t>(l_heap_ptr_copy) + r_smaller * Load<idx_t>(r_heap_ptr_copy);
					D_ASSERT(entry_size >= sizeof(idx_t));
					D_ASSERT(l_heap_ptr_copy - l_data.heap_handle->Ptr() + l_smaller * entry_size <=
					         l_data.heap_blocks[l_data.block_idx].byte_offset);
					D_ASSERT(r_heap_ptr_copy - r_data.heap_handle->Ptr() + r_smaller * entry_size <=
					         r_data.heap_blocks[r_data.block_idx].byte_offset);
					l_heap_ptr_copy += l_smaller * entry_size;
					r_heap_ptr_copy += r_smaller * entry_size;
					copy_bytes += entry_size;
				}
				// Reallocate result heap block size (if needed)
				if (result_heap_block->byte_offset + copy_bytes > result_heap_block->capacity) {
					idx_t new_capacity = result_heap_block->byte_offset + copy_bytes;
					buffer_manager.ReAllocate(result_heap_block->block, new_capacity);
					result_heap_block->capacity = new_capacity;
					result_heap_ptr = result_heap_handle->Ptr() + result_heap_block->byte_offset;
				}
				D_ASSERT(result_heap_block->byte_offset + copy_bytes <= result_heap_block->capacity);
				// Now copy the heap data
				for (idx_t i = 0; i < merged; i++) {
					const bool &l_smaller = left_smaller[copied + i];
					const bool r_smaller = !l_smaller;
					const auto &entry_size = next_entry_sizes[copied + i];
					memcpy(result_heap_ptr, l_heap_ptr, l_smaller * entry_size);
					memcpy(result_heap_ptr, r_heap_ptr, r_smaller * entry_size);
					D_ASSERT(Load<idx_t>(result_heap_ptr) == entry_size);
					result_heap_ptr += entry_size;
					l_heap_ptr += l_smaller * entry_size;
					r_heap_ptr += r_smaller * entry_size;
					l_data.entry_idx += l_smaller;
					r_data.entry_idx += r_smaller;
				}
				// Update result indices and pointers
				result_heap_block->count += merged;
				result_heap_block->byte_offset += copy_bytes;
				copied += merged;
			} else if (r_done) {
				// Right side is exhausted - flush left
				FlushBlobs(layout, l_count, l_ptr, l_data.entry_idx, l_heap_ptr, result_data_block, result_data_ptr,
				           result_heap_block, *result_heap_handle, result_heap_ptr, copied, count);
			} else {
				// Left side is exhausted - flush right
				FlushBlobs(layout, r_count, r_ptr, r_data.entry_idx, r_heap_ptr, result_data_block, result_data_ptr,
				           result_heap_block, *result_heap_handle, result_heap_ptr, copied, count);
			}
			D_ASSERT(result_data_block->count == result_heap_block->count);
		}
	}
}

void MergeSorter::MergeRows(data_ptr_t &l_ptr, idx_t &l_entry_idx, const idx_t &l_count, data_ptr_t &r_ptr,
                            idx_t &r_entry_idx, const idx_t &r_count, RowDataBlock *target_block,
                            data_ptr_t &target_ptr, const idx_t &entry_size, const bool left_smaller[], idx_t &copied,
                            const idx_t &count) {
	const idx_t next = MinValue(count - copied, state.block_capacity - target_block->count);
	idx_t i;
	for (i = 0; i < next && l_entry_idx < l_count && r_entry_idx < r_count; i++) {
		const bool &l_smaller = left_smaller[copied + i];
		const bool r_smaller = !l_smaller;
		// Use comparison bool (0 or 1) to copy an entry from either side
		memcpy(target_ptr, l_ptr, l_smaller * entry_size);
		memcpy(target_ptr, r_ptr, r_smaller * entry_size);
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
	idx_t next = MinValue(count - copied, state.block_capacity - target_block->count);
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
		auto entry_size = Load<idx_t>(source_heap_ptr_copy);
		D_ASSERT(entry_size >= sizeof(idx_t));
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
