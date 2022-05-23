#include "duckdb/common/radix_partitioning.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row_data_collection.hpp"
#include "duckdb/common/types/row_layout.hpp"

namespace duckdb {

template <class OP, class RETURN_TYPE, typename... ARGS>
RETURN_TYPE RadixBitsSwitch(idx_t radix_bits, ARGS &&...args) {
	D_ASSERT(radix_bits <= sizeof(hash_t) * 8);
	switch (radix_bits) {
	case 0:
		return OP::template Operation<0>(std::forward<ARGS>(args)...);
	case 1:
		return OP::template Operation<1>(std::forward<ARGS>(args)...);
	case 2:
		return OP::template Operation<2>(std::forward<ARGS>(args)...);
	case 3:
		return OP::template Operation<3>(std::forward<ARGS>(args)...);
	case 4:
		return OP::template Operation<4>(std::forward<ARGS>(args)...);
	case 5:
		return OP::template Operation<5>(std::forward<ARGS>(args)...);
	case 6:
		return OP::template Operation<6>(std::forward<ARGS>(args)...);
	case 7:
		return OP::template Operation<7>(std::forward<ARGS>(args)...);
	case 8:
		return OP::template Operation<8>(std::forward<ARGS>(args)...);
	case 9:
		return OP::template Operation<9>(std::forward<ARGS>(args)...);
	case 10:
		return OP::template Operation<10>(std::forward<ARGS>(args)...);
	default:
		throw InternalException("TODO");
	}
}

template <class OP, class RETURN_TYPE, idx_t radix_bits_1, typename... ARGS>
RETURN_TYPE DoubleRadixBitsSwitch2(idx_t radix_bits_2, ARGS &&...args) {
	D_ASSERT(radix_bits_2 <= sizeof(hash_t) * 8);
	switch (radix_bits_2) {
	case 0:
		return OP::template Operation<radix_bits_1, 0>(std::forward<ARGS>(args)...);
	case 1:
		return OP::template Operation<radix_bits_1, 1>(std::forward<ARGS>(args)...);
	case 2:
		return OP::template Operation<radix_bits_1, 2>(std::forward<ARGS>(args)...);
	case 3:
		return OP::template Operation<radix_bits_1, 3>(std::forward<ARGS>(args)...);
	case 4:
		return OP::template Operation<radix_bits_1, 4>(std::forward<ARGS>(args)...);
	case 5:
		return OP::template Operation<radix_bits_1, 5>(std::forward<ARGS>(args)...);
	case 6:
		return OP::template Operation<radix_bits_1, 6>(std::forward<ARGS>(args)...);
	case 7:
		return OP::template Operation<radix_bits_1, 7>(std::forward<ARGS>(args)...);
	case 8:
		return OP::template Operation<radix_bits_1, 8>(std::forward<ARGS>(args)...);
	case 9:
		return OP::template Operation<radix_bits_1, 9>(std::forward<ARGS>(args)...);
	case 10:
		return OP::template Operation<radix_bits_1, 10>(std::forward<ARGS>(args)...);
	default:
		throw InternalException("TODO");
	}
}

template <class OP, class RETURN_TYPE, typename... ARGS>
RETURN_TYPE DoubleRadixBitsSwitch1(idx_t radix_bits_1, idx_t radix_bits_2, ARGS &&...args) {
	D_ASSERT(radix_bits_1 <= sizeof(hash_t) * 8);
	switch (radix_bits_1) {
	case 0:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 0>(radix_bits_2, std::forward<ARGS>(args)...);
	case 1:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 1>(radix_bits_2, std::forward<ARGS>(args)...);
	case 2:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 2>(radix_bits_2, std::forward<ARGS>(args)...);
	case 3:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 3>(radix_bits_2, std::forward<ARGS>(args)...);
	case 4:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 4>(radix_bits_2, std::forward<ARGS>(args)...);
	case 5:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 5>(radix_bits_2, std::forward<ARGS>(args)...);
	case 6:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 6>(radix_bits_2, std::forward<ARGS>(args)...);
	case 7:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 7>(radix_bits_2, std::forward<ARGS>(args)...);
	case 8:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 8>(radix_bits_2, std::forward<ARGS>(args)...);
	case 9:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 9>(radix_bits_2, std::forward<ARGS>(args)...);
	case 10:
		return DoubleRadixBitsSwitch2<OP, RETURN_TYPE, 10>(radix_bits_2, std::forward<ARGS>(args)...);
	default:
		throw InternalException("TODO");
	}
}

unique_ptr<idx_t[]> RadixPartitioning::InitializeHistogram(idx_t radix_bits) {
	auto result = unique_ptr<idx_t[]>(new idx_t[1 << radix_bits]);
	memset(result.get(), 0, (1 << radix_bits) * sizeof(idx_t));
	return result;
}

struct UpdateHistogramFunctor {
	template <idx_t radix_bits>
	static void Operation(const VectorData &hash_data, const idx_t count, const bool has_rsel, idx_t histogram[]) {
		using CONSTANTS = RadixPartitioningConstants<radix_bits>;

		const auto hashes = (hash_t *)hash_data.data;
		if (has_rsel) {
			for (idx_t i = 0; i < count; i++) {
				auto idx = hash_data.sel->get_index(i);
				histogram[CONSTANTS::ApplyMask(hashes[idx])]++;
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				histogram[CONSTANTS::ApplyMask(hashes[i])]++;
			}
		}
	}
};

void RadixPartitioning::UpdateHistogram(const VectorData &hash_data, const idx_t count, const bool has_rsel,
                                        idx_t histogram[], idx_t radix_bits) {
	return RadixBitsSwitch<UpdateHistogramFunctor, void>(radix_bits, hash_data, count, has_rsel, histogram);
}

struct ReduceHistogramFunctor {
	template <idx_t radix_bits_from, idx_t radix_bits_to>
	static unique_ptr<idx_t[]> Operation(const idx_t histogram_from[]) {
		using CONSTANTS_FROM = RadixPartitioningConstants<radix_bits_from>;
		using CONSTANTS_TO = RadixPartitioningConstants<radix_bits_to>;

		auto result = RadixPartitioning::InitializeHistogram(radix_bits_to);
		auto histogram_to = result.get();
		for (idx_t i = 0; i < CONSTANTS_FROM::NUM_PARTITIONS; i++) {
			histogram_to[CONSTANTS_TO::ApplyMask(i)] += histogram_from[i];
		}
		return result;
	}
};

unique_ptr<idx_t[]> RadixPartitioning::ReduceHistogram(const idx_t histogram_from[], idx_t radix_bits_from,
                                                       idx_t radix_bits_to) {
	return DoubleRadixBitsSwitch1<ReduceHistogramFunctor, unique_ptr<idx_t[]>>(radix_bits_from, radix_bits_to,
	                                                                           histogram_from);
}

template <idx_t radix_bits>
static void InitPartitions(BufferManager &buffer_manager, vector<unique_ptr<RowDataCollection>> &partition_collections,
                           RowDataBlock *partition_blocks[], vector<unique_ptr<BufferHandle>> &partition_handles,
                           data_ptr_t partition_ptrs[], idx_t block_capacity, idx_t row_width) {
	using CONSTANTS = RadixPartitioningConstants<radix_bits>;

	partition_collections.reserve(CONSTANTS::NUM_PARTITIONS);
	partition_handles.reserve(CONSTANTS::NUM_PARTITIONS);
	for (idx_t i = 0; i < CONSTANTS::NUM_PARTITIONS; i++) {
		partition_collections.push_back(make_unique<RowDataCollection>(buffer_manager, block_capacity, row_width));
		partition_blocks[i] = &partition_collections[i]->CreateBlock();
		partition_handles.push_back(buffer_manager.Pin(partition_blocks[i]->block));
		partition_ptrs[i] = partition_handles[i]->Ptr();
	}
}

static inline void PinAndSet(BufferManager &buffer_manager, RowDataBlock &block, RowDataBlock **block_ptr,
                             unique_ptr<BufferHandle> &handle, data_ptr_t &ptr) {
	*block_ptr = &block;
	handle = buffer_manager.Pin(block.block);
	ptr = handle->Ptr();
}

static inline void PartitionHeap(BufferManager &buffer_manager, const RowLayout &layout, RowDataBlock &data_block,
                                 const data_ptr_t data_ptr, RowDataBlock &heap_block, BufferHandle &heap_handle,
                                 data_ptr_t &heap_ptr) {
	D_ASSERT(heap_block.block->BlockId() == heap_handle.handle->BlockId());
	const auto count = data_block.count - heap_block.count;
	if (count == 0) {
		return;
	}
	const auto row_width = layout.GetRowWidth();
	auto base_row_ptr = data_ptr - count * row_width;

	// Compute size of remaining heap rows
	idx_t size = 0;
	auto row_ptr = base_row_ptr + layout.GetHeapOffset();
	for (idx_t i = 0; i < count; i++) {
		size += Load<uint32_t>(Load<data_ptr_t>(row_ptr));
	}

	// Resize block if it doesn't fit
	auto required_size = heap_block.byte_offset + size;
	if (required_size > heap_block.capacity) {
		buffer_manager.ReAllocate(heap_block.block, required_size);
		heap_block.capacity = required_size;
		heap_ptr = heap_handle.Ptr() + heap_block.byte_offset;
	}

	// Copy corresponding heap rows, swizzle, and update counts
	RowOperations::CopyHeapAndSwizzle(layout, base_row_ptr, heap_handle.Ptr(), heap_ptr, count);
	heap_block.count += count;
	heap_block.byte_offset += size;
	heap_ptr += size;
}

struct PartitionFunctor {
	template <idx_t radix_bits>
	static void Operation(BufferManager &buffer_manager, const RowLayout &layout, const idx_t hash_offset,
	                      RowDataCollection &block_collection, RowDataCollection &string_heap,
	                      vector<unique_ptr<RowDataCollection>> &partition_block_collections,
	                      vector<unique_ptr<RowDataCollection>> &partition_string_heaps) {
		using CONSTANTS = RadixPartitioningConstants<radix_bits>;

		const auto block_capacity = partition_block_collections[0]->block_capacity;
		D_ASSERT(block_capacity % CONSTANTS::TMP_BUF_SIZE == 0);
		const auto row_width = layout.GetRowWidth();
		const auto has_heap = !layout.AllConstant();

		// Fixed-size data
		RowDataBlock *partition_data_blocks[CONSTANTS::NUM_PARTITIONS];
		vector<unique_ptr<BufferHandle>> partition_data_handles;
		data_ptr_t partition_data_ptrs[CONSTANTS::NUM_PARTITIONS];
		InitPartitions<radix_bits>(buffer_manager, partition_block_collections, partition_data_blocks,
		                           partition_data_handles, partition_data_ptrs, block_capacity, row_width);

		// Variable-size data
		RowDataBlock *partition_heap_blocks[CONSTANTS::NUM_PARTITIONS];
		vector<unique_ptr<BufferHandle>> partition_heap_handles;
		data_ptr_t partition_heap_ptrs[CONSTANTS::NUM_PARTITIONS];
		if (has_heap) {
			InitPartitions<radix_bits>(buffer_manager, partition_string_heaps, partition_heap_blocks,
			                           partition_heap_handles, partition_heap_ptrs, (idx_t)Storage::BLOCK_SIZE, 1);
		}

		// Init local counts of the partition blocks
		uint32_t block_counts[CONSTANTS::NUM_PARTITIONS];
		memset(block_counts, 0, sizeof(block_counts));

		// Allocate "SWWCB" temporal buffer
		auto temp_buf_ptr =
		    unique_ptr<data_t[]>(new data_t[CONSTANTS::TMP_BUF_SIZE * CONSTANTS::NUM_PARTITIONS * row_width]);
		const auto tmp_buf = temp_buf_ptr.get();

		// Initialize temporal buffer count
		idx_t pos[CONSTANTS::NUM_PARTITIONS];
		for (idx_t idx = 0; idx < CONSTANTS::NUM_PARTITIONS; idx++) {
			pos[idx] = idx * CONSTANTS::TMP_BUF_SIZE;
		}

		auto &data_blocks = block_collection.blocks;
		auto &heap_blocks = string_heap.blocks;
		for (idx_t block_idx = 0; block_idx < data_blocks.size(); block_idx++) {
			RowDataBlock *data_block;
			unique_ptr<BufferHandle> data_handle;
			data_ptr_t data_ptr;
			PinAndSet(buffer_manager, *data_blocks[block_idx], &data_block, data_handle, data_ptr);

			// Pin the heap block (if necessary)
			RowDataBlock *heap_block;
			unique_ptr<BufferHandle> heap_handle;
			data_ptr_t heap_ptr;
			if (has_heap) {
				PinAndSet(buffer_manager, *heap_blocks[block_idx], &heap_block, heap_handle, heap_ptr);
			}

			idx_t remaining = data_block->count;
			while (remaining != 0) {
				const auto next = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);

				if (has_heap) {
					// Unswizzle so that the rows that we copy have a pointer to their heap rows
					RowOperations::UnswizzleHeapPointer(layout, data_ptr, heap_ptr, next);
				}

				for (idx_t i = 0; i < next; i++) {
					const auto idx = CONSTANTS::ApplyMask(Load<hash_t>(data_ptr + hash_offset));

					// Temporal write
					FastMemcpy(tmp_buf + pos[idx] * row_width, data_ptr, row_width);
					data_ptr += row_width;

					if (++pos[idx] & (CONSTANTS::TMP_BUF_SIZE - 1) == 0) {
						// Non-temporal write
						pos[idx] -= CONSTANTS::TMP_BUF_SIZE;
						memcpy(partition_data_ptrs[idx], tmp_buf + pos[idx] * row_width,
						       CONSTANTS::TMP_BUF_SIZE * row_width);
						partition_data_ptrs[idx] += CONSTANTS::TMP_BUF_SIZE * row_width;
						block_counts[idx] += CONSTANTS::TMP_BUF_SIZE;

						// Check if block is full
						if (block_counts[idx] == block_capacity) {
							auto &p_data_block = *partition_data_blocks[idx];
							if (has_heap) {
								// Copy data from the input heap block to the partition heaps so we can unpin it
								auto &p_heap_block = *partition_heap_blocks[idx];
								PartitionHeap(buffer_manager, layout, p_data_block, partition_data_ptrs[idx],
								              p_heap_block, *partition_heap_handles[idx], partition_heap_ptrs[idx]);

								// Update counts and create new blocks to write to
								p_heap_block.count = block_counts[idx];
								partition_string_heaps[idx]->count += block_counts[idx];

								if (p_heap_block.byte_offset != p_heap_block.capacity) {
									// More data fits on the heap block, just copy (reference) the block
									partition_string_heaps[idx]->blocks.push_back(partition_heap_blocks[idx]->Copy());
									partition_string_heaps[idx]->blocks.back()->count = 0;
								} else {
									// Heap block is full, create a new one
									partition_string_heaps[idx]->CreateBlock();
								}

								PinAndSet(buffer_manager, *partition_string_heaps[idx]->blocks.back(),
								          &partition_heap_blocks[idx], partition_heap_handles[idx],
								          partition_heap_ptrs[idx]);
							}

							// Update counts and create new blocks to write to
							p_data_block.count = block_counts[idx];
							partition_block_collections[idx]->count += block_counts[idx];
							PinAndSet(buffer_manager, partition_block_collections[idx]->CreateBlock(),
							          &partition_data_blocks[idx], partition_data_handles[idx],
							          partition_data_ptrs[idx]);

							// Reset local count
							block_counts[idx] = 0;
						}
					}
				}
				remaining -= next;
			}

			// We are done with this input block, clean up the temporal buffer
			for (idx_t idx = 0; idx < CONSTANTS::NUM_PARTITIONS; idx++) {
				auto rest = pos[idx] & (CONSTANTS::TMP_BUF_SIZE - 1);
				if (rest == 0) {
					continue;
				}
				pos[idx] -= rest;
				memcpy(partition_data_ptrs[idx], tmp_buf + pos[idx] * row_width, rest * row_width);
				partition_data_ptrs[idx] += rest * row_width;
				block_counts[idx] += rest;
			}

			if (has_heap) {
				// Copy data from the input heap block to the partition heaps so we can unpin it
				for (idx_t idx = 0; idx < CONSTANTS::NUM_PARTITIONS; idx++) {
					PartitionHeap(buffer_manager, layout, *partition_data_blocks[idx], partition_data_ptrs[idx],
					              *partition_heap_blocks[idx], *partition_heap_handles[idx], partition_heap_ptrs[idx]);
				}
			}

			// Delete references to input blocks that have been processed to free up memory
			data_blocks[block_idx] = nullptr;
			heap_blocks[block_idx] = nullptr;
		}
		// TODO: maybe delete empty blocks at the end? (although they are small and unlikely)
	}
};

void RadixPartitioning::Partition(BufferManager &buffer_manager, const RowLayout &layout, const idx_t hash_offset,
                                  RowDataCollection &block_collection, RowDataCollection &string_heap,
                                  vector<unique_ptr<RowDataCollection>> &partition_block_collections,
                                  vector<unique_ptr<RowDataCollection>> &partition_string_heaps, idx_t radix_bits) {
	return RadixBitsSwitch<PartitionFunctor, void>(radix_bits, buffer_manager, layout, hash_offset, block_collection,
	                                               string_heap, partition_block_collections, partition_string_heaps);
}

} // namespace duckdb
