#include "duckdb/common/radix_partitioning.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/row_data_collection.hpp"
#include "duckdb/common/types/row_layout.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace duckdb {

template <class OP, class RETURN_TYPE, typename... ARGS>
RETURN_TYPE RadixBitsSwitch(idx_t radix_bits, ARGS &&...args) {
	D_ASSERT(radix_bits <= sizeof(hash_t) * 8);
	switch (radix_bits) {
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

template <idx_t radix_bits>
static void InitPartitions(BufferManager &buffer_manager, vector<unique_ptr<RowDataCollection>> &partition_collections,
                           RowDataBlock *partition_blocks[], vector<BufferHandle> &partition_handles,
                           data_ptr_t partition_ptrs[], idx_t block_capacity, idx_t row_width) {
	using CONSTANTS = RadixPartitioningConstants<radix_bits>;

	partition_collections.reserve(CONSTANTS::NUM_PARTITIONS);
	partition_handles.reserve(CONSTANTS::NUM_PARTITIONS);
	for (idx_t i = 0; i < CONSTANTS::NUM_PARTITIONS; i++) {
		partition_collections.push_back(make_unique<RowDataCollection>(buffer_manager, block_capacity, row_width));
		partition_blocks[i] = &partition_collections[i]->CreateBlock();
		partition_handles.push_back(buffer_manager.Pin(partition_blocks[i]->block));
		if (partition_ptrs) {
			partition_ptrs[i] = partition_handles[i].Ptr();
		}
	}
}

struct PartitionFunctor {
	template <idx_t radix_bits>
	static void Operation(BufferManager &buffer_manager, const RowLayout &layout, const idx_t hash_offset,
	                      RowDataCollection &block_collection, RowDataCollection &string_heap,
	                      vector<unique_ptr<RowDataCollection>> &partition_block_collections,
	                      vector<unique_ptr<RowDataCollection>> &partition_string_heaps) {
		using CONSTANTS = RadixPartitioningConstants<radix_bits>;

		const auto block_capacity = block_collection.block_capacity;
		const auto row_width = layout.GetRowWidth();
		const auto has_heap = !layout.AllConstant();

		// Fixed-size data
		RowDataBlock *partition_data_blocks[CONSTANTS::NUM_PARTITIONS];
		vector<BufferHandle> partition_data_handles;
		data_ptr_t partition_data_ptrs[CONSTANTS::NUM_PARTITIONS];
		InitPartitions<radix_bits>(buffer_manager, partition_block_collections, partition_data_blocks,
		                           partition_data_handles, partition_data_ptrs, block_capacity, row_width);

		// Variable-size data
		RowDataBlock *partition_heap_blocks[CONSTANTS::NUM_PARTITIONS];
		vector<BufferHandle> partition_heap_handles;
		if (has_heap) {
			InitPartitions<radix_bits>(buffer_manager, partition_string_heaps, partition_heap_blocks,
			                           partition_heap_handles, nullptr, (idx_t)Storage::BLOCK_SIZE, 1);
		}

		// Init local counts of the partition blocks
		uint32_t block_counts[CONSTANTS::NUM_PARTITIONS];
		memset(block_counts, 0, sizeof(block_counts));

		// Allocate "SWWCB" temporal buffer
		auto temp_buf_ptr =
		    unique_ptr<data_t[]>(new data_t[CONSTANTS::TMP_BUF_SIZE * CONSTANTS::NUM_PARTITIONS * row_width]);
		const auto tmp_buf = temp_buf_ptr.get();

		// Initialize temporal buffer offsets
		uint32_t pos[CONSTANTS::NUM_PARTITIONS];
		for (uint32_t idx = 0; idx < CONSTANTS::NUM_PARTITIONS; idx++) {
			pos[idx] = idx * CONSTANTS::TMP_BUF_SIZE;
		}

		auto &data_blocks = block_collection.blocks;
		auto &heap_blocks = string_heap.blocks;
		for (idx_t block_idx = 0; block_idx < data_blocks.size(); block_idx++) {
			RowDataBlock *data_block;
			BufferHandle data_handle;
			data_ptr_t data_ptr;
			PinAndSet(buffer_manager, *data_blocks[block_idx], &data_block, data_handle, data_ptr);

			// Pin the heap block (if necessary)
			RowDataBlock *heap_block;
			BufferHandle heap_handle;
			if (has_heap) {
				heap_block = heap_blocks[block_idx].get();
				heap_handle = buffer_manager.Pin(heap_block->block);
			}

			idx_t remaining = data_block->count;
			while (remaining != 0) {
				const auto next = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);

				if (has_heap) {
					// Unswizzle so that the rows that we copy have a pointer to their heap rows
					RowOperations::UnswizzleHeapPointer(layout, data_ptr, heap_handle.Ptr(), next);
				}

				for (idx_t i = 0; i < next; i++) {
					const auto idx = CONSTANTS::ApplyMask(Load<hash_t>(data_ptr + hash_offset));

					// Temporal write
					FastMemcpy(tmp_buf + pos[idx] * row_width, data_ptr, row_width);
					data_ptr += row_width;

					if ((++pos[idx] & (CONSTANTS::TMP_BUF_SIZE - 1)) == 0) {
						auto &block_count = block_counts[idx];
						NonTemporalWrite(partition_data_ptrs[idx], row_width, block_count, tmp_buf, pos[idx],
						                 CONSTANTS::TMP_BUF_SIZE);
						D_ASSERT(block_count <= block_capacity);
						if (block_count + CONSTANTS::TMP_BUF_SIZE > block_capacity) {
							// The block can't fit the next non-temporal write
							partition_data_blocks[idx]->count = block_count;
							if (has_heap) {
								// Write last bit of heap data
								PartitionHeap(buffer_manager, layout, *partition_string_heaps[idx],
								              *partition_data_blocks[idx], partition_data_ptrs[idx],
								              *partition_heap_blocks[idx], partition_heap_handles[idx]);
							}
							// Now we can create new blocks for this partition
							CreateNewBlock(buffer_manager, has_heap, partition_block_collections, partition_data_blocks,
							               partition_data_handles, partition_data_ptrs, partition_string_heaps,
							               partition_heap_blocks, partition_heap_handles, block_counts, idx);
						}
					}
				}
				remaining -= next;
			}

			// We are done with this input block
			for (idx_t idx = 0; idx < CONSTANTS::NUM_PARTITIONS; idx++) {
				auto count = pos[idx] & (CONSTANTS::TMP_BUF_SIZE - 1);
				if (count != 0) {
					// Clean up the temporal buffer
					NonTemporalWrite(partition_data_ptrs[idx], row_width, block_counts[idx], tmp_buf, pos[idx], count);
				}
				D_ASSERT(block_counts[idx] <= block_capacity);
				partition_data_blocks[idx]->count = block_counts[idx];
				if (has_heap) {
					// Write heap data so we can safely unpin the current input heap block
					PartitionHeap(buffer_manager, layout, *partition_string_heaps[idx], *partition_data_blocks[idx],
					              partition_data_ptrs[idx], *partition_heap_blocks[idx], partition_heap_handles[idx]);
				}
				if (block_counts[idx] + CONSTANTS::TMP_BUF_SIZE > block_capacity) {
					// The block can't fit the next non-temporal write
					CreateNewBlock(buffer_manager, has_heap, partition_block_collections, partition_data_blocks,
					               partition_data_handles, partition_data_ptrs, partition_string_heaps,
					               partition_heap_blocks, partition_heap_handles, block_counts, idx);
				}
			}

			// Delete references to the input block we just finished processing to free up memory
			data_blocks[block_idx] = nullptr;
			if (has_heap) {
				heap_blocks[block_idx] = nullptr;
			}
		}

		// Update counts
		for (idx_t idx = 0; idx < CONSTANTS::NUM_PARTITIONS; idx++) {
			partition_block_collections[idx]->count += block_counts[idx];
			if (has_heap) {
				partition_string_heaps[idx]->count += block_counts[idx];
			}
		}

		// Input data collections are empty, reset them
		block_collection.Clear();
		string_heap.Clear();

#ifdef DEBUG
		for (idx_t p = 0; p < CONSTANTS::NUM_PARTITIONS; p++) {
			auto &p_block_collection = *partition_block_collections[p];
			idx_t p_count = 0;
			for (idx_t b = 0; b < p_block_collection.blocks.size(); b++) {
				auto &data_block = *p_block_collection.blocks[b];
				p_count += data_block.count;
				if (!layout.AllConstant()) {
					auto &p_string_heap = *partition_string_heaps[p];
					D_ASSERT(p_block_collection.blocks.size() == p_string_heap.blocks.size());
					auto &heap_block = *p_string_heap.blocks[b];
					D_ASSERT(data_block.count == heap_block.count);
				}
			}
			D_ASSERT(p_count == p_block_collection.count);
		}
#endif
	}

	static inline void NonTemporalWrite(data_ptr_t &data_ptr, const idx_t &row_width, uint32_t &block_count,
	                                    const data_ptr_t &tmp_buf, uint32_t &pos, const idx_t count) {
		pos -= count;
		memcpy(data_ptr, tmp_buf + pos * row_width, count * row_width);
		data_ptr += count * row_width;
		block_count += count;
	}

	static inline void CreateNewBlock(BufferManager &buffer_manager, const bool &has_heap,
	                                  vector<unique_ptr<RowDataCollection>> &partition_block_collections,
	                                  RowDataBlock *partition_data_blocks[],
	                                  vector<BufferHandle> &partition_data_handles, data_ptr_t partition_data_ptrs[],
	                                  vector<unique_ptr<RowDataCollection>> &partition_string_heaps,
	                                  RowDataBlock *partition_heap_blocks[],
	                                  vector<BufferHandle> &partition_heap_handles, uint32_t block_counts[],
	                                  const idx_t &idx) {
		D_ASSERT(partition_data_blocks[idx]->count == block_counts[idx]);
		partition_block_collections[idx]->count += block_counts[idx];
		PinAndSet(buffer_manager, partition_block_collections[idx]->CreateBlock(), &partition_data_blocks[idx],
		          partition_data_handles[idx], partition_data_ptrs[idx]);

		if (has_heap) {
			partition_string_heaps[idx]->count += block_counts[idx];

			auto &p_heap_block = *partition_heap_blocks[idx];
			// Set a new heap block
			if (p_heap_block.byte_offset != p_heap_block.capacity) {
				// More data fits on the heap block, just copy (reference) the block
				partition_string_heaps[idx]->blocks.push_back(partition_heap_blocks[idx]->Copy());
				partition_string_heaps[idx]->blocks.back()->count = 0;
			} else {
				// Heap block is full, create a new one
				partition_string_heaps[idx]->CreateBlock();
			}

			partition_heap_blocks[idx] = partition_string_heaps[idx]->blocks.back().get();
			partition_heap_handles[idx] = buffer_manager.Pin(partition_heap_blocks[idx]->block);
		}

		block_counts[idx] = 0;
	}

	static inline void PinAndSet(BufferManager &buffer_manager, RowDataBlock &block, RowDataBlock **block_ptr,
	                             BufferHandle &handle, data_ptr_t &ptr) {
		*block_ptr = &block;
		handle = buffer_manager.Pin(block.block);
		ptr = handle.Ptr();
	}

	static inline void PartitionHeap(BufferManager &buffer_manager, const RowLayout &layout,
	                                 RowDataCollection &string_heap, RowDataBlock &data_block,
	                                 const data_ptr_t data_ptr, RowDataBlock &heap_block, BufferHandle &heap_handle) {
		D_ASSERT(!layout.AllConstant());
		D_ASSERT(heap_block.block->BlockId() == heap_handle.GetBlockId());
		D_ASSERT(data_block.count >= heap_block.count);
		const auto count = data_block.count - heap_block.count;
		if (count == 0) {
			return;
		}
		const auto row_width = layout.GetRowWidth();
		const auto base_row_ptr = data_ptr - count * row_width;

		// Compute size of remaining heap rows
		idx_t size = 0;
		auto row_ptr = base_row_ptr + layout.GetHeapOffset();
		for (idx_t i = 0; i < count; i++) {
			size += Load<uint32_t>(Load<data_ptr_t>(row_ptr));
			row_ptr += row_width;
		}

		// Resize block if it doesn't fit
		auto required_size = heap_block.byte_offset + size;
		if (required_size > heap_block.capacity) {
			buffer_manager.ReAllocate(heap_block.block, required_size);
			heap_block.capacity = required_size;
		}
		auto heap_ptr = heap_handle.Ptr() + heap_block.byte_offset;

#ifdef DEBUG
		if (data_block.count > count) {
			auto previous_row_heap_offset = Load<idx_t>(base_row_ptr - layout.GetRowWidth() + layout.GetHeapOffset());
			auto previous_row_heap_ptr = heap_handle.Ptr() + previous_row_heap_offset;
			auto current_heap_ptr = previous_row_heap_ptr + Load<uint32_t>(previous_row_heap_ptr);
			D_ASSERT(current_heap_ptr == heap_ptr);
		}
#endif

		// Copy corresponding heap rows, swizzle, and update counts
		RowOperations::CopyHeapAndSwizzle(layout, base_row_ptr, heap_handle.Ptr(), heap_ptr, count);
		heap_block.count += count;
		heap_block.byte_offset += size;
		D_ASSERT(data_block.count == heap_block.count);
		D_ASSERT(heap_ptr + size == heap_handle.Ptr() + heap_block.byte_offset);
		D_ASSERT(heap_ptr <= heap_handle.Ptr() + heap_block.capacity);
	}
};

void RadixPartitioning::Partition(BufferManager &buffer_manager, const RowLayout &layout, const idx_t hash_offset,
                                  RowDataCollection &block_collection, RowDataCollection &string_heap,
                                  vector<unique_ptr<RowDataCollection>> &partition_block_collections,
                                  vector<unique_ptr<RowDataCollection>> &partition_string_heaps, idx_t radix_bits) {
	return RadixBitsSwitch<PartitionFunctor, void>(radix_bits, buffer_manager, layout, hash_offset, block_collection,
	                                               string_heap, partition_block_collections, partition_string_heaps);
}

template <idx_t radix_bits>
struct RadixLessThan {
	static inline bool Operation(hash_t hash, hash_t cutoff) {
		using CONSTANTS = RadixPartitioningConstants<radix_bits>;
		return CONSTANTS::ApplyMask(hash) < cutoff;
	}
};

struct SelectFunctor {
	template <idx_t radix_bits>
	static idx_t Operation(Vector &hashes, const SelectionVector *sel, idx_t count, idx_t cutoff,
	                       SelectionVector *true_sel, SelectionVector *false_sel) {
		Vector cutoff_vector(Value::HASH(cutoff));
		return BinaryExecutor::Select<hash_t, hash_t, RadixLessThan<radix_bits>>(hashes, cutoff_vector, sel, count,
		                                                                         true_sel, false_sel);
	}
};

idx_t RadixPartitioning::Select(Vector &hashes, const SelectionVector *sel, idx_t count, idx_t radix_bits, idx_t cutoff,
                                SelectionVector *true_sel, SelectionVector *false_sel) {
	return RadixBitsSwitch<SelectFunctor, idx_t>(radix_bits, hashes, sel, count, cutoff, true_sel, false_sel);
}

} // namespace duckdb
