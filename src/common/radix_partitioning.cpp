#include "duckdb/common/radix_partitioning.hpp"

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/types/column/partitioned_column_data.hpp"
#include "duckdb/common/types/row/row_data_collection.hpp"
#include "duckdb/common/types/row/row_layout.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

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

//===--------------------------------------------------------------------===//
// Row Data Partitioning
//===--------------------------------------------------------------------===//
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

		block_collection.VerifyBlockSizes();
		string_heap.VerifyBlockSizes();

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

		// We track the count of the current block for each partition in this array
		uint32_t block_counts[CONSTANTS::NUM_PARTITIONS];
		memset(block_counts, 0, sizeof(block_counts));

		// Allocate "SWWCB" temporary buffer
		auto temp_buf_ptr =
		    unique_ptr<data_t[]>(new data_t[CONSTANTS::TMP_BUF_SIZE * CONSTANTS::NUM_PARTITIONS * row_width]);
		const auto tmp_buf = temp_buf_ptr.get();

		// Initialize temporary buffer offsets
		uint32_t pos[CONSTANTS::NUM_PARTITIONS];
		for (uint32_t idx = 0; idx < CONSTANTS::NUM_PARTITIONS; idx++) {
			pos[idx] = idx * CONSTANTS::TMP_BUF_SIZE;
		}

		auto &data_blocks = block_collection.blocks;
		auto &heap_blocks = string_heap.blocks;
		for (idx_t block_idx_plus_one = data_blocks.size(); block_idx_plus_one > 0; block_idx_plus_one--) {
			// We loop through blocks in reverse to save some of that PRECIOUS I/O
			idx_t block_idx = block_idx_plus_one - 1;

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
					const auto bin = CONSTANTS::ApplyMask(Load<hash_t>(data_ptr + hash_offset));

					// Write entry to bin in temp buf
					FastMemcpy(tmp_buf + pos[bin] * row_width, data_ptr, row_width);
					data_ptr += row_width;

					if ((++pos[bin] & (CONSTANTS::TMP_BUF_SIZE - 1)) == 0) {
						// Temp buf for this bin is full, flush temp buf to partition
						auto &block_count = block_counts[bin];
						FlushTempBuf(partition_data_ptrs[bin], row_width, block_count, tmp_buf, pos[bin],
						             CONSTANTS::TMP_BUF_SIZE);
						D_ASSERT(block_count <= block_capacity);
						if (block_count + CONSTANTS::TMP_BUF_SIZE > block_capacity) {
							// The block can't fit the next flush of the temp buf
							partition_data_blocks[bin]->count = block_count;
							if (has_heap) {
								// Write last bit of heap data
								PartitionHeap(buffer_manager, layout, *partition_string_heaps[bin],
								              *partition_data_blocks[bin], partition_data_ptrs[bin],
								              *partition_heap_blocks[bin], partition_heap_handles[bin]);
							}
							// Now we can create new blocks for this partition
							CreateNewBlock(buffer_manager, has_heap, partition_block_collections, partition_data_blocks,
							               partition_data_handles, partition_data_ptrs, partition_string_heaps,
							               partition_heap_blocks, partition_heap_handles, block_counts, bin);
						}
					}
				}
				remaining -= next;
			}

			// We are done with this input block
			for (idx_t bin = 0; bin < CONSTANTS::NUM_PARTITIONS; bin++) {
				auto count = pos[bin] & (CONSTANTS::TMP_BUF_SIZE - 1);
				if (count != 0) {
					// Clean up the temporary buffer
					FlushTempBuf(partition_data_ptrs[bin], row_width, block_counts[bin], tmp_buf, pos[bin], count);
				}
				D_ASSERT(block_counts[bin] <= block_capacity);
				partition_data_blocks[bin]->count = block_counts[bin];
				if (has_heap) {
					// Write heap data so we can safely unpin the current input heap block
					PartitionHeap(buffer_manager, layout, *partition_string_heaps[bin], *partition_data_blocks[bin],
					              partition_data_ptrs[bin], *partition_heap_blocks[bin], partition_heap_handles[bin]);
				}
				if (block_counts[bin] + CONSTANTS::TMP_BUF_SIZE > block_capacity) {
					// The block can't fit the next flush of the temp buf
					CreateNewBlock(buffer_manager, has_heap, partition_block_collections, partition_data_blocks,
					               partition_data_handles, partition_data_ptrs, partition_string_heaps,
					               partition_heap_blocks, partition_heap_handles, block_counts, bin);
				}
			}

			// Delete references to the input block we just finished processing to free up memory
			data_blocks[block_idx] = nullptr;
			if (has_heap) {
				heap_blocks[block_idx] = nullptr;
			}
		}

		// Update counts
		for (idx_t bin = 0; bin < CONSTANTS::NUM_PARTITIONS; bin++) {
			partition_block_collections[bin]->count += block_counts[bin];
			if (has_heap) {
				partition_string_heaps[bin]->count += block_counts[bin];
			}
		}

		// Input data collections are empty, reset them
		block_collection.Clear();
		string_heap.Clear();

#ifdef DEBUG
		for (idx_t bin = 0; bin < CONSTANTS::NUM_PARTITIONS; bin++) {
			auto &p_block_collection = *partition_block_collections[bin];
			p_block_collection.VerifyBlockSizes();
			if (!layout.AllConstant()) {
				partition_string_heaps[bin]->VerifyBlockSizes();
			}
			idx_t p_count = 0;
			for (idx_t b = 0; b < p_block_collection.blocks.size(); b++) {
				auto &data_block = *p_block_collection.blocks[b];
				p_count += data_block.count;
				if (!layout.AllConstant()) {
					auto &p_string_heap = *partition_string_heaps[bin];
					D_ASSERT(p_block_collection.blocks.size() == p_string_heap.blocks.size());
					auto &heap_block = *p_string_heap.blocks[b];
					D_ASSERT(data_block.count == heap_block.count);
				}
			}
			D_ASSERT(p_count == p_block_collection.count);
		}
#endif
	}

	static inline void FlushTempBuf(data_ptr_t &data_ptr, const idx_t &row_width, uint32_t &block_count,
	                                const data_ptr_t &tmp_buf, uint32_t &pos, const idx_t count) {
		pos -= count;
		FastMemcpy(data_ptr, tmp_buf + pos * row_width, count * row_width);
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
	                                  const idx_t &bin) {
		D_ASSERT(partition_data_blocks[bin]->count == block_counts[bin]);
		partition_block_collections[bin]->count += block_counts[bin];
		PinAndSet(buffer_manager, partition_block_collections[bin]->CreateBlock(), &partition_data_blocks[bin],
		          partition_data_handles[bin], partition_data_ptrs[bin]);

		if (has_heap) {
			partition_string_heaps[bin]->count += block_counts[bin];

			auto &p_heap_block = *partition_heap_blocks[bin];
			// Set a new heap block
			if (p_heap_block.byte_offset != p_heap_block.capacity) {
				// More data fits on the heap block, just copy (reference) the block
				partition_string_heaps[bin]->blocks.push_back(partition_heap_blocks[bin]->Copy());
				partition_string_heaps[bin]->blocks.back()->count = 0;
			} else {
				// Heap block is full, create a new one
				partition_string_heaps[bin]->CreateBlock();
			}

			partition_heap_blocks[bin] = partition_string_heaps[bin]->blocks.back().get();
			partition_heap_handles[bin] = buffer_manager.Pin(partition_heap_blocks[bin]->block);
		}

		block_counts[bin] = 0;
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
		D_ASSERT(heap_block.block == heap_handle.GetBlockHandle());
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

void RadixPartitioning::PartitionRowData(BufferManager &buffer_manager, const RowLayout &layout,
                                         const idx_t hash_offset, RowDataCollection &block_collection,
                                         RowDataCollection &string_heap,
                                         vector<unique_ptr<RowDataCollection>> &partition_block_collections,
                                         vector<unique_ptr<RowDataCollection>> &partition_string_heaps,
                                         idx_t radix_bits) {
	return RadixBitsSwitch<PartitionFunctor, void>(radix_bits, buffer_manager, layout, hash_offset, block_collection,
	                                               string_heap, partition_block_collections, partition_string_heaps);
}

//===--------------------------------------------------------------------===//
// Column Data Partitioning
//===--------------------------------------------------------------------===//
RadixPartitionedColumnData::RadixPartitionedColumnData(ClientContext &context_p, vector<LogicalType> types_p,
                                                       idx_t radix_bits_p, idx_t hash_col_idx_p)
    : PartitionedColumnData(PartitionedColumnDataType::RADIX, context_p, move(types_p)), radix_bits(radix_bits_p),
      hash_col_idx(hash_col_idx_p) {
	D_ASSERT(hash_col_idx < types.size());
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	allocators->allocators.reserve(num_partitions);
	for (idx_t i = 0; i < num_partitions; i++) {
		CreateAllocator();
	}
	D_ASSERT(allocators->allocators.size() == num_partitions);
}

RadixPartitionedColumnData::RadixPartitionedColumnData(const RadixPartitionedColumnData &other)
    : PartitionedColumnData(other), radix_bits(other.radix_bits), hash_col_idx(other.hash_col_idx) {
	for (idx_t i = 0; i < RadixPartitioning::NumberOfPartitions(radix_bits); i++) {
		partitions.emplace_back(CreatePartitionCollection(i));
	}
}

RadixPartitionedColumnData::~RadixPartitionedColumnData() {
}

void RadixPartitionedColumnData::InitializeAppendStateInternal(PartitionedColumnDataAppendState &state) const {
	const auto num_partitions = RadixPartitioning::NumberOfPartitions(radix_bits);
	state.partition_buffers.reserve(num_partitions);
	state.partition_append_states.reserve(num_partitions);
	for (idx_t i = 0; i < num_partitions; i++) {
		// TODO only initialize the append if partition idx > ...
		state.partition_append_states.emplace_back(make_unique<ColumnDataAppendState>());
		partitions[i]->InitializeAppend(*state.partition_append_states[i]);
		state.partition_buffers.emplace_back(CreatePartitionBuffer());
	}
}

struct ComputePartitionIndicesFunctor {
	template <idx_t radix_bits>
	static void Operation(Vector &hashes, Vector &partition_indices, idx_t count) {
		UnaryExecutor::Execute<hash_t, hash_t>(hashes, partition_indices, count, [&](hash_t hash) {
			using CONSTANTS = RadixPartitioningConstants<radix_bits>;
			return CONSTANTS::ApplyMask(hash);
		});
	}
};

void RadixPartitionedColumnData::ComputePartitionIndices(PartitionedColumnDataAppendState &state, DataChunk &input) {
	D_ASSERT(partitions.size() == RadixPartitioning::NumberOfPartitions(radix_bits));
	D_ASSERT(state.partition_buffers.size() == RadixPartitioning::NumberOfPartitions(radix_bits));
	RadixBitsSwitch<ComputePartitionIndicesFunctor, void>(radix_bits, input.data[hash_col_idx], state.partition_indices,
	                                                      input.size());
}

} // namespace duckdb
