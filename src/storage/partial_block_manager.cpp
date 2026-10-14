#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/storage/table/in_memory_checkpoint.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include <cstdint>
#include <cstring>

namespace duckdb {

//===--------------------------------------------------------------------===//
// PartialBlock
//===--------------------------------------------------------------------===//

PartialBlock::PartialBlock(BlockManager &block_manager, block_id_t block_id)
    : block_manager(block_manager), block_handle(nullptr), block_id(block_id) {
}

PartialBlock::PartialBlock(BlockManager &block_manager, const shared_ptr<BlockHandle> &block_handle)
    : block_manager(block_manager), block_handle(block_handle) {
}

idx_t PartialBlock::AvailableSpace() {
	return block_manager.GetBlockSize() - offset;
}

uint32_t PartialBlock::UseCount() {
	return block_use_count;
}

void PartialBlock::AddUninitializedRegion(idx_t start, idx_t end) {
	uninitialized_regions.push_back({start, end});
}

void PartialBlock::AddPackable(BlockPackable &packable) {
	packables.emplace_back(packable, offset);
}

shared_ptr<BlockHandle> PartialBlock::ReuseBuffer(BufferHandle &source) {
	auto source_handle = source.GetBlockHandle();
	if (source_handle->BlockId() < MAXIMUM_BLOCK) {
		// if the buffer has a backing disk block, we might overwrite other segments in the block when growing the
		// buffer for the partial block manager.
		return nullptr;
	}

	auto &source_buffer = source.GetFileBuffer();
	if (source_buffer.GetBufferType() != FileBufferType::MANAGED_BUFFER) {
		// only ordinary managed storage for now
		return nullptr;
	}
	if (!source_buffer.OwnsInternalBuffer()) {
		// memory mapped buffer is not supported
		return nullptr;
	}
	if (source_buffer.AllocSize() != block_manager.GetBlockAllocSize()) {
		// allocated space should be equal to the block size so we never write outside the allocated region when writing
		// other packables
		return nullptr;
	}
	if (source_buffer.GetHeaderSize() != block_manager.GetBlockHeaderSize()) {
		// prevent misinterpretation of payload bytes as header bytes and vice versa
		return nullptr;
	}

	auto &source_memory = source.GetBlockHandle()->GetMemory();
	if (!source_memory.MustWriteToTemporaryFile()) {
		// under memory pressure we should spill in flight partial blocks, otherwise data may be lost when we want to
		// flush buffer to the backing disk blocks.
		return nullptr;
	}
	if (&source_memory.GetBufferManager() != &block_manager.buffer_manager) {
		// moving buffers across managed memory pools might lead to accounting inconsistencies
		return nullptr;
	}

	return source_handle;
}

BlockPointer PartialBlock::Store(BlockPackable &packable, BufferHandle source, uint32_t byte_size) {
	D_ASSERT(source.IsValid());
	D_ASSERT(byte_size > 0);
	D_ASSERT(ValueIsAligned(offset));

	const auto unaligned_size = idx_t(offset) + byte_size;
	const auto aligned_size = AlignValue(unaligned_size);
	D_ASSERT(aligned_size <= block_manager.GetBlockSize());

	const BlockPointer block_ptr(block_id, offset);
	auto &buffer_manager = block_manager.buffer_manager;

	if (block_handle) {
		D_ASSERT(source.GetBlockHandle() != block_handle);
		auto dst = buffer_manager.Pin(block_handle);
		D_ASSERT(dst.GetFileBuffer().Size() == block_manager.GetBlockSize());
		memcpy(dst.GetDataMutable() + offset, source.Ptr(), byte_size);
	} else {
		if (auto source_handle = ReuseBuffer(source)) {
			block_handle = std::move(source_handle);
		} else {
			auto &source_memory = source.GetBlockHandle()->GetMemory();
			auto dst = buffer_manager.Allocate(source_memory.GetMemoryTag(), &block_manager, false);
			memcpy(dst.GetDataMutable(), source.Ptr(), byte_size);
			block_handle = dst.GetBlockHandle();
		}
	}

	if (unaligned_size != aligned_size) {
		AddUninitializedRegion(unaligned_size, aligned_size);
	}

	AddPackable(packable);
	offset = NumericCast<uint32_t>(aligned_size);
	block_use_count++;

	if (block_id != INVALID_BLOCK && block_use_count > 1) {
		block_manager.IncreaseBlockReferenceCount(block_id);
	}

	return block_ptr;
}

void PartialBlock::Flush(QueryContext context) {
	if (!block_handle) {
		throw InternalException("Flush called on partial block that was already flushed");
	}

	FlushInternal(AvailableSpace());

	bool fetch_new_block = block_id == INVALID_BLOCK;
	if (fetch_new_block) {
		block_id = block_manager.GetFreeBlockId();
	}
	block_handle = block_manager.ConvertToPersistent(context, block_id, std::move(block_handle));

	if (fetch_new_block) {
		for (idx_t i = 1; i < block_use_count; i++) {
			block_manager.IncreaseBlockReferenceCount(block_id);
		}
	}

	FinalizePackables();
	Clear();
}

void PartialBlock::FinalizePackables() {
	for (auto &[packable, offset] : packables) {
		packable.get().Finalize(block_handle, offset);
	}
}

void PartialBlock::Merge(PartialBlock &other) {
	if (&block_manager != &other.block_manager) {
		throw InternalException("Cannot merge partial blocks with different block managers");
	}
	if (block_id != INVALID_BLOCK || other.block_id != INVALID_BLOCK) {
		throw InternalException("Cannot merge partial blocks with assigned block IDs");
	}

	auto &buffer_manager = block_manager.buffer_manager;
	auto old_handle = buffer_manager.Pin(other.block_handle);
	auto new_handle = buffer_manager.Pin(block_handle);
	memcpy(new_handle.GetDataMutable() + offset, old_handle.Ptr(), other.offset);

	for (auto &region : other.uninitialized_regions) {
		uninitialized_regions.push_back({region.start + offset, region.end + offset});
	}
	for (auto &[packable, other_offset] : other.packables) {
		packables.emplace_back(packable, NumericCast<uint32_t>(other_offset + offset));
	}

	offset += other.offset;
	block_use_count += other.block_use_count;

	other.Clear();
}

void PartialBlock::Clear() {
	uninitialized_regions.clear();
	block_handle.reset();
	packables.clear();
}

void PartialBlock::FlushInternal(const idx_t free_space_left) {
	// ensure that we do not leak any data
	if (free_space_left > 0 || !uninitialized_regions.empty()) {
		auto buffer_handle = block_manager.buffer_manager.Pin(block_handle);

		// memset any uninitialized regions
		for (auto &uninitialized : uninitialized_regions) {
			memset(buffer_handle.GetDataMutable() + uninitialized.start, 0, uninitialized.end - uninitialized.start);
		}
		// memset any free space at the end of the block to 0 prior to writing to disk
		memset(buffer_handle.GetDataMutable() + block_manager.GetBlockSize() - free_space_left, 0, free_space_left);
	}
}

//===--------------------------------------------------------------------===//
// PartialBlockManager
//===--------------------------------------------------------------------===//

PartialBlockManager::PartialBlockManager(QueryContext context, BlockManager &block_manager,
                                         PartialBlockType partial_block_type, optional_idx max_partial_block_size_p,
                                         uint32_t max_use_count)
    : context(context.GetClientContext()), block_manager(block_manager), partial_block_type(partial_block_type),
      max_use_count(max_use_count) {
	if (max_partial_block_size_p.IsValid()) {
		max_partial_block_size = NumericCast<uint32_t>(max_partial_block_size_p.GetIndex());
		return;
	}

	// Use the default maximum partial block size with a ratio of 20% free and 80% utilization.
	max_partial_block_size = NumericCast<uint32_t>(block_manager.GetBlockSize() / 5 * 4);
}

PartialBlockManager::~PartialBlockManager() {
}

block_id_t PartialBlockManager::GetFreeBlockId() {
	if (partial_block_type == PartialBlockType::FULL_CHECKPOINT) {
		return block_manager.GetFreeBlockIdForCheckpoint();
	} else {
		return block_manager.GetFreeBlockId();
	}
}

// unique_ptr<PartialBlock> PartialBlockManager::CreatePartialBlock(BlockPackable &segment, PartialBlockState state) {
// 	if (partial_block_type == PartialBlockType::IN_MEMORY_CHECKPOINT) {
// 		return make_uniq<InMemoryPartialBlock>(segment, state, block_manager);
// 	}
// 	auto partial_block = make_uniq<PartialBlock>(block_manager, segment.GetBlockHandle());
// 	partial_block->AddSegmentToTail(segment);
// 	return partial_block;
// }

void PartialBlockManager::Merge(PartialBlockManager &other) {
	if (&other == this) {
		throw InternalException("Cannot merge into itself");
	}
	if (&block_manager != &other.block_manager || partial_block_type != other.partial_block_type) {
		throw InternalException("Cannot merge partial block managers with different block managers or block types");
	}

	for (auto &[other_offset, other_block] : other.partially_filled_blocks) {
		if (!other_block) {
			throw InternalException("Empty partially filled block found");
		}

		auto used_space = NumericCast<uint32_t>(block_manager.GetBlockSize() - other_offset);
		if (used_space > max_partial_block_size) {
			RetainOrFlush(std::move(other_block));
			continue;
		}
		if (auto partial_block = TakePartialBlock(used_space)) {
			partial_block->Merge(*other_block);
			RetainOrFlush(std::move(partial_block));
			continue;
		}

		RetainOrFlush(std::move(other_block));
	}

	other.partially_filled_blocks.clear();
}

void PartialBlockManager::ClearBlocks() {
	for (auto &e : partially_filled_blocks) {
		e.second->Clear();
	}
	partially_filled_blocks.clear();
}

void PartialBlockManager::FlushPartialBlocks() {
	for (auto &e : partially_filled_blocks) {
		e.second->Flush(context);
	}
	partially_filled_blocks.clear();
}

unique_ptr<PartialBlock> PartialBlockManager::TakePartialBlock(uint32_t byte_size) {
	auto entry = partially_filled_blocks.lower_bound(byte_size);
	if (entry == partially_filled_blocks.end()) {
		// no available partial blocks have enough space available to fit the requested size
		return nullptr;
	}

	auto partial_block = std::move(entry->second);
	partially_filled_blocks.erase(entry);

	D_ASSERT(partial_block->state.offset > 0);
	D_ASSERT(ValueIsAligned(partial_block->state.offset));

	return partial_block;
}

unique_ptr<PartialBlock> PartialBlockManager::CreatePartialBlock() {
	D_ASSERT(segment_size <= block_manager.GetBlockSize());

	if (partial_block_type == PartialBlockType::IN_MEMORY_CHECKPOINT) {
		return make_uniq<InMemoryPartialBlock>(block_manager);
	}

	block_id_t block_id;
	if (partial_block_type == PartialBlockType::FULL_CHECKPOINT) {
		block_id = GetFreeBlockId();
	} else {
		block_id = INVALID_BLOCK;
	}

	return make_uniq<PartialBlock>(block_manager, block_id);
}

unique_ptr<PartialBlock> PartialBlockManager::AcquirePartialBlock(uint32_t byte_size) {
	unique_ptr<PartialBlock> partial_block;

	if (byte_size <= max_partial_block_size) {
		partial_block = TakePartialBlock(byte_size);
	}
	if (!partial_block) {
		partial_block = CreatePartialBlock();
	}

	return partial_block;
}

void PartialBlockManager::Retain(unique_ptr<PartialBlock> partial_block) {
	partially_filled_blocks.insert(make_pair(partial_block->AvailableSpace(), std::move(partial_block)));
}

unique_ptr<PartialBlock> PartialBlockManager::PruneFreeList() {
	if (partially_filled_blocks.size() <= MAX_BLOCK_MAP_SIZE) {
		return nullptr;
	}

	auto entry = partially_filled_blocks.begin();
	auto block = std::move(entry->second);
	partially_filled_blocks.erase(entry);

	return block;
}

void PartialBlockManager::RetainOrFlush(unique_ptr<PartialBlock> partial_block) {
	auto min_reusable_space = block_manager.GetBlockSize() - max_partial_block_size;
	auto available_space = partial_block->AvailableSpace();

	if (partial_block->UseCount() >= max_use_count) {
		partial_block->Flush(context);
		return;
	}
	if (available_space < min_reusable_space) {
		partial_block->Flush(context);
		return;
	}

	Retain(std::move(partial_block));

	if (auto pruned_block = PruneFreeList()) {
		pruned_block->Flush(context);
	}
}

optional<BlockPointer> PartialBlockManager::Write(BlockPackable &packable, BufferHandle source, uint32_t byte_size) {
	unique_lock<mutex> lock(partial_block_lock);

	auto block = AcquirePartialBlock(byte_size);
	BlockPointer block_ptr = block->Store(packable, std::move(source), byte_size);
	RetainOrFlush(std::move(block));

	if (partial_block_type == PartialBlockType::FULL_CHECKPOINT) {
		D_ASSERT(block_ptr != INVALID_BLOCK);
		return block_ptr;
	}

	return nullopt;
}

BlockManager &PartialBlockManager::GetBlockManager() const {
	return block_manager;
}

optional_ptr<ClientContext> PartialBlockManager::GetClientContext() const {
	return context;
}

void PartialBlockManager::Rollback() {
	ClearBlocks();
}

} // namespace duckdb
