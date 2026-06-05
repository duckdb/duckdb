#include "duckdb/storage/buffer/block_handle.hpp"

#include "duckdb/common/file_buffer.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BlockMemory::BlockMemory(BufferManager &buffer_manager, block_id_t block_id_p, MemoryTag tag_p,
                         idx_t block_alloc_size_p)
    : buffer_manager(buffer_manager), block_id(block_id_p), state(BlockState::BLOCK_UNLOADED), readers(0), tag(tag_p),
      buffer_type(FileBufferType::BLOCK), buffer(nullptr), eviction_seq_num(0), lru_timestamp_msec(),
      destroy_buffer_upon(DestroyBufferUpon::BLOCK), memory_usage(block_alloc_size_p),
      memory_charge(tag, buffer_manager.GetBufferPool()), unswizzled(nullptr),
      eviction_queue_idx(DConstants::INVALID_INDEX) {
}

BlockMemory::BlockMemory(BufferManager &buffer_manager, block_id_t block_id_p, MemoryTag tag_p,
                         unique_ptr<FileBuffer> buffer_p, DestroyBufferUpon destroy_buffer_upon_p, idx_t size_p,
                         BufferPoolReservation &&reservation)
    : buffer_manager(buffer_manager), block_id(block_id_p), state(BlockState::BLOCK_LOADED), readers(0), tag(tag_p),
      buffer_type(buffer_p->GetBufferType()), buffer(std::move(buffer_p)), eviction_seq_num(0), lru_timestamp_msec(),
      destroy_buffer_upon(destroy_buffer_upon_p), memory_usage(size_p),
      memory_charge(tag, buffer_manager.GetBufferPool()), unswizzled(nullptr),
      eviction_queue_idx(DConstants::INVALID_INDEX) {
	memory_charge = std::move(reservation); // Moved to constructor body due to tidy check.
}

BlockMemory::~BlockMemory() { // NOLINT: allow internal exceptions
	// The block memory is being destroyed, meaning that any unswizzled pointers are now binary junk.
	SetSwizzling(nullptr);
	D_ASSERT(!GetBuffer() || GetBuffer()->GetBufferType() == GetBufferType());
	if (GetBuffer() && GetBufferType() != FileBufferType::TINY_BUFFER) {
		// Kill the latest version in the eviction queue.
		GetBufferManager().GetBufferPool().IncrementDeadNodes(*this);
	}

	// Erase the block memory, if it is still loaded.
	if (GetBuffer() && GetState() == BlockState::BLOCK_LOADED) {
		D_ASSERT(GetMemoryCharge().size > 0);
		SetBuffer(nullptr);
		GetMemoryCharge().Resize(0);
	} else {
		D_ASSERT(GetMemoryCharge().size == 0);
	}

	try {
		if (BlockId() >= MAXIMUM_BLOCK) {
			// The memory buffer lives in memory.
			// Thus, it could've been offloaded to disk, and we should remove the file.
			GetBufferManager().DeleteTemporaryFile(*this);
		}
	} catch (...) { // NOLINT
		            // FIXME: log silent exceptions.
	}
}

void BlockMemory::ChangeMemoryUsage(BlockLock &l, int64_t delta) {
	VerifyMutex(l);
	// FIXME: Too clever ATM. The crux here is that the unsigned overflow is defined.
	// FIXME: It overflows twice to lead to the correct subtraction.
	D_ASSERT(delta < 0);
	memory_usage += static_cast<idx_t>(delta);
	GetMemoryCharge().Resize(GetMemoryUsage());
}

void BlockMemory::ConvertToPersistent(BlockLock &l, BlockHandle &new_block, unique_ptr<FileBuffer> new_buffer) {
	VerifyMutex(l);

	auto &new_block_memory = new_block.GetMemory();
	D_ASSERT(GetMemoryTag() == memory_charge.tag);
	if (GetMemoryTag() != new_block_memory.GetMemoryTag()) {
		const auto memory_charge_size = memory_charge.size;
		memory_charge.Resize(0);
		memory_charge.tag = new_block_memory.tag;
		memory_charge.Resize(memory_charge_size);
	}

	// Move the old block memory to the new block memory.
	new_block_memory.SetState(BlockState::BLOCK_LOADED);
	new_block_memory.GetBuffer() = std::move(new_buffer);
	new_block_memory.memory_usage = memory_usage.load();
	new_block_memory.memory_charge = std::move(memory_charge);

	// Clear the buffered data of this block.
	buffer.reset();
	SetState(BlockState::BLOCK_UNLOADED);
	memory_usage = 0;
}

void BlockMemory::ResizeBuffer(BlockLock &l, idx_t block_size, idx_t block_header_size, int64_t memory_delta) {
	// Resize and adjust the current memory.
	VerifyMutex(l);
	D_ASSERT(buffer);
	buffer->Resize(block_size, block_header_size);
	const auto new_memory_usage = NumericCast<idx_t>(NumericCast<int64_t>(memory_usage.load()) + memory_delta);
	memory_usage = new_memory_usage;
	D_ASSERT(memory_usage == buffer->AllocSize());
}

bool BlockMemory::CanUnload() const {
	if (GetState() == BlockState::BLOCK_UNLOADED) {
		// The block has already been unloaded.
		return false;
	}
	if (GetReaders() > 0) {
		// There are active readers.
		return false;
	}
	if (BlockId() >= MAXIMUM_BLOCK && MustWriteToTemporaryFile() && !GetBufferManager().HasTemporaryDirectory()) {
		// The block memory cannot be destroyed upon eviction/unpinning.
		// In order to unload this block we need to write it to a temporary buffer.
		// However, no temporary directory is specified, hence, we cannot unload.
		return false;
	}
	return true;
}

unique_ptr<FileBuffer> BlockMemory::UnloadAndTakeBlock(BlockLock &l) {
	VerifyMutex(l);

	if (GetState() == BlockState::BLOCK_UNLOADED) {
		// The block was already unloaded: nothing to do.
		return nullptr;
	}
	D_ASSERT(IsSwizzled());
	D_ASSERT(CanUnload());

	if (BlockId() >= MAXIMUM_BLOCK && MustWriteToTemporaryFile()) {
		// This is a temporary block that cannot be destroyed upon evict/unpin.
		// Thus, we write to it to a temporary file.
		buffer_manager.WriteTemporaryBuffer(GetMemoryTag(), BlockId(), *GetBuffer());
	}
	memory_charge.Resize(0);
	SetState(BlockState::BLOCK_UNLOADED);
	return std::move(GetBuffer());
}

void BlockMemory::Unload(BlockLock &l) {
	auto block = UnloadAndTakeBlock(l);
	block.reset();
}

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p, MemoryTag tag_p)
    : block_manager(block_manager), block_alloc_size(block_manager.GetBlockAllocSize()),
      block_header_size(block_manager.GetBlockHeaderSize()), block_id(block_id_p),
      memory_p(make_shared_ptr<BlockMemory>(block_manager.GetBufferManager(), block_id_p, tag_p, block_alloc_size)),
      memory(*memory_p) {
}

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p, MemoryTag tag_p,
                         unique_ptr<FileBuffer> buffer_p, DestroyBufferUpon destroy_buffer_upon_p, idx_t size_p,
                         BufferPoolReservation &&reservation)
    : block_manager(block_manager), block_alloc_size(block_manager.GetBlockAllocSize()),
      block_header_size(block_manager.GetBlockHeaderSize()), block_id(block_id_p),
      memory_p(make_shared_ptr<BlockMemory>(block_manager.GetBufferManager(), block_id_p, tag_p, std::move(buffer_p),
                                            destroy_buffer_upon_p, size_p, std::move(reservation))),
      memory(*memory_p) {
}

BlockHandle::~BlockHandle() { // NOLINT: allow internal exceptions
	// Early-out for temporary blocks.
	if (block_id >= MAXIMUM_BLOCK) {
		return;
	}
	try {
		block_manager.UnregisterPersistentBlock(*this);
	} catch (...) { // NOLINT
		            // FIXME: log silent exceptions.
	}
}

unique_ptr<Block> AllocateBlock(BlockManager &block_manager, unique_ptr<FileBuffer> reusable_buffer,
                                block_id_t block_id) {
	if (reusable_buffer && reusable_buffer->GetHeaderSize() == block_manager.GetBlockHeaderSize()) {
		// Reusable buffer: reuse it.
		if (reusable_buffer->GetBufferType() == FileBufferType::BLOCK) {
			// Reuse the entire buffer.
			auto &block = reinterpret_cast<Block &>(*reusable_buffer);
			block.id = block_id;
			return unique_ptr_cast<FileBuffer, Block>(std::move(reusable_buffer));
		}
		auto block = block_manager.CreateBlock(block_id, reusable_buffer.get());
		reusable_buffer.reset();
		return block;
	}

	// Not a reusable buffer: allocate a new block.
	return block_manager.CreateBlock(block_id, nullptr);
}

BufferHandle BlockHandle::LoadFromBuffer(BlockLock &l, data_ptr_t data, unique_ptr<FileBuffer> reusable_buffer,
                                         BufferPoolReservation reservation) {
	memory.VerifyMutex(l);
	// Copy the data of the file buffer into the block.
	D_ASSERT(memory.GetState() != BlockState::BLOCK_LOADED);
	D_ASSERT(memory.GetReaders() == 0);
	auto block = AllocateBlock(block_manager, std::move(reusable_buffer), block_id);
	memcpy(block->InternalBuffer(), data, block->AllocSize());
	memory.GetBuffer() = std::move(block);
	memory.SetState(BlockState::BLOCK_LOADED);
	memory.SetReaders(1);
	memory.GetMemoryCharge() = std::move(reservation);
	return BufferHandle(shared_from_this(), memory.GetBuffer());
}

BufferHandle BlockHandle::Load(QueryContext context, unique_ptr<FileBuffer> reusable_buffer) {
	if (memory.GetState() == BlockState::BLOCK_LOADED) {
		// The block has already been loaded.
		D_ASSERT(memory.GetBuffer());
		memory.IncrementReaders();
		return BufferHandle(shared_from_this(), memory.GetBuffer());
	}

	if (BlockId() < MAXIMUM_BLOCK) {
		auto block = AllocateBlock(block_manager, std::move(reusable_buffer), block_id);
		block_manager.Read(context, *block);
		memory.GetBuffer() = std::move(block);
	} else {
		if (!memory.MustWriteToTemporaryFile()) {
			// The buffer was destroyed upon unpin/evict, so there is no temporary buffer to read.
			return BufferHandle();
		}
		auto &buffer_manager = memory.GetBufferManager();
		auto &buffer = memory.GetBuffer();
		buffer = buffer_manager.ReadTemporaryBuffer(QueryContext(), memory.GetMemoryTag(), *this,
		                                            std::move(reusable_buffer));
	}
	memory.SetState(BlockState::BLOCK_LOADED);
	memory.SetReaders(1);
	return BufferHandle(shared_from_this(), memory.GetBuffer());
}

} // namespace duckdb
