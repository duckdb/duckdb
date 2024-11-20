#include "duckdb/storage/buffer/block_handle.hpp"

#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p, MemoryTag tag)
    : block_manager(block_manager), readers(0), block_id(block_id_p), tag(tag), buffer_type(FileBufferType::BLOCK),
      buffer(nullptr), eviction_seq_num(0), destroy_buffer_upon(DestroyBufferUpon::BLOCK),
      memory_charge(tag, block_manager.buffer_manager.GetBufferPool()), unswizzled(nullptr),
      eviction_queue_idx(DConstants::INVALID_INDEX) {
	eviction_seq_num = 0;
	state = BlockState::BLOCK_UNLOADED;
	memory_usage = block_manager.GetBlockAllocSize();
}

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p, MemoryTag tag,
                         unique_ptr<FileBuffer> buffer_p, DestroyBufferUpon destroy_buffer_upon_p, idx_t block_size,
                         BufferPoolReservation &&reservation)
    : block_manager(block_manager), readers(0), block_id(block_id_p), tag(tag), buffer_type(buffer_p->GetBufferType()),
      eviction_seq_num(0), destroy_buffer_upon(destroy_buffer_upon_p),
      memory_charge(tag, block_manager.buffer_manager.GetBufferPool()), unswizzled(nullptr),
      eviction_queue_idx(DConstants::INVALID_INDEX) {
	buffer = std::move(buffer_p);
	state = BlockState::BLOCK_LOADED;
	memory_usage = block_size;
	memory_charge = std::move(reservation);
}

BlockHandle::~BlockHandle() { // NOLINT: allow internal exceptions
	// being destroyed, so any unswizzled pointers are just binary junk now.
	unswizzled = nullptr;
	D_ASSERT(!buffer || buffer->GetBufferType() == buffer_type);
	if (buffer && buffer_type != FileBufferType::TINY_BUFFER) {
		// we kill the latest version in the eviction queue
		auto &buffer_manager = block_manager.buffer_manager;
		buffer_manager.GetBufferPool().IncrementDeadNodes(*this);
	}

	// no references remain to this block: erase
	if (buffer && state == BlockState::BLOCK_LOADED) {
		D_ASSERT(memory_charge.size > 0);
		// the block is still loaded in memory: erase it
		buffer.reset();
		memory_charge.Resize(0);
	} else {
		D_ASSERT(memory_charge.size == 0);
	}

	block_manager.UnregisterBlock(*this);
}

unique_ptr<Block> AllocateBlock(BlockManager &block_manager, unique_ptr<FileBuffer> reusable_buffer,
                                block_id_t block_id) {
	if (reusable_buffer) {
		// re-usable buffer: re-use it
		if (reusable_buffer->GetBufferType() == FileBufferType::BLOCK) {
			// we can reuse the buffer entirely
			auto &block = reinterpret_cast<Block &>(*reusable_buffer);
			block.id = block_id;
			return unique_ptr_cast<FileBuffer, Block>(std::move(reusable_buffer));
		}
		auto block = block_manager.CreateBlock(block_id, reusable_buffer.get());
		reusable_buffer.reset();
		return block;
	} else {
		// no re-usable buffer: allocate a new block
		return block_manager.CreateBlock(block_id, nullptr);
	}
}

void BlockHandle::ChangeMemoryUsage(BlockLock &l, int64_t delta) {
	VerifyMutex(l);

	D_ASSERT(delta < 0);
	memory_usage += static_cast<idx_t>(delta);
	memory_charge.Resize(memory_usage);
}

unique_ptr<FileBuffer> &BlockHandle::GetBuffer(BlockLock &l) {
	VerifyMutex(l);
	return buffer;
}

void BlockHandle::VerifyMutex(BlockLock &l) const {
	D_ASSERT(l.owns_lock());
	D_ASSERT(l.mutex() == &lock);
}

BufferPoolReservation &BlockHandle::GetMemoryCharge(BlockLock &l) {
	VerifyMutex(l);
	return memory_charge;
}

void BlockHandle::MergeMemoryReservation(BlockLock &l, BufferPoolReservation reservation) {
	VerifyMutex(l);
	memory_charge.Merge(std::move(reservation));
}

void BlockHandle::ResizeMemory(BlockLock &l, idx_t alloc_size) {
	VerifyMutex(l);
	memory_charge.Resize(alloc_size);
}

void BlockHandle::ResizeBuffer(BlockLock &l, idx_t block_size, int64_t memory_delta) {
	VerifyMutex(l);

	D_ASSERT(buffer);
	// resize and adjust current memory
	buffer->Resize(block_size);
	memory_usage = NumericCast<idx_t>(NumericCast<int64_t>(memory_usage.load()) + memory_delta);
	D_ASSERT(memory_usage == buffer->AllocSize());
}

BufferHandle BlockHandle::LoadFromBuffer(BlockLock &l, data_ptr_t data, unique_ptr<FileBuffer> reusable_buffer,
                                         BufferPoolReservation reservation) {
	VerifyMutex(l);

	D_ASSERT(state != BlockState::BLOCK_LOADED);
	D_ASSERT(readers == 0);
	// copy over the data into the block from the file buffer
	auto block = AllocateBlock(block_manager, std::move(reusable_buffer), block_id);
	memcpy(block->InternalBuffer(), data, block->AllocSize());
	buffer = std::move(block);
	state = BlockState::BLOCK_LOADED;
	readers = 1;
	memory_charge = std::move(reservation);
	return BufferHandle(shared_from_this(), buffer.get());
}

BufferHandle BlockHandle::Load(unique_ptr<FileBuffer> reusable_buffer) {
	if (state == BlockState::BLOCK_LOADED) {
		// already loaded
		D_ASSERT(buffer);
		++readers;
		return BufferHandle(shared_from_this(), buffer.get());
	}

	if (block_id < MAXIMUM_BLOCK) {
		auto block = AllocateBlock(block_manager, std::move(reusable_buffer), block_id);
		block_manager.Read(*block);
		buffer = std::move(block);
	} else {
		if (MustWriteToTemporaryFile()) {
			buffer = block_manager.buffer_manager.ReadTemporaryBuffer(tag, *this, std::move(reusable_buffer));
		} else {
			return BufferHandle(); // Destroyed upon unpin/evict, so there is no temp buffer to read
		}
	}
	state = BlockState::BLOCK_LOADED;
	readers = 1;
	return BufferHandle(shared_from_this(), buffer.get());
}

unique_ptr<FileBuffer> BlockHandle::UnloadAndTakeBlock(BlockLock &lock) {
	VerifyMutex(lock);

	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded: nothing to do
		return nullptr;
	}
	D_ASSERT(!unswizzled);
	D_ASSERT(CanUnload());

	if (block_id >= MAXIMUM_BLOCK && MustWriteToTemporaryFile()) {
		// temporary block that cannot be destroyed upon evict/unpin: write to temporary file
		block_manager.buffer_manager.WriteTemporaryBuffer(tag, block_id, *buffer);
	}
	memory_charge.Resize(0);
	state = BlockState::BLOCK_UNLOADED;
	return std::move(buffer);
}

void BlockHandle::Unload(BlockLock &lock) {
	auto block = UnloadAndTakeBlock(lock);
	block.reset();
}

bool BlockHandle::CanUnload() const {
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded
		return false;
	}
	if (readers > 0) {
		// there are active readers
		return false;
	}
	if (block_id >= MAXIMUM_BLOCK && MustWriteToTemporaryFile() &&
	    !block_manager.buffer_manager.HasTemporaryDirectory()) {
		// this block cannot be destroyed upon evict/unpin
		// in order to unload this block we need to write it to a temporary buffer
		// however, no temporary directory is specified!
		// hence we cannot unload the block
		return false;
	}
	return true;
}

void BlockHandle::ConvertToPersistent(BlockLock &l, BlockHandle &new_block, unique_ptr<FileBuffer> new_buffer) {
	VerifyMutex(l);

	// move the data from the old block into data for the new block
	new_block.state = BlockState::BLOCK_LOADED;
	new_block.buffer = std::move(new_buffer);
	new_block.memory_usage = memory_usage.load();
	new_block.memory_charge = std::move(memory_charge);

	// clear out the buffer data from this block
	buffer.reset();
	state = BlockState::BLOCK_UNLOADED;
	memory_usage = 0;
}

} // namespace duckdb
