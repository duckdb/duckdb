#include "duckdb/storage/buffer/block_handle.hpp"

#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p, MemoryTag tag)
    : block_manager(block_manager), readers(0), block_id(block_id_p), tag(tag), buffer(nullptr), eviction_seq_num(0),
      destroy_buffer_upon(DestroyBufferUpon::BLOCK), memory_charge(tag, block_manager.buffer_manager.GetBufferPool()),
      unswizzled(nullptr) {
	eviction_seq_num = 0;
	state = BlockState::BLOCK_UNLOADED;
	memory_usage = block_manager.GetBlockAllocSize();
}

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p, MemoryTag tag,
                         unique_ptr<FileBuffer> buffer_p, DestroyBufferUpon destroy_buffer_upon_p, idx_t block_size,
                         BufferPoolReservation &&reservation)
    : block_manager(block_manager), readers(0), block_id(block_id_p), tag(tag), eviction_seq_num(0),
      destroy_buffer_upon(destroy_buffer_upon_p), memory_charge(tag, block_manager.buffer_manager.GetBufferPool()),
      unswizzled(nullptr) {
	buffer = std::move(buffer_p);
	state = BlockState::BLOCK_LOADED;
	memory_usage = block_size;
	memory_charge = std::move(reservation);
}

BlockHandle::~BlockHandle() { // NOLINT: allow internal exceptions
	// being destroyed, so any unswizzled pointers are just binary junk now.
	unswizzled = nullptr;
	if (buffer && buffer->type != FileBufferType::TINY_BUFFER) {
		// we kill the latest version in the eviction queue
		auto &buffer_manager = block_manager.buffer_manager;
		buffer_manager.GetBufferPool().IncrementDeadNodes(buffer->type);
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
		if (reusable_buffer->type == FileBufferType::BLOCK) {
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

BufferHandle BlockHandle::LoadFromBuffer(data_ptr_t data, unique_ptr<FileBuffer> reusable_buffer) {
	D_ASSERT(state != BlockState::BLOCK_LOADED);
	// copy over the data into the block from the file buffer
	auto block = AllocateBlock(block_manager, std::move(reusable_buffer), block_id);
	memcpy(block->InternalBuffer(), data, block->AllocSize());
	buffer = std::move(block);
	state = BlockState::BLOCK_LOADED;
	return BufferHandle(shared_from_this());
}

BufferHandle BlockHandle::Load(unique_ptr<FileBuffer> reusable_buffer) {
	if (state == BlockState::BLOCK_LOADED) {
		// already loaded
		D_ASSERT(buffer);
		return BufferHandle(shared_from_this());
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
	return BufferHandle(shared_from_this());
}

unique_ptr<FileBuffer> BlockHandle::UnloadAndTakeBlock() {
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

void BlockHandle::Unload() {
	auto block = UnloadAndTakeBlock();
	block.reset();
}

bool BlockHandle::CanUnload() {
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

} // namespace duckdb
