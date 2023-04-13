#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/file_buffer.hpp"

namespace duckdb {

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p)
    : block_manager(block_manager), readers(0), block_id(block_id_p), buffer(nullptr), eviction_timestamp(0),
      can_destroy(false), memory_charge(block_manager.buffer_manager.GetBufferPool()), unswizzled(nullptr) {
	eviction_timestamp = 0;
	state = BlockState::BLOCK_UNLOADED;
	memory_usage = Storage::BLOCK_ALLOC_SIZE;
}

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p, unique_ptr<FileBuffer> buffer_p,
                         bool can_destroy_p, idx_t block_size, BufferPoolReservation &&reservation)
    : block_manager(block_manager), readers(0), block_id(block_id_p), eviction_timestamp(0), can_destroy(can_destroy_p),
      memory_charge(block_manager.buffer_manager.GetBufferPool()), unswizzled(nullptr) {
	buffer = std::move(buffer_p);
	state = BlockState::BLOCK_LOADED;
	memory_usage = block_size;
	memory_charge = std::move(reservation);
}

BlockHandle::~BlockHandle() { // NOLINT: allow internal exceptions
	// being destroyed, so any unswizzled pointers are just binary junk now.
	unswizzled = nullptr;
	auto &buffer_manager = block_manager.buffer_manager;
	// no references remain to this block: erase
	if (buffer && state == BlockState::BLOCK_LOADED) {
		D_ASSERT(memory_charge.size > 0);
		// the block is still loaded in memory: erase it
		buffer.reset();
		memory_charge.Resize(0);
	} else {
		D_ASSERT(memory_charge.size == 0);
	}
	buffer_manager.buffer_pool.PurgeQueue();
	block_manager.UnregisterBlock(block_id, can_destroy);
}

unique_ptr<Block> AllocateBlock(BlockManager &block_manager, unique_ptr<FileBuffer> reusable_buffer,
                                block_id_t block_id) {
	if (reusable_buffer) {
		// re-usable buffer: re-use it
		if (reusable_buffer->type == FileBufferType::BLOCK) {
			// we can reuse the buffer entirely
			auto &block = (Block &)*reusable_buffer;
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

BufferHandle BlockHandle::Load(shared_ptr<BlockHandle> &handle, unique_ptr<FileBuffer> reusable_buffer) {
	if (handle->state == BlockState::BLOCK_LOADED) {
		// already loaded
		D_ASSERT(handle->buffer);
		return BufferHandle(handle, handle->buffer.get());
	}

	auto &block_manager = handle->block_manager;
	if (handle->block_id < MAXIMUM_BLOCK) {
		auto block = AllocateBlock(block_manager, std::move(reusable_buffer), handle->block_id);
		block_manager.Read(*block);
		handle->buffer = std::move(block);
	} else {
		if (handle->can_destroy) {
			return BufferHandle();
		} else {
			handle->buffer =
			    block_manager.buffer_manager.ReadTemporaryBuffer(handle->block_id, std::move(reusable_buffer));
		}
	}
	handle->state = BlockState::BLOCK_LOADED;
	return BufferHandle(handle, handle->buffer.get());
}

unique_ptr<FileBuffer> BlockHandle::UnloadAndTakeBlock() {
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded: nothing to do
		return nullptr;
	}
	D_ASSERT(!unswizzled);
	D_ASSERT(CanUnload());

	if (block_id >= MAXIMUM_BLOCK && !can_destroy) {
		// temporary block that cannot be destroyed: write to temporary file
		block_manager.buffer_manager.WriteTemporaryBuffer(block_id, *buffer);
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
	if (block_id >= MAXIMUM_BLOCK && !can_destroy && block_manager.buffer_manager.temp_directory.empty()) {
		// in order to unload this block we need to write it to a temporary buffer
		// however, no temporary directory is specified!
		// hence we cannot unload the block
		return false;
	}
	return true;
}

} // namespace duckdb
