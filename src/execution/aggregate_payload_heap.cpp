#include "duckdb/execution/aggregate_payload_heap.hpp"

#include <cstring>

namespace duckdb {

AggregatePayloadRegistry::AggregatePayloadRegistry(BufferManager &buffer_manager_p) : buffer_manager(buffer_manager_p) {
}

uint32_t AggregatePayloadRegistry::RegisterBlock(shared_ptr<BlockHandle> block) {
	lock_guard<mutex> guard(lock);
	blocks.push_back(std::move(block));
	return UnsafeNumericCast<uint32_t>(blocks.size() - 1);
}

BufferHandle AggregatePayloadRegistry::PinBlock(uint32_t block_idx) {
	shared_ptr<BlockHandle> block;
	{
		lock_guard<mutex> guard(lock);
		block = blocks[block_idx];
	}
	return buffer_manager.Pin(block);
}

AggregatePayloadHeap::AggregatePayloadHeap(AggregatePayloadRegistry &registry_p) : registry(registry_p) {
}

AggregatePayloadHandle AggregatePayloadHeap::Append(const char *data, uint32_t length) {
	if (current_offset + length > current_capacity) {
		// Dropping the pin makes the previous block evictable, only the block being written stays pinned
		const auto capacity = MaxValue<idx_t>(registry.buffer_manager.GetBlockSize(), length);
		auto buffer_handle = registry.buffer_manager.Allocate(MemoryTag::HASH_TABLE, capacity, false);
		current_block_idx = registry.RegisterBlock(buffer_handle.GetBlockHandle());
		current_pin = std::move(buffer_handle);
		current_offset = 0;
		current_capacity = capacity;
	}
	memcpy(current_pin.GetDataMutable() + current_offset, data, length);
	AggregatePayloadHandle handle;
	handle.block_idx = current_block_idx;
	handle.offset = UnsafeNumericCast<uint32_t>(current_offset);
	handle.length = length;
	current_offset += length;
	return handle;
}

const char *AggregatePayloadHeap::Read(const AggregatePayloadHandle &handle, BufferHandle &pin) {
	pin = registry.PinBlock(handle.block_idx);
	return const_char_ptr_cast(pin.Ptr()) + handle.offset;
}

} // namespace duckdb
