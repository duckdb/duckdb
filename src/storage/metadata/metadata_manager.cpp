#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

MetadataManager::MetadataManager(BlockManager &block_manager, BufferManager &buffer_manager) :
	block_manager(block_manager), buffer_manager(buffer_manager) {
}

MetadataManager::~MetadataManager() {
}

MetadataHandle MetadataManager::AllocateHandle() {
	// check if there is any free space left in an existing block
	// if not allocate a new block
	if (free_blocks.empty()) {
		AllocateNewBlock();
	}
	D_ASSERT(!free_blocks.empty());

	// select the first free metadata block we can find
	MetadataPointer pointer;
	pointer.block_index = free_blocks.front();
	auto &block = blocks[pointer.block_index];
	D_ASSERT(!block.free_blocks.empty());
	pointer.index = block.free_blocks.back();
	// mark the block as used
	block.free_blocks.pop_back();
	if (block.free_blocks.empty()) {
		free_blocks.erase(free_blocks.begin());
	}
	D_ASSERT(pointer.index < METADATA_BLOCK_COUNT);
	// pin the block
	return Pin(pointer);
}

MetadataHandle MetadataManager::Pin(MetadataPointer pointer) {
	D_ASSERT(pointer.index < METADATA_BLOCK_COUNT);
	auto &block = blocks[pointer.block_index];

	MetadataHandle handle;
	handle.pointer.block_index = pointer.block_index;
	handle.pointer.index = pointer.index;
	handle.handle = buffer_manager.Pin(block.block);
	return handle;
}

void MetadataManager::AllocateNewBlock() {
	free_blocks.push_back(blocks.size());
	MetadataBlock new_block;
	buffer_manager.Allocate(Storage::BLOCK_ALLOC_SIZE, false, &new_block.block);
	new_block.block_id = GetNextBlockId();
	for(idx_t i = 0; i < METADATA_BLOCK_COUNT; i++) {
		new_block.free_blocks.push_back(METADATA_BLOCK_COUNT - i - 1);
	}
	blocks.push_back(std::move(new_block));
}

MetaBlockPointer MetadataManager::GetDiskPointer(MetadataPointer pointer, uint32_t offset) {
	idx_t block_pointer = blocks[pointer.block_index].block_id;
	block_pointer |= idx_t(pointer.index) << 56ULL;
	return MetaBlockPointer(block_pointer, offset);

}

MetadataPointer MetadataManager::FromDiskPointer(MetaBlockPointer pointer) {
	auto block_id = block_id_t(pointer.block_pointer & ~(idx_t(0xFF) << 56ULL));
	auto index = pointer.block_pointer >> 56ULL;
	for(idx_t i = 0; i < blocks.size(); i++) {
		auto &block = blocks[i];
		if (block.block->BlockId() == block_id) {
			MetadataPointer result;
			result.block_index = i;
			result.index = index;
			return result;
		}
	}
	throw InternalException("Failed to load metadata pointer (block id %llu, index %llu, pointer %llu)\n", block_id, index, pointer.block_pointer);
}

idx_t MetadataManager::BlockCount() {
	return blocks.size();
}

void MetadataManager::Flush() {
	// write the blocks of the metadata manager to disk
	for(auto &block : blocks) {
		auto handle = buffer_manager.Pin(block.block);
		block_manager.Write(handle.GetFileBuffer(), block.block_id);
	}
}

void MetadataManager::Serialize(Serializer &serializer) {
	serializer.Write<uint64_t>(blocks.size());
	for(auto &block : blocks) {
		block.Serialize(serializer);
	}
}

void MetadataManager::Deserialize(Deserializer &source) {
	auto block_count = source.Read<uint64_t>();
	for(idx_t i = 0; i < block_count; i++) {
		auto block = MetadataBlock::Deserialize(source);
		block.block = block_manager.RegisterBlock(block.block_id);
		blocks.push_back(std::move(block));
	}
}

void MetadataBlock::Serialize(Serializer &serializer) {
	serializer.Write<block_id_t>(block_id);
	serializer.Write<idx_t>(FreeBlocksToInteger());
}

MetadataBlock MetadataBlock::Deserialize(Deserializer &source) {
	MetadataBlock result;
	result.block_id = source.Read<block_id_t>();
	auto free_list = source.Read<idx_t>();
	result.FreeBlocksFromInteger(free_list);
	return result;
}

idx_t MetadataBlock::FreeBlocksToInteger() {
	idx_t result = 0;
	for(idx_t i = 0; i < free_blocks.size(); i++) {
		D_ASSERT(free_blocks[i] < idx_t(64));
		idx_t mask = idx_t(1) << idx_t(free_blocks[i]);
		result |= mask;
	}
	return result;
}

void MetadataBlock::FreeBlocksFromInteger(idx_t free_list) {
	if (free_list == 0) {
		return;
	}
	for(idx_t i = 0; i < 64; i++) {
		idx_t mask = idx_t(1) >> i;
		if (free_list & mask) {
			free_blocks.push_back(i);
		}
	}
}



void MetadataManager::MarkWrittenBlocks() {
//	throw InternalException("FIXME: MarkWrittenBlocks");
}

block_id_t MetadataManager::GetNextBlockId() {
	return block_manager.GetFreeBlockId();
}

}
