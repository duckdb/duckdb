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
	buffer_manager.Allocate(Storage::BLOCK_SIZE, false, &new_block.block);
	new_block.block_id = GetNextBlockId();
	for(idx_t i = 0; i < METADATA_BLOCK_COUNT; i++) {
		new_block.free_blocks.push_back(METADATA_BLOCK_COUNT - i - 1);
	}
	AddBlock(std::move(new_block));
}

void MetadataManager::AddBlock(MetadataBlock new_block) {
	if (block_map.find(new_block.block_id) != block_map.end()) {
		throw InternalException("Block id with id %llu already exists", new_block.block_id);
	}
	block_map[new_block.block_id] = blocks.size();
	blocks.push_back(std::move(new_block));
}

void MetadataManager::AddAndRegisterBlock(MetadataBlock block) {
	if (block.block) {
		throw InternalException("Calling AddAndRegisterBlock on block that already exists");
	}
	block.block = block_manager.RegisterBlock(block.block_id);
	AddBlock(std::move(block));
}

MetaBlockPointer MetadataManager::GetDiskPointer(MetadataPointer pointer, uint32_t offset) {
	idx_t block_pointer = blocks[pointer.block_index].block_id;
	block_pointer |= idx_t(pointer.index) << 56ULL;
	return MetaBlockPointer(block_pointer, offset);
}

block_id_t MetaBlockPointer::GetBlockId() {
	return block_id_t(block_pointer & ~(idx_t(0xFF) << 56ULL));;
}

uint32_t MetaBlockPointer::GetBlockIndex() {
	return block_pointer >> 56ULL;
}

MetadataPointer MetadataManager::FromDiskPointer(MetaBlockPointer pointer) {
	auto block_id = pointer.GetBlockId();
	auto index = pointer.GetBlockIndex();
	auto entry = block_map.find(block_id);
	if (entry == block_map.end()) {
		throw InternalException("Failed to load metadata pointer (block id %llu, index %llu, pointer %llu)\n", block_id, index, pointer.block_pointer);
	}
	MetadataPointer result;
	result.block_index = entry->second;
	result.index = index;
	return result;
}

MetadataPointer MetadataManager::RegisterDiskPointer(MetaBlockPointer pointer) {
	auto block_id = pointer.GetBlockId();
	MetadataBlock block;
	block.block_id = block_id;
	AddAndRegisterBlock(block);
	return FromDiskPointer(pointer);
}

BlockPointer MetadataManager::ToBlockPointer(MetaBlockPointer meta_pointer) {
	BlockPointer result;
	result.block_id = meta_pointer.GetBlockId();
	result.offset = meta_pointer.GetBlockIndex() * MetadataManager::METADATA_BLOCK_SIZE + meta_pointer.offset;
	D_ASSERT(result.offset < MetadataManager::METADATA_BLOCK_SIZE * MetadataManager::METADATA_BLOCK_COUNT);
	return result;
}

MetaBlockPointer MetadataManager::FromBlockPointer(BlockPointer block_pointer) {
	if (!block_pointer.IsValid()) {
		return MetaBlockPointer();
	}
	idx_t index = block_pointer.offset / MetadataManager::METADATA_BLOCK_SIZE;
	auto offset = block_pointer.offset % MetadataManager::METADATA_BLOCK_SIZE;
	D_ASSERT(index < MetadataManager::METADATA_BLOCK_COUNT);
	D_ASSERT(offset < MetadataManager::METADATA_BLOCK_SIZE);
	MetaBlockPointer result;
	result.block_pointer = idx_t(block_pointer.block_id) | index << 56ULL;
	result.offset = offset;
	return result;
}

idx_t MetadataManager::BlockCount() {
	return blocks.size();
}

void MetadataManager::Flush() {
	// write the blocks of the metadata manager to disk
	for(auto &block : blocks) {
		auto handle = buffer_manager.Pin(block.block);
		// FIXME: zero-initialize any free blocks
//		for(auto &free_block : free_blocks) {
//			memset(handle.Ptr() + free_block * MetadataManager::METADATA_BLOCK_SIZE, 0, MetadataManager::METADATA_BLOCK_SIZE);
//		}
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
		auto entry = block_map.find(block.block_id);
		if (entry == block_map.end()) {
			// block does not exist yet
			AddAndRegisterBlock(std::move(block));
		} else {
			// block was already created - only copy over the free list
			blocks[entry->second].free_blocks = std::move(block.free_blocks);
		}
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
	for(idx_t i = 64; i > 0; i--) {
		auto index = i - 1;
		idx_t mask = idx_t(1) << index;
		if (free_list & mask) {
			free_blocks.push_back(index);
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
