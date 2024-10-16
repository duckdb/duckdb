#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/serializer/read_stream.hpp"
#include "duckdb/storage/database_size.hpp"

namespace duckdb {

MetadataManager::MetadataManager(BlockManager &block_manager, BufferManager &buffer_manager)
    : block_manager(block_manager), buffer_manager(buffer_manager) {
}

MetadataManager::~MetadataManager() {
}

MetadataHandle MetadataManager::AllocateHandle() {
	// check if there is any free space left in an existing block
	// if not allocate a new block
	block_id_t free_block = INVALID_BLOCK;
	for (auto &kv : blocks) {
		auto &block = kv.second;
		D_ASSERT(kv.first == block.block_id);
		if (!block.free_blocks.empty()) {
			free_block = kv.first;
			break;
		}
	}
	if (free_block == INVALID_BLOCK || free_block > PeekNextBlockId()) {
		free_block = AllocateNewBlock();
	}
	D_ASSERT(free_block != INVALID_BLOCK);

	// select the first free metadata block we can find
	MetadataPointer pointer;
	pointer.block_index = UnsafeNumericCast<idx_t>(free_block);
	auto &block = blocks[free_block];
	if (block.block->BlockId() < MAXIMUM_BLOCK) {
		// this block is a disk-backed block, yet we are planning to write to it
		// we need to convert it into a transient block before we can write to it
		ConvertToTransient(block);
		D_ASSERT(block.block->BlockId() >= MAXIMUM_BLOCK);
	}
	D_ASSERT(!block.free_blocks.empty());
	pointer.index = block.free_blocks.back();
	// mark the block as used
	block.free_blocks.pop_back();
	D_ASSERT(pointer.index < METADATA_BLOCK_COUNT);
	// pin the block
	return Pin(pointer);
}

MetadataHandle MetadataManager::Pin(MetadataPointer pointer) {
	D_ASSERT(pointer.index < METADATA_BLOCK_COUNT);
	auto &block = blocks[UnsafeNumericCast<int64_t>(pointer.block_index)];

	MetadataHandle handle;
	handle.pointer.block_index = pointer.block_index;
	handle.pointer.index = pointer.index;
	handle.handle = buffer_manager.Pin(block.block);
	return handle;
}

void MetadataManager::ConvertToTransient(MetadataBlock &metadata_block) {
	// pin the old block
	auto old_buffer = buffer_manager.Pin(metadata_block.block);

	// allocate a new transient block to replace it
	auto new_buffer = buffer_manager.Allocate(MemoryTag::METADATA, block_manager.GetBlockSize(), false);
	auto new_block = new_buffer.GetBlockHandle();

	// copy the data to the transient block
	memcpy(new_buffer.Ptr(), old_buffer.Ptr(), block_manager.GetBlockSize());
	metadata_block.block = std::move(new_block);

	// unregister the old block
	block_manager.UnregisterBlock(metadata_block.block_id);
}

block_id_t MetadataManager::AllocateNewBlock() {
	auto new_block_id = GetNextBlockId();

	MetadataBlock new_block;
	auto handle = buffer_manager.Allocate(MemoryTag::METADATA, block_manager.GetBlockSize(), false);
	new_block.block = handle.GetBlockHandle();
	new_block.block_id = new_block_id;
	for (idx_t i = 0; i < METADATA_BLOCK_COUNT; i++) {
		new_block.free_blocks.push_back(NumericCast<uint8_t>(METADATA_BLOCK_COUNT - i - 1));
	}
	// zero-initialize the handle
	memset(handle.Ptr(), 0, block_manager.GetBlockSize());
	AddBlock(std::move(new_block));
	return new_block_id;
}

void MetadataManager::AddBlock(MetadataBlock new_block, bool if_exists) {
	if (blocks.find(new_block.block_id) != blocks.end()) {
		if (if_exists) {
			return;
		}
		throw InternalException("Block id with id %llu already exists", new_block.block_id);
	}
	blocks[new_block.block_id] = std::move(new_block);
}

void MetadataManager::AddAndRegisterBlock(MetadataBlock block) {
	if (block.block) {
		throw InternalException("Calling AddAndRegisterBlock on block that already exists");
	}
	block.block = block_manager.RegisterBlock(block.block_id);
	AddBlock(std::move(block), true);
}

MetaBlockPointer MetadataManager::GetDiskPointer(MetadataPointer pointer, uint32_t offset) {
	idx_t block_pointer = idx_t(pointer.block_index);
	block_pointer |= idx_t(pointer.index) << 56ULL;
	return MetaBlockPointer(block_pointer, offset);
}

block_id_t MetaBlockPointer::GetBlockId() const {
	return block_id_t(block_pointer & ~(idx_t(0xFF) << 56ULL));
}

uint32_t MetaBlockPointer::GetBlockIndex() const {
	return block_pointer >> 56ULL;
}

MetadataPointer MetadataManager::FromDiskPointer(MetaBlockPointer pointer) {
	auto block_id = pointer.GetBlockId();
	auto index = pointer.GetBlockIndex();
	auto entry = blocks.find(block_id);
	if (entry == blocks.end()) { // LCOV_EXCL_START
		throw InternalException("Failed to load metadata pointer (id %llu, idx %llu, ptr %llu)\n", block_id, index,
		                        pointer.block_pointer);
	} // LCOV_EXCL_STOP
	MetadataPointer result;
	result.block_index = UnsafeNumericCast<idx_t>(block_id);
	result.index = UnsafeNumericCast<uint8_t>(index);
	return result;
}

MetadataPointer MetadataManager::RegisterDiskPointer(MetaBlockPointer pointer) {
	auto block_id = pointer.GetBlockId();
	MetadataBlock block;
	block.block_id = block_id;
	AddAndRegisterBlock(block);
	return FromDiskPointer(pointer);
}

BlockPointer MetadataManager::ToBlockPointer(MetaBlockPointer meta_pointer, const idx_t metadata_block_size) {
	BlockPointer result;
	result.block_id = meta_pointer.GetBlockId();
	result.offset = meta_pointer.GetBlockIndex() * NumericCast<uint32_t>(metadata_block_size) + meta_pointer.offset;
	D_ASSERT(result.offset < metadata_block_size * MetadataManager::METADATA_BLOCK_COUNT);
	return result;
}

MetaBlockPointer MetadataManager::FromBlockPointer(BlockPointer block_pointer, const idx_t metadata_block_size) {
	if (!block_pointer.IsValid()) {
		return MetaBlockPointer();
	}
	idx_t index = block_pointer.offset / metadata_block_size;
	auto offset = block_pointer.offset % metadata_block_size;
	D_ASSERT(index < MetadataManager::METADATA_BLOCK_COUNT);
	D_ASSERT(offset < metadata_block_size);
	MetaBlockPointer result;
	result.block_pointer = idx_t(block_pointer.block_id) | index << 56ULL;
	result.offset = UnsafeNumericCast<uint32_t>(offset);
	return result;
}

idx_t MetadataManager::BlockCount() {
	return blocks.size();
}

void MetadataManager::Flush() {
	const idx_t total_metadata_size = GetMetadataBlockSize() * MetadataManager::METADATA_BLOCK_COUNT;

	// write the blocks of the metadata manager to disk
	for (auto &kv : blocks) {
		auto &block = kv.second;
		auto handle = buffer_manager.Pin(block.block);
		// there are a few bytes left-over at the end of the block, zero-initialize them
		memset(handle.Ptr() + total_metadata_size, 0, block_manager.GetBlockSize() - total_metadata_size);
		D_ASSERT(kv.first == block.block_id);
		if (block.block->BlockId() >= MAXIMUM_BLOCK) {
			// temporary block - convert to persistent
			block.block = block_manager.ConvertToPersistent(kv.first, std::move(block.block));
		} else {
			// already a persistent block - only need to write it
			D_ASSERT(block.block->BlockId() == block.block_id);
			block_manager.Write(handle.GetFileBuffer(), block.block_id);
		}
	}
}

void MetadataManager::Write(WriteStream &sink) {
	sink.Write<uint64_t>(blocks.size());
	for (auto &kv : blocks) {
		kv.second.Write(sink);
	}
}

void MetadataManager::Read(ReadStream &source) {
	auto block_count = source.Read<uint64_t>();
	for (idx_t i = 0; i < block_count; i++) {
		auto block = MetadataBlock::Read(source);
		auto entry = blocks.find(block.block_id);
		if (entry == blocks.end()) {
			// block does not exist yet
			AddAndRegisterBlock(std::move(block));
		} else {
			// block was already created - only copy over the free list
			entry->second.free_blocks = std::move(block.free_blocks);
		}
	}
}

void MetadataBlock::Write(WriteStream &sink) {
	sink.Write<block_id_t>(block_id);
	sink.Write<idx_t>(FreeBlocksToInteger());
}

idx_t MetadataManager::GetMetadataBlockSize() const {
	return AlignValueFloor(block_manager.GetBlockSize() / METADATA_BLOCK_COUNT);
}

MetadataBlock MetadataBlock::Read(ReadStream &source) {
	MetadataBlock result;
	result.block_id = source.Read<block_id_t>();
	auto free_list = source.Read<idx_t>();
	result.FreeBlocksFromInteger(free_list);
	return result;
}

idx_t MetadataBlock::FreeBlocksToInteger() {
	idx_t result = 0;
	for (idx_t i = 0; i < free_blocks.size(); i++) {
		D_ASSERT(free_blocks[i] < idx_t(64));
		idx_t mask = idx_t(1) << idx_t(free_blocks[i]);
		result |= mask;
	}
	return result;
}

void MetadataBlock::FreeBlocksFromInteger(idx_t free_list) {
	free_blocks.clear();
	if (free_list == 0) {
		return;
	}
	for (idx_t i = 64; i > 0; i--) {
		auto index = i - 1;
		idx_t mask = idx_t(1) << index;
		if (free_list & mask) {
			free_blocks.push_back(UnsafeNumericCast<uint8_t>(index));
		}
	}
}

void MetadataManager::MarkBlocksAsModified() {
	// for any blocks that were modified in the last checkpoint - set them to free blocks currently
	for (auto &kv : modified_blocks) {
		auto block_id = kv.first;
		idx_t modified_list = kv.second;
		auto entry = blocks.find(block_id);
		D_ASSERT(entry != blocks.end());
		auto &block = entry->second;
		idx_t current_free_blocks = block.FreeBlocksToInteger();
		// merge the current set of free blocks with the modified blocks
		idx_t new_free_blocks = current_free_blocks | modified_list;
		if (new_free_blocks == NumericLimits<idx_t>::Maximum()) {
			// if new free_blocks is all blocks - mark entire block as modified
			blocks.erase(entry);
			block_manager.MarkBlockAsModified(block_id);
		} else {
			// set the new set of free blocks
			block.FreeBlocksFromInteger(new_free_blocks);
		}
	}

	modified_blocks.clear();
	for (auto &kv : blocks) {
		auto &block = kv.second;
		idx_t free_list = block.FreeBlocksToInteger();
		idx_t occupied_list = ~free_list;
		modified_blocks[block.block_id] = occupied_list;
	}
}

void MetadataManager::ClearModifiedBlocks(const vector<MetaBlockPointer> &pointers) {
	for (auto &pointer : pointers) {
		auto block_id = pointer.GetBlockId();
		auto block_index = pointer.GetBlockIndex();
		auto entry = modified_blocks.find(block_id);
		if (entry == modified_blocks.end()) {
			throw InternalException("ClearModifiedBlocks - Block id %llu not found in modified_blocks", block_id);
		}
		auto &modified_list = entry->second;
		// verify the block has been modified
		D_ASSERT(modified_list && (1ULL << block_index));
		// unset the bit
		modified_list &= ~(1ULL << block_index);
	}
}

vector<MetadataBlockInfo> MetadataManager::GetMetadataInfo() const {
	vector<MetadataBlockInfo> result;
	for (auto &block : blocks) {
		MetadataBlockInfo block_info;
		block_info.block_id = block.second.block_id;
		block_info.total_blocks = MetadataManager::METADATA_BLOCK_COUNT;
		for (auto free_block : block.second.free_blocks) {
			block_info.free_list.push_back(free_block);
		}
		std::sort(block_info.free_list.begin(), block_info.free_list.end());
		result.push_back(std::move(block_info));
	}
	std::sort(result.begin(), result.end(),
	          [](const MetadataBlockInfo &a, const MetadataBlockInfo &b) { return a.block_id < b.block_id; });
	return result;
}

vector<shared_ptr<BlockHandle>> MetadataManager::GetBlocks() const {
	vector<shared_ptr<BlockHandle>> result;
	for (auto &entry : blocks) {
		result.push_back(entry.second.block);
	}
	return result;
}

block_id_t MetadataManager::PeekNextBlockId() {
	return block_manager.PeekFreeBlockId();
}

block_id_t MetadataManager::GetNextBlockId() {
	return block_manager.GetFreeBlockId();
}

} // namespace duckdb
