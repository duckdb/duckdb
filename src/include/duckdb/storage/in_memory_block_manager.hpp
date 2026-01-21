//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/in_memory_block_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/common/checksum.hpp"
#include <unordered_map>
#include <string>
#include "duckdb/common/file_buffer.hpp"
#include "duckdb/main/config.hpp"
namespace duckdb {

//! InMemoryBlockManager is an implementation for a BlockManager
class InMemoryBlockManager : public BlockManager {
public:
	using BlockManager::BlockManager;

	static constexpr uint64_t BLOCK_START = Storage::FILE_HEADER_SIZE * 3;

	// LCOV_EXCL_START
	unique_ptr<Block> ConvertBlock(block_id_t block_id, FileBuffer &source_buffer) override {
		D_ASSERT(source_buffer.AllocSize() == GetBlockAllocSize());
		return make_uniq<Block>(source_buffer, block_id, GetBlockHeaderSize());
	}
	unique_ptr<Block> CreateBlock(block_id_t block_id, FileBuffer *source_buffer) override {
		auto &db = buffer_manager.GetDatabase();

		unique_ptr<Block> result;
		if (source_buffer) {
			result = ConvertBlock(block_id, *source_buffer);
		} else {
			result = make_uniq<Block>(BlockAllocator::Get(db), block_id, *this);
		}
		result->Initialize(db.config.options.debug_initialize);
		return result;
	}
	block_id_t GetFreeBlockId() override {
		return max_block++;
	}
	block_id_t GetFreeBlockIdForCheckpoint() override {
		throw InternalException("Cannot perform IO in in-memory database - GetFreeBlockIdForCheckpoint!");
	}
	block_id_t PeekFreeBlockId() override {
		throw InternalException("Cannot perform IO in in-memory database - PeekFreeBlockId!");
	}
	bool IsRootBlock(MetaBlockPointer root) override {
		throw InternalException("Cannot perform IO in in-memory database - IsRootBlock!");
	}
	void MarkBlockACheckpointed(block_id_t block_id) override {
		// NO OP
	}
	void MarkBlockAsUsed(block_id_t block_id) override {
		throw InternalException("Cannot perform IO in in-memory database - MarkBlockAsUsed!");
	}
	void MarkBlockAsModified(block_id_t block_id) override {
		multi_use_blocks[block_id]--;
		if (multi_use_blocks[block_id] <= 1) {
			blocks.erase(block_id);
			multi_use_blocks.erase(block_id);
		}
	}
	void IncreaseBlockReferenceCount(block_id_t block_id) override {
		throw InternalException("Cannot perform IO in in-memory database - IncreaseBlockReferenceCount!");
	}
	idx_t GetMetaBlock() override {
		throw InternalException("Cannot perform IO in in-memory database - GetMetaBlock!");
	}
	void CheckChecksum(FileBuffer &block, uint64_t location) const {
		uint64_t stored_checksum = Load<uint64_t>(block.InternalBuffer());
		uint64_t computed_checksum = Checksum(block.buffer, block.Size());

		// verify the checksum
		if (stored_checksum != computed_checksum) {
			throw IOException(
			    "Corrupt in-memory block: computed checksum %llu does not match stored checksum %llu in block "
			    "at location %llu",
			    computed_checksum, stored_checksum, location);
		}
	}
	void ReadAndChecksum(QueryContext context, Block &block, uint64_t location) const {
		if (context.GetClientContext() != nullptr) {
			context.GetClientContext()->client_data->profiler->AddToCounter(MetricType::TOTAL_BYTES_READ,
			                                                                block.AllocSize());
		}
		lock_guard<mutex> guard(lock);
		auto it = blocks.find(block.id);
		if (it == blocks.end()) {
			throw InternalException("Trying to read a block that does not exist in-memory: " + to_string(block.id));
		}

		// copy from our stored block to the block provided by the buffer manager
		memcpy(block.InternalBuffer(), it->second.get(), GetBlockAllocSize());

		CheckChecksum(block, location);
	}

	idx_t GetBlockLocation(block_id_t block_id) const {
		return BLOCK_START + NumericCast<idx_t>(block_id) * GetBlockAllocSize();
	}

	void Read(QueryContext context, Block &block) override {
		D_ASSERT(block.id >= 0);
		ReadAndChecksum(context, block, GetBlockLocation(block.id));
	}
	void ReadBlocks(FileBuffer &buffer, block_id_t start_block, idx_t block_count) override {
		throw InternalException("Cannot perform IO in in-memory database - ReadBlocks!");
	}
	void Write(FileBuffer &block, block_id_t block_id) override {
		throw InternalException("Cannot perform IO in in-memory database - Write!");
	}
	// We currently do not encrypt in-memory storage
	void Write(QueryContext context, FileBuffer &block, block_id_t block_id) override {
		D_ASSERT(block_id >= 0);
		uint64_t checksum = Checksum(block.buffer, block.Size());
		Store<uint64_t>(checksum, block.InternalBuffer());

		// copy the block to our own storage
		auto &allocator = BufferAllocator::Get(buffer_manager.GetDatabase());
		auto copy = allocator.Allocate(GetBlockAllocSize());
		memcpy(copy.get(), block.InternalBuffer(), GetBlockAllocSize());

		lock_guard<mutex> guard(lock);
		blocks[block_id] = std::move(copy);
		multi_use_blocks[block_id] = 1;
	}
	void WriteHeader(QueryContext context, DatabaseHeader header) override {
		throw InternalException("Cannot perform IO in in-memory database - WriteHeader!");
	}
	bool InMemory() override {
		return true;
	}
	void FileSync() override {
		throw InternalException("Cannot perform IO in in-memory database - FileSync!");
	}
	idx_t TotalBlocks() override {
		throw InternalException("Cannot perform IO in in-memory database - TotalBlocks!");
	}
	idx_t FreeBlocks() override {
		throw InternalException("Cannot perform IO in in-memory database - FreeBlocks!");
	}
	// LCOV_EXCL_STOP
private:
	mutable mutex lock;
	mutable unordered_map<block_id_t, AllocatedData> blocks;

	unordered_map<block_id_t, uint32_t> multi_use_blocks;

	//! The current maximum block id
	block_id_t max_block = 0;
};
} // namespace duckdb
