//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/buffer/buffer_handle.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/storage_info.hpp"

namespace duckdb {
class Block;
class BufferManager;
class ManagedBuffer;

class BufferHandle {
public:
	BufferHandle(BufferManager &manager,block_id_t block_id);
	virtual ~BufferHandle();

	BufferManager &manager;
	//! The block id of the block
	block_id_t block_id;
};

class BlockHandle : public BufferHandle {
public:
	BlockHandle(BufferManager &manager, Block *block, block_id_t block_id) :
		BufferHandle(manager, block_id), block(block) {}

	//! The managed block
	Block *block;
};

class ManagedBufferHandle : public BufferHandle {
public:
	ManagedBufferHandle(BufferManager &manager, ManagedBuffer *buffer, block_id_t block_id) :
		BufferHandle(manager, block_id), buffer(buffer) {}

	//! The managed buffer
	ManagedBuffer *buffer;
};

}
