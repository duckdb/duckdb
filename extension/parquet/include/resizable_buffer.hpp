//===----------------------------------------------------------------------===//
//                         DuckDB
//
// resizable_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/allocator.hpp"

#include <exception>

#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

class ByteBuffer { // on to the 10 thousandth impl
public:
	ByteBuffer() {};
	ByteBuffer(const data_ptr_t ptr, const idx_t len) : ptr(ptr), len(len) {};

public:
	data_ptr_t operator[](const idx_t index) const {
		Available(index + 1);
		return ptr + offset + index;
	}

	data_ptr_t GetCurrentLoc() const {
		return ptr + offset;
	}

	idx_t GetOffset() const {
		return offset;
	}

	idx_t GetLength() const {
		return len;
	}

	idx_t GetRemaining() const {
		return len - offset;
	}

	//! Hands off everything left in the buffer to a caller-managed sub-region: returns the current
	//! location, writes its length to length_out, and consumes the buffer up to its end.
	data_ptr_t ConsumeRemaining(idx_t &length_out) {
		length_out = GetRemaining();
		auto loc = GetCurrentLoc();
		UnsafeInc(length_out);
		return loc;
	}

	void Inc(const idx_t increment) {
		Available(increment);
		UnsafeInc(increment);
	}

	void UnsafeInc(const idx_t increment) {
		offset += increment;
	}

	template <class T>
	T Read() {
		Available(sizeof(T));
		return UnsafeRead<T>();
	}

	template <class T>
	T UnsafeRead() {
		T val = UnsafeGet<T>();
		UnsafeInc(sizeof(T));
		return val;
	}

	template <class T>
	T Get() {
		Available(sizeof(T));
		return UnsafeGet<T>();
	}

	template <class T>
	T UnsafeGet() {
		return Load<T>(ptr + offset);
	}

	void CopyTo(char *dest, const idx_t copy_len) const {
		Available(copy_len);
		UnsafeCopyTo(dest, copy_len);
	}

	void UnsafeCopyTo(char *dest, const idx_t copy_len) const {
		std::memcpy(dest, ptr + offset, copy_len);
	}

	void Zero() const {
		std::memset(ptr + offset, 0, len - offset);
	}

	void Available(const idx_t req_len) const {
		if (!CheckAvailable(req_len)) {
			throw std::runtime_error("Out of buffer");
		}
	}

	bool CheckAvailable(const idx_t req_len) const {
		return req_len <= len - offset;
	}

protected:
	data_ptr_t ptr = nullptr;

	idx_t offset = 0;
	idx_t len = 0;
};

class ResizeableBuffer : public ByteBuffer {
public:
	ResizeableBuffer() {
	}

	ResizeableBuffer(Allocator &allocator, const idx_t new_size) {
		Resize(allocator, new_size);
	}

	ResizeableBuffer(BufferManager &buffer_manager, const idx_t new_size) {
		Resize(buffer_manager, new_size);
	}

	void Resize(Allocator &allocator, const idx_t new_size) {
		len = new_size;
		offset = 0;
		if (new_size == 0) {
			return;
		}
		if (new_size > alloc_len) {
			alloc_len = NextPowerOfTwo(new_size);
			allocated_data.Reset(); // Have to reset before allocating new buffer (otherwise we use ~2x the memory)
			allocated_data = allocator.Allocate(alloc_len);
			ptr = allocated_data.get();
		}
	}

	void Resize(BufferManager &buffer_manager, const idx_t new_size) {
		len = new_size;
		offset = 0;
		if (new_size == 0) {
			return;
		}
		if (new_size > alloc_len) {
			alloc_len = NextPowerOfTwo(new_size);
			handle = buffer_manager.Allocate(MemoryTag::PARQUET_READER, alloc_len, true);
			block = handle.GetBlockHandle();
			ptr = handle.GetDataMutable();
		}
	}

	void Reset() {
		if (block) {
			ptr = handle.GetDataMutable();
		} else {
			ptr = allocated_data.get();
		}
		len = alloc_len;
		offset = 0;
	}

private:
	AllocatedData allocated_data;
	shared_ptr<BlockHandle> block;
	BufferHandle handle;

	idx_t alloc_len = 0;
};

} // namespace duckdb
