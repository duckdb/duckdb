//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/map_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"

namespace duckdb {

class VectorStringBuffer : public VectorBuffer {
public:
	VectorStringBuffer();
	explicit VectorStringBuffer(Allocator &allocator);
	explicit VectorStringBuffer(VectorBufferType type);

public:
	string_t AddString(const char *data, idx_t len) {
		return heap.AddString(data, len);
	}
	string_t AddString(string_t data) {
		return heap.AddString(data);
	}
	string_t AddBlob(string_t data) {
		return heap.AddBlob(data.GetData(), data.GetSize());
	}
	string_t EmptyString(idx_t len) {
		return heap.EmptyString(len);
	}

	ArenaAllocator &GetStringAllocator() {
		return heap.GetAllocator();
	}
	//! Allocate a buffer to store up to "len" bytes for a string
	//! This can be turned into a proper string by using FinalizeBuffer afterwards
	//! Note that alloc_len only has to be an upper bound, the final string may be smaller
	data_ptr_t AllocateShrinkableBuffer(idx_t alloc_len) {
		auto &allocator = heap.GetAllocator();
		return allocator.Allocate(alloc_len);
	}
	//! Finalize a buffer allocated with AllocateShrinkableBuffer into a string of size str_len
	//! str_len must be <= alloc_len
	string_t FinalizeShrinkableBuffer(data_ptr_t buffer, idx_t alloc_len, idx_t str_len) {
		auto &allocator = heap.GetAllocator();
		D_ASSERT(str_len <= alloc_len);
		D_ASSERT(buffer == allocator.GetHead()->data.get() + allocator.GetHead()->current_position - alloc_len);
		bool is_not_inlined = str_len > string_t::INLINE_LENGTH;
		idx_t shrink_count = alloc_len - (str_len * is_not_inlined);
		allocator.ShrinkHead(shrink_count);
		return string_t(const_char_ptr_cast(buffer), UnsafeNumericCast<uint32_t>(str_len));
	}

	void AddHeapReference(buffer_ptr<VectorBuffer> heap) {
		references.push_back(std::move(heap));
	}

private:
	//! The string heap of this buffer
	StringHeap heap;
	//! References to additional vector buffers referenced by this string buffer
	vector<buffer_ptr<VectorBuffer>> references;
};

struct StringVector {
	//! Add a string to the string heap of the vector (auxiliary data)
	DUCKDB_API static string_t AddString(Vector &vector, const char *data, idx_t len);
	//! Add a string or a blob to the string heap of the vector (auxiliary data)
	//! This function is the same as ::AddString, except the added data does not need to be valid UTF8
	DUCKDB_API static string_t AddStringOrBlob(Vector &vector, const char *data, idx_t len);
	//! Add a string to the string heap of the vector (auxiliary data)
	DUCKDB_API static string_t AddString(Vector &vector, const char *data);
	//! Add a string to the string heap of the vector (auxiliary data)
	DUCKDB_API static string_t AddString(Vector &vector, string_t data);
	//! Add a string to the string heap of the vector (auxiliary data)
	DUCKDB_API static string_t AddString(Vector &vector, const string &data);
	//! Add a string or a blob to the string heap of the vector (auxiliary data)
	//! This function is the same as ::AddString, except the added data does not need to be valid UTF8
	DUCKDB_API static string_t AddStringOrBlob(Vector &vector, string_t data);
	//! Allocates an empty string of the specified size, and returns a writable pointer that can be used to store the
	//! result of an operation
	DUCKDB_API static string_t EmptyString(Vector &vector, idx_t len);
	//! Returns a reference to the underlying VectorStringBuffer - throws an error if vector is not of type VARCHAR
	DUCKDB_API static VectorStringBuffer &GetStringBuffer(Vector &vector);
	//! Adds a reference to a handle that stores strings of this vector
	DUCKDB_API static void AddHandle(Vector &vector, BufferHandle handle);
	//! Adds a reference to an unspecified vector buffer that stores strings of this vector
	DUCKDB_API static void AddBuffer(Vector &vector, buffer_ptr<VectorBuffer> buffer);
	//! Add a reference from this vector to the string heap of the provided vector
	DUCKDB_API static void AddHeapReference(Vector &vector, const Vector &other);
};

} // namespace duckdb
