//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/memory_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/serializer/read_stream.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class MemoryStream : public WriteStream, public ReadStream {
private:
	idx_t position;
	idx_t capacity;
	bool owns_data;
	data_ptr_t data;

public:
	// Create a new owning MemoryStream with an internal  backing buffer with the specified capacity. The stream will
	// own the backing buffer, resize it when needed and free its memory when the stream is destroyed
	explicit MemoryStream(idx_t capacity = 512);

	// Create a new non-owning MemoryStream over the specified external buffer and capacity. The stream will not take
	// ownership of the backing buffer, will not attempt to resize it and will not free the memory when the stream
	// is destroyed
	explicit MemoryStream(data_ptr_t buffer, idx_t capacity);

	~MemoryStream() override;

	// Write data to the stream.
	// Throws if the write would exceed the capacity of the stream and the backing buffer is not owned by the stream
	void WriteData(const_data_ptr_t buffer, idx_t write_size) override;

	// Read data from the stream.
	// Throws if the read would exceed the capacity of the stream
	void ReadData(data_ptr_t buffer, idx_t read_size) override;

	// Rewind the stream to the start, keeping the capacity and the backing buffer intact
	void Rewind();

	// Release ownership of the backing buffer and turn a owning stream into a non-owning one.
	// The stream will no longer be responsible for freeing the data.
	// The stream will also no longer attempt to automatically resize the buffer when the capacity is reached.
	void Release();

	// Get a pointer to the underlying backing buffer
	data_ptr_t GetData() const;

	// Get the current position in the stream
	idx_t GetPosition() const;

	// Get the capacity of the stream
	idx_t GetCapacity() const;
};

} // namespace duckdb
