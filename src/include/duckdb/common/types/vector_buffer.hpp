//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

class VectorBuffer;

enum class VectorBufferType : uint8_t {
	STANDARD_BUFFER, // standard buffer, holds a single array of data
	STRING_BUFFER    // string buffer, holds a string heap
};

template <class T> using buffer_ptr = std::shared_ptr<T>;

//! The VectorBuffer is a class used by the vector to hold its data
class VectorBuffer {
public:
	VectorBuffer(VectorBufferType type) : type(type) {
	}
	VectorBuffer(index_t data_size);
	virtual ~VectorBuffer() {
	}

	VectorBufferType type;

public:
	data_ptr_t GetData() {
		return data.get();
	}

	static buffer_ptr<VectorBuffer> CreateStandardVector(TypeId type, index_t count = STANDARD_VECTOR_SIZE);
	static buffer_ptr<VectorBuffer> CreateConstantVector(TypeId type);

private:
	unique_ptr<data_t[]> data;
};

//! The VectorStringBuffer is
class VectorStringBuffer : public VectorBuffer {
public:
	VectorStringBuffer();

public:
	string_t AddString(const char *data, index_t len) {
		return heap.AddString(data, len);
	}
	string_t AddString(string_t data) {
		return heap.AddString(data);
	}
	string_t EmptyString(index_t len) {
		return heap.EmptyString(len);
	}

	void AddHeapReference(buffer_ptr<VectorBuffer> heap) {
		references.push_back(move(heap));
	}

private:
	//! The string heap of this buffer
	StringHeap heap;
	// References to additional vector buffers referenced by this string buffer
	vector<buffer_ptr<VectorBuffer>> references;
};

template <class T, typename... Args> buffer_ptr<T> make_buffer(Args &&... args) {
	return std::make_shared<T>(std::forward<Args>(args)...);
}

} // namespace duckdb
