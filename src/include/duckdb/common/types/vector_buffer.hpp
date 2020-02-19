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

namespace duckdb {

class VectorBuffer;
class Vector;
class FlatVector;

enum class VectorBufferType : uint8_t {
	STANDARD_BUFFER, // standard buffer, holds a single array of data
	STRING_BUFFER,    // string buffer, holds a string heap
	STRUCT_BUFFER, // struct buffer, holds a ordered mapping from name to child vector
	LIST_BUFFER // list buffer, holds a single flatvector child
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
	const char *AddString(const char *data, index_t len) {
		return heap.AddString(data, len);
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



class VectorStructBuffer : public VectorBuffer {
public:
	VectorStructBuffer();
	~VectorStructBuffer();

public:
	child_list_t<unique_ptr<Vector>> &GetChildren() {
		return children;
	}
	void AddChild(string name, unique_ptr<Vector> vector) {
		children.push_back(std::make_pair(name, move(vector)));
	}

private:
	//! child vectors used for nested data
	child_list_t<unique_ptr<Vector>> children;
};

class VectorListBuffer : public VectorBuffer {
public:
	VectorListBuffer();

	~VectorListBuffer();
public:
	FlatVector& GetChild() {
		return *child;
	}

private:
	//! child vectors used for nested data
	unique_ptr<FlatVector> child;
};

template <class T, typename... Args> buffer_ptr<T> make_buffer(Args &&... args) {
	return std::make_shared<T>(std::forward<Args>(args)...);
}

} // namespace duckdb
