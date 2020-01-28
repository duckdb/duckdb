//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/vector_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class VectorBuffer;

template<class T>
using buffer_ptr = std::shared_ptr<T>;

//! The VectorBuffer is a class used by the vector to hold
class VectorBuffer {
public:
	VectorBuffer(index_t data_size);
	// virtual ~VectorBuffer(){}
public:
	data_ptr_t GetData() {
		return data.get();
	}

	static buffer_ptr<VectorBuffer> CreateStandardVector(TypeId type);
	static buffer_ptr<VectorBuffer> CreateConstantVector(TypeId type);
private:
	unique_ptr<data_t[]> data;
};

template <typename... Args>
buffer_ptr<VectorBuffer> make_buffer(Args &&... args) {
	return std::make_shared<VectorBuffer>(std::forward<Args>(args)...);
}

}
