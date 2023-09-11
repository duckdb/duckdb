//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/read_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector.hpp"
#include <type_traits>

namespace duckdb {

class ReadStream {
public:
	// Reads a set amount of data from the stream into the specified buffer and moves the stream forward accordingly
	virtual void ReadData(data_ptr_t buffer, idx_t read_size) = 0;

	// Reads a type from the stream and moves the stream forward sizeof(T) bytes
	// The type must be a standard layout type
	template <class T>
	T Read() {
		static_assert(std::is_standard_layout<T>(), "Read element must be a standard layout data type");
		T value;
		ReadData(data_ptr_cast(&value), sizeof(T));
		return value;
	}

	virtual ~ReadStream() {
	}
};

} // namespace duckdb
