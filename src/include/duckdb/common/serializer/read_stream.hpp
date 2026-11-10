//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/read_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/context.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector.hpp"

#include <type_traits>

namespace duckdb {

class ReadStream {
public:
	// Reads a set amount of data from the stream into the specified buffer and moves the stream forward accordingly
	virtual void ReadData(data_ptr_t buffer, idx_t read_size) = 0;
	// Variant that attributes the work (e.g. buffer pins) to the given context. Defaults to the plain variant
	virtual void ReadData(Context &context, data_ptr_t buffer, idx_t read_size) {
		ReadData(buffer, read_size);
	}

	// Reads a type from the stream and moves the stream forward sizeof(T) bytes
	// The type must be a standard layout type
	template <class T>
	T Read() {
		T value;
		ReadData(data_ptr_cast(&value), sizeof(T));
		return value;
	}

	template <class T>
	T Read(Context &context) {
		static_assert(std::is_standard_layout<T>(), "Read element must be a standard layout data type");
		T value;
		ReadData(context, data_ptr_cast(&value), sizeof(T));
		return value;
	}

	virtual ~ReadStream() {
	}
};

} // namespace duckdb
