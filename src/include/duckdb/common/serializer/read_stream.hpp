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
	virtual ~ReadStream() {
	}

	//! Reads [read_size] bytes into the buffer
	virtual void ReadData(data_ptr_t buffer, idx_t read_size) = 0;

	template <class T>
	T Read() {
		T value;
		ReadData(data_ptr_cast(&value), sizeof(T));
		return value;
	}
};

template <>
DUCKDB_API string ReadStream::Read();

} // namespace duckdb
