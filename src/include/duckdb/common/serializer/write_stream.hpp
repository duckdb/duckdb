//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/write_stream.hpp
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

class WriteStream {
public:
	virtual ~WriteStream() {
	}

	virtual void WriteData(const_data_ptr_t buffer, idx_t write_size) = 0;

	template <class T>
	void Write(T element) {
		static_assert(std::is_trivially_destructible<T>(), "Write element must be trivially destructible");

		WriteData(const_data_ptr_cast(&element), sizeof(T));
	}

	/*
	//! Write data from a string buffer directly (without length prefix)
	void WriteBufferData(const string &str) {
	    WriteData(const_data_ptr_cast(str.c_str()), str.size());
	}
	//! Write a string with a length prefix
	void WriteString(const string &val) {
	    WriteStringLen(const_data_ptr_cast(val.c_str()), val.size());
	}
	void WriteStringLen(const_data_ptr_t val, idx_t len) {
	    Write<uint32_t>((uint32_t)len);
	    if (len > 0) {
	        WriteData(val, len);
	    }
	}

	template <class T>
	void WriteList(const vector<unique_ptr<T>> &list) {
	    Write<uint32_t>((uint32_t)list.size());
	    for (auto &child : list) {
	        child->Serialize(*this);
	    }
	}

	void WriteStringVector(const vector<string> &list) {
	    Write<uint32_t>((uint32_t)list.size());
	    for (auto &child : list) {
	        WriteString(child);
	    }
	}

	template <class T>
	void WriteOptional(const unique_ptr<T> &element) {
	    Write<bool>(element ? true : false);
	    if (element) {
	        element->Serialize(*this);
	    }
	}
	 */
};

} // namespace duckdb
