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

	template <class T, typename... ARGS>
	void ReadList(vector<unique_ptr<T>> &list, ARGS &&... args) {
		auto select_count = Read<uint32_t>();
		for (uint32_t i = 0; i < select_count; i++) {
			auto child = T::Deserialize(*this, std::forward<ARGS>(args)...);
			list.push_back(std::move(child));
		}
	}

	template <class T, class RETURN_TYPE = T, typename... ARGS>
	unique_ptr<RETURN_TYPE> ReadOptional(ARGS &&... args) {
		auto has_entry = Read<bool>();
		if (has_entry) {
			return T::Deserialize(*this, std::forward<ARGS>(args)...);
		}
		return nullptr;
	}

	void ReadStringVector(vector<string> &list);
};

template <>
DUCKDB_API string ReadStream::Read();

} // namespace duckdb
