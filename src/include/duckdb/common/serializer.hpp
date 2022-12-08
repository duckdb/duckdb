//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector.hpp"
#include <type_traits>

namespace duckdb {

//! The Serialize class is a base class that can be used to serializing objects into a binary buffer
class Serializer {
private:
	uint64_t version = 0L;

public:
	virtual ~Serializer() {
	}

	//! Sets the version of the serialization that writers are expected to use
	//! The version is mostly the most recent one, unless modifying old data or streaming to
	//! an older version
	void SetVersion(uint64_t v) {
		D_ASSERT(this->version == 0); // version can only be set once
		this->version = v;
	}

	//! Returns the version of serialization that writers are expected to use
	uint64_t GetVersion() {
		return version;
	}

	virtual void WriteData(const_data_ptr_t buffer, idx_t write_size) = 0;

	template <class T>
	void Write(T element) {
		static_assert(std::is_trivially_destructible<T>(), "Write element must be trivially destructible");

		WriteData((const_data_ptr_t)&element, sizeof(T));
	}

	//! Write data from a string buffer directly (without length prefix)
	void WriteBufferData(const string &str) {
		WriteData((const_data_ptr_t)str.c_str(), str.size());
	}
	//! Write a string with a length prefix
	void WriteString(const string &val) {
		WriteStringLen((const_data_ptr_t)val.c_str(), val.size());
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
};

//! The Deserializer class assists in deserializing a binary blob back into an
//! object
class Deserializer {
private:
	uint64_t version = 0L;

public:
	virtual ~Deserializer() {
	}

	//! Sets the version of the serialization that readers are expected to use
	//! The version is mostly the most recent one, unless reading old data or streaming from
	//! an older version
	void SetVersion(uint64_t v) {
		D_ASSERT(this->version == 0); // version can only be set once
		this->version = v;
	}

	//! Returns the version of serialization that readers are expected to use
	uint64_t GetVersion() {
		return version;
	}

	//! Reads [read_size] bytes into the buffer
	virtual void ReadData(data_ptr_t buffer, idx_t read_size) = 0;

	template <class T>
	T Read() {
		T value;
		ReadData((data_ptr_t)&value, sizeof(T));
		return value;
	}

	template <class T, typename... ARGS>
	void ReadList(vector<unique_ptr<T>> &list, ARGS &&... args) {
		auto select_count = Read<uint32_t>();
		for (uint32_t i = 0; i < select_count; i++) {
			auto child = T::Deserialize(*this, std::forward<ARGS>(args)...);
			list.push_back(move(child));
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
DUCKDB_API string Deserializer::Read();

} // namespace duckdb
