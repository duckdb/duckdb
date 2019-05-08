//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/exception.hpp"

namespace duckdb {

#define SERIALIZER_DEFAULT_SIZE 1024

//! The Serialize class contains helper functions for serializing objects to
//! binary data
class Serializer {
	friend class Deserializer;

private:
	inline void PotentialResize(uint64_t new_element_size) {
		if (blob.size + new_element_size >= maximum_size) {
			do {
				maximum_size *= 2;
			} while (blob.size + new_element_size > maximum_size);
			auto new_data = new uint8_t[maximum_size];
			memcpy(new_data, data, blob.size);
			data = new_data;
			blob.data = unique_ptr<uint8_t[]>(new_data);
		}
	}

public:
	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold
	Serializer(uint64_t maximum_size = SERIALIZER_DEFAULT_SIZE);
	//! Serializes to a provided (owned) data pointer
	Serializer(unique_ptr<uint8_t[]> data, uint64_t size);
	//! Serializes to a provided non-owned data pointer, bounds on writing are
	//! not checked
	Serializer(uint8_t *data);

	template <class T> void Write(T element) {
		PotentialResize(sizeof(T));

		*((T *)(data + blob.size)) = element;
		blob.size += sizeof(T);
	}

	void WriteString(const string &val) {
		Write<uint32_t>((uint32_t)val.size());
		if (val.size() > 0) {
			PotentialResize(val.size());

			memcpy(data + blob.size, &val[0], val.size());
			blob.size += val.size();
		}
	}

	void WriteData(uint8_t *dataptr, uint64_t data_size) {
		PotentialResize(data_size);

		memcpy(data + blob.size, dataptr, data_size);
		blob.size += data_size;
	}

	// Used for a manual write of data; simply reserves <size> bytes to write
	// and returns a pointer to where to write them
	uint8_t *ManualWrite(uint64_t size) {
		PotentialResize(size);

		auto dataptr = data + blob.size;
		blob.size += size;
		return dataptr;
	}
	//! Retrieves the data after the writing has been completed
	BinaryData GetData() {
		return std::move(blob);
	}

	template <class T> void WriteList(vector<unique_ptr<T>> &list) {
		assert(list.size() <= std::numeric_limits<uint32_t>::max());
		Write<uint32_t>((uint32_t)list.size());
		for (auto &child : list) {
			child->Serialize(*this);
		}
	}

	template <class T> void WriteOptional(unique_ptr<T> &element) {
		Write<bool>(element ? true : false);
		if (element) {
			element->Serialize(*this);
		}
	}

private:
	uint64_t maximum_size;
	uint8_t *data;

	BinaryData blob;
};

//! The Deserializer class assists in deserializing a binary blob back into an
//! object
class Deserializer {
public:
	Deserializer(Serializer &serializer) : Deserializer(serializer.data, serializer.blob.size) {
	}
	Deserializer(uint8_t *ptr, uint64_t data);

	// Read an element of class T [sizeof(T)] from the stream. [CAN THROW:
	// SerializationException]
	template <class T> T Read() {
		if (ptr + sizeof(T) > endptr) {
			throw SerializationException("Failed to deserialize object");
		}
		T value = *((T *)ptr);
		ptr += sizeof(T);
		return value;
	}

	//! Returns <data_size> elements into a pointer. [CAN THROW:
	//! SerializationException]
	uint8_t *ReadData(uint64_t data_size) {
		if (ptr + data_size > endptr) {
			throw SerializationException("Failed to deserialize object");
		}
		auto dataptr = ptr;
		ptr += data_size;
		return dataptr;
	}

	template <class T> void ReadList(vector<unique_ptr<T>> &list) {
		auto select_count = Read<uint32_t>();
		for (uint32_t i = 0; i < select_count; i++) {
			auto child = T::Deserialize(*this);
			list.push_back(move(child));
		}
	}

	template <class T> unique_ptr<T> ReadOptional() {
		auto has_entry = Read<bool>();
		if (has_entry) {
			return T::Deserialize(*this);
		}
		return nullptr;
	}

private:
	uint8_t *ptr;
	uint8_t *endptr;
};

template <> string Deserializer::Read();

} // namespace duckdb
