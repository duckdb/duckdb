//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// common/serializer.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/helper.hpp"
#include "common/internal_types.hpp"

namespace duckdb {

#define SERIALIZER_DEFAULT_SIZE 1024

//! The Serialize class contains helper functions for serializing objects to
//! binary data
class Serializer {
  private:
	inline void PotentialResize(size_t new_element_size) {
		if (blob.size + new_element_size >= maximum_size) {
			do {
				maximum_size *= 2;
			} while(blob.size + new_element_size > maximum_size);
			auto new_data = new uint8_t[maximum_size];
			memcpy(new_data, data, blob.size);
			data = new_data;
			blob.data = std::unique_ptr<uint8_t[]>(new_data);
		}
	}

  public:
	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold
	Serializer(size_t maximum_size = SERIALIZER_DEFAULT_SIZE);
	//! Serializes to a provided (owned) data pointer
	Serializer(std::unique_ptr<uint8_t[]> data, size_t size);
	//! Serializes to a provided non-owned data pointer, bounds on writing are
	//! not checked
	Serializer(uint8_t *data);

	template <class T> void Write(T element) {
		PotentialResize(sizeof(T));

		*((T *)(data + blob.size)) = element;
		blob.size += sizeof(T);
	}

	void WriteString(const std::string &val) {
		Write<uint32_t>(val.size());
		if (val.size() > 0) {
			PotentialResize(val.size());

			memcpy(data + blob.size, &val[0], val.size());
			blob.size += val.size();
		}
	}

	void WriteData(uint8_t *dataptr, size_t data_size) {
		PotentialResize(data_size);

		memcpy(data + blob.size, dataptr, data_size);
		blob.size += data_size;
	}

	// Used for a manual write of data; simply reserves <size> bytes to write
	// and returns a pointer to where to write them
	uint8_t *ManualWrite(size_t size) {
		PotentialResize(size);

		auto dataptr = data + blob.size;
		blob.size += size;
		return dataptr;
	}
	//! Retrieves the data after the writing has been completed
	BinaryData GetData() { return std::move(blob); }

  private:
	size_t maximum_size;
	uint8_t *data;

	BinaryData blob;
};

//! The Deserializer class assists in deserializing a binary blob back into an
//! object
class Deserializer {
  public:
	Deserializer(uint8_t *ptr, size_t data);

	template <class T> T Read(bool &failed) {
		if (ptr + sizeof(T) > endptr) {
			failed = true;
			return T();
		}
		T value = *((T *)ptr);
		ptr += sizeof(T);
		return value;
	}

	//! Returns <data_size> elements into a pointer. Returns a nullptr if it was
	//! not possible to read <data_size> elements.
	uint8_t *ReadData(size_t data_size) {
		if (ptr + data_size > endptr) {
			return nullptr;
		}
		auto dataptr = ptr;
		ptr += data_size;
		return dataptr;
	}

  private:
	uint8_t *ptr;
	uint8_t *endptr;
};

template <> std::string Deserializer::Read(bool &failed);

} // namespace duckdb
