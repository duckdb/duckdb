//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/buffered_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/serializer.hpp"

namespace duckdb {

#define SERIALIZER_DEFAULT_SIZE 1024

struct BinaryData {
	unique_ptr<uint8_t[]> data;
	uint64_t size;
};

class BufferedSerializer : public Serializer {
public:
	//! Serializes to a buffer allocated by the serializer, will expand when
	//! writing past the initial threshold
	BufferedSerializer(uint64_t maximum_size = SERIALIZER_DEFAULT_SIZE);
	//! Serializes to a provided (owned) data pointer
	BufferedSerializer(unique_ptr<uint8_t[]> data, uint64_t size);
	// //! Serializes to a provided non-owned data pointer, bounds on writing are
	// //! not checked
	// BufferedSerializer(uint8_t *data);

	void Write(const uint8_t *buffer, uint64_t write_size) override;

	//! Retrieves the data after the writing has been completed
	BinaryData GetData() {
		return std::move(blob);
	}
public:
	uint64_t maximum_size;
	uint8_t *data;

	BinaryData blob;
};

}

