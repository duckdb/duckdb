//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/buffered_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/serializer.hpp"
#include "common/buffered_serializer.hpp"

namespace duckdb {

class BufferedDeserializer : public Deserializer {
public:
	BufferedDeserializer(uint8_t *ptr, uint64_t data_size);
	BufferedDeserializer(BufferedSerializer &serializer);

	void Read(uint8_t *buffer, uint64_t read_size) override;
public:
	uint8_t *ptr;
	uint8_t *endptr;
};

}

