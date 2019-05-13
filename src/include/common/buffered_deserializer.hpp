//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/buffered_deserializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/buffered_serializer.hpp"
#include "common/serializer.hpp"

namespace duckdb {

class BufferedDeserializer : public Deserializer {
public:
	BufferedDeserializer(data_t ptr, index_t data_size);
	BufferedDeserializer(BufferedSerializer &serializer);

	void Read(data_t buffer, index_t read_size) override;

public:
	data_t ptr;
	data_t endptr;
};

} // namespace duckdb
