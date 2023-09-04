//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/buffered_deserializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/read_stream.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"

namespace duckdb {
class ClientContext;

class BufferedDeserializer : public ReadStream {
public:
	BufferedDeserializer(data_ptr_t ptr, idx_t data_size);
	explicit BufferedDeserializer(BufferedSerializer &serializer);

	data_ptr_t ptr;
	data_ptr_t endptr;

public:
	void ReadData(data_ptr_t buffer, uint64_t read_size) override;
};

} // namespace duckdb
