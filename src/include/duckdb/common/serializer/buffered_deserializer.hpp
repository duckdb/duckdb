//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/serializer/buffered_deserializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"

namespace duckdb {
class ClientContext;

class BufferedDeserializer : public Deserializer {
public:
	BufferedDeserializer(data_ptr_t ptr, idx_t data_size);
	explicit BufferedDeserializer(BufferedSerializer &serializer);

	data_ptr_t ptr;
	data_ptr_t endptr;

public:
	void ReadData(data_ptr_t buffer, uint64_t read_size) override;
};

class BufferentContextDeserializer : public BufferedDeserializer {
public:
	BufferentContextDeserializer(ClientContext &context_p, data_ptr_t ptr, idx_t data_size)
	    : BufferedDeserializer(ptr, data_size), context(context_p) {
	}

public:
	ClientContext &context;
};

} // namespace duckdb
