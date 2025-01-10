//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_dlba_encoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parquet_dbp_encoder.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {

class DlbaEncoder {
public:
	DlbaEncoder(const idx_t total_value_count_p, const idx_t total_string_size_p) : dbp_encoder(total_value_count_p) {
		if (total_string_size_p != 0) {
			buffer = Allocator::DefaultAllocator().Allocate(total_string_size_p);
			stream = MemoryStream(buffer.get(), buffer.GetSize());
		}
	}

public:
	void BeginWrite(WriteStream &writer, const string_t &first_value) {
		dbp_encoder.BeginWrite(writer, UnsafeNumericCast<int64_t>(first_value.GetSize()));
		stream.WriteData(const_data_ptr_cast(first_value.GetData()), first_value.GetSize());
	}

	void WriteValue(WriteStream &writer, const string_t &value) {
		dbp_encoder.WriteValue(writer, UnsafeNumericCast<int64_t>(value.GetSize()));
		stream.WriteData(const_data_ptr_cast(value.GetData()), value.GetSize());
	}

	void FinishWrite(WriteStream &writer) {
		D_ASSERT(stream.GetPosition() == buffer.GetSize());
		dbp_encoder.FinishWrite(writer);
		writer.WriteData(buffer.get(), buffer.GetSize());
	}

private:
	DbpEncoder dbp_encoder;
	AllocatedData buffer;
	MemoryStream stream;
};

} // namespace duckdb
