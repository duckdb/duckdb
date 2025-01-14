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
	DlbaEncoder(const idx_t total_value_count_p, const idx_t total_string_size_p)
	    : dbp_encoder(total_value_count_p), total_string_size(total_string_size_p),
	      buffer(Allocator::DefaultAllocator().Allocate(total_string_size + 1)),
	      stream(make_unsafe_uniq<MemoryStream>(buffer.get(), buffer.GetSize())) {
	}

public:
	void BeginWrite(WriteStream &writer, const string_t &first_value) {
		dbp_encoder.BeginWrite(writer, UnsafeNumericCast<int64_t>(first_value.GetSize()));
		stream->WriteData(const_data_ptr_cast(first_value.GetData()), first_value.GetSize());
	}

	void WriteValue(WriteStream &writer, const string_t &value) {
		dbp_encoder.WriteValue(writer, UnsafeNumericCast<int64_t>(value.GetSize()));
		stream->WriteData(const_data_ptr_cast(value.GetData()), value.GetSize());
	}

	void FinishWrite(WriteStream &writer) {
		D_ASSERT(stream->GetPosition() == total_string_size);
		dbp_encoder.FinishWrite(writer);
		writer.WriteData(buffer.get(), total_string_size);
	}

private:
	DbpEncoder dbp_encoder;
	const idx_t total_string_size;
	AllocatedData buffer;
	unsafe_unique_ptr<MemoryStream> stream;
};

} // namespace duckdb
