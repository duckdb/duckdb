#include "decoder/delta_binary_packed_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

DeltaBinaryPackedDecoder::DeltaBinaryPackedDecoder(ColumnReader &reader)
    : reader(reader), decoded_data_buffer(reader.encoding_buffers[0]) {
}

void DeltaBinaryPackedDecoder::InitializePage() {
	auto &block = reader.block;
	dbp_decoder = make_uniq<DbpDecoder>(block->ptr, block->len);
	block->inc(block->len);
}

void DeltaBinaryPackedDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	idx_t valid_count = reader.GetValidCount(defines, read_count, result_offset);

	auto &allocator = reader.reader.allocator;
	decoded_data_buffer.reset();
	switch (reader.Schema().parquet_type) {
	case duckdb_parquet::Type::INT32:
		decoded_data_buffer.resize(allocator, sizeof(int32_t) * (valid_count));
		dbp_decoder->GetBatch<int32_t>(decoded_data_buffer.ptr, valid_count);
		break;
	case duckdb_parquet::Type::INT64:
		decoded_data_buffer.resize(allocator, sizeof(int64_t) * (valid_count));
		dbp_decoder->GetBatch<int64_t>(decoded_data_buffer.ptr, valid_count);
		break;

	default:
		throw std::runtime_error("DELTA_BINARY_PACKED should only be INT32 or INT64");
	}
	// Plain() will put NULLs in the right place
	reader.Plain(decoded_data_buffer, defines, read_count, result_offset, result);
}

void DeltaBinaryPackedDecoder::Skip(uint8_t *defines, idx_t skip_count) {
	idx_t valid_count = reader.GetValidCount(defines, skip_count);
	switch (reader.Schema().parquet_type) {
	case duckdb_parquet::Type::INT32:
		dbp_decoder->Skip<int32_t>(valid_count);
		break;
	case duckdb_parquet::Type::INT64:
		dbp_decoder->Skip<int64_t>(valid_count);
		break;

	default:
		throw std::runtime_error("DELTA_BINARY_PACKED should only be INT32 or INT64");
	}
}

} // namespace duckdb
