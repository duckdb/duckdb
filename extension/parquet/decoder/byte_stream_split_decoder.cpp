#include "decoder/byte_stream_split_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

ByteStreamSplitDecoder::ByteStreamSplitDecoder(ColumnReader &reader)
    : reader(reader), decoded_data_buffer(reader.encoding_buffers[0]) {
}

void ByteStreamSplitDecoder::InitializePage() {
	auto &block = reader.block;
	// Subtract 1 from length as the block is allocated with 1 extra byte,
	// but the byte stream split encoder needs to know the correct data size.
	bss_decoder = make_uniq<BssDecoder>(block->ptr, block->len - 1);
	block->inc(block->len);
}

void ByteStreamSplitDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	idx_t valid_count = reader.GetValidCount(defines, read_count, result_offset);

	auto &allocator = reader.reader.allocator;
	decoded_data_buffer.reset();
	switch (reader.Schema().parquet_type) {
	case duckdb_parquet::Type::FLOAT:
		decoded_data_buffer.resize(allocator, sizeof(float) * valid_count);
		bss_decoder->GetBatch<float>(decoded_data_buffer.ptr, valid_count);
		break;
	case duckdb_parquet::Type::DOUBLE:
		decoded_data_buffer.resize(allocator, sizeof(double) * valid_count);
		bss_decoder->GetBatch<double>(decoded_data_buffer.ptr, valid_count);
		break;
	default:
		throw std::runtime_error("BYTE_STREAM_SPLIT encoding is only supported for FLOAT or DOUBLE data");
	}

	reader.Plain(decoded_data_buffer, defines, read_count, result_offset, result);
}

void ByteStreamSplitDecoder::Skip(uint8_t *defines, idx_t skip_count) {
	idx_t valid_count = reader.GetValidCount(defines, skip_count);
	switch (reader.Schema().parquet_type) {
	case duckdb_parquet::Type::FLOAT:
		bss_decoder->Skip<float>(valid_count);
		break;
	case duckdb_parquet::Type::DOUBLE:
		bss_decoder->Skip<double>(valid_count);
		break;
	default:
		throw std::runtime_error("BYTE_STREAM_SPLIT encoding is only supported for FLOAT or DOUBLE data");
	}
}

} // namespace duckdb
