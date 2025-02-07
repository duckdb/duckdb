#include "decoder/byte_stream_split_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

ByteStreamSplitDecoder::ByteStreamSplitDecoder(ColumnReader &reader) : reader(reader) {
}

void ByteStreamSplitDecoder::InitializePage() {
	auto &block = reader.block;
	// Subtract 1 from length as the block is allocated with 1 extra byte,
	// but the byte stream split encoder needs to know the correct data size.
	bss_decoder = make_uniq<BssDecoder>(block->ptr, block->len - 1);
	block->inc(block->len);
}

void ByteStreamSplitDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	idx_t null_count = 0;

	if (defines) {
		// we need the null count because the dictionary offsets have no entries for nulls
		for (idx_t i = result_offset; i < result_offset + read_count; i++) {
			null_count += defines[i] != reader.max_define;
		}
	}
	idx_t valid_count = read_count - null_count;

	auto read_buf = make_shared_ptr<ResizeableBuffer>();
	auto &allocator = reader.reader.allocator;
	switch (reader.schema.type) {
	case duckdb_parquet::Type::FLOAT:
		read_buf->resize(allocator, sizeof(float) * valid_count);
		bss_decoder->GetBatch<float>(read_buf->ptr, valid_count);
		break;
	case duckdb_parquet::Type::DOUBLE:
		read_buf->resize(allocator, sizeof(double) * valid_count);
		bss_decoder->GetBatch<double>(read_buf->ptr, valid_count);
		break;
	default:
		throw std::runtime_error("BYTE_STREAM_SPLIT encoding is only supported for FLOAT or DOUBLE data");
	}

	reader.Plain(read_buf, defines, read_count, nullptr, result_offset, result);
}

} // namespace duckdb
