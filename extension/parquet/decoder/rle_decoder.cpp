#include "decoder/rle_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"
#include "reader/templated_column_reader.hpp"

namespace duckdb {

RLEDecoder::RLEDecoder(ColumnReader &reader) : reader(reader) {
}

void RLEDecoder::InitializePage() {
	if (reader.type.id() != LogicalTypeId::BOOLEAN) {
		throw std::runtime_error("RLE encoding is only supported for boolean data");
	}
	auto &block = reader.block;
	block->inc(sizeof(uint32_t));
	rle_decoder = make_uniq<RleBpDecoder>(block->ptr, block->len, 1);
}

void RLEDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	// RLE encoding for boolean
	D_ASSERT(reader.type.id() == LogicalTypeId::BOOLEAN);
	idx_t null_count = 0;
	if (defines) {
		// we need the null count because the dictionary offsets have no entries for nulls
		for (idx_t i = result_offset; i < result_offset + read_count; i++) {
			null_count += (defines[i] != reader.max_define);
		}
	}
	idx_t valid_count = read_count - null_count;
	auto read_buf = make_shared_ptr<ResizeableBuffer>();
	read_buf->resize(reader.reader.allocator, sizeof(bool) * valid_count);
	rle_decoder->GetBatch<uint8_t>(read_buf->ptr, valid_count);
	reader.PlainTemplated<bool, TemplatedParquetValueConversion<bool>>(read_buf, defines, read_count, nullptr,
																result_offset, result);
}


}
