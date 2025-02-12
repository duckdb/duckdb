#include "decoder/rle_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"
#include "reader/templated_column_reader.hpp"

namespace duckdb {

RLEDecoder::RLEDecoder(ColumnReader &reader) : reader(reader), decoded_data_buffer(reader.encoding_buffers[0]) {
}

void RLEDecoder::InitializePage() {
	if (reader.Type().id() != LogicalTypeId::BOOLEAN) {
		throw std::runtime_error("RLE encoding is only supported for boolean data");
	}
	auto &block = reader.block;
	block->inc(sizeof(uint32_t));
	rle_decoder = make_uniq<RleBpDecoder>(block->ptr, block->len, 1);
}

void RLEDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	// RLE encoding for boolean
	D_ASSERT(reader.Type().id() == LogicalTypeId::BOOLEAN);
	idx_t valid_count = reader.GetValidCount(defines, read_count, result_offset);
	decoded_data_buffer.reset();
	decoded_data_buffer.resize(reader.reader.allocator, sizeof(bool) * valid_count);
	rle_decoder->GetBatch<uint8_t>(decoded_data_buffer.ptr, valid_count);
	reader.PlainTemplated<bool, TemplatedParquetValueConversion<bool>>(decoded_data_buffer, defines, read_count,
	                                                                   result_offset, result);
}

void RLEDecoder::Skip(uint8_t *defines, idx_t skip_count) {
	idx_t valid_count = reader.GetValidCount(defines, skip_count);
	rle_decoder->Skip(valid_count);
}

} // namespace duckdb
