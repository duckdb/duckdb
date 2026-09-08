#include "decoder/byte_stream_split_decoder.hpp"

#include <stdexcept>

#include "column_reader.hpp"
#include "parquet_reader.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "parquet_column_schema.hpp"
#include "parquet_types.h"
#include "resizable_buffer.hpp"

namespace duckdb {
class Vector;

ByteStreamSplitDecoder::ByteStreamSplitDecoder(ColumnReader &reader)
    : reader(reader), decoded_data_buffer(reader.encoding_buffers[0]) {
}

void ByteStreamSplitDecoder::InitializePage() {
	auto &block = reader.block;
	// Subtract 1 from length as the block is allocated with 1 extra byte,
	// but the byte stream split encoder needs to know the correct data size.
	idx_t bss_len;
	auto loc = block->ConsumeRemaining(bss_len);
	bss_decoder = make_uniq<BssDecoder>(loc, bss_len - 1);
}

void ByteStreamSplitDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	idx_t valid_count = reader.GetValidCount(defines, read_count, result_offset);

	auto &buffer_manager = reader.reader.buffer_manager;
	decoded_data_buffer.Reset();
	switch (reader.Schema().parquet_type) {
	case duckdb_parquet::Type::FLOAT:
		decoded_data_buffer.Resize(buffer_manager, sizeof(float) * valid_count);
		bss_decoder->GetBatch<float>(decoded_data_buffer.GetCurrentLoc(), valid_count);
		break;
	case duckdb_parquet::Type::DOUBLE:
		decoded_data_buffer.Resize(buffer_manager, sizeof(double) * valid_count);
		bss_decoder->GetBatch<double>(decoded_data_buffer.GetCurrentLoc(), valid_count);
		break;
	case duckdb_parquet::Type::INT32:
		decoded_data_buffer.Resize(buffer_manager, sizeof(int32_t) * valid_count);
		bss_decoder->GetBatch<int32_t>(decoded_data_buffer.GetCurrentLoc(), valid_count);
		break;
	case duckdb_parquet::Type::INT64:
		decoded_data_buffer.Resize(buffer_manager, sizeof(int64_t) * valid_count);
		bss_decoder->GetBatch<int64_t>(decoded_data_buffer.GetCurrentLoc(), valid_count);
		break;
	default:
		throw std::runtime_error("BYTE_STREAM_SPLIT encoding is only supported for FLOAT, DOUBLE, INT32 or INT64 data");
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
	case duckdb_parquet::Type::INT32:
		bss_decoder->Skip<int32_t>(valid_count);
		break;
	case duckdb_parquet::Type::INT64:
		bss_decoder->Skip<int64_t>(valid_count);
		break;
	default:
		throw std::runtime_error("BYTE_STREAM_SPLIT encoding is only supported for FLOAT, DOUBLE, INT32 or INT64 data");
	}
}

} // namespace duckdb
