#include "decoder/byte_stream_split_decoder.hpp"

#include <stdexcept>

#include "column_reader.hpp"
#include "parquet_reader.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/numeric_utils.hpp"
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
	auto data_len = block->len - 1;
	auto &schema = reader.Schema();
	if (schema.parquet_type == duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY) {
		// The value width is the column's type_length. Un-split the whole page into the PLAIN layout so that every
		// FIXED_LEN_BYTE_ARRAY reader (DECIMAL, UUID, FLOAT16, BLOB) can consume it through its PLAIN path.
		idx_t type_length = schema.type_length;
		if (type_length == 0 || data_len % type_length != 0) {
			throw std::runtime_error("BYTE_STREAM_SPLIT data size is not a multiple of the FIXED_LEN_BYTE_ARRAY "
			                         "type length - corrupt file?");
		}
		BssDecoder page_decoder(block->ptr, data_len);
		plain_data = make_shared_ptr<ResizeableBuffer>(reader.reader.allocator, data_len);
		page_decoder.GetBatch(plain_data->ptr, NumericCast<uint32_t>(data_len / type_length), type_length);
		bss_decoder.reset();
	} else {
		plain_data.reset();
		bss_decoder = make_uniq<BssDecoder>(block->ptr, data_len);
	}
	block->inc(block->len);
}

void ByteStreamSplitDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	if (plain_data) {
		reader.Plain(plain_data, defines, read_count, result_offset, result);
		return;
	}
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
	case duckdb_parquet::Type::INT32:
		decoded_data_buffer.resize(allocator, sizeof(int32_t) * valid_count);
		bss_decoder->GetBatch<int32_t>(decoded_data_buffer.ptr, valid_count);
		break;
	case duckdb_parquet::Type::INT64:
		decoded_data_buffer.resize(allocator, sizeof(int64_t) * valid_count);
		bss_decoder->GetBatch<int64_t>(decoded_data_buffer.ptr, valid_count);
		break;
	default:
		throw std::runtime_error("BYTE_STREAM_SPLIT encoding is only supported for FLOAT, DOUBLE, INT32, INT64 or "
		                         "FIXED_LEN_BYTE_ARRAY data");
	}

	reader.Plain(decoded_data_buffer, defines, read_count, result_offset, result);
}

void ByteStreamSplitDecoder::Skip(uint8_t *defines, idx_t skip_count) {
	if (plain_data) {
		reader.PlainSkip(*plain_data, defines, skip_count);
		return;
	}
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
		throw std::runtime_error("BYTE_STREAM_SPLIT encoding is only supported for FLOAT, DOUBLE, INT32, INT64 or "
		                         "FIXED_LEN_BYTE_ARRAY data");
	}
}

} // namespace duckdb
