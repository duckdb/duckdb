#include "decoder/delta_binary_packed_decoder.hpp"
#include "column_reader.hpp"
#include "parquet_reader.hpp"

namespace duckdb {

DeltaBinaryPackedDecoder::DeltaBinaryPackedDecoder(ColumnReader &reader) : reader(reader) {
}

void DeltaBinaryPackedDecoder::InitializePage() {
	auto &block = reader.block;
	dbp_decoder = make_uniq<DbpDecoder>(block->ptr, block->len);
	block->inc(block->len);
}

void DeltaBinaryPackedDecoder::Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset) {
	// TODO keep this in the state
	auto read_buf = make_shared_ptr<ResizeableBuffer>();

	idx_t null_count = 0;
	if (defines) {
		// we need the null count because the dictionary offsets have no entries for nulls
		for (idx_t i = result_offset; i < result_offset + read_count; i++) {
			null_count += (defines[i] != reader.max_define);
		}
	}
	idx_t valid_count = read_count - null_count;

	auto &allocator = reader.reader.allocator;
	switch (reader.schema.type) {
	case duckdb_parquet::Type::INT32:
		read_buf->resize(allocator, sizeof(int32_t) * (valid_count));
		dbp_decoder->GetBatch<int32_t>(read_buf->ptr, valid_count);

		break;
	case duckdb_parquet::Type::INT64:
		read_buf->resize(allocator, sizeof(int64_t) * (valid_count));
		dbp_decoder->GetBatch<int64_t>(read_buf->ptr, valid_count);
		break;

	default:
		throw std::runtime_error("DELTA_BINARY_PACKED should only be INT32 or INT64");
	}
	// Plain() will put NULLs in the right place
	reader.Plain(read_buf, defines, read_count, nullptr, result_offset, result);
}

}
