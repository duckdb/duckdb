#include "column_reader.hpp"

#include "reader/boolean_column_reader.hpp"
#include "brotli/decode.h"
#include "reader/callback_column_reader.hpp"
#include "reader/cast_column_reader.hpp"
#include "reader/decimal_column_reader.hpp"
#include "duckdb.hpp"
#include "reader/expression_column_reader.hpp"
#include "reader/interval_column_reader.hpp"
#include "reader/list_column_reader.hpp"
#include "lz4.hpp"
#include "miniz_wrapper.hpp"
#include "reader/null_column_reader.hpp"
#include "parquet_reader.hpp"
#include "parquet_timestamp.hpp"
#include "reader/row_number_column_reader.hpp"
#include "snappy.h"
#include "reader/string_column_reader.hpp"
#include "reader/struct_column_reader.hpp"
#include "reader/templated_column_reader.hpp"
#include "reader/uuid_column_reader.hpp"
#include "zstd.h"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/bit.hpp"
#endif

namespace duckdb {

using duckdb_parquet::CompressionCodec;
using duckdb_parquet::ConvertedType;
using duckdb_parquet::Encoding;
using duckdb_parquet::PageType;
using duckdb_parquet::Type;

const uint64_t ParquetDecodeUtils::BITPACK_MASKS[] = {0,
                                                      1,
                                                      3,
                                                      7,
                                                      15,
                                                      31,
                                                      63,
                                                      127,
                                                      255,
                                                      511,
                                                      1023,
                                                      2047,
                                                      4095,
                                                      8191,
                                                      16383,
                                                      32767,
                                                      65535,
                                                      131071,
                                                      262143,
                                                      524287,
                                                      1048575,
                                                      2097151,
                                                      4194303,
                                                      8388607,
                                                      16777215,
                                                      33554431,
                                                      67108863,
                                                      134217727,
                                                      268435455,
                                                      536870911,
                                                      1073741823,
                                                      2147483647,
                                                      4294967295,
                                                      8589934591,
                                                      17179869183,
                                                      34359738367,
                                                      68719476735,
                                                      137438953471,
                                                      274877906943,
                                                      549755813887,
                                                      1099511627775,
                                                      2199023255551,
                                                      4398046511103,
                                                      8796093022207,
                                                      17592186044415,
                                                      35184372088831,
                                                      70368744177663,
                                                      140737488355327,
                                                      281474976710655,
                                                      562949953421311,
                                                      1125899906842623,
                                                      2251799813685247,
                                                      4503599627370495,
                                                      9007199254740991,
                                                      18014398509481983,
                                                      36028797018963967,
                                                      72057594037927935,
                                                      144115188075855871,
                                                      288230376151711743,
                                                      576460752303423487,
                                                      1152921504606846975,
                                                      2305843009213693951,
                                                      4611686018427387903,
                                                      9223372036854775807,
                                                      18446744073709551615ULL};

const uint64_t ParquetDecodeUtils::BITPACK_MASKS_SIZE = sizeof(ParquetDecodeUtils::BITPACK_MASKS) / sizeof(uint64_t);

const uint8_t ParquetDecodeUtils::BITPACK_DLEN = 8;

ColumnReader::ColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t file_idx_p,
                           idx_t max_define_p, idx_t max_repeat_p)
    : schema(schema_p), file_idx(file_idx_p), max_define(max_define_p), max_repeat(max_repeat_p), reader(reader),
      type(std::move(type_p)), page_rows_available(0), dictionary_decoder(*this), delta_binary_packed_decoder(*this), rle_decoder(*this), delta_byte_array_decoder(*this) {

	// dummies for Skip()
	dummy_define.resize(reader.allocator, STANDARD_VECTOR_SIZE);
	dummy_repeat.resize(reader.allocator, STANDARD_VECTOR_SIZE);
}

ColumnReader::~ColumnReader() {
}

Allocator &ColumnReader::GetAllocator() {
	return reader.allocator;
}

ParquetReader &ColumnReader::Reader() {
	return reader;
}

const LogicalType &ColumnReader::Type() const {
	return type;
}

const SchemaElement &ColumnReader::Schema() const {
	return schema;
}

optional_ptr<const SchemaElement> ColumnReader::GetParentSchema() const {
	return parent_schema;
}

void ColumnReader::SetParentSchema(const SchemaElement &parent_schema_p) {
	parent_schema = &parent_schema_p;
}

idx_t ColumnReader::FileIdx() const {
	return file_idx;
}

idx_t ColumnReader::MaxDefine() const {
	return max_define;
}

idx_t ColumnReader::MaxRepeat() const {
	return max_repeat;
}

void ColumnReader::RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) {
	if (chunk) {
		uint64_t size = chunk->meta_data.total_compressed_size;
		transport.RegisterPrefetch(FileOffset(), size, allow_merge);
	}
}

uint64_t ColumnReader::TotalCompressedSize() {
	if (!chunk) {
		return 0;
	}

	return chunk->meta_data.total_compressed_size;
}

// Note: It's not trivial to determine where all Column data is stored. Chunk->file_offset
// apparently is not the first page of the data. Therefore we determine the address of the first page by taking the
// minimum of all page offsets.
idx_t ColumnReader::FileOffset() const {
	if (!chunk) {
		throw std::runtime_error("FileOffset called on ColumnReader with no chunk");
	}
	auto min_offset = NumericLimits<idx_t>::Maximum();
	if (chunk->meta_data.__isset.dictionary_page_offset) {
		min_offset = MinValue<idx_t>(min_offset, chunk->meta_data.dictionary_page_offset);
	}
	if (chunk->meta_data.__isset.index_page_offset) {
		min_offset = MinValue<idx_t>(min_offset, chunk->meta_data.index_page_offset);
	}
	min_offset = MinValue<idx_t>(min_offset, chunk->meta_data.data_page_offset);

	return min_offset;
}

idx_t ColumnReader::GroupRowsAvailable() {
	return group_rows_available;
}

unique_ptr<BaseStatistics> ColumnReader::Stats(idx_t row_group_idx_p, const vector<ColumnChunk> &columns) {
	return ParquetStatisticsUtils::TransformColumnStatistics(*this, columns);
}

void ColumnReader::Plain(shared_ptr<ByteBuffer> plain_data, uint8_t *defines, idx_t num_values, // NOLINT
                         parquet_filter_t *filter, idx_t result_offset, Vector &result) {
	throw NotImplementedException("Plain");
}

void ColumnReader::PlainReference(shared_ptr<ResizeableBuffer> &, Vector &result) { // NOLINT
}

void ColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns, TProtocol &protocol_p) {
	D_ASSERT(file_idx < columns.size());
	chunk = &columns[file_idx];
	protocol = &protocol_p;
	D_ASSERT(chunk);
	D_ASSERT(chunk->__isset.meta_data);

	if (chunk->__isset.file_path) {
		throw std::runtime_error("Only inlined data files are supported (no references)");
	}

	// ugh. sometimes there is an extra offset for the dict. sometimes it's wrong.
	chunk_read_offset = chunk->meta_data.data_page_offset;
	if (chunk->meta_data.__isset.dictionary_page_offset && chunk->meta_data.dictionary_page_offset >= 4) {
		// this assumes the data pages follow the dict pages directly.
		chunk_read_offset = chunk->meta_data.dictionary_page_offset;
	}
	group_rows_available = chunk->meta_data.num_values;
}

void ColumnReader::PrepareRead(parquet_filter_t &filter) {
	encoding = ColumnEncoding::INVALID;
	defined_decoder.reset();
	bss_decoder.reset();
	block.reset();
	PageHeader page_hdr;
	reader.Read(page_hdr, *protocol);
	// some basic sanity check
	if (page_hdr.compressed_page_size < 0 || page_hdr.uncompressed_page_size < 0) {
		throw std::runtime_error("Page sizes can't be < 0");
	}

	switch (page_hdr.type) {
	case PageType::DATA_PAGE_V2:
		PreparePageV2(page_hdr);
		PrepareDataPage(page_hdr);
		break;
	case PageType::DATA_PAGE:
		PreparePage(page_hdr);
		PrepareDataPage(page_hdr);
		break;
	case PageType::DICTIONARY_PAGE: {
		PreparePage(page_hdr);
		auto dictionary_size = page_hdr.dictionary_page_header.num_values;
		if (dictionary_size < 0) {
			throw std::runtime_error("Invalid dictionary page header (num_values < 0)");
		}
		dictionary_decoder.InitializeDictionary(dictionary_size);
		break;
	}
	default:
		break; // ignore INDEX page type and any other custom extensions
	}
	ResetPage();
}

void ColumnReader::ResetPage() {
}

void ColumnReader::PreparePageV2(PageHeader &page_hdr) {
	D_ASSERT(page_hdr.type == PageType::DATA_PAGE_V2);
	auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol->getTransport());

	AllocateBlock(page_hdr.uncompressed_page_size + 1);
	bool uncompressed = false;
	if (page_hdr.data_page_header_v2.__isset.is_compressed && !page_hdr.data_page_header_v2.is_compressed) {
		uncompressed = true;
	}
	if (chunk->meta_data.codec == CompressionCodec::UNCOMPRESSED) {
		if (page_hdr.compressed_page_size != page_hdr.uncompressed_page_size) {
			throw std::runtime_error("Page size mismatch");
		}
		uncompressed = true;
	}
	if (uncompressed) {
		reader.ReadData(*protocol, block->ptr, page_hdr.compressed_page_size);
		return;
	}

	// copy repeats & defines as-is because FOR SOME REASON they are uncompressed
	auto uncompressed_bytes = page_hdr.data_page_header_v2.repetition_levels_byte_length +
	                          page_hdr.data_page_header_v2.definition_levels_byte_length;
	if (uncompressed_bytes > page_hdr.uncompressed_page_size) {
		throw std::runtime_error("Page header inconsistency, uncompressed_page_size needs to be larger than "
		                         "repetition_levels_byte_length + definition_levels_byte_length");
	}
	trans.read(block->ptr, uncompressed_bytes);

	auto compressed_bytes = page_hdr.compressed_page_size - uncompressed_bytes;

	AllocateCompressed(compressed_bytes);
	reader.ReadData(*protocol, compressed_buffer.ptr, compressed_bytes);

	DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, compressed_bytes, block->ptr + uncompressed_bytes,
	                   page_hdr.uncompressed_page_size - uncompressed_bytes);
}

void ColumnReader::AllocateBlock(idx_t size) {
	if (!block) {
		block = make_shared_ptr<ResizeableBuffer>(GetAllocator(), size);
	} else {
		block->resize(GetAllocator(), size);
	}
}

void ColumnReader::AllocateCompressed(idx_t size) {
	compressed_buffer.resize(GetAllocator(), size);
}

void ColumnReader::PreparePage(PageHeader &page_hdr) {
	AllocateBlock(page_hdr.uncompressed_page_size + 1);
	if (chunk->meta_data.codec == CompressionCodec::UNCOMPRESSED) {
		if (page_hdr.compressed_page_size != page_hdr.uncompressed_page_size) {
			throw std::runtime_error("Page size mismatch");
		}
		reader.ReadData(*protocol, block->ptr, page_hdr.compressed_page_size);
		return;
	}

	AllocateCompressed(page_hdr.compressed_page_size + 1);
	reader.ReadData(*protocol, compressed_buffer.ptr, page_hdr.compressed_page_size);

	DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, page_hdr.compressed_page_size, block->ptr,
	                   page_hdr.uncompressed_page_size);
}

void ColumnReader::DecompressInternal(CompressionCodec::type codec, const_data_ptr_t src, idx_t src_size,
                                      data_ptr_t dst, idx_t dst_size) {
	switch (codec) {
	case CompressionCodec::UNCOMPRESSED:
		throw InternalException("Parquet data unexpectedly uncompressed");
	case CompressionCodec::GZIP: {
		MiniZStream s;
		s.Decompress(const_char_ptr_cast(src), src_size, char_ptr_cast(dst), dst_size);
		break;
	}
	case CompressionCodec::LZ4_RAW: {
		auto res =
		    duckdb_lz4::LZ4_decompress_safe(const_char_ptr_cast(src), char_ptr_cast(dst),
		                                    UnsafeNumericCast<int32_t>(src_size), UnsafeNumericCast<int32_t>(dst_size));
		if (res != NumericCast<int>(dst_size)) {
			throw std::runtime_error("LZ4 decompression failure");
		}
		break;
	}
	case CompressionCodec::SNAPPY: {
		{
			size_t uncompressed_size = 0;
			auto res = duckdb_snappy::GetUncompressedLength(const_char_ptr_cast(src), src_size, &uncompressed_size);
			if (!res) {
				throw std::runtime_error("Snappy decompression failure");
			}
			if (uncompressed_size != dst_size) {
				throw std::runtime_error("Snappy decompression failure: Uncompressed data size mismatch");
			}
		}
		auto res = duckdb_snappy::RawUncompress(const_char_ptr_cast(src), src_size, char_ptr_cast(dst));
		if (!res) {
			throw std::runtime_error("Snappy decompression failure");
		}
		break;
	}
	case CompressionCodec::ZSTD: {
		auto res = duckdb_zstd::ZSTD_decompress(dst, dst_size, src, src_size);
		if (duckdb_zstd::ZSTD_isError(res) || res != dst_size) {
			throw std::runtime_error("ZSTD Decompression failure");
		}
		break;
	}
	case CompressionCodec::BROTLI: {
		auto state = duckdb_brotli::BrotliDecoderCreateInstance(nullptr, nullptr, nullptr);
		size_t total_out = 0;
		auto src_size_size_t = NumericCast<size_t>(src_size);
		auto dst_size_size_t = NumericCast<size_t>(dst_size);

		auto res = duckdb_brotli::BrotliDecoderDecompressStream(state, &src_size_size_t, &src, &dst_size_size_t, &dst,
		                                                        &total_out);
		if (res != duckdb_brotli::BROTLI_DECODER_RESULT_SUCCESS) {
			throw std::runtime_error("Brotli Decompression failure");
		}
		duckdb_brotli::BrotliDecoderDestroyInstance(state);
		break;
	}

	default: {
		std::stringstream codec_name;
		codec_name << codec;
		throw std::runtime_error("Unsupported compression codec \"" + codec_name.str() +
		                         "\". Supported options are uncompressed, brotli, gzip, lz4_raw, snappy or zstd");
	}
	}
}

void ColumnReader::PrepareDataPage(PageHeader &page_hdr) {
	if (page_hdr.type == PageType::DATA_PAGE && !page_hdr.__isset.data_page_header) {
		throw std::runtime_error("Missing data page header from data page");
	}
	if (page_hdr.type == PageType::DATA_PAGE_V2 && !page_hdr.__isset.data_page_header_v2) {
		throw std::runtime_error("Missing data page header from data page v2");
	}

	bool is_v1 = page_hdr.type == PageType::DATA_PAGE;
	bool is_v2 = page_hdr.type == PageType::DATA_PAGE_V2;
	auto &v1_header = page_hdr.data_page_header;
	auto &v2_header = page_hdr.data_page_header_v2;

	page_rows_available = is_v1 ? v1_header.num_values : v2_header.num_values;
	auto page_encoding = is_v1 ? v1_header.encoding : v2_header.encoding;

	if (HasRepeats()) {
		uint32_t rep_length = is_v1 ? block->read<uint32_t>() : v2_header.repetition_levels_byte_length;
		block->available(rep_length);
		repeated_decoder = make_uniq<RleBpDecoder>(block->ptr, rep_length, RleBpDecoder::ComputeBitWidth(max_repeat));
		block->inc(rep_length);
	} else if (is_v2 && v2_header.repetition_levels_byte_length > 0) {
		block->inc(v2_header.repetition_levels_byte_length);
	}

	if (HasDefines()) {
		uint32_t def_length = is_v1 ? block->read<uint32_t>() : v2_header.definition_levels_byte_length;
		block->available(def_length);
		defined_decoder = make_uniq<RleBpDecoder>(block->ptr, def_length, RleBpDecoder::ComputeBitWidth(max_define));
		block->inc(def_length);
	} else if (is_v2 && v2_header.definition_levels_byte_length > 0) {
		block->inc(v2_header.definition_levels_byte_length);
	}

	switch (page_encoding) {
	case Encoding::RLE_DICTIONARY:
	case Encoding::PLAIN_DICTIONARY: {
		encoding = ColumnEncoding::DICTIONARY;
		dictionary_decoder.InitializePage();
		break;
	}
	case Encoding::RLE: {
		encoding = ColumnEncoding::RLE;
		rle_decoder.InitializePage();
		break;
	}
	case Encoding::DELTA_BINARY_PACKED: {
		encoding = ColumnEncoding::DELTA_BINARY_PACKED;
		delta_binary_packed_decoder.InitializePage();
		break;
	}
	case Encoding::DELTA_LENGTH_BYTE_ARRAY: {
		encoding = ColumnEncoding::DELTA_LENGTH_BYTE_ARRAY;
		delta_byte_array_decoder.InitializeDeltaLengthByteArray();
		break;
	}
	case Encoding::DELTA_BYTE_ARRAY: {
		encoding = ColumnEncoding::DELTA_BYTE_ARRAY;
		delta_byte_array_decoder.InitializeDeltaByteArray();
		break;
	}
	case Encoding::BYTE_STREAM_SPLIT: {
		// Subtract 1 from length as the block is allocated with 1 extra byte,
		// but the byte stream split encoder needs to know the correct data size.
		bss_decoder = make_uniq<BssDecoder>(block->ptr, block->len - 1);
		block->inc(block->len);
		encoding = ColumnEncoding::BYTE_STREAM_SPLIT;
		break;
	}
	case Encoding::PLAIN:
		// nothing to do here, will be read directly below
		encoding = ColumnEncoding::PLAIN;
		break;

	default:
		throw std::runtime_error("Unsupported page encoding");
	}
}

idx_t ColumnReader::Read(uint64_t num_values, parquet_filter_t &filter, data_ptr_t define_out, data_ptr_t repeat_out,
                         Vector &result) {
	// we need to reset the location because multiple column readers share the same protocol
	auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol->getTransport());
	trans.SetLocation(chunk_read_offset);

	// Perform any skips that were not applied yet.
	if (pending_skips > 0) {
		ApplyPendingSkips(pending_skips);
	}

	idx_t result_offset = 0;
	auto to_read = num_values;
	D_ASSERT(to_read <= STANDARD_VECTOR_SIZE);

	while (to_read > 0) {
		while (page_rows_available == 0) {
			PrepareRead(filter);
		}

		D_ASSERT(block);
		auto read_now = MinValue<idx_t>(to_read, page_rows_available);

		D_ASSERT(read_now + result_offset <= STANDARD_VECTOR_SIZE);

		if (HasRepeats()) {
			D_ASSERT(repeated_decoder);
			repeated_decoder->GetBatch<uint8_t>(repeat_out + result_offset, read_now);
		}

		if (HasDefines()) {
			D_ASSERT(defined_decoder);
			defined_decoder->GetBatch<uint8_t>(define_out + result_offset, read_now);
		}

		idx_t null_count = 0;

		if ((bss_decoder) && HasDefines()) {
			// we need the null count because the dictionary offsets have no entries for nulls
			for (idx_t i = result_offset; i < result_offset + read_now; i++) {
				null_count += (define_out[i] != max_define);
			}
		}

		if (result_offset != 0 && result.GetVectorType() != VectorType::FLAT_VECTOR) {
			result.Flatten(result_offset);
			result.Resize(result_offset, STANDARD_VECTOR_SIZE);
		}

		auto define_ptr = HasDefines() ? static_cast<uint8_t *>(define_out) : nullptr;
		if (encoding == ColumnEncoding::DICTIONARY) {
			dictionary_decoder.Read(define_ptr, read_now, result, result_offset);
		} else if (encoding == ColumnEncoding::DELTA_BINARY_PACKED) {
			delta_binary_packed_decoder.Read(define_ptr, read_now, result, result_offset);
		} else if (encoding == ColumnEncoding::RLE) {
			rle_decoder.Read(define_ptr, read_now, result, result_offset);
		} else if (encoding == ColumnEncoding::DELTA_LENGTH_BYTE_ARRAY || encoding == ColumnEncoding::DELTA_BYTE_ARRAY) {
			// DELTA_BYTE_ARRAY or DELTA_LENGTH_BYTE_ARRAY
			delta_byte_array_decoder.Read(define_ptr, read_now, result, result_offset);
		} else if (bss_decoder) {
			auto read_buf = make_shared_ptr<ResizeableBuffer>();

			switch (schema.type) {
			case duckdb_parquet::Type::FLOAT:
				read_buf->resize(reader.allocator, sizeof(float) * (read_now - null_count));
				bss_decoder->GetBatch<float>(read_buf->ptr, read_now - null_count);
				break;
			case duckdb_parquet::Type::DOUBLE:
				read_buf->resize(reader.allocator, sizeof(double) * (read_now - null_count));
				bss_decoder->GetBatch<double>(read_buf->ptr, read_now - null_count);
				break;
			default:
				throw std::runtime_error("BYTE_STREAM_SPLIT encoding is only supported for FLOAT or DOUBLE data");
			}

			Plain(read_buf, define_out, read_now, &filter, result_offset, result);
		} else {
			PlainReference(block, result);
			Plain(block, define_out, read_now, &filter, result_offset, result);
		}

		result_offset += read_now;
		page_rows_available -= read_now;
		to_read -= read_now;
	}
	group_rows_available -= num_values;
	chunk_read_offset = trans.GetLocation();

	return num_values;
}

void ColumnReader::Skip(idx_t num_values) {
	pending_skips += num_values;
}

void ColumnReader::ApplyPendingSkips(idx_t num_values) {
	pending_skips -= num_values;

	dummy_define.zero();
	dummy_repeat.zero();

	// TODO this can be optimized, for example we dont actually have to bitunpack offsets
	Vector base_result(type, nullptr);

	idx_t remaining = num_values;
	idx_t read = 0;

	while (remaining) {
		Vector dummy_result(base_result);
		idx_t to_read = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
		read += Read(to_read, none_filter, dummy_define.ptr, dummy_repeat.ptr, dummy_result);
		remaining -= to_read;
	}

	if (read != num_values) {
		throw std::runtime_error("Row count mismatch when skipping rows");
	}
}

//===--------------------------------------------------------------------===//
// Create Column Reader
//===--------------------------------------------------------------------===//
template <class T>
unique_ptr<ColumnReader> CreateDecimalReader(ParquetReader &reader, const LogicalType &type_p,
                                             const SchemaElement &schema_p, idx_t file_idx_p, idx_t max_define,
                                             idx_t max_repeat) {
	switch (type_p.InternalType()) {
	case PhysicalType::INT16:
		return make_uniq<TemplatedColumnReader<int16_t, TemplatedParquetValueConversion<T>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case PhysicalType::INT32:
		return make_uniq<TemplatedColumnReader<int32_t, TemplatedParquetValueConversion<T>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case PhysicalType::INT64:
		return make_uniq<TemplatedColumnReader<int64_t, TemplatedParquetValueConversion<T>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	default:
		throw NotImplementedException("Unimplemented internal type for CreateDecimalReader");
	}
}

unique_ptr<ColumnReader> ColumnReader::CreateReader(ParquetReader &reader, const LogicalType &type_p,
                                                    const SchemaElement &schema_p, idx_t file_idx_p, idx_t max_define,
                                                    idx_t max_repeat) {
	switch (type_p.id()) {
	case LogicalTypeId::BOOLEAN:
		return make_uniq<BooleanColumnReader>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::UTINYINT:
		return make_uniq<TemplatedColumnReader<uint8_t, TemplatedParquetValueConversion<uint32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::USMALLINT:
		return make_uniq<TemplatedColumnReader<uint16_t, TemplatedParquetValueConversion<uint32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::UINTEGER:
		return make_uniq<TemplatedColumnReader<uint32_t, TemplatedParquetValueConversion<uint32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::UBIGINT:
		return make_uniq<TemplatedColumnReader<uint64_t, TemplatedParquetValueConversion<uint64_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::TINYINT:
		return make_uniq<TemplatedColumnReader<int8_t, TemplatedParquetValueConversion<int32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::SMALLINT:
		return make_uniq<TemplatedColumnReader<int16_t, TemplatedParquetValueConversion<int32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::INTEGER:
		return make_uniq<TemplatedColumnReader<int32_t, TemplatedParquetValueConversion<int32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::BIGINT:
		return make_uniq<TemplatedColumnReader<int64_t, TemplatedParquetValueConversion<int64_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::FLOAT:
		return make_uniq<TemplatedColumnReader<float, TemplatedParquetValueConversion<float>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::DOUBLE:
		switch (schema_p.type) {
		case Type::BYTE_ARRAY:
		case Type::FIXED_LEN_BYTE_ARRAY:
			return ParquetDecimalUtils::CreateReader(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
		default:
			return make_uniq<TemplatedColumnReader<double, TemplatedParquetValueConversion<double>>>(
			    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
		}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		switch (schema_p.type) {
		case Type::INT96:
			return make_uniq<CallbackColumnReader<Int96, timestamp_t, ImpalaTimestampToTimestamp>>(
			    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
		case Type::INT64:
			if (schema_p.__isset.logicalType && schema_p.logicalType.__isset.TIMESTAMP) {
				if (schema_p.logicalType.TIMESTAMP.unit.__isset.MILLIS) {
					return make_uniq<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampMsToTimestamp>>(
					    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
				} else if (schema_p.logicalType.TIMESTAMP.unit.__isset.MICROS) {
					return make_uniq<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampMicrosToTimestamp>>(
					    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
				} else if (schema_p.logicalType.TIMESTAMP.unit.__isset.NANOS) {
					return make_uniq<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampNsToTimestamp>>(
					    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
				}
			} else if (schema_p.__isset.converted_type) {
				switch (schema_p.converted_type) {
				case ConvertedType::TIMESTAMP_MICROS:
					return make_uniq<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampMicrosToTimestamp>>(
					    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
				case ConvertedType::TIMESTAMP_MILLIS:
					return make_uniq<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampMsToTimestamp>>(
					    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
				default:
					break;
				}
			}
		default:
			break;
		}
		break;
	case LogicalTypeId::TIMESTAMP_NS:
		switch (schema_p.type) {
		case Type::INT96:
			return make_uniq<CallbackColumnReader<Int96, timestamp_ns_t, ImpalaTimestampToTimestampNS>>(
			    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
		case Type::INT64:
			if (schema_p.__isset.logicalType && schema_p.logicalType.__isset.TIMESTAMP) {
				if (schema_p.logicalType.TIMESTAMP.unit.__isset.MILLIS) {
					return make_uniq<CallbackColumnReader<int64_t, timestamp_ns_t, ParquetTimestampMsToTimestampNs>>(
					    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
				} else if (schema_p.logicalType.TIMESTAMP.unit.__isset.MICROS) {
					return make_uniq<CallbackColumnReader<int64_t, timestamp_ns_t, ParquetTimestampUsToTimestampNs>>(
					    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
				} else if (schema_p.logicalType.TIMESTAMP.unit.__isset.NANOS) {
					return make_uniq<CallbackColumnReader<int64_t, timestamp_ns_t, ParquetTimestampNsToTimestampNs>>(
					    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
				}
			} else if (schema_p.__isset.converted_type) {
				switch (schema_p.converted_type) {
				case ConvertedType::TIMESTAMP_MICROS:
					return make_uniq<CallbackColumnReader<int64_t, timestamp_ns_t, ParquetTimestampUsToTimestampNs>>(
					    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
				case ConvertedType::TIMESTAMP_MILLIS:
					return make_uniq<CallbackColumnReader<int64_t, timestamp_ns_t, ParquetTimestampMsToTimestampNs>>(
					    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
				default:
					break;
				}
			}
		default:
			break;
		}
		break;
	case LogicalTypeId::DATE:
		return make_uniq<CallbackColumnReader<int32_t, date_t, ParquetIntToDate>>(reader, type_p, schema_p, file_idx_p,
		                                                                          max_define, max_repeat);
	case LogicalTypeId::TIME:
		if (schema_p.__isset.logicalType && schema_p.logicalType.__isset.TIME) {
			if (schema_p.logicalType.TIME.unit.__isset.MILLIS) {
				return make_uniq<CallbackColumnReader<int32_t, dtime_t, ParquetIntToTimeMs>>(
				    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
			} else if (schema_p.logicalType.TIME.unit.__isset.MICROS) {
				return make_uniq<CallbackColumnReader<int64_t, dtime_t, ParquetIntToTime>>(
				    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
			} else if (schema_p.logicalType.TIME.unit.__isset.NANOS) {
				return make_uniq<CallbackColumnReader<int64_t, dtime_t, ParquetIntToTimeNs>>(
				    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
			}
		} else if (schema_p.__isset.converted_type) {
			switch (schema_p.converted_type) {
			case ConvertedType::TIME_MICROS:
				return make_uniq<CallbackColumnReader<int64_t, dtime_t, ParquetIntToTime>>(
				    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
			case ConvertedType::TIME_MILLIS:
				return make_uniq<CallbackColumnReader<int32_t, dtime_t, ParquetIntToTimeMs>>(
				    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
			default:
				break;
			}
		}
		throw NotImplementedException("Unsupported time encoding in Parquet file");
	case LogicalTypeId::TIME_TZ:
		if (schema_p.__isset.logicalType && schema_p.logicalType.__isset.TIME) {
			if (schema_p.logicalType.TIME.unit.__isset.MILLIS) {
				return make_uniq<CallbackColumnReader<int32_t, dtime_tz_t, ParquetIntToTimeMsTZ>>(
				    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
			} else if (schema_p.logicalType.TIME.unit.__isset.MICROS) {
				return make_uniq<CallbackColumnReader<int64_t, dtime_tz_t, ParquetIntToTimeTZ>>(
				    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
			} else if (schema_p.logicalType.TIME.unit.__isset.NANOS) {
				return make_uniq<CallbackColumnReader<int64_t, dtime_tz_t, ParquetIntToTimeNsTZ>>(
				    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
			}
		} else if (schema_p.__isset.converted_type) {
			switch (schema_p.converted_type) {
			case ConvertedType::TIME_MICROS:
				return make_uniq<CallbackColumnReader<int64_t, dtime_tz_t, ParquetIntToTimeTZ>>(
				    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
			default:
				break;
			}
		}
		throw NotImplementedException("Unsupported time encoding in Parquet file");
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return make_uniq<StringColumnReader>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::DECIMAL:
		// we have to figure out what kind of int we need
		switch (schema_p.type) {
		case Type::INT32:
			return CreateDecimalReader<int32_t>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
		case Type::INT64:
			return CreateDecimalReader<int64_t>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
		case Type::BYTE_ARRAY:
		case Type::FIXED_LEN_BYTE_ARRAY:
			return ParquetDecimalUtils::CreateReader(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
		default:
			throw NotImplementedException("Unrecognized Parquet type for Decimal");
		}
		break;
	case LogicalTypeId::UUID:
		return make_uniq<UUIDColumnReader>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::INTERVAL:
		return make_uniq<IntervalColumnReader>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::SQLNULL:
		return make_uniq<NullColumnReader>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	default:
		break;
	}
	throw NotImplementedException(type_p.ToString());
}

} // namespace duckdb
