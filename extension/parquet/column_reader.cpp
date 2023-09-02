#include "column_reader.hpp"

#include "boolean_column_reader.hpp"
#include "callback_column_reader.hpp"
#include "cast_column_reader.hpp"
#include "duckdb.hpp"
#include "list_column_reader.hpp"
#include "miniz_wrapper.hpp"
#include "parquet_decimal_utils.hpp"
#include "parquet_reader.hpp"
#include "parquet_timestamp.hpp"
#include "row_number_column_reader.hpp"
#include "snappy.h"
#include "string_column_reader.hpp"
#include "struct_column_reader.hpp"
#include "templated_column_reader.hpp"
#include "utf8proc_wrapper.hpp"
#include "zstd.h"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#endif

namespace duckdb {

using duckdb_parquet::format::CompressionCodec;
using duckdb_parquet::format::ConvertedType;
using duckdb_parquet::format::Encoding;
using duckdb_parquet::format::PageType;
using duckdb_parquet::format::Type;

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
      type(std::move(type_p)), page_rows_available(0) {

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
	if (Type().id() == LogicalTypeId::LIST || Type().id() == LogicalTypeId::STRUCT ||
	    Type().id() == LogicalTypeId::MAP) {
		return nullptr;
	}
	return ParquetStatisticsUtils::TransformColumnStatistics(Schema(), Type(), columns[file_idx]);
}

void ColumnReader::Plain(shared_ptr<ByteBuffer> plain_data, uint8_t *defines, idx_t num_values, // NOLINT
                         parquet_filter_t &filter, idx_t result_offset, Vector &result) {
	throw NotImplementedException("Plain");
}

void ColumnReader::Dictionary(shared_ptr<ResizeableBuffer> dictionary_data, idx_t num_entries) { // NOLINT
	throw NotImplementedException("Dictionary");
}

void ColumnReader::Offsets(uint32_t *offsets, uint8_t *defines, idx_t num_values, parquet_filter_t &filter,
                           idx_t result_offset, Vector &result) {
	throw NotImplementedException("Offsets");
}

void ColumnReader::PrepareDeltaLengthByteArray(ResizeableBuffer &buffer) {
	throw std::runtime_error("DELTA_LENGTH_BYTE_ARRAY encoding is only supported for text or binary data");
}

void ColumnReader::PrepareDeltaByteArray(ResizeableBuffer &buffer) {
	throw std::runtime_error("DELTA_BYTE_ARRAY encoding is only supported for text or binary data");
}

void ColumnReader::DeltaByteArray(uint8_t *defines, idx_t num_values, // NOLINT
                                  parquet_filter_t &filter, idx_t result_offset, Vector &result) {
	throw NotImplementedException("DeltaByteArray");
}

void ColumnReader::DictReference(Vector &result) {
}
void ColumnReader::PlainReference(shared_ptr<ByteBuffer>, Vector &result) { // NOLINT
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
	dict_decoder.reset();
	defined_decoder.reset();
	block.reset();
	PageHeader page_hdr;
	page_hdr.read(protocol);

	switch (page_hdr.type) {
	case PageType::DATA_PAGE_V2:
		PreparePageV2(page_hdr);
		PrepareDataPage(page_hdr);
		break;
	case PageType::DATA_PAGE:
		PreparePage(page_hdr);
		PrepareDataPage(page_hdr);
		break;
	case PageType::DICTIONARY_PAGE:
		PreparePage(page_hdr);
		Dictionary(std::move(block), page_hdr.dictionary_page_header.num_values);
		break;
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
		trans.read(block->ptr, page_hdr.compressed_page_size);
		return;
	}

	// copy repeats & defines as-is because FOR SOME REASON they are uncompressed
	auto uncompressed_bytes = page_hdr.data_page_header_v2.repetition_levels_byte_length +
	                          page_hdr.data_page_header_v2.definition_levels_byte_length;
	trans.read(block->ptr, uncompressed_bytes);

	auto compressed_bytes = page_hdr.compressed_page_size - uncompressed_bytes;

	AllocateCompressed(compressed_bytes);
	trans.read(compressed_buffer.ptr, compressed_bytes);

	DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, compressed_bytes, block->ptr + uncompressed_bytes,
	                   page_hdr.uncompressed_page_size - uncompressed_bytes);
}

void ColumnReader::AllocateBlock(idx_t size) {
	if (!block) {
		block = make_shared<ResizeableBuffer>(GetAllocator(), size);
	} else {
		block->resize(GetAllocator(), size);
	}
}

void ColumnReader::AllocateCompressed(idx_t size) {
	compressed_buffer.resize(GetAllocator(), size);
}

void ColumnReader::PreparePage(PageHeader &page_hdr) {
	auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol->getTransport());

	AllocateBlock(page_hdr.uncompressed_page_size + 1);
	if (chunk->meta_data.codec == CompressionCodec::UNCOMPRESSED) {
		if (page_hdr.compressed_page_size != page_hdr.uncompressed_page_size) {
			throw std::runtime_error("Page size mismatch");
		}
		trans.read((uint8_t *)block->ptr, page_hdr.compressed_page_size);
		return;
	}

	AllocateCompressed(page_hdr.compressed_page_size + 1);
	trans.read((uint8_t *)compressed_buffer.ptr, page_hdr.compressed_page_size);

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
	case CompressionCodec::SNAPPY: {
		{
			size_t uncompressed_size = 0;
			auto res = duckdb_snappy::GetUncompressedLength(const_char_ptr_cast(src), src_size, &uncompressed_size);
			if (!res) {
				throw std::runtime_error("Snappy decompression failure");
			}
			if (uncompressed_size != (size_t)dst_size) {
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
		if (duckdb_zstd::ZSTD_isError(res) || res != (size_t)dst_size) {
			throw std::runtime_error("ZSTD Decompression failure");
		}
		break;
	}
	default: {
		std::stringstream codec_name;
		codec_name << codec;
		throw std::runtime_error("Unsupported compression codec \"" + codec_name.str() +
		                         "\". Supported options are uncompressed, gzip, snappy or zstd");
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
		// where is it otherwise??
		auto dict_width = block->read<uint8_t>();
		// TODO somehow dict_width can be 0 ?
		dict_decoder = make_uniq<RleBpDecoder>(block->ptr, block->len, dict_width);
		block->inc(block->len);
		break;
	}
	case Encoding::RLE: {
		if (type.id() != LogicalTypeId::BOOLEAN) {
			throw std::runtime_error("RLE encoding is only supported for boolean data");
		}
		block->inc(sizeof(uint32_t));
		rle_decoder = make_uniq<RleBpDecoder>(block->ptr, block->len, 1);
		break;
	}
	case Encoding::DELTA_BINARY_PACKED: {
		dbp_decoder = make_uniq<DbpDecoder>(block->ptr, block->len);
		block->inc(block->len);
		break;
	}
	case Encoding::DELTA_LENGTH_BYTE_ARRAY: {
		PrepareDeltaLengthByteArray(*block);
		break;
	}
	case Encoding::DELTA_BYTE_ARRAY: {
		PrepareDeltaByteArray(*block);
		break;
	}
	case Encoding::PLAIN:
		// nothing to do here, will be read directly below
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

	while (to_read > 0) {
		while (page_rows_available == 0) {
			PrepareRead(filter);
		}

		D_ASSERT(block);
		auto read_now = MinValue<idx_t>(to_read, page_rows_available);

		D_ASSERT(read_now <= STANDARD_VECTOR_SIZE);

		if (HasRepeats()) {
			D_ASSERT(repeated_decoder);
			repeated_decoder->GetBatch<uint8_t>(repeat_out + result_offset, read_now);
		}

		if (HasDefines()) {
			D_ASSERT(defined_decoder);
			defined_decoder->GetBatch<uint8_t>(define_out + result_offset, read_now);
		}

		idx_t null_count = 0;

		if ((dict_decoder || dbp_decoder || rle_decoder) && HasDefines()) {
			// we need the null count because the dictionary offsets have no entries for nulls
			for (idx_t i = 0; i < read_now; i++) {
				if (define_out[i + result_offset] != max_define) {
					null_count++;
				}
			}
		}

		if (dict_decoder) {
			offset_buffer.resize(reader.allocator, sizeof(uint32_t) * (read_now - null_count));
			dict_decoder->GetBatch<uint32_t>(offset_buffer.ptr, read_now - null_count);
			DictReference(result);
			Offsets(reinterpret_cast<uint32_t *>(offset_buffer.ptr), define_out, read_now, filter, result_offset,
			        result);
		} else if (dbp_decoder) {
			// TODO keep this in the state
			auto read_buf = make_shared<ResizeableBuffer>();

			switch (schema.type) {
			case duckdb_parquet::format::Type::INT32:
				read_buf->resize(reader.allocator, sizeof(int32_t) * (read_now - null_count));
				dbp_decoder->GetBatch<int32_t>(read_buf->ptr, read_now - null_count);

				break;
			case duckdb_parquet::format::Type::INT64:
				read_buf->resize(reader.allocator, sizeof(int64_t) * (read_now - null_count));
				dbp_decoder->GetBatch<int64_t>(read_buf->ptr, read_now - null_count);
				break;

			default:
				throw std::runtime_error("DELTA_BINARY_PACKED should only be INT32 or INT64");
			}
			// Plain() will put NULLs in the right place
			Plain(read_buf, define_out, read_now, filter, result_offset, result);
		} else if (rle_decoder) {
			// RLE encoding for boolean
			D_ASSERT(type.id() == LogicalTypeId::BOOLEAN);
			auto read_buf = make_shared<ResizeableBuffer>();
			read_buf->resize(reader.allocator, sizeof(bool) * (read_now - null_count));
			rle_decoder->GetBatch<uint8_t>(read_buf->ptr, read_now - null_count);
			PlainTemplated<bool, TemplatedParquetValueConversion<bool>>(read_buf, define_out, read_now, filter,
			                                                            result_offset, result);
		} else if (byte_array_data) {
			// DELTA_BYTE_ARRAY or DELTA_LENGTH_BYTE_ARRAY
			DeltaByteArray(define_out, read_now, filter, result_offset, result);
		} else {
			PlainReference(block, result);
			Plain(block, define_out, read_now, filter, result_offset, result);
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
	Vector dummy_result(type, nullptr);

	idx_t remaining = num_values;
	idx_t read = 0;

	while (remaining) {
		idx_t to_read = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
		read += Read(to_read, none_filter, dummy_define.ptr, dummy_repeat.ptr, dummy_result);
		remaining -= to_read;
	}

	if (read != num_values) {
		throw std::runtime_error("Row count mismatch when skipping rows");
	}
}

//===--------------------------------------------------------------------===//
// String Column Reader
//===--------------------------------------------------------------------===//
StringColumnReader::StringColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p,
                                       idx_t schema_idx_p, idx_t max_define_p, idx_t max_repeat_p)
    : TemplatedColumnReader<string_t, StringParquetValueConversion>(reader, std::move(type_p), schema_p, schema_idx_p,
                                                                    max_define_p, max_repeat_p) {
	fixed_width_string_length = 0;
	if (schema_p.type == Type::FIXED_LEN_BYTE_ARRAY) {
		D_ASSERT(schema_p.__isset.type_length);
		fixed_width_string_length = schema_p.type_length;
	}
}

uint32_t StringColumnReader::VerifyString(const char *str_data, uint32_t str_len, const bool is_varchar) {
	if (!is_varchar) {
		return str_len;
	}
	// verify if a string is actually UTF8, and if there are no null bytes in the middle of the string
	// technically Parquet should guarantee this, but reality is often disappointing
	UnicodeInvalidReason reason;
	size_t pos;
	auto utf_type = Utf8Proc::Analyze(str_data, str_len, &reason, &pos);
	if (utf_type == UnicodeType::INVALID) {
		throw InvalidInputException("Invalid string encoding found in Parquet file: value \"" +
		                            Blob::ToString(string_t(str_data, str_len)) + "\" is not valid UTF8!");
	}
	return str_len;
}

uint32_t StringColumnReader::VerifyString(const char *str_data, uint32_t str_len) {
	return VerifyString(str_data, str_len, Type() == LogicalTypeId::VARCHAR);
}

void StringColumnReader::Dictionary(shared_ptr<ResizeableBuffer> data, idx_t num_entries) {
	dict = std::move(data);
	dict_strings = unique_ptr<string_t[]>(new string_t[num_entries]);
	for (idx_t dict_idx = 0; dict_idx < num_entries; dict_idx++) {
		uint32_t str_len;
		if (fixed_width_string_length == 0) {
			// variable length string: read from dictionary
			str_len = dict->read<uint32_t>();
		} else {
			// fixed length string
			str_len = fixed_width_string_length;
		}
		dict->available(str_len);

		auto dict_str = reinterpret_cast<const char *>(dict->ptr);
		auto actual_str_len = VerifyString(dict_str, str_len);
		dict_strings[dict_idx] = string_t(dict_str, actual_str_len);
		dict->inc(str_len);
	}
}

static shared_ptr<ResizeableBuffer> ReadDbpData(Allocator &allocator, ResizeableBuffer &buffer, idx_t &value_count) {
	auto decoder = make_uniq<DbpDecoder>(buffer.ptr, buffer.len);
	value_count = decoder->TotalValues();
	auto result = make_shared<ResizeableBuffer>();
	result->resize(allocator, sizeof(uint32_t) * value_count);
	decoder->GetBatch<uint32_t>(result->ptr, value_count);
	decoder->Finalize();
	buffer.inc(buffer.len - decoder->BufferPtr().len);
	return result;
}

void StringColumnReader::PrepareDeltaLengthByteArray(ResizeableBuffer &buffer) {
	idx_t value_count;
	auto length_buffer = ReadDbpData(reader.allocator, buffer, value_count);
	if (value_count == 0) {
		// no values
		byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, nullptr);
		return;
	}
	auto length_data = reinterpret_cast<uint32_t *>(length_buffer->ptr);
	byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, value_count);
	byte_array_count = value_count;
	delta_offset = 0;
	auto string_data = FlatVector::GetData<string_t>(*byte_array_data);
	for (idx_t i = 0; i < value_count; i++) {
		auto str_len = length_data[i];
		string_data[i] = StringVector::EmptyString(*byte_array_data, str_len);
		auto result_data = string_data[i].GetDataWriteable();
		memcpy(result_data, buffer.ptr, length_data[i]);
		buffer.inc(length_data[i]);
		string_data[i].Finalize();
	}
}

void StringColumnReader::PrepareDeltaByteArray(ResizeableBuffer &buffer) {
	idx_t prefix_count, suffix_count;
	auto prefix_buffer = ReadDbpData(reader.allocator, buffer, prefix_count);
	auto suffix_buffer = ReadDbpData(reader.allocator, buffer, suffix_count);
	if (prefix_count != suffix_count) {
		throw std::runtime_error("DELTA_BYTE_ARRAY - prefix and suffix counts are different - corrupt file?");
	}
	if (prefix_count == 0) {
		// no values
		byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, nullptr);
		return;
	}
	auto prefix_data = reinterpret_cast<uint32_t *>(prefix_buffer->ptr);
	auto suffix_data = reinterpret_cast<uint32_t *>(suffix_buffer->ptr);
	byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, prefix_count);
	byte_array_count = prefix_count;
	delta_offset = 0;
	auto string_data = FlatVector::GetData<string_t>(*byte_array_data);
	for (idx_t i = 0; i < prefix_count; i++) {
		auto str_len = prefix_data[i] + suffix_data[i];
		string_data[i] = StringVector::EmptyString(*byte_array_data, str_len);
		auto result_data = string_data[i].GetDataWriteable();
		if (prefix_data[i] > 0) {
			if (i == 0 || prefix_data[i] > string_data[i - 1].GetSize()) {
				throw std::runtime_error("DELTA_BYTE_ARRAY - prefix is out of range - corrupt file?");
			}
			memcpy(result_data, string_data[i - 1].GetData(), prefix_data[i]);
		}
		memcpy(result_data + prefix_data[i], buffer.ptr, suffix_data[i]);
		buffer.inc(suffix_data[i]);
		string_data[i].Finalize();
	}
}

void StringColumnReader::DeltaByteArray(uint8_t *defines, idx_t num_values, parquet_filter_t &filter,
                                        idx_t result_offset, Vector &result) {
	if (!byte_array_data) {
		throw std::runtime_error("Internal error - DeltaByteArray called but there was no byte_array_data set");
	}
	auto result_ptr = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Validity(result);
	auto string_data = FlatVector::GetData<string_t>(*byte_array_data);
	for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
		if (HasDefines() && defines[row_idx + result_offset] != max_define) {
			result_mask.SetInvalid(row_idx + result_offset);
			continue;
		}
		if (filter[row_idx + result_offset]) {
			if (delta_offset >= byte_array_count) {
				throw IOException("DELTA_BYTE_ARRAY - length mismatch between values and byte array lengths (attempted "
				                  "read of %d from %d entries) - corrupt file?",
				                  delta_offset + 1, byte_array_count);
			}
			result_ptr[row_idx + result_offset] = string_data[delta_offset++];
		} else {
			delta_offset++;
		}
	}
	StringVector::AddHeapReference(result, *byte_array_data);
}

class ParquetStringVectorBuffer : public VectorBuffer {
public:
	explicit ParquetStringVectorBuffer(shared_ptr<ByteBuffer> buffer_p)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), buffer(std::move(buffer_p)) {
	}

private:
	shared_ptr<ByteBuffer> buffer;
};

void StringColumnReader::DictReference(Vector &result) {
	StringVector::AddBuffer(result, make_buffer<ParquetStringVectorBuffer>(dict));
}
void StringColumnReader::PlainReference(shared_ptr<ByteBuffer> plain_data, Vector &result) {
	StringVector::AddBuffer(result, make_buffer<ParquetStringVectorBuffer>(std::move(plain_data)));
}

string_t StringParquetValueConversion::DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
	auto &dict_strings = reader.Cast<StringColumnReader>().dict_strings;
	return dict_strings[offset];
}

string_t StringParquetValueConversion::PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
	auto &scr = reader.Cast<StringColumnReader>();
	uint32_t str_len = scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() : scr.fixed_width_string_length;
	plain_data.available(str_len);
	auto plain_str = char_ptr_cast(plain_data.ptr);
	auto actual_str_len = reader.Cast<StringColumnReader>().VerifyString(plain_str, str_len);
	auto ret_str = string_t(plain_str, actual_str_len);
	plain_data.inc(str_len);
	return ret_str;
}

void StringParquetValueConversion::PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
	auto &scr = reader.Cast<StringColumnReader>();
	uint32_t str_len = scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() : scr.fixed_width_string_length;
	plain_data.inc(str_len);
}

//===--------------------------------------------------------------------===//
// List Column Reader
//===--------------------------------------------------------------------===//
idx_t ListColumnReader::Read(uint64_t num_values, parquet_filter_t &filter, data_ptr_t define_out,
                             data_ptr_t repeat_out, Vector &result_out) {
	idx_t result_offset = 0;
	auto result_ptr = FlatVector::GetData<list_entry_t>(result_out);
	auto &result_mask = FlatVector::Validity(result_out);

	if (pending_skips > 0) {
		ApplyPendingSkips(pending_skips);
	}

	D_ASSERT(ListVector::GetListSize(result_out) == 0);
	// if an individual list is longer than STANDARD_VECTOR_SIZE we actually have to loop the child read to fill it
	bool finished = false;
	while (!finished) {
		idx_t child_actual_num_values = 0;

		// check if we have any overflow from a previous read
		if (overflow_child_count == 0) {
			// we don't: read elements from the child reader
			child_defines.zero();
			child_repeats.zero();
			// we don't know in advance how many values to read because of the beautiful repetition/definition setup
			// we just read (up to) a vector from the child column, and see if we have read enough
			// if we have not read enough, we read another vector
			// if we have read enough, we leave any unhandled elements in the overflow vector for a subsequent read
			auto child_req_num_values =
			    MinValue<idx_t>(STANDARD_VECTOR_SIZE, child_column_reader->GroupRowsAvailable());
			read_vector.ResetFromCache(read_cache);
			child_actual_num_values = child_column_reader->Read(child_req_num_values, child_filter, child_defines_ptr,
			                                                    child_repeats_ptr, read_vector);
		} else {
			// we do: use the overflow values
			child_actual_num_values = overflow_child_count;
			overflow_child_count = 0;
		}

		if (child_actual_num_values == 0) {
			// no more elements available: we are done
			break;
		}
		read_vector.Verify(child_actual_num_values);
		idx_t current_chunk_offset = ListVector::GetListSize(result_out);

		// hard-won piece of code this, modify at your own risk
		// the intuition is that we have to only collapse values into lists that are repeated *on this level*
		// the rest is pretty much handed up as-is as a single-valued list or NULL
		idx_t child_idx;
		for (child_idx = 0; child_idx < child_actual_num_values; child_idx++) {
			if (child_repeats_ptr[child_idx] == max_repeat) {
				// value repeats on this level, append
				D_ASSERT(result_offset > 0);
				result_ptr[result_offset - 1].length++;
				continue;
			}

			if (result_offset >= num_values) {
				// we ran out of output space
				finished = true;
				break;
			}
			if (child_defines_ptr[child_idx] >= max_define) {
				// value has been defined down the stack, hence its NOT NULL
				result_ptr[result_offset].offset = child_idx + current_chunk_offset;
				result_ptr[result_offset].length = 1;
			} else if (child_defines_ptr[child_idx] == max_define - 1) {
				// empty list
				result_ptr[result_offset].offset = child_idx + current_chunk_offset;
				result_ptr[result_offset].length = 0;
			} else {
				// value is NULL somewhere up the stack
				result_mask.SetInvalid(result_offset);
				result_ptr[result_offset].offset = 0;
				result_ptr[result_offset].length = 0;
			}

			repeat_out[result_offset] = child_repeats_ptr[child_idx];
			define_out[result_offset] = child_defines_ptr[child_idx];

			result_offset++;
		}
		// actually append the required elements to the child list
		ListVector::Append(result_out, read_vector, child_idx);

		// we have read more values from the child reader than we can fit into the result for this read
		// we have to pass everything from child_idx to child_actual_num_values into the next call
		if (child_idx < child_actual_num_values && result_offset == num_values) {
			read_vector.Slice(read_vector, child_idx, child_actual_num_values);
			overflow_child_count = child_actual_num_values - child_idx;
			read_vector.Verify(overflow_child_count);

			// move values in the child repeats and defines *backward* by child_idx
			for (idx_t repdef_idx = 0; repdef_idx < overflow_child_count; repdef_idx++) {
				child_defines_ptr[repdef_idx] = child_defines_ptr[child_idx + repdef_idx];
				child_repeats_ptr[repdef_idx] = child_repeats_ptr[child_idx + repdef_idx];
			}
		}
	}
	result_out.Verify(result_offset);
	return result_offset;
}

ListColumnReader::ListColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p,
                                   idx_t schema_idx_p, idx_t max_define_p, idx_t max_repeat_p,
                                   unique_ptr<ColumnReader> child_column_reader_p)
    : ColumnReader(reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p),
      child_column_reader(std::move(child_column_reader_p)),
      read_cache(reader.allocator, ListType::GetChildType(Type())), read_vector(read_cache), overflow_child_count(0) {

	child_defines.resize(reader.allocator, STANDARD_VECTOR_SIZE);
	child_repeats.resize(reader.allocator, STANDARD_VECTOR_SIZE);
	child_defines_ptr = (uint8_t *)child_defines.ptr;
	child_repeats_ptr = (uint8_t *)child_repeats.ptr;

	child_filter.set();
}

void ListColumnReader::ApplyPendingSkips(idx_t num_values) {
	pending_skips -= num_values;

	auto define_out = unique_ptr<uint8_t[]>(new uint8_t[num_values]);
	auto repeat_out = unique_ptr<uint8_t[]>(new uint8_t[num_values]);

	idx_t remaining = num_values;
	idx_t read = 0;

	while (remaining) {
		Vector result_out(Type());
		parquet_filter_t filter;
		idx_t to_read = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
		read += Read(to_read, filter, define_out.get(), repeat_out.get(), result_out);
		remaining -= to_read;
	}

	if (read != num_values) {
		throw InternalException("Not all skips done!");
	}
}

//===--------------------------------------------------------------------===//
// Row NumberColumn Reader
//===--------------------------------------------------------------------===//
RowNumberColumnReader::RowNumberColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p,
                                             idx_t schema_idx_p, idx_t max_define_p, idx_t max_repeat_p)
    : ColumnReader(reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p) {
}

unique_ptr<BaseStatistics> RowNumberColumnReader::Stats(idx_t row_group_idx_p, const vector<ColumnChunk> &columns) {
	auto stats = NumericStats::CreateUnknown(type);
	auto &row_groups = reader.GetFileMetadata()->row_groups;
	D_ASSERT(row_group_idx_p < row_groups.size());
	idx_t row_group_offset_min = 0;
	for (idx_t i = 0; i < row_group_idx_p; i++) {
		row_group_offset_min += row_groups[i].num_rows;
	}

	NumericStats::SetMin(stats, Value::BIGINT(row_group_offset_min));
	NumericStats::SetMax(stats, Value::BIGINT(row_group_offset_min + row_groups[row_group_idx_p].num_rows));
	stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
	return stats.ToUnique();
}

void RowNumberColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                           TProtocol &protocol_p) {
	row_group_offset = 0;
	auto &row_groups = reader.GetFileMetadata()->row_groups;
	for (idx_t i = 0; i < row_group_idx_p; i++) {
		row_group_offset += row_groups[i].num_rows;
	}
}

idx_t RowNumberColumnReader::Read(uint64_t num_values, parquet_filter_t &filter, data_ptr_t define_out,
                                  data_ptr_t repeat_out, Vector &result) {

	auto data_ptr = FlatVector::GetData<int64_t>(result);
	for (idx_t i = 0; i < num_values; i++) {
		data_ptr[i] = row_group_offset++;
	}
	return num_values;
}

//===--------------------------------------------------------------------===//
// Cast Column Reader
//===--------------------------------------------------------------------===//
CastColumnReader::CastColumnReader(unique_ptr<ColumnReader> child_reader_p, LogicalType target_type_p)
    : ColumnReader(child_reader_p->Reader(), std::move(target_type_p), child_reader_p->Schema(),
                   child_reader_p->FileIdx(), child_reader_p->MaxDefine(), child_reader_p->MaxRepeat()),
      child_reader(std::move(child_reader_p)) {
	vector<LogicalType> intermediate_types {child_reader->Type()};
	intermediate_chunk.Initialize(reader.allocator, intermediate_types);
}

unique_ptr<BaseStatistics> CastColumnReader::Stats(idx_t row_group_idx_p, const vector<ColumnChunk> &columns) {
	// casting stats is not supported (yet)
	return nullptr;
}

void CastColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                      TProtocol &protocol_p) {
	child_reader->InitializeRead(row_group_idx_p, columns, protocol_p);
}

idx_t CastColumnReader::Read(uint64_t num_values, parquet_filter_t &filter, data_ptr_t define_out,
                             data_ptr_t repeat_out, Vector &result) {
	intermediate_chunk.Reset();
	auto &intermediate_vector = intermediate_chunk.data[0];

	auto amount = child_reader->Read(num_values, filter, define_out, repeat_out, intermediate_vector);
	if (!filter.all()) {
		// work-around for filters: set all values that are filtered to NULL to prevent the cast from failing on
		// uninitialized data
		intermediate_vector.Flatten(amount);
		auto &validity = FlatVector::Validity(intermediate_vector);
		for (idx_t i = 0; i < amount; i++) {
			if (!filter[i]) {
				validity.SetInvalid(i);
			}
		}
	}
	VectorOperations::DefaultCast(intermediate_vector, result, amount);
	return amount;
}

void CastColumnReader::Skip(idx_t num_values) {
	child_reader->Skip(num_values);
}

idx_t CastColumnReader::GroupRowsAvailable() {
	return child_reader->GroupRowsAvailable();
}

//===--------------------------------------------------------------------===//
// Struct Column Reader
//===--------------------------------------------------------------------===//
StructColumnReader::StructColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p,
                                       idx_t schema_idx_p, idx_t max_define_p, idx_t max_repeat_p,
                                       vector<unique_ptr<ColumnReader>> child_readers_p)
    : ColumnReader(reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p),
      child_readers(std::move(child_readers_p)) {
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
}

ColumnReader *StructColumnReader::GetChildReader(idx_t child_idx) {
	D_ASSERT(child_idx < child_readers.size());
	return child_readers[child_idx].get();
}

void StructColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                        TProtocol &protocol_p) {
	for (auto &child : child_readers) {
		child->InitializeRead(row_group_idx_p, columns, protocol_p);
	}
}

idx_t StructColumnReader::Read(uint64_t num_values, parquet_filter_t &filter, data_ptr_t define_out,
                               data_ptr_t repeat_out, Vector &result) {
	auto &struct_entries = StructVector::GetEntries(result);
	D_ASSERT(StructType::GetChildTypes(Type()).size() == struct_entries.size());

	if (pending_skips > 0) {
		ApplyPendingSkips(pending_skips);
	}

	idx_t read_count = num_values;
	for (idx_t i = 0; i < struct_entries.size(); i++) {
		auto child_num_values = child_readers[i]->Read(num_values, filter, define_out, repeat_out, *struct_entries[i]);
		if (i == 0) {
			read_count = child_num_values;
		} else if (read_count != child_num_values) {
			throw std::runtime_error("Struct child row count mismatch");
		}
	}
	// set the validity mask for this level
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < read_count; i++) {
		if (define_out[i] < max_define) {
			validity.SetInvalid(i);
		}
	}

	return read_count;
}

void StructColumnReader::Skip(idx_t num_values) {
	for (auto &child_reader : child_readers) {
		child_reader->Skip(num_values);
	}
}

void StructColumnReader::RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) {
	for (auto &child : child_readers) {
		child->RegisterPrefetch(transport, allow_merge);
	}
}

uint64_t StructColumnReader::TotalCompressedSize() {
	uint64_t size = 0;
	for (auto &child : child_readers) {
		size += child->TotalCompressedSize();
	}
	return size;
}

static bool TypeHasExactRowCount(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return false;
	case LogicalTypeId::STRUCT:
		for (auto &kv : StructType::GetChildTypes(type)) {
			if (TypeHasExactRowCount(kv.second)) {
				return true;
			}
		}
		return false;
	default:
		return true;
	}
}

idx_t StructColumnReader::GroupRowsAvailable() {
	for (idx_t i = 0; i < child_readers.size(); i++) {
		if (TypeHasExactRowCount(child_readers[i]->Type())) {
			return child_readers[i]->GroupRowsAvailable();
		}
	}
	return child_readers[0]->GroupRowsAvailable();
}

//===--------------------------------------------------------------------===//
// Decimal Column Reader
//===--------------------------------------------------------------------===//
template <class DUCKDB_PHYSICAL_TYPE, bool FIXED_LENGTH>
struct DecimalParquetValueConversion {
	static DUCKDB_PHYSICAL_TYPE DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
		auto dict_ptr = reinterpret_cast<DUCKDB_PHYSICAL_TYPE *>(dict.ptr);
		return dict_ptr[offset];
	}

	static DUCKDB_PHYSICAL_TYPE PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		idx_t byte_len;
		if (FIXED_LENGTH) {
			byte_len = (idx_t)reader.Schema().type_length; /* sure, type length needs to be a signed int */
		} else {
			byte_len = plain_data.read<uint32_t>();
		}
		plain_data.available(byte_len);
		auto res =
		    ParquetDecimalUtils::ReadDecimalValue<DUCKDB_PHYSICAL_TYPE>(const_data_ptr_cast(plain_data.ptr), byte_len);

		plain_data.inc(byte_len);
		return res;
	}

	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		uint32_t decimal_len = FIXED_LENGTH ? reader.Schema().type_length : plain_data.read<uint32_t>();
		plain_data.inc(decimal_len);
	}
};

template <class DUCKDB_PHYSICAL_TYPE, bool FIXED_LENGTH>
class DecimalColumnReader
    : public TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE,
                                   DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE, FIXED_LENGTH>> {
	using BaseType =
	    TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE, DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE, FIXED_LENGTH>>;

public:
	DecimalColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, // NOLINT
	                    idx_t file_idx_p, idx_t max_define_p, idx_t max_repeat_p)
	    : TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE,
	                            DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE, FIXED_LENGTH>>(
	          reader, std::move(type_p), schema_p, file_idx_p, max_define_p, max_repeat_p) {};

protected:
	void Dictionary(shared_ptr<ResizeableBuffer> dictionary_data, idx_t num_entries) { // NOLINT
		BaseType::AllocateDict(num_entries * sizeof(DUCKDB_PHYSICAL_TYPE));
		auto dict_ptr = (DUCKDB_PHYSICAL_TYPE *)this->dict->ptr;
		for (idx_t i = 0; i < num_entries; i++) {
			dict_ptr[i] =
			    DecimalParquetValueConversion<DUCKDB_PHYSICAL_TYPE, FIXED_LENGTH>::PlainRead(*dictionary_data, *this);
		}
	}
};

template <bool FIXED_LENGTH>
static unique_ptr<ColumnReader> CreateDecimalReaderInternal(ParquetReader &reader, const LogicalType &type_p,
                                                            const SchemaElement &schema_p, idx_t file_idx_p,
                                                            idx_t max_define, idx_t max_repeat) {
	switch (type_p.InternalType()) {
	case PhysicalType::INT16:
		return make_uniq<DecimalColumnReader<int16_t, FIXED_LENGTH>>(reader, type_p, schema_p, file_idx_p, max_define,
		                                                             max_repeat);
	case PhysicalType::INT32:
		return make_uniq<DecimalColumnReader<int32_t, FIXED_LENGTH>>(reader, type_p, schema_p, file_idx_p, max_define,
		                                                             max_repeat);
	case PhysicalType::INT64:
		return make_uniq<DecimalColumnReader<int64_t, FIXED_LENGTH>>(reader, type_p, schema_p, file_idx_p, max_define,
		                                                             max_repeat);
	case PhysicalType::INT128:
		return make_uniq<DecimalColumnReader<hugeint_t, FIXED_LENGTH>>(reader, type_p, schema_p, file_idx_p, max_define,
		                                                               max_repeat);
	default:
		throw InternalException("Unrecognized type for Decimal");
	}
}

unique_ptr<ColumnReader> ParquetDecimalUtils::CreateReader(ParquetReader &reader, const LogicalType &type_p,
                                                           const SchemaElement &schema_p, idx_t file_idx_p,
                                                           idx_t max_define, idx_t max_repeat) {
	if (schema_p.__isset.type_length) {
		return CreateDecimalReaderInternal<true>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	} else {
		return CreateDecimalReaderInternal<false>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	}
}

//===--------------------------------------------------------------------===//
// UUID Column Reader
//===--------------------------------------------------------------------===//
struct UUIDValueConversion {
	static hugeint_t DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
		auto dict_ptr = reinterpret_cast<hugeint_t *>(dict.ptr);
		return dict_ptr[offset];
	}

	static hugeint_t ReadParquetUUID(const_data_ptr_t input) {
		hugeint_t result;
		result.lower = 0;
		uint64_t unsigned_upper = 0;
		for (idx_t i = 0; i < sizeof(uint64_t); i++) {
			unsigned_upper <<= 8;
			unsigned_upper += input[i];
		}
		for (idx_t i = sizeof(uint64_t); i < sizeof(hugeint_t); i++) {
			result.lower <<= 8;
			result.lower += input[i];
		}
		result.upper = unsigned_upper;
		result.upper ^= (int64_t(1) << 63);
		return result;
	}

	static hugeint_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		idx_t byte_len = sizeof(hugeint_t);
		plain_data.available(byte_len);
		auto res = ReadParquetUUID(const_data_ptr_cast(plain_data.ptr));

		plain_data.inc(byte_len);
		return res;
	}

	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.inc(sizeof(hugeint_t));
	}
};

class UUIDColumnReader : public TemplatedColumnReader<hugeint_t, UUIDValueConversion> {

public:
	UUIDColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t file_idx_p,
	                 idx_t max_define_p, idx_t max_repeat_p)
	    : TemplatedColumnReader<hugeint_t, UUIDValueConversion>(reader, std::move(type_p), schema_p, file_idx_p,
	                                                            max_define_p, max_repeat_p) {};

protected:
	void Dictionary(shared_ptr<ResizeableBuffer> dictionary_data, idx_t num_entries) { // NOLINT
		AllocateDict(num_entries * sizeof(hugeint_t));
		auto dict_ptr = reinterpret_cast<hugeint_t *>(this->dict->ptr);
		for (idx_t i = 0; i < num_entries; i++) {
			dict_ptr[i] = UUIDValueConversion::PlainRead(*dictionary_data, *this);
		}
	}
};

//===--------------------------------------------------------------------===//
// Interval Column Reader
//===--------------------------------------------------------------------===//
struct IntervalValueConversion {
	static constexpr const idx_t PARQUET_INTERVAL_SIZE = 12;

	static interval_t DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
		auto dict_ptr = reinterpret_cast<interval_t *>(dict.ptr);
		return dict_ptr[offset];
	}

	static interval_t ReadParquetInterval(const_data_ptr_t input) {
		interval_t result;
		result.months = Load<uint32_t>(input);
		result.days = Load<uint32_t>(input + sizeof(uint32_t));
		result.micros = int64_t(Load<uint32_t>(input + sizeof(uint32_t) * 2)) * 1000;
		return result;
	}

	static interval_t PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
		idx_t byte_len = PARQUET_INTERVAL_SIZE;
		plain_data.available(byte_len);
		auto res = ReadParquetInterval(const_data_ptr_cast(plain_data.ptr));

		plain_data.inc(byte_len);
		return res;
	}

	static void PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
		plain_data.inc(PARQUET_INTERVAL_SIZE);
	}
};

class IntervalColumnReader : public TemplatedColumnReader<interval_t, IntervalValueConversion> {

public:
	IntervalColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t file_idx_p,
	                     idx_t max_define_p, idx_t max_repeat_p)
	    : TemplatedColumnReader<interval_t, IntervalValueConversion>(reader, std::move(type_p), schema_p, file_idx_p,
	                                                                 max_define_p, max_repeat_p) {};

protected:
	void Dictionary(shared_ptr<ResizeableBuffer> dictionary_data, idx_t num_entries) override { // NOLINT
		AllocateDict(num_entries * sizeof(interval_t));
		auto dict_ptr = reinterpret_cast<interval_t *>(this->dict->ptr);
		for (idx_t i = 0; i < num_entries; i++) {
			dict_ptr[i] = IntervalValueConversion::PlainRead(*dictionary_data, *this);
		}
	}
};

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
		return make_uniq<TemplatedColumnReader<double, TemplatedParquetValueConversion<double>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
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
	case LogicalTypeId::TIME_TZ:
		if (schema_p.__isset.logicalType && schema_p.logicalType.__isset.TIME) {
			if (schema_p.logicalType.TIME.unit.__isset.MICROS) {
				return make_uniq<CallbackColumnReader<int64_t, dtime_tz_t, ParquetIntToTimeTZ>>(
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
	default:
		break;
	}
	throw NotImplementedException(type_p.ToString());
}

} // namespace duckdb
