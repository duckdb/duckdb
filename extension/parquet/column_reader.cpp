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

#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/bit.hpp"

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

ColumnReader::ColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema_p)
    : column_schema(schema_p), reader(reader), page_rows_available(0), dictionary_decoder(*this),
      delta_binary_packed_decoder(*this), rle_decoder(*this), delta_length_byte_array_decoder(*this),
      delta_byte_array_decoder(*this), byte_stream_split_decoder(*this) {
}

ColumnReader::~ColumnReader() {
}

Allocator &ColumnReader::GetAllocator() {
	return reader.allocator;
}

ParquetReader &ColumnReader::Reader() {
	return reader;
}

void ColumnReader::RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) {
	if (chunk) {
		uint64_t size = chunk->meta_data.total_compressed_size;
		transport.RegisterPrefetch(FileOffset(), size, allow_merge);
	}
}

unique_ptr<BaseStatistics> ColumnReader::Stats(idx_t row_group_idx_p, const vector<ColumnChunk> &columns) {
	return Schema().Stats(reader, row_group_idx_p, columns);
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

void ColumnReader::PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values) {
	throw NotImplementedException("PlainSkip not implemented");
}

void ColumnReader::Plain(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values, // NOLINT
                         idx_t result_offset, Vector &result) {
	throw NotImplementedException("Plain not implemented");
}

void ColumnReader::Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
                         idx_t result_offset, Vector &result) {
	Plain(*plain_data, defines, num_values, result_offset, result);
}

void ColumnReader::PlainSelect(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
                               Vector &result, const SelectionVector &sel, idx_t count) {
	throw NotImplementedException("PlainSelect not implemented");
}

void ColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns, TProtocol &protocol_p) {
	D_ASSERT(ColumnIndex() < columns.size());
	chunk = &columns[ColumnIndex()];
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

bool ColumnReader::PageIsFilteredOut(PageHeader &page_hdr) {
	if (!dictionary_decoder.HasFilteredOutAllValues()) {
		return false;
	}
	if (page_hdr.type != PageType::DATA_PAGE && page_hdr.type != PageType::DATA_PAGE_V2) {
		// we can only filter out data pages
		return false;
	}
	bool is_v1 = page_hdr.type == PageType::DATA_PAGE;
	auto &v1_header = page_hdr.data_page_header;
	auto &v2_header = page_hdr.data_page_header_v2;
	auto page_encoding = is_v1 ? v1_header.encoding : v2_header.encoding;
	if (page_encoding != Encoding::PLAIN_DICTIONARY && page_encoding != Encoding::RLE_DICTIONARY) {
		// not a dictionary page
		return false;
	}
	// the page has been filtered out!
	// skip forward
	auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol->getTransport());
	trans.Skip(page_hdr.compressed_page_size);

	page_rows_available = is_v1 ? v1_header.num_values : v2_header.num_values;
	encoding = ColumnEncoding::DICTIONARY;
	page_is_filtered_out = true;
	return true;
}

void ColumnReader::PrepareRead(optional_ptr<const TableFilter> filter) {
	encoding = ColumnEncoding::INVALID;
	defined_decoder.reset();
	page_is_filtered_out = false;
	block.reset();
	PageHeader page_hdr;
	reader.Read(page_hdr, *protocol);
	// some basic sanity check
	if (page_hdr.compressed_page_size < 0 || page_hdr.uncompressed_page_size < 0) {
		throw std::runtime_error("Page sizes can't be < 0");
	}

	if (PageIsFilteredOut(page_hdr)) {
		// this page has been filtered out so we don't need to read it
		return;
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
		dictionary_decoder.InitializeDictionary(dictionary_size, filter, HasDefines());
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
		repeated_decoder = make_uniq<RleBpDecoder>(block->ptr, rep_length, RleBpDecoder::ComputeBitWidth(MaxRepeat()));
		block->inc(rep_length);
	} else if (is_v2 && v2_header.repetition_levels_byte_length > 0) {
		block->inc(v2_header.repetition_levels_byte_length);
	}

	if (HasDefines()) {
		uint32_t def_length = is_v1 ? block->read<uint32_t>() : v2_header.definition_levels_byte_length;
		block->available(def_length);
		defined_decoder = make_uniq<RleBpDecoder>(block->ptr, def_length, RleBpDecoder::ComputeBitWidth(MaxDefine()));
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
		delta_length_byte_array_decoder.InitializePage();
		break;
	}
	case Encoding::DELTA_BYTE_ARRAY: {
		encoding = ColumnEncoding::DELTA_BYTE_ARRAY;
		delta_byte_array_decoder.InitializePage();
		break;
	}
	case Encoding::BYTE_STREAM_SPLIT: {
		encoding = ColumnEncoding::BYTE_STREAM_SPLIT;
		byte_stream_split_decoder.InitializePage();
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

void ColumnReader::BeginRead(data_ptr_t define_out, data_ptr_t repeat_out) {
	// we need to reset the location because multiple column readers share the same protocol
	auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol->getTransport());
	trans.SetLocation(chunk_read_offset);

	// Perform any skips that were not applied yet.
	if (define_out && repeat_out) {
		ApplyPendingSkips(define_out, repeat_out);
	}
}

idx_t ColumnReader::ReadPageHeaders(idx_t max_read, optional_ptr<const TableFilter> filter) {
	while (page_rows_available == 0) {
		PrepareRead(filter);
	}
	return MinValue<idx_t>(MinValue<idx_t>(max_read, page_rows_available), STANDARD_VECTOR_SIZE);
}

void ColumnReader::PrepareRead(idx_t read_now, data_ptr_t define_out, data_ptr_t repeat_out, idx_t result_offset) {
	D_ASSERT(block);

	D_ASSERT(read_now + result_offset <= STANDARD_VECTOR_SIZE);
	D_ASSERT(!page_is_filtered_out);

	if (HasRepeats()) {
		D_ASSERT(repeated_decoder);
		repeated_decoder->GetBatch<uint8_t>(repeat_out + result_offset, read_now);
	}

	if (HasDefines()) {
		D_ASSERT(defined_decoder);
		defined_decoder->GetBatch<uint8_t>(define_out + result_offset, read_now);
	}
}

void ColumnReader::ReadData(idx_t read_now, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result,
                            idx_t result_offset) {
	// flatten the result vector if required
	if (result_offset != 0 && result.GetVectorType() != VectorType::FLAT_VECTOR) {
		result.Flatten(result_offset);
		result.Resize(result_offset, STANDARD_VECTOR_SIZE);
	}
	if (page_is_filtered_out) {
		// page is filtered out - emit NULL for any rows
		auto &validity = FlatVector::Validity(result);
		for (idx_t i = 0; i < read_now; i++) {
			validity.SetInvalid(result_offset + i);
		}
		page_rows_available -= read_now;
		return;
	}
	// read the defines/repeats
	PrepareRead(read_now, define_out, repeat_out, result_offset);
	// read the data according to the encoder
	auto define_ptr = HasDefines() ? static_cast<uint8_t *>(define_out) : nullptr;
	switch (encoding) {
	case ColumnEncoding::DICTIONARY:
		dictionary_decoder.Read(define_ptr, read_now, result, result_offset);
		break;
	case ColumnEncoding::DELTA_BINARY_PACKED:
		delta_binary_packed_decoder.Read(define_ptr, read_now, result, result_offset);
		break;
	case ColumnEncoding::RLE:
		rle_decoder.Read(define_ptr, read_now, result, result_offset);
		break;
	case ColumnEncoding::DELTA_LENGTH_BYTE_ARRAY:
		delta_length_byte_array_decoder.Read(block, define_ptr, read_now, result, result_offset);
		break;
	case ColumnEncoding::DELTA_BYTE_ARRAY:
		delta_byte_array_decoder.Read(define_ptr, read_now, result, result_offset);
		break;
	case ColumnEncoding::BYTE_STREAM_SPLIT:
		byte_stream_split_decoder.Read(define_ptr, read_now, result, result_offset);
		break;
	default:
		Plain(block, define_out, read_now, result_offset, result);
		break;
	}
	page_rows_available -= read_now;
}

void ColumnReader::FinishRead(idx_t read_count) {
	auto &trans = reinterpret_cast<ThriftFileTransport &>(*protocol->getTransport());
	chunk_read_offset = trans.GetLocation();

	group_rows_available -= read_count;
}

idx_t ColumnReader::ReadInternal(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) {
	idx_t result_offset = 0;
	auto to_read = num_values;
	D_ASSERT(to_read <= STANDARD_VECTOR_SIZE);

	while (to_read > 0) {
		auto read_now = ReadPageHeaders(to_read);

		ReadData(read_now, define_out, repeat_out, result, result_offset);

		result_offset += read_now;
		to_read -= read_now;
	}
	FinishRead(num_values);

	return num_values;
}

idx_t ColumnReader::Read(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result) {
	BeginRead(define_out, repeat_out);
	return ReadInternal(num_values, define_out, repeat_out, result);
}

void ColumnReader::Select(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result_out,
                          const SelectionVector &sel, idx_t approved_tuple_count) {
	if (SupportsDirectSelect() && approved_tuple_count < num_values) {
		DirectSelect(num_values, define_out, repeat_out, result_out, sel, approved_tuple_count);
		return;
	}
	Read(num_values, define_out, repeat_out, result_out);
}

void ColumnReader::DirectSelect(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result,
                                const SelectionVector &sel, idx_t approved_tuple_count) {
	auto to_read = num_values;

	// prepare the first read if we haven't yet
	BeginRead(define_out, repeat_out);
	auto read_now = ReadPageHeaders(num_values);

	// we can only push the filter into the decoder if we are reading the ENTIRE vector in one go
	if (read_now == to_read && encoding == ColumnEncoding::PLAIN) {
		PrepareRead(read_now, define_out, repeat_out, 0);

		PlainSelect(block, define_out, read_now, result, sel, approved_tuple_count);

		page_rows_available -= read_now;
		FinishRead(num_values);
		return;
	}
	// fallback to regular read + filter
	ReadInternal(num_values, define_out, repeat_out, result);
}

void ColumnReader::Filter(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result,
                          const TableFilter &filter, SelectionVector &sel, idx_t &approved_tuple_count,
                          bool is_first_filter) {
	if (SupportsDirectFilter() && is_first_filter) {
		DirectFilter(num_values, define_out, repeat_out, result, filter, sel, approved_tuple_count);
		return;
	}
	Select(num_values, define_out, repeat_out, result, sel, approved_tuple_count);
	ApplyFilter(result, filter, num_values, sel, approved_tuple_count);
}

void ColumnReader::DirectFilter(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result,
                                const TableFilter &filter, SelectionVector &sel, idx_t &approved_tuple_count) {
	auto to_read = num_values;

	// prepare the first read if we haven't yet
	BeginRead(define_out, repeat_out);
	auto read_now = ReadPageHeaders(num_values, &filter);

	// we can only push the filter into the decoder if we are reading the ENTIRE vector in one go
	if (encoding == ColumnEncoding::DICTIONARY && read_now == to_read && dictionary_decoder.HasFilter()) {
		if (page_is_filtered_out) {
			// the page has been filtered out entirely - skip
			approved_tuple_count = 0;
		} else {
			// Push filter into dictionary directly
			// read the defines/repeats
			PrepareRead(read_now, define_out, repeat_out, 0);
			auto define_ptr = HasDefines() ? static_cast<uint8_t *>(define_out) : nullptr;
			dictionary_decoder.Filter(define_ptr, read_now, result, filter, sel, approved_tuple_count);
		}
		page_rows_available -= read_now;
		FinishRead(num_values);
		return;
	}
	// fallback to regular read + filter
	ReadInternal(num_values, define_out, repeat_out, result);
	ApplyFilter(result, filter, num_values, sel, approved_tuple_count);
}

void ColumnReader::ApplyFilter(Vector &v, const TableFilter &filter, idx_t scan_count, SelectionVector &sel,
                               idx_t &approved_tuple_count) {
	UnifiedVectorFormat vdata;
	v.ToUnifiedFormat(scan_count, vdata);
	ColumnSegment::FilterSelection(sel, v, vdata, filter, scan_count, approved_tuple_count);
}

void ColumnReader::Skip(idx_t num_values) {
	pending_skips += num_values;
}

void ColumnReader::ApplyPendingSkips(data_ptr_t define_out, data_ptr_t repeat_out) {
	if (pending_skips == 0) {
		return;
	}
	idx_t num_values = pending_skips;
	pending_skips = 0;

	auto to_skip = num_values;
	// start reading but do not apply skips (we are skipping now)
	BeginRead(nullptr, nullptr);

	while (to_skip > 0) {
		auto skip_now = ReadPageHeaders(to_skip);
		PrepareRead(skip_now, define_out, repeat_out, 0);

		auto define_ptr = HasDefines() ? static_cast<uint8_t *>(define_out) : nullptr;
		switch (encoding) {
		case ColumnEncoding::DICTIONARY:
			dictionary_decoder.Skip(define_ptr, skip_now);
			break;
		case ColumnEncoding::DELTA_BINARY_PACKED:
			delta_binary_packed_decoder.Skip(define_ptr, skip_now);
			break;
		case ColumnEncoding::RLE:
			rle_decoder.Skip(define_ptr, skip_now);
			break;
		case ColumnEncoding::DELTA_LENGTH_BYTE_ARRAY:
			delta_length_byte_array_decoder.Skip(define_ptr, skip_now);
			break;
		case ColumnEncoding::DELTA_BYTE_ARRAY:
			delta_byte_array_decoder.Skip(define_ptr, skip_now);
			break;
		case ColumnEncoding::BYTE_STREAM_SPLIT:
			byte_stream_split_decoder.Skip(define_ptr, skip_now);
			break;
		default:
			PlainSkip(*block, define_out, skip_now);
			break;
		}
		page_rows_available -= skip_now;
		to_skip -= skip_now;
	}
	FinishRead(num_values);
}

//===--------------------------------------------------------------------===//
// Create Column Reader
//===--------------------------------------------------------------------===//
template <class T>
unique_ptr<ColumnReader> CreateDecimalReader(ParquetReader &reader, const ParquetColumnSchema &schema) {
	switch (schema.type.InternalType()) {
	case PhysicalType::INT16:
		return make_uniq<TemplatedColumnReader<int16_t, TemplatedParquetValueConversion<T>>>(reader, schema);
	case PhysicalType::INT32:
		return make_uniq<TemplatedColumnReader<int32_t, TemplatedParquetValueConversion<T>>>(reader, schema);
	case PhysicalType::INT64:
		return make_uniq<TemplatedColumnReader<int64_t, TemplatedParquetValueConversion<T>>>(reader, schema);
	default:
		throw NotImplementedException("Unimplemented internal type for CreateDecimalReader");
	}
}

unique_ptr<ColumnReader> ColumnReader::CreateReader(ParquetReader &reader, const ParquetColumnSchema &schema) {
	switch (schema.type.id()) {
	case LogicalTypeId::BOOLEAN:
		return make_uniq<BooleanColumnReader>(reader, schema);
	case LogicalTypeId::UTINYINT:
		return make_uniq<TemplatedColumnReader<uint8_t, TemplatedParquetValueConversion<uint32_t>>>(reader, schema);
	case LogicalTypeId::USMALLINT:
		return make_uniq<TemplatedColumnReader<uint16_t, TemplatedParquetValueConversion<uint32_t>>>(reader, schema);
	case LogicalTypeId::UINTEGER:
		return make_uniq<TemplatedColumnReader<uint32_t, TemplatedParquetValueConversion<uint32_t>>>(reader, schema);
	case LogicalTypeId::UBIGINT:
		return make_uniq<TemplatedColumnReader<uint64_t, TemplatedParquetValueConversion<uint64_t>>>(reader, schema);
	case LogicalTypeId::TINYINT:
		return make_uniq<TemplatedColumnReader<int8_t, TemplatedParquetValueConversion<int32_t>>>(reader, schema);
	case LogicalTypeId::SMALLINT:
		return make_uniq<TemplatedColumnReader<int16_t, TemplatedParquetValueConversion<int32_t>>>(reader, schema);
	case LogicalTypeId::INTEGER:
		return make_uniq<TemplatedColumnReader<int32_t, TemplatedParquetValueConversion<int32_t>>>(reader, schema);
	case LogicalTypeId::BIGINT:
		return make_uniq<TemplatedColumnReader<int64_t, TemplatedParquetValueConversion<int64_t>>>(reader, schema);
	case LogicalTypeId::FLOAT:
		return make_uniq<TemplatedColumnReader<float, TemplatedParquetValueConversion<float>>>(reader, schema);
	case LogicalTypeId::DOUBLE:
		if (schema.type_info == ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY) {
			return ParquetDecimalUtils::CreateReader(reader, schema);
		}
		return make_uniq<TemplatedColumnReader<double, TemplatedParquetValueConversion<double>>>(reader, schema);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		switch (schema.type_info) {
		case ParquetExtraTypeInfo::IMPALA_TIMESTAMP:
			return make_uniq<CallbackColumnReader<Int96, timestamp_t, ImpalaTimestampToTimestamp>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_MS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampMsToTimestamp>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_MICROS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampMicrosToTimestamp>>(reader,
			                                                                                                schema);
		case ParquetExtraTypeInfo::UNIT_NS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampNsToTimestamp>>(reader, schema);
		default:
			throw InternalException("TIMESTAMP requires type info");
		}
	case LogicalTypeId::TIMESTAMP_NS:
		switch (schema.type_info) {
		case ParquetExtraTypeInfo::IMPALA_TIMESTAMP:
			return make_uniq<CallbackColumnReader<Int96, timestamp_ns_t, ImpalaTimestampToTimestampNS>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_MS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_ns_t, ParquetTimestampMsToTimestampNs>>(reader,
			                                                                                                 schema);
		case ParquetExtraTypeInfo::UNIT_MICROS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_ns_t, ParquetTimestampUsToTimestampNs>>(reader,
			                                                                                                 schema);
		case ParquetExtraTypeInfo::UNIT_NS:
			return make_uniq<CallbackColumnReader<int64_t, timestamp_ns_t, ParquetTimestampNsToTimestampNs>>(reader,
			                                                                                                 schema);
		default:
			throw InternalException("TIMESTAMP_NS requires type info");
		}
	case LogicalTypeId::DATE:
		return make_uniq<CallbackColumnReader<int32_t, date_t, ParquetIntToDate>>(reader, schema);
	case LogicalTypeId::TIME:
		switch (schema.type_info) {
		case ParquetExtraTypeInfo::UNIT_MS:
			return make_uniq<CallbackColumnReader<int32_t, dtime_t, ParquetIntToTimeMs>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_MICROS:
			return make_uniq<CallbackColumnReader<int64_t, dtime_t, ParquetIntToTime>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_NS:
			return make_uniq<CallbackColumnReader<int64_t, dtime_t, ParquetIntToTimeNs>>(reader, schema);
		default:
			throw InternalException("TIME requires type info");
		}
	case LogicalTypeId::TIME_TZ:
		switch (schema.type_info) {
		case ParquetExtraTypeInfo::UNIT_MS:
			return make_uniq<CallbackColumnReader<int32_t, dtime_tz_t, ParquetIntToTimeMsTZ>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_MICROS:
			return make_uniq<CallbackColumnReader<int64_t, dtime_tz_t, ParquetIntToTimeTZ>>(reader, schema);
		case ParquetExtraTypeInfo::UNIT_NS:
			return make_uniq<CallbackColumnReader<int64_t, dtime_tz_t, ParquetIntToTimeNsTZ>>(reader, schema);
		default:
			throw InternalException("TIME_TZ requires type info");
		}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return make_uniq<StringColumnReader>(reader, schema);
	case LogicalTypeId::DECIMAL:
		// we have to figure out what kind of int we need
		switch (schema.type_info) {
		case ParquetExtraTypeInfo::DECIMAL_INT32:
			return CreateDecimalReader<int32_t>(reader, schema);
		case ParquetExtraTypeInfo::DECIMAL_INT64:
			return CreateDecimalReader<int64_t>(reader, schema);
		case ParquetExtraTypeInfo::DECIMAL_BYTE_ARRAY:
			return ParquetDecimalUtils::CreateReader(reader, schema);
		default:
			throw NotImplementedException("Unrecognized Parquet type for Decimal");
		}
		break;
	case LogicalTypeId::UUID:
		return make_uniq<UUIDColumnReader>(reader, schema);
	case LogicalTypeId::INTERVAL:
		return make_uniq<IntervalColumnReader>(reader, schema);
	case LogicalTypeId::SQLNULL:
		return make_uniq<NullColumnReader>(reader, schema);
	default:
		break;
	}
	throw NotImplementedException(schema.type.ToString());
}

} // namespace duckdb
