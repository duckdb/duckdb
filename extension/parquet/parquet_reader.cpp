#include "parquet_reader.hpp"
#include "parquet_timestamp.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"

#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"
#include "snappy.h"
#include "miniz.hpp"

#include "utf8proc_wrapper.hpp"

#include <sstream>

namespace duckdb {

using namespace parquet;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace duckdb_miniz;

using parquet::format::CompressionCodec;
using parquet::format::Encoding;
using parquet::format::FieldRepetitionType;
using parquet::format::FileMetaData;
using parquet::format::PageHeader;
using parquet::format::PageType;
using parquet::format::RowGroup;
using parquet::format::Type;

// adapted from arrow parquet reader
class RleBpDecoder {
public:
	/// Create a decoder object. buffer/buffer_len is the decoded data.
	/// bit_width is the width of each value (before encoding).
	RleBpDecoder(const uint8_t *buffer, uint32_t buffer_len, uint32_t bit_width)
	    : buffer(buffer), bit_width_(bit_width), current_value_(0), repeat_count_(0), literal_count_(0) {

		if (bit_width >= 64) {
			throw runtime_error("Decode bit width too large");
		}
		byte_encoded_len = ((bit_width_ + 7) / 8);
		max_val = (1 << bit_width_) - 1;
	}

	/// Gets a batch of values.  Returns the number of decoded elements.
	template <typename T> void GetBatch(char *values_target_ptr, uint32_t batch_size) {
		auto values = (T *)values_target_ptr;
		uint32_t values_read = 0;

		while (values_read < batch_size) {
			if (repeat_count_ > 0) {
				int repeat_batch = std::min(batch_size - values_read, static_cast<uint32_t>(repeat_count_));
				std::fill(values + values_read, values + values_read + repeat_batch, static_cast<T>(current_value_));
				repeat_count_ -= repeat_batch;
				values_read += repeat_batch;
			} else if (literal_count_ > 0) {
				uint32_t literal_batch = std::min(batch_size - values_read, static_cast<uint32_t>(literal_count_));
				uint32_t actual_read = BitUnpack<T>(values + values_read, literal_batch);
				if (literal_batch != actual_read) {
					throw runtime_error("Did not find enough values");
				}
				literal_count_ -= literal_batch;
				values_read += literal_batch;
			} else {
				if (!NextCounts<T>()) {
					if (values_read != batch_size) {
						throw runtime_error("RLE decode did not find enough values");
					}
					return;
				}
			}
		}
		if (values_read != batch_size) {
			throw runtime_error("RLE decode did not find enough values");
		}
	}

private:
	const uint8_t *buffer;

	/// Number of bits needed to encode the value. Must be between 0 and 64.
	int bit_width_;
	uint64_t current_value_;
	uint32_t repeat_count_;
	uint32_t literal_count_;
	uint8_t byte_encoded_len;
	uint32_t max_val;

	int8_t bitpack_pos = 0;

	// this is slow but whatever, calls are rare
	static uint8_t VarintDecode(const uint8_t *source, uint32_t *result_out) {
		uint32_t result = 0;
		uint8_t shift = 0;
		uint8_t len = 0;
		while (true) {
			auto byte = *source++;
			len++;
			result |= (byte & 127) << shift;
			if ((byte & 128) == 0)
				break;
			shift += 7;
			if (shift > 32) {
				throw runtime_error("Varint-decoding found too large number");
			}
		}
		*result_out = result;
		return len;
	}

	/// Fills literal_count_ and repeat_count_ with next values. Returns false if there
	/// are no more.
	template <typename T> bool NextCounts() {
		// Read the next run's indicator int, it could be a literal or repeated run.
		// The int is encoded as a vlq-encoded value.
		uint32_t indicator_value;
		if (bitpack_pos != 0) {
			buffer++;
			bitpack_pos = 0;
		}
		buffer += VarintDecode(buffer, &indicator_value);

		// lsb indicates if it is a literal run or repeated run
		bool is_literal = indicator_value & 1;
		if (is_literal) {
			literal_count_ = (indicator_value >> 1) * 8;
		} else {
			repeat_count_ = indicator_value >> 1;
			// (ARROW-4018) this is not big-endian compatible, lol
			current_value_ = 0;
			for (auto i = 0; i < byte_encoded_len; i++) {
				current_value_ |= ((uint8_t)*buffer++) << (i * 8);
			}
			// sanity check
			if (repeat_count_ > 0 && current_value_ > max_val) {
				throw runtime_error("Payload value bigger than allowed. Corrupted file?");
			}
		}
		// TODO complain if we run out of buffer
		return true;
	}

	// somewhat optimized implementation that avoids non-alignment

	static const uint32_t BITPACK_MASKS[];
	static const uint8_t BITPACK_DLEN;

	template <typename T> uint32_t BitUnpack(T *dest, uint32_t count) {
		assert(bit_width_ < 32);

		// auto source = buffer;
		auto mask = BITPACK_MASKS[bit_width_];

		for (uint32_t i = 0; i < count; i++) {
			T val = (*buffer >> bitpack_pos) & mask;
			bitpack_pos += bit_width_;
			while (bitpack_pos > BITPACK_DLEN) {
				val |= (*++buffer << (BITPACK_DLEN - (bitpack_pos - bit_width_))) & mask;
				bitpack_pos -= BITPACK_DLEN;
			}
			dest[i] = val;
		}
		return count;
	}
};

const uint32_t RleBpDecoder::BITPACK_MASKS[] = {
    0,       1,       3,        7,        15,       31,        63,        127,       255,        511,       1023,
    2047,    4095,    8191,     16383,    32767,    65535,     131071,    262143,    524287,     1048575,   2097151,
    4194303, 8388607, 16777215, 33554431, 67108863, 134217727, 268435455, 536870911, 1073741823, 2147483647};

const uint8_t RleBpDecoder::BITPACK_DLEN = 8;

static TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;

template <class T> static void thrift_unpack(const uint8_t *buf, uint32_t *len, T *deserialized_msg) {
	shared_ptr<TMemoryBuffer> tmem_transport(new TMemoryBuffer(const_cast<uint8_t *>(buf), *len));
	shared_ptr<TProtocol> tproto = tproto_factory.getProtocol(tmem_transport);
	try {
		deserialized_msg->read(tproto.get());
	} catch (std::exception &e) {
		std::stringstream ss;
		ss << "Couldn't deserialize thrift: " << e.what() << "\n";
		throw std::runtime_error(ss.str());
	}
	uint32_t bytes_left = tmem_transport->available_read();
	*len = *len - bytes_left;
}

ParquetReader::ParquetReader(ClientContext &context, string file_name_, vector<LogicalType> expected_types)
    : file_name(move(file_name_)), context(context) {
	auto &fs = FileSystem::GetFileSystem(context);

	auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);

	ResizeableBuffer buf;
	buf.resize(4);
	memset(buf.ptr, '\0', 4);
	// check for magic bytes at start of file
	fs.Read(*handle, buf.ptr, 4);
	if (strncmp(buf.ptr, "PAR1", 4) != 0) {
		throw FormatException("Missing magic bytes in front of Parquet file");
	}

	// check for magic bytes at end of file
	auto file_size_signed = fs.GetFileSize(*handle);
	if (file_size_signed < 12) {
		throw FormatException("File too small to be a Parquet file");
	}
	auto file_size = (uint64_t)file_size_signed;
	fs.Read(*handle, buf.ptr, 4, file_size - 4);
	if (strncmp(buf.ptr, "PAR1", 4) != 0) {
		throw FormatException("No magic bytes found at end of file");
	}

	// read four-byte footer length from just before the end magic bytes
	fs.Read(*handle, buf.ptr, 4, file_size - 8);
	auto footer_len = *(uint32_t *)buf.ptr;
	if (footer_len <= 0) {
		throw FormatException("Footer length can't be 0");
	}
	if (file_size < 12 + footer_len) {
		throw FormatException("Footer length %d is too big for the file of size %d", footer_len, file_size);
	}

	// read footer into buffer and de-thrift
	buf.resize(footer_len);
	fs.Read(*handle, buf.ptr, footer_len, file_size - (footer_len + 8));

	thrift_unpack((const uint8_t *)buf.ptr, &footer_len, &file_meta_data);

	if (file_meta_data.__isset.encryption_algorithm) {
		throw FormatException("Encrypted Parquet files are not supported");
	}
	// check if we like this schema
	if (file_meta_data.schema.size() < 2) {
		throw FormatException("Need at least one column in the file");
	}
	if (file_meta_data.schema[0].num_children != (int32_t)(file_meta_data.schema.size() - 1)) {
		throw FormatException("Only flat tables are supported (no nesting)");
	}

	this->return_types = expected_types;
	bool has_expected_types = expected_types.size() > 0;

	// skip the first column its the root and otherwise useless
	for (uint64_t col_idx = 1; col_idx < file_meta_data.schema.size(); col_idx++) {
		auto &s_ele = file_meta_data.schema[col_idx];
		if (!s_ele.__isset.type || s_ele.num_children > 0) {
			throw FormatException("Only flat tables are supported (no nesting)");
		}
		// if this is REQUIRED, there are no defined levels in file
		// if field is REPEATED, no bueno
		if (s_ele.repetition_type == FieldRepetitionType::REPEATED) {
			throw FormatException("REPEATED fields are not supported");
		}

		LogicalType type;
		switch (s_ele.type) {
		case Type::BOOLEAN:
			type = LogicalType::BOOLEAN;
			break;
		case Type::INT32:
			type = LogicalType::INTEGER;
			break;
		case Type::INT64:
			type = LogicalType::BIGINT;
			break;
		case Type::INT96: // always a timestamp?
			type = LogicalType::TIMESTAMP;
			break;
		case Type::FLOAT:
			type = LogicalType::FLOAT;
			break;
		case Type::DOUBLE:
			type = LogicalType::DOUBLE;
			break;
			//			case parquet::format::Type::FIXED_LEN_BYTE_ARRAY: {
			// TODO some decimals yuck
		case Type::BYTE_ARRAY:
			type = LogicalType::VARCHAR;
			break;
		default:
			throw FormatException("Unsupported type");
		}
		if (has_expected_types) {
			if (return_types[col_idx - 1] != type) {
				throw FormatException("PARQUET file contains type %s, could not auto cast to type %s", type.ToString(),
				                      return_types[col_idx - 1].ToString());
			}
		} else {
			names.push_back(s_ele.name);
			return_types.push_back(type);
		}
	}
}

ParquetReader::~ParquetReader() {
}

ParquetReaderColumnData::~ParquetReaderColumnData() {
}

struct ValueIsValid {
	template<class T>
	static bool Operation(T value) {
		return true;
	}
};

template<> bool ValueIsValid::Operation(float value) {
	return Value::FloatIsValid(value);
}

template<> bool ValueIsValid::Operation(double value) {
	return Value::DoubleIsValid(value);
}

template <class T>
void ParquetReader::fill_from_dict(ParquetReaderColumnData &col_data, idx_t count, Vector &target,
                                   idx_t target_offset) {
	for (idx_t i = 0; i < count; i++) {
		if (!col_data.has_nulls || col_data.defined_buf.ptr[i]) {
			auto offset = col_data.offset_buf.read<uint32_t>();
			if (offset > col_data.dict_size) {
				throw runtime_error("Offset " + to_string(offset) + " greater than dictionary size " +
				                    to_string(col_data.dict_size) + " at " + to_string(i + target_offset) +
				                    ". Corrupt file?");
			}
			auto value = ((const T *)col_data.dict.ptr)[offset];
			if (ValueIsValid::Operation(value)) {
				((T *)FlatVector::GetData(target))[i + target_offset] = value;
			} else {
				FlatVector::SetNull(target, i + target_offset, true);
			}
		} else {
			FlatVector::SetNull(target, i + target_offset, true);
		}
	}
}

template <class T>
void ParquetReader::fill_from_plain(ParquetReaderColumnData &col_data, idx_t count, Vector &target,
                                    idx_t target_offset) {
	for (idx_t i = 0; i < count; i++) {
		if (!col_data.has_nulls || col_data.defined_buf.ptr[i]) {
			auto value = col_data.payload.read<T>();
			if (ValueIsValid::Operation(value)) {
				((T *)FlatVector::GetData(target))[i + target_offset] = value;
			} else {
				FlatVector::SetNull(target, i + target_offset, true);
			}
		} else {
			FlatVector::SetNull(target, i + target_offset, true);
		}
	}
}

RowGroup &ParquetReader::GetGroup(ParquetReaderScanState &state) {
	assert(state.current_group >= 0 && (idx_t)state.current_group < state.group_idx_list.size());
	assert(state.group_idx_list[state.current_group] >= 0 &&
	       state.group_idx_list[state.current_group] < file_meta_data.row_groups.size());
	return file_meta_data.row_groups[state.group_idx_list[state.current_group]];
}

bool ParquetReader::PreparePageBuffers(ParquetReaderScanState &state, idx_t col_idx) {
	auto &col_data = *state.column_data[col_idx];
	auto &chunk = GetGroup(state).columns[col_idx];

	// clean up a bit to avoid nasty surprises
	col_data.payload.ptr = nullptr;
	col_data.payload.len = 0;
	col_data.dict_decoder = nullptr;
	col_data.defined_decoder = nullptr;
	col_data.byte_pos = 0;

	auto page_header_len = col_data.buf.len;
	if (page_header_len < 1) {
		throw FormatException("Ran out of bytes to read header from. File corrupt?");
	}
	PageHeader page_hdr;
	thrift_unpack((const uint8_t *)col_data.buf.ptr + col_data.chunk_offset, (uint32_t *)&page_header_len, &page_hdr);

	// the payload starts behind the header, obvsl.
	col_data.buf.inc(page_header_len);

	col_data.payload.len = page_hdr.uncompressed_page_size;

	// handle compression, in the end we expect a pointer to uncompressed parquet data in payload_ptr
	switch (chunk.meta_data.codec) {
	case CompressionCodec::UNCOMPRESSED:
		col_data.payload.ptr = col_data.buf.ptr;
		break;

	case CompressionCodec::SNAPPY: {
		col_data.decompressed_buf.resize(page_hdr.uncompressed_page_size);
		auto res =
		    snappy::RawUncompress(col_data.buf.ptr, page_hdr.compressed_page_size, col_data.decompressed_buf.ptr);
		if (!res) {
			throw FormatException("Decompression failure");
		}
		col_data.payload.ptr = col_data.decompressed_buf.ptr;
		break;
	}
	case CompressionCodec::GZIP: {
		struct MiniZStream {
			~MiniZStream() {
				if (init) {
					mz_inflateEnd(&stream);
				}
			}

			mz_stream stream;
			bool init = false;
		} s;
		auto &stream = s.stream;
		memset(&stream, 0, sizeof(mz_stream));

		auto mz_ret = mz_inflateInit2(&stream, -MZ_DEFAULT_WINDOW_BITS);
		if (mz_ret != MZ_OK) {
			throw FormatException("Failed to initialize miniz");
		}
		s.init = true;

		col_data.buf.available(GZIP_HEADER_MINSIZE);
		auto gzip_hdr = (const unsigned char *)col_data.buf.ptr;

		if (gzip_hdr[0] != 0x1F || gzip_hdr[1] != 0x8B || gzip_hdr[2] != GZIP_COMPRESSION_DEFLATE ||
		    gzip_hdr[3] & GZIP_FLAG_UNSUPPORTED) {
			throw FormatException("Input is invalid/unsupported GZIP stream");
		}

		col_data.decompressed_buf.resize(page_hdr.uncompressed_page_size);

		stream.next_in = (const unsigned char *)col_data.buf.ptr + GZIP_HEADER_MINSIZE;
		stream.avail_in = page_hdr.compressed_page_size - GZIP_HEADER_MINSIZE;
		stream.next_out = (unsigned char *)col_data.decompressed_buf.ptr;
		stream.avail_out = page_hdr.uncompressed_page_size;

		mz_ret = mz_inflate(&stream, MZ_FINISH);
		if (mz_ret != MZ_OK && mz_ret != MZ_STREAM_END) {
			throw FormatException("Decompression failure: " + string(mz_error(mz_ret)));
		}

		col_data.payload.ptr = col_data.decompressed_buf.ptr;
		break;
	}
	default: {
		std::stringstream codec_name;
		codec_name << chunk.meta_data.codec;
		throw FormatException("Unsupported compression codec \"" + codec_name.str() + "\". Supported options are uncompressed, gzip or snappy");
	}
	}
	col_data.buf.inc(page_hdr.compressed_page_size);

	// handle page contents
	switch (page_hdr.type) {
	case PageType::DICTIONARY_PAGE: {
		// fill the dictionary vector

		if (page_hdr.__isset.data_page_header || !page_hdr.__isset.dictionary_page_header) {
			throw FormatException("Dictionary page header mismatch");
		}

		// make sure we like the encoding
		switch (page_hdr.dictionary_page_header.encoding) {
		case Encoding::PLAIN:
		case Encoding::PLAIN_DICTIONARY: // deprecated
			break;

		default:
			throw FormatException("Dictionary page has unsupported/invalid encoding");
		}

		col_data.dict_size = page_hdr.dictionary_page_header.num_values;
		auto dict_byte_size = col_data.dict_size * GetTypeIdSize(return_types[col_idx].InternalType());

		col_data.dict.resize(dict_byte_size);

		switch (return_types[col_idx].id()) {
		case LogicalTypeId::BOOLEAN:
		case LogicalTypeId::INTEGER:
		case LogicalTypeId::BIGINT:
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			col_data.payload.available(dict_byte_size);
			// TODO this copy could be avoided if we use different buffers for dicts
			col_data.payload.copy_to(col_data.dict.ptr, dict_byte_size);
			break;
		case LogicalTypeId::TIMESTAMP:
			col_data.payload.available(dict_byte_size);
			// immediately convert timestamps to duckdb format, potentially fewer conversions
			for (idx_t dict_index = 0; dict_index < col_data.dict_size; dict_index++) {
				auto impala_ts = Load<Int96>((data_ptr_t)(col_data.payload.ptr + dict_index * sizeof(Int96)));
				((timestamp_t *)col_data.dict.ptr)[dict_index] = impala_timestamp_to_timestamp_t(impala_ts);
			}

			break;
		case LogicalTypeId::VARCHAR: {
			// strings we directly fill a string heap that we can use for the vectors later
			col_data.string_collection = make_unique<ChunkCollection>();

			// we hand-roll a chunk collection to avoid copying strings
			auto append_chunk = make_unique<DataChunk>();
			vector<LogicalType> types = {LogicalType::VARCHAR};
			col_data.string_collection->types = types;
			append_chunk->Initialize(types);

			for (idx_t dict_index = 0; dict_index < col_data.dict_size; dict_index++) {
				uint32_t str_len = col_data.payload.read<uint32_t>();
				col_data.payload.available(str_len);

				if (append_chunk->size() == STANDARD_VECTOR_SIZE) {
					col_data.string_collection->count += append_chunk->size();
					col_data.string_collection->chunks.push_back(move(append_chunk));
					append_chunk = make_unique<DataChunk>();
					append_chunk->Initialize(types);
				}

				auto utf_type = Utf8Proc::Analyze(col_data.payload.ptr, str_len);
				if (utf_type == UnicodeType::INVALID) {
					throw FormatException("invalid string encoding");
				}
				FlatVector::GetData<string_t>(append_chunk->data[0])[append_chunk->size()] =
				    StringVector::AddString(append_chunk->data[0], col_data.payload.ptr, str_len);

				append_chunk->SetCardinality(append_chunk->size() + 1);
				col_data.payload.inc(str_len);
			}
			// FLUSH last chunk!
			if (append_chunk->size() > 0) {
				col_data.string_collection->count += append_chunk->size();
				col_data.string_collection->chunks.push_back(move(append_chunk));
			}
			col_data.string_collection->Verify();
		} break;
		default:
			throw FormatException(return_types[col_idx].ToString());
		}
		// important, move to next page which should be a data page
		return false;
	}
	case PageType::DATA_PAGE:
	case PageType::DATA_PAGE_V2: {

		col_data.page_value_count = page_hdr.type == PageType::DATA_PAGE ? page_hdr.data_page_header.num_values
		                                                                 : page_hdr.data_page_header_v2.num_values;
		col_data.page_encoding = page_hdr.type == PageType::DATA_PAGE ? page_hdr.data_page_header.encoding
		                                                              : page_hdr.data_page_header_v2.encoding;

		if (col_data.has_nulls) {
			// we have to first decode the define levels
			switch (page_hdr.data_page_header.definition_level_encoding) {
			case Encoding::RLE: {
				// read length of define payload, always
				uint32_t def_length = col_data.payload.read<uint32_t>();
				col_data.payload.available(def_length);
				col_data.defined_decoder = make_unique<RleBpDecoder>((const uint8_t *)col_data.payload.ptr, def_length, 1);
				col_data.payload.inc(def_length);
			} break;
			default:
				throw FormatException("Definition levels have unsupported/invalid encoding");
			}
		}

		switch (col_data.page_encoding) {
		case Encoding::RLE_DICTIONARY:
		case Encoding::PLAIN_DICTIONARY: {
			auto enc_length = col_data.payload.read<uint8_t>();
			col_data.dict_decoder =
			    make_unique<RleBpDecoder>((const uint8_t *)col_data.payload.ptr, col_data.payload.len, enc_length);
			break;
		}
		case Encoding::PLAIN:
			// nothing here, see below
			break;

		default:
			throw FormatException("Data page has unsupported/invalid encoding");
		}

		break;
	}
	default:
		break; // ignore INDEX page type and any other custom extensions
	}
	return true;
}

void ParquetReader::PrepareChunkBuffer(ParquetReaderScanState &state, idx_t col_idx) {
	auto &chunk = GetGroup(state).columns[col_idx];
	if (chunk.__isset.file_path) {
		throw FormatException("Only inlined data files are supported (no references)");
	}

	if (chunk.meta_data.path_in_schema.size() != 1) {
		throw FormatException("Only flat tables are supported (no nesting)");
	}

	// ugh. sometimes there is an extra offset for the dict. sometimes it's wrong.
	auto chunk_start = chunk.meta_data.data_page_offset;
	if (chunk.meta_data.__isset.dictionary_page_offset && chunk.meta_data.dictionary_page_offset >= 4) {
		// this assumes the data pages follow the dict pages directly.
		chunk_start = chunk.meta_data.dictionary_page_offset;
	}
	auto chunk_len = chunk.meta_data.total_compressed_size;

	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(file_name, FileFlags::FILE_FLAGS_READ);

	state.column_data[col_idx]->has_nulls =
	             file_meta_data.schema[col_idx + 1].repetition_type == FieldRepetitionType::OPTIONAL;

	// read entire chunk into RAM
	state.column_data[col_idx]->buf.resize(chunk_len);
	fs.Read(*handle, state.column_data[col_idx]->buf.ptr, chunk_len, chunk_start);
}

idx_t ParquetReader::NumRows() {
	return file_meta_data.num_rows;
}

idx_t ParquetReader::NumRowGroups() {
	return file_meta_data.row_groups.size();
}

void ParquetReader::Initialize(ParquetReaderScanState &state, vector<column_t> column_ids,
                               vector<idx_t> groups_to_read) {
	state.current_group = -1;
	state.finished = false;
	state.column_ids = move(column_ids);
	state.group_offset = 0;
	state.group_idx_list = move(groups_to_read);
	for (idx_t i = 0; i < return_types.size(); i++) {
		state.column_data.push_back(make_unique<ParquetReaderColumnData>());
	}
}

void ParquetReader::ReadChunk(ParquetReaderScanState &state, DataChunk &output) {
	if (state.finished) {
		return;
	}

	// see if we have to switch to the next row group in the parquet file
	if (state.current_group < 0 || (int64_t)state.group_offset >= GetGroup(state).num_rows) {
		state.current_group++;
		state.group_offset = 0;

		if ((idx_t)state.current_group == state.group_idx_list.size()) {
			state.finished = true;
			return;
		}

		for (idx_t out_col_idx = 0; out_col_idx < output.column_count(); out_col_idx++) {
			auto file_col_idx = state.column_ids[out_col_idx];

			// this is a special case where we are not interested in the actual contents of the file
			if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}

			PrepareChunkBuffer(state, file_col_idx);
			// trigger the reading of a new page below
			state.column_data[file_col_idx]->page_value_count = 0;
		}
	}

	output.SetCardinality(MinValue<int64_t>(STANDARD_VECTOR_SIZE, GetGroup(state).num_rows - state.group_offset));

	if (output.size() == 0) {
		return;
	}

	for (idx_t out_col_idx = 0; out_col_idx < output.column_count(); out_col_idx++) {
		auto file_col_idx = state.column_ids[out_col_idx];
		if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
			Value constant_42 = Value::BIGINT(42);
			output.data[out_col_idx].Reference(constant_42);
			continue;
		}

		auto &col_data = *state.column_data[file_col_idx];

		// we might need to read multiple pages to fill the data chunk
		idx_t output_offset = 0;
		while (output_offset < output.size()) {
			// do this unpack business only if we run out of stuff from the current page
			if (col_data.page_offset >= col_data.page_value_count) {
				// read dictionaries and data page headers so that we are ready to go for scan
				if (!PreparePageBuffers(state, file_col_idx)) {
					continue;
				}
				col_data.page_offset = 0;
			}

			auto current_batch_size =
			    MinValue<idx_t>(col_data.page_value_count - col_data.page_offset, output.size() - output_offset);

			assert(current_batch_size > 0);

			if (col_data.has_nulls) {
				col_data.defined_buf.resize(current_batch_size);
				col_data.defined_decoder->GetBatch<uint8_t>(col_data.defined_buf.ptr, current_batch_size);
			}

			switch (col_data.page_encoding) {
			case Encoding::RLE_DICTIONARY:
			case Encoding::PLAIN_DICTIONARY: {
				idx_t null_count = 0;
				if (col_data.has_nulls) {
					for (idx_t i = 0; i < current_batch_size; i++) {
						if (!col_data.defined_buf.ptr[i]) {
							null_count++;
						}
					}
				}

				col_data.offset_buf.resize(current_batch_size * sizeof(uint32_t));
				col_data.dict_decoder->GetBatch<uint32_t>(col_data.offset_buf.ptr, current_batch_size - null_count);

				// TODO ensure we had seen a dict page IN THIS CHUNK before getting here

				switch (return_types[file_col_idx].id()) {
				case LogicalTypeId::BOOLEAN:
					fill_from_dict<bool>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
					break;
				case LogicalTypeId::INTEGER:
					fill_from_dict<int32_t>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
					break;
				case LogicalTypeId::BIGINT:
					fill_from_dict<int64_t>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
					break;
				case LogicalTypeId::FLOAT:
					fill_from_dict<float>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
					break;
				case LogicalTypeId::DOUBLE:
					fill_from_dict<double>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
					break;
				case LogicalTypeId::TIMESTAMP:
					fill_from_dict<timestamp_t>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
					break;
				case LogicalTypeId::VARCHAR: {
					if (!col_data.string_collection) {
						throw FormatException("Did not see a dictionary for strings. Corrupt file?");
					}

					// the strings can be anywhere in the collection so just reference it all
					for (auto &chunk : col_data.string_collection->chunks) {
						StringVector::AddHeapReference(output.data[out_col_idx], chunk->data[0]);
					}

					auto out_data_ptr = FlatVector::GetData<string_t>(output.data[out_col_idx]);
					for (idx_t i = 0; i < current_batch_size; i++) {
						if (!col_data.has_nulls || col_data.defined_buf.ptr[i]) {
							auto offset = col_data.offset_buf.read<uint32_t>();
							if (offset >= col_data.string_collection->count) {
								throw FormatException("string dictionary offset out of bounds");
							}
							auto &chunk = col_data.string_collection->chunks[offset / STANDARD_VECTOR_SIZE];
							auto &vec = chunk->data[0];

							out_data_ptr[i + output_offset] =
							    FlatVector::GetData<string_t>(vec)[offset % STANDARD_VECTOR_SIZE];
						} else {
							FlatVector::SetNull(output.data[out_col_idx], i + output_offset, true);
						}
					}
				} break;
				default:
					throw FormatException(return_types[file_col_idx].ToString());
				}

				break;
			}
			case Encoding::PLAIN:
				assert(col_data.payload.ptr);
				switch (return_types[file_col_idx].id()) {
				case LogicalTypeId::BOOLEAN: {
					// bit packed this
					auto target_ptr = FlatVector::GetData<bool>(output.data[out_col_idx]);
					for (idx_t i = 0; i < current_batch_size; i++) {
						if (col_data.has_nulls && !col_data.defined_buf.ptr[i]) {
							FlatVector::SetNull(output.data[out_col_idx], i + output_offset, true);
							continue;
						}
						col_data.payload.available(1);
						target_ptr[i + output_offset] = (*col_data.payload.ptr >> col_data.byte_pos) & 1;
						col_data.byte_pos++;
						if (col_data.byte_pos == 8) {
							col_data.byte_pos = 0;
							col_data.payload.inc(1);
						}
					}
					break;
				}
				case LogicalTypeId::INTEGER:
					fill_from_plain<int32_t>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
					break;
				case LogicalTypeId::BIGINT:
					fill_from_plain<int64_t>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
					break;
				case LogicalTypeId::FLOAT:
					fill_from_plain<float>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
					break;
				case LogicalTypeId::DOUBLE:
					fill_from_plain<double>(col_data, current_batch_size, output.data[out_col_idx], output_offset);
					break;
				case LogicalTypeId::TIMESTAMP: {
					for (idx_t i = 0; i < current_batch_size; i++) {
						if (!col_data.has_nulls || col_data.defined_buf.ptr[i]) {
							((timestamp_t *)FlatVector::GetData(output.data[out_col_idx]))[i + output_offset] =
							    impala_timestamp_to_timestamp_t(col_data.payload.read<Int96>());
						} else {
							FlatVector::SetNull(output.data[out_col_idx], i + output_offset, true);
						}
					}

					break;
				}
				case LogicalTypeId::VARCHAR: {
					for (idx_t i = 0; i < current_batch_size; i++) {
						if (!col_data.has_nulls || col_data.defined_buf.ptr[i]) {
							uint32_t str_len = col_data.payload.read<uint32_t>();
							col_data.payload.available(str_len);
							FlatVector::GetData<string_t>(output.data[out_col_idx])[i + output_offset] =
							    StringVector::AddString(output.data[out_col_idx], col_data.payload.ptr, str_len);
							col_data.payload.inc(str_len);
						} else {
							FlatVector::SetNull(output.data[out_col_idx], i + output_offset, true);
						}
					}
					break;
				}
				default:
					throw FormatException(return_types[file_col_idx].ToString());
				}

				break;

			default:
				throw FormatException("Data page has unsupported/invalid encoding");
			}

			output_offset += current_batch_size;
			col_data.page_offset += current_batch_size;
		}
	}
	state.group_offset += output.size();
}

} // namespace duckdb
