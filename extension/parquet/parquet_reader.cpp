#include "parquet_reader.hpp"
#include "parquet_timestamp.hpp"
#include "parquet_file_metadata_cache.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"

#include "duckdb/planner/table_filter.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/to_string.hpp"

#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/storage/object_cache.hpp"

#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"
#include "snappy.h"
#include "miniz_wrapper.hpp"

#include "zstd.h"

#include "utf8proc_wrapper.hpp"

#include <sstream>
#include <cassert>
#include <chrono>

namespace duckdb {

const uint32_t RleBpDecoder::BITPACK_MASKS[] = {
    0,       1,       3,        7,        15,       31,        63,        127,       255,        511,       1023,
    2047,    4095,    8191,     16383,    32767,    65535,     131071,    262143,    524287,     1048575,   2097151,
    4194303, 8388607, 16777215, 33554431, 67108863, 134217727, 268435455, 536870911, 1073741823, 2147483647};

const uint8_t RleBpDecoder::BITPACK_DLEN = 8;

using namespace parquet;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using parquet::format::ColumnChunk;
using parquet::format::CompressionCodec;
using parquet::format::ConvertedType;
using parquet::format::Encoding;
using parquet::format::FieldRepetitionType;
using parquet::format::FileMetaData;
using parquet::format::PageHeader;
using parquet::format::PageType;
using parquet::format::RowGroup;
using parquet::format::SchemaElement;
using parquet::format::Statistics;
using parquet::format::Type;

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

static unique_ptr<FileMetaData> read_metadata(duckdb::FileSystem &fs, duckdb::FileHandle *handle, uint32_t footer_len,
                                              uint64_t file_size) {
	auto metadata = make_unique<FileMetaData>();
	// read footer into buffer and de-thrift
	ResizeableBuffer buf;
	buf.resize(footer_len);
	fs.Read(*handle, buf.ptr, footer_len, file_size - (footer_len + 8));
	thrift_unpack((const uint8_t *)buf.ptr, &footer_len, metadata.get());
	return metadata;
}

static shared_ptr<ParquetFileMetadataCache> load_metadata(duckdb::FileSystem &fs, duckdb::FileHandle *handle,
                                                          uint32_t footer_len, uint64_t file_size) {
	auto current_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	return make_shared<ParquetFileMetadataCache>(read_metadata(fs, handle, footer_len, file_size), current_time);
}

ParquetReader::ParquetReader(ClientContext &context, string file_name_, vector<LogicalType> expected_types,
                             string initial_filename)
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

	// If object cached is disabled
	// or if this file has cached metadata
	// or if the cached version already expired
	if (!context.db.config.object_cache_enable) {
		metadata = load_metadata(fs, handle.get(), footer_len, file_size);
	} else {
		metadata = std::dynamic_pointer_cast<ParquetFileMetadataCache>(context.db.object_cache->Get(file_name));
		if (!metadata || (fs.GetLastModifiedTime(*handle) + 10 >= metadata->read_time)) {
			metadata = load_metadata(fs, handle.get(), footer_len, file_size);
			context.db.object_cache->Put(file_name, std::dynamic_pointer_cast<ObjectCacheEntry>(metadata));
		}
	}

	auto file_meta_data = GetFileMetadata();

	if (file_meta_data->__isset.encryption_algorithm) {
		throw FormatException("Encrypted Parquet files are not supported");
	}
	// check if we like this schema
	if (file_meta_data->schema.size() < 2) {
		throw FormatException("Need at least one column in the file");
	}
	if (file_meta_data->schema[0].num_children != (int32_t)(file_meta_data->schema.size() - 1)) {
		throw FormatException("Only flat tables are supported (no nesting)");
	}

	this->return_types = expected_types;
	bool has_expected_types = expected_types.size() > 0;

	// skip the first column its the root and otherwise useless
	for (uint64_t col_idx = 1; col_idx < file_meta_data->schema.size(); col_idx++) {
		auto &s_ele = file_meta_data->schema[col_idx];
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
			if (s_ele.__isset.converted_type) {
				switch (s_ele.converted_type) {
				case ConvertedType::TIMESTAMP_MICROS:
				case ConvertedType::TIMESTAMP_MILLIS:
					type = LogicalType::TIMESTAMP;
					break;
				default:
					type = LogicalType::BIGINT;
					break;
				}
			} else {
				type = LogicalType::BIGINT;
			}
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
			if (s_ele.__isset.converted_type) {
				switch (s_ele.converted_type) {
				case ConvertedType::UTF8:
					type = LogicalType::VARCHAR;
					break;
				default:
					type = LogicalType::BLOB;
					break;
				}
			} else {
				type = LogicalType::BLOB;
			}
			break;
		default:
			throw FormatException("Unsupported type");
		}
		if (has_expected_types) {
			if (return_types[col_idx - 1] != type) {
				if (initial_filename.empty()) {
					throw FormatException("column \"%s\" in parquet file is of type %s, could not auto cast to "
					                      "expected type %s for this column",
					                      s_ele.name, type.ToString(), return_types[col_idx - 1].ToString());
				} else {
					throw FormatException("schema mismatch in Parquet glob: column \"%s\" in parquet file is of type "
					                      "%s, but in the original file \"%s\" this column is of type \"%s\"",
					                      s_ele.name, type.ToString(), initial_filename,
					                      return_types[col_idx - 1].ToString());
				}
			}
		} else {
			names.push_back(s_ele.name);
			return_types.push_back(type);
		}
	}
}

ParquetReader::~ParquetReader() {
}

const FileMetaData *ParquetReader::GetFileMetadata() {
	D_ASSERT(metadata);
	D_ASSERT(metadata->metadata);
	return metadata->metadata.get();
}

ParquetReaderColumnData::~ParquetReaderColumnData() {
}

struct ValueIsValid {
	template <class T> static bool Operation(T value) {
		return true;
	}
};

template <> bool ValueIsValid::Operation(float value) {
	return Value::FloatIsValid(value);
}

template <> bool ValueIsValid::Operation(double value) {
	return Value::DoubleIsValid(value);
}

timestamp_t arrow_timestamp_micros_to_timestamp(const int64_t &raw_ts) {
	return Timestamp::FromEpochMicroSeconds(raw_ts);
}
timestamp_t arrow_timestamp_ms_to_timestamp(const int64_t &raw_ts) {
	return Timestamp::FromEpochMs(raw_ts);
}

// statistics handling

template <Value (*FUNC)(const_data_ptr_t input)>
static unique_ptr<BaseStatistics> templated_get_numeric_stats(const LogicalType &type,
                                                              const Statistics &parquet_stats) {
	auto stats = make_unique<NumericStatistics>(type);

	// for reasons unknown to science, Parquet defines *both* `min` and `min_value` as well as `max` and
	// `max_value`. All are optional. such elegance.
	if (parquet_stats.__isset.min) {
		stats->min = FUNC((const_data_ptr_t)parquet_stats.min.data());
	} else if (parquet_stats.__isset.min_value) {
		stats->min = FUNC((const_data_ptr_t)parquet_stats.min_value.data());
	} else {
		stats->min.is_null = true;
	}
	if (parquet_stats.__isset.max) {
		stats->max = FUNC((const_data_ptr_t)parquet_stats.max.data());
	} else if (parquet_stats.__isset.max_value) {
		stats->max = FUNC((const_data_ptr_t)parquet_stats.max_value.data());
	} else {
		stats->max.is_null = true;
	}
	// GCC 4.x insists on a move() here
	return move(stats);
}

template <class T> static Value transform_statistics_plain(const_data_ptr_t input) {
	return Value(Load<T>(input));
}

static Value transform_statistics_timestamp_ms(const_data_ptr_t input) {
	return Value::TIMESTAMP(arrow_timestamp_ms_to_timestamp(Load<int64_t>(input)));
}

static Value transform_statistics_timestamp_micros(const_data_ptr_t input) {
	return Value::TIMESTAMP(arrow_timestamp_micros_to_timestamp(Load<int64_t>(input)));
}

static Value transform_statistics_timestamp_impala(const_data_ptr_t input) {
	return Value::TIMESTAMP(impala_timestamp_to_timestamp_t(Load<Int96>(input)));
}

static unique_ptr<BaseStatistics> get_col_chunk_stats(const SchemaElement &s_ele, const LogicalType &type,
                                                      const ColumnChunk &column_chunk) {
	if (!column_chunk.__isset.meta_data || !column_chunk.meta_data.__isset.statistics) {
		// no stats present for row group
		return nullptr;
	}
	auto &parquet_stats = column_chunk.meta_data.statistics;
	unique_ptr<BaseStatistics> row_group_stats;

	switch (type.id()) {
	case LogicalTypeId::INTEGER:
		row_group_stats = templated_get_numeric_stats<transform_statistics_plain<int32_t>>(type, parquet_stats);
		break;

	case LogicalTypeId::BIGINT:
		row_group_stats = templated_get_numeric_stats<transform_statistics_plain<int64_t>>(type, parquet_stats);
		break;

	case LogicalTypeId::FLOAT:
		row_group_stats = templated_get_numeric_stats<transform_statistics_plain<float>>(type, parquet_stats);
		break;

	case LogicalTypeId::DOUBLE:
		row_group_stats = templated_get_numeric_stats<transform_statistics_plain<double>>(type, parquet_stats);
		break;

		// here we go, our favorite type
	case LogicalTypeId::TIMESTAMP: {
		switch (s_ele.type) {
		case Type::INT64:
			// arrow timestamp
			switch (s_ele.converted_type) {
			case ConvertedType::TIMESTAMP_MICROS:
				row_group_stats =
				    templated_get_numeric_stats<transform_statistics_timestamp_micros>(type, parquet_stats);
				break;
			case ConvertedType::TIMESTAMP_MILLIS:
				row_group_stats = templated_get_numeric_stats<transform_statistics_timestamp_ms>(type, parquet_stats);
				break;
			default:
				return nullptr;
			}
			break;
		case Type::INT96:
			// impala timestamp
			row_group_stats = templated_get_numeric_stats<transform_statistics_timestamp_impala>(type, parquet_stats);
			break;
		default:
			return nullptr;
		}
		break;
	}
	case LogicalTypeId::VARCHAR: {
		auto string_stats = make_unique<StringStatistics>(type);
		if (parquet_stats.__isset.min) {
			memcpy(string_stats->min, (data_ptr_t)parquet_stats.min.data(),
			       MinValue<idx_t>(parquet_stats.min.size(), StringStatistics::MAX_STRING_MINMAX_SIZE));
		} else if (parquet_stats.__isset.min_value) {
			memcpy(string_stats->min, (data_ptr_t)parquet_stats.min_value.data(),
			       MinValue<idx_t>(parquet_stats.min_value.size(), StringStatistics::MAX_STRING_MINMAX_SIZE));
		} else {
			return nullptr;
		}
		if (parquet_stats.__isset.max) {
			memcpy(string_stats->max, (data_ptr_t)parquet_stats.max.data(),
			       MinValue<idx_t>(parquet_stats.max.size(), StringStatistics::MAX_STRING_MINMAX_SIZE));
		} else if (parquet_stats.__isset.max_value) {
			memcpy(string_stats->max, (data_ptr_t)parquet_stats.max_value.data(),
			       MinValue<idx_t>(parquet_stats.max_value.size(), StringStatistics::MAX_STRING_MINMAX_SIZE));
		} else {
			return nullptr;
		}

		string_stats->has_unicode = true; // we dont know better
		row_group_stats = move(string_stats);
		break;
	}
	default:
		// no stats for you
		break;
	} // end of type switch

	// null count is generic
	if (row_group_stats) {
		if (parquet_stats.__isset.null_count) {
			row_group_stats->has_null = parquet_stats.null_count != 0;
		} else {
			row_group_stats->has_null = true;
		}
	} else {
		// if stats are missing from any row group we know squat
		return nullptr;
	}

	return row_group_stats;
}

unique_ptr<BaseStatistics> ParquetReader::ReadStatistics(LogicalType &type, column_t column_index,
                                                         const FileMetaData *file_meta_data) {
	unique_ptr<BaseStatistics> column_stats;

	for (auto &row_group : file_meta_data->row_groups) {

		D_ASSERT(column_index < row_group.columns.size());
		auto &column_chunk = row_group.columns[column_index];
		auto &s_ele = file_meta_data->schema[column_index + 1];

		auto chunk_stats = get_col_chunk_stats(s_ele, type, column_chunk);

		if (!column_stats) {
			column_stats = move(chunk_stats);
		} else {
			column_stats->Merge(*chunk_stats);
		}
	}
	return column_stats;
}

template <class T>
static void fill_from_dict(ParquetReaderColumnData &col_data, idx_t count, parquet_filter_t &filter_mask,
                           Vector &target, idx_t target_offset) {

	if (!col_data.has_nulls && filter_mask.none()) {
		col_data.offset_buf.inc(sizeof(uint32_t) * count);
		return;
	}

	for (idx_t i = 0; i < count; i++) {
		if (!col_data.has_nulls || col_data.defined_buf.ptr[i]) {
			auto offset = col_data.offset_buf.read<uint32_t>();
			if (!filter_mask[i + target_offset]) {
				continue; // early out if this value is skipped
			}

			if (offset > col_data.dict_size) {
				throw std::runtime_error("Offset " + to_string(offset) + " greater than dictionary size " +
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
static void fill_from_plain(ParquetReaderColumnData &col_data, idx_t count, parquet_filter_t &filter_mask,
                            Vector &target, idx_t target_offset) {
	if (!col_data.has_nulls && filter_mask.none()) {
		col_data.payload.inc(sizeof(T) * count);
		return;
	}

	for (idx_t i = 0; i < count; i++) {
		if (!col_data.has_nulls || col_data.defined_buf.ptr[i]) {
			auto value = col_data.payload.read<T>();

			if (!filter_mask[i + target_offset]) {
				continue; // early out if this value is skipped
			}

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

template <class T, timestamp_t (*FUNC)(const T &input)>
static void fill_timestamp_plain(ParquetReaderColumnData &col_data, idx_t count, parquet_filter_t &filter_mask,
                                 Vector &target, idx_t target_offset) {
	if (!col_data.has_nulls && filter_mask.none()) {
		col_data.payload.inc(sizeof(T) * count);
		return;
	}

	for (idx_t i = 0; i < count; i++) {
		if (!col_data.has_nulls || col_data.defined_buf.ptr[i]) {
			auto value = col_data.payload.read<T>();

			if (!filter_mask[i + target_offset]) {
				continue; // early out if this value is skipped
			}

			((timestamp_t *)FlatVector::GetData(target))[i + target_offset] = FUNC(value);
		} else {
			FlatVector::SetNull(target, i + target_offset, true);
		}
	}
}

const RowGroup &ParquetReader::GetGroup(ParquetReaderScanState &state) {
	auto file_meta_data = GetFileMetadata();
	D_ASSERT(state.current_group >= 0 && (idx_t)state.current_group < state.group_idx_list.size());
	D_ASSERT(state.group_idx_list[state.current_group] >= 0 &&
	         state.group_idx_list[state.current_group] < file_meta_data->row_groups.size());
	return file_meta_data->row_groups[state.group_idx_list[state.current_group]];
}

template <class T, timestamp_t (*FUNC)(const T &input)>
static void fill_timestamp_dict(ParquetReaderColumnData &col_data) {
	// immediately convert timestamps to duckdb format, potentially fewer conversions
	for (idx_t dict_index = 0; dict_index < col_data.dict_size; dict_index++) {
		auto impala_ts = Load<T>((data_ptr_t)(col_data.payload.ptr + dict_index * sizeof(T)));
		((timestamp_t *)col_data.dict.ptr)[dict_index] = FUNC(impala_ts);
	}
}

void ParquetReader::VerifyString(LogicalTypeId id, const char *str_data, idx_t str_len) {
	if (id != LogicalTypeId::VARCHAR) {
		return;
	}
	// verify if a string is actually UTF8, and if there are no null bytes in the middle of the string
	// technically Parquet should guarantee this, but reality is often disappointing
	auto utf_type = Utf8Proc::Analyze(str_data, str_len);
	if (utf_type == UnicodeType::INVALID) {
		throw FormatException("Invalid string encoding found in Parquet file: value is not valid UTF8!");
	}
}

bool ParquetReader::PreparePageBuffers(ParquetReaderScanState &state, idx_t col_idx) {
	auto &col_data = *state.column_data[col_idx];
	auto &s_ele = GetFileMetadata()->schema[col_idx + 1];
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
		MiniZStream s;

		col_data.decompressed_buf.resize(page_hdr.uncompressed_page_size);
		s.Decompress(col_data.buf.ptr, page_hdr.compressed_page_size, col_data.decompressed_buf.ptr,
		             page_hdr.uncompressed_page_size);

		col_data.payload.ptr = col_data.decompressed_buf.ptr;
		break;
	}
	case CompressionCodec::ZSTD: {
		col_data.decompressed_buf.resize(page_hdr.uncompressed_page_size);
		auto res = duckdb_zstd::ZSTD_decompress(col_data.decompressed_buf.ptr, page_hdr.uncompressed_page_size,
		                                        col_data.buf.ptr, page_hdr.compressed_page_size);
		if (duckdb_zstd::ZSTD_isError(res) || res != (size_t)page_hdr.uncompressed_page_size) {
			throw FormatException("ZSTD Decompression failure");
		}
		col_data.payload.ptr = col_data.decompressed_buf.ptr;
		break;
	}
	default: {
		std::stringstream codec_name;
		codec_name << chunk.meta_data.codec;
		throw FormatException("Unsupported compression codec \"" + codec_name.str() +
		                      "\". Supported options are uncompressed, gzip or snappy");
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
			switch (s_ele.type) {
			case Type::INT64:
				// arrow timestamp
				switch (s_ele.converted_type) {
				case ConvertedType::TIMESTAMP_MICROS:
					fill_timestamp_dict<int64_t, arrow_timestamp_micros_to_timestamp>(col_data);
					break;
				case ConvertedType::TIMESTAMP_MILLIS:
					fill_timestamp_dict<int64_t, arrow_timestamp_ms_to_timestamp>(col_data);
					break;
				default:
					throw InternalException("Unsupported converted type for timestamp");
				}
				break;
			case Type::INT96:
				// impala timestamp
				fill_timestamp_dict<Int96, impala_timestamp_to_timestamp_t>(col_data);
				break;
			default:
				throw InternalException("Unsupported type for timestamp");
			}
			break;
		case LogicalTypeId::BLOB:
		case LogicalTypeId::VARCHAR: {
			// strings we directly fill a string heap that we can use for the vectors later
			col_data.string_collection = make_unique<ChunkCollection>();

			auto append_chunk = make_unique<DataChunk>();
			vector<LogicalType> types = {return_types[col_idx]};
			append_chunk->Initialize(types);

			for (idx_t dict_index = 0; dict_index < col_data.dict_size; dict_index++) {
				uint32_t str_len = col_data.payload.read<uint32_t>();
				col_data.payload.available(str_len);

				if (append_chunk->size() == STANDARD_VECTOR_SIZE) {
					col_data.string_collection->Append(*append_chunk);
					append_chunk->SetCardinality(0);
				}

				VerifyString(return_types[col_idx].id(), col_data.payload.ptr, str_len);
				FlatVector::GetData<string_t>(append_chunk->data[0])[append_chunk->size()] =
				    StringVector::AddStringOrBlob(append_chunk->data[0], string_t(col_data.payload.ptr, str_len));

				append_chunk->SetCardinality(append_chunk->size() + 1);
				col_data.payload.inc(str_len);
			}
			// FLUSH last chunk!
			if (append_chunk->size() > 0) {
				col_data.string_collection->Append(*append_chunk);
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
		if (page_hdr.type == PageType::DATA_PAGE) {
			D_ASSERT(page_hdr.__isset.data_page_header);
		}
		if (page_hdr.type == PageType::DATA_PAGE_V2) {
			D_ASSERT(page_hdr.__isset.data_page_header_v2);
		}

		col_data.page_value_count = page_hdr.type == PageType::DATA_PAGE ? page_hdr.data_page_header.num_values
		                                                                 : page_hdr.data_page_header_v2.num_values;
		col_data.page_encoding = page_hdr.type == PageType::DATA_PAGE ? page_hdr.data_page_header.encoding
		                                                              : page_hdr.data_page_header_v2.encoding;

		if (!col_data.has_nulls && page_hdr.type == PageType::DATA_PAGE &&
		    page_hdr.data_page_header.__isset.statistics && page_hdr.data_page_header.statistics.__isset.null_count &&
		    page_hdr.data_page_header.statistics.null_count > 0) {
			throw FormatException("Column is defined as REQUIRED but statistics still claim NULL present");
		}
		if (!col_data.has_nulls && page_hdr.type == PageType::DATA_PAGE_V2 &&
		    page_hdr.data_page_header_v2.num_nulls > 0) {
			throw FormatException("Column is defined as REQUIRED but statistics still claim NULL present");
		}

		if (col_data.has_nulls) {
			// we have to first decode the define levels
			switch (page_hdr.data_page_header.definition_level_encoding) {
			case Encoding::RLE: {
				// read length of define payload, always
				uint32_t def_length = col_data.payload.read<uint32_t>();
				col_data.payload.available(def_length);
				col_data.defined_decoder =
				    make_unique<RleBpDecoder>((const uint8_t *)col_data.payload.ptr, def_length, 1);
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

void ParquetReader::PrepareRowGroupBuffer(ParquetReaderScanState &state, idx_t col_idx, LogicalType &type) {
	auto &group = GetGroup(state);
	auto &chunk = group.columns[col_idx];
	if (chunk.__isset.file_path) {
		throw FormatException("Only inlined data files are supported (no references)");
	}

	if (chunk.meta_data.path_in_schema.size() != 1) {
		throw FormatException("Only flat tables are supported (no nesting)");
	}

	if (state.filters) {
		auto &s_ele = GetFileMetadata()->schema[col_idx + 1];
		auto stats = get_col_chunk_stats(s_ele, type, group.columns[col_idx]);
		auto filter_entry = state.filters->filters.find(col_idx);
		if (stats && filter_entry != state.filters->filters.end()) {
			bool skip_chunk = false;
			switch (type.id()) {
			case LogicalTypeId::INTEGER:
			case LogicalTypeId::BIGINT:
			case LogicalTypeId::FLOAT:
			case LogicalTypeId::TIMESTAMP:
			case LogicalTypeId::DOUBLE: {
				auto num_stats = (NumericStatistics &)*stats;
				for (auto &filter : filter_entry->second) {
					skip_chunk = !num_stats.CheckZonemap(filter.comparison_type, filter.constant);
					if (skip_chunk) {
						break;
					}
				}
				break;
			}
			case LogicalTypeId::BLOB:
			case LogicalTypeId::VARCHAR: {
				auto str_stats = (StringStatistics &)*stats;
				for (auto &filter : filter_entry->second) {
					skip_chunk = !str_stats.CheckZonemap(filter.comparison_type, filter.constant.str_value);
					if (skip_chunk) {
						break;
					}
				}
				break;
			}
			default:
				D_ASSERT(0);
			}
			if (skip_chunk) {
				state.group_offset = group.num_rows;
				return;
				// this effectively will skip this chunk
			}
		}
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
	    GetFileMetadata()->schema[col_idx + 1].repetition_type == FieldRepetitionType::OPTIONAL;

	// read entire chunk into RAM
	state.column_data[col_idx]->buf.resize(chunk_len);
	fs.Read(*handle, state.column_data[col_idx]->buf.ptr, chunk_len, chunk_start);
	return;
}

idx_t ParquetReader::NumRows() {
	return GetFileMetadata()->num_rows;
}

idx_t ParquetReader::NumRowGroups() {
	return GetFileMetadata()->row_groups.size();
}

void ParquetReader::Initialize(ParquetReaderScanState &state, vector<column_t> column_ids, vector<idx_t> groups_to_read,
                               TableFilterSet *filters) {
	state.current_group = -1;
	state.finished = false;
	state.column_ids = move(column_ids);
	state.group_offset = 0;
	state.group_idx_list = move(groups_to_read);
	state.filters = filters;
	for (idx_t i = 0; i < return_types.size(); i++) {
		state.column_data.push_back(make_unique<ParquetReaderColumnData>());
	}
	state.sel.Initialize(STANDARD_VECTOR_SIZE);
}

void ParquetReader::ScanColumn(ParquetReaderScanState &state, parquet_filter_t &filter_mask, idx_t count,
                               idx_t out_col_idx, Vector &out) {
	auto file_col_idx = state.column_ids[out_col_idx];
	if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
		Value constant_42 = Value::BIGINT(42);
		out.Reference(constant_42);
		return;
	}
	auto &s_ele = GetFileMetadata()->schema[file_col_idx + 1];
	auto &col_data = *state.column_data[file_col_idx];

	// we might need to read multiple pages to fill the data chunk
	idx_t output_offset = 0;
	while (output_offset < count) {
		// do this unpack business only if we run out of stuff from the current page
		if (col_data.page_offset >= col_data.page_value_count) {
			// read dictionaries and data page headers so that we are ready to go for scan
			if (!PreparePageBuffers(state, file_col_idx)) {
				continue;
			}
			col_data.page_offset = 0;
		}

		auto current_batch_size =
		    MinValue<idx_t>(col_data.page_value_count - col_data.page_offset, count - output_offset);

		D_ASSERT(current_batch_size > 0);

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
				fill_from_dict<bool>(col_data, current_batch_size, filter_mask, out, output_offset);
				break;
			case LogicalTypeId::INTEGER:
				fill_from_dict<int32_t>(col_data, current_batch_size, filter_mask, out, output_offset);
				break;
			case LogicalTypeId::BIGINT:
				fill_from_dict<int64_t>(col_data, current_batch_size, filter_mask, out, output_offset);
				break;
			case LogicalTypeId::FLOAT:
				fill_from_dict<float>(col_data, current_batch_size, filter_mask, out, output_offset);
				break;
			case LogicalTypeId::DOUBLE:
				fill_from_dict<double>(col_data, current_batch_size, filter_mask, out, output_offset);
				break;
			case LogicalTypeId::TIMESTAMP:
				fill_from_dict<timestamp_t>(col_data, current_batch_size, filter_mask, out, output_offset);
				break;
			case LogicalTypeId::BLOB:
			case LogicalTypeId::VARCHAR: {
				if (!col_data.string_collection) {
					throw FormatException("Did not see a dictionary for strings. Corrupt file?");
				}

				if (!col_data.has_nulls && filter_mask.none()) {
					col_data.offset_buf.inc(sizeof(uint32_t) * count);
					break;
				}

				// the strings can be anywhere in the collection so just reference it all
				for (auto &chunk : col_data.string_collection->Chunks()) {
					StringVector::AddHeapReference(out, chunk->data[0]);
				}

				auto out_data_ptr = FlatVector::GetData<string_t>(out);

				for (idx_t i = 0; i < current_batch_size; i++) {
					if (!col_data.has_nulls || col_data.defined_buf.ptr[i]) {
						auto offset = col_data.offset_buf.read<uint32_t>();

						if (!filter_mask[i + output_offset]) {
							continue; // early out if this value is skipped
						}

						if (offset >= col_data.string_collection->Count()) {
							throw FormatException("string dictionary offset out of bounds");
						}
						auto &chunk = col_data.string_collection->GetChunk(offset / STANDARD_VECTOR_SIZE);
						auto &vec = chunk.data[0];

						out_data_ptr[i + output_offset] =
						    FlatVector::GetData<string_t>(vec)[offset % STANDARD_VECTOR_SIZE];
					} else {
						FlatVector::SetNull(out, i + output_offset, true);
					}
				}
			} break;
			default:
				throw FormatException(return_types[file_col_idx].ToString());
			}

			break;
		}
		case Encoding::PLAIN:
			D_ASSERT(col_data.payload.ptr);
			switch (return_types[file_col_idx].id()) {
			case LogicalTypeId::BOOLEAN: {
				// bit packed this
				auto target_ptr = FlatVector::GetData<bool>(out);
				for (idx_t i = 0; i < current_batch_size; i++) {
					if (col_data.has_nulls && !col_data.defined_buf.ptr[i]) {
						FlatVector::SetNull(out, i + output_offset, true);
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
				fill_from_plain<int32_t>(col_data, current_batch_size, filter_mask, out, output_offset);
				break;
			case LogicalTypeId::BIGINT:
				fill_from_plain<int64_t>(col_data, current_batch_size, filter_mask, out, output_offset);
				break;
			case LogicalTypeId::FLOAT:
				fill_from_plain<float>(col_data, current_batch_size, filter_mask, out, output_offset);
				break;
			case LogicalTypeId::DOUBLE:
				fill_from_plain<double>(col_data, current_batch_size, filter_mask, out, output_offset);
				break;
			case LogicalTypeId::TIMESTAMP:
				switch (s_ele.type) {
				case Type::INT64:
					// arrow timestamp
					switch (s_ele.converted_type) {
					case ConvertedType::TIMESTAMP_MICROS:
						fill_timestamp_plain<int64_t, arrow_timestamp_micros_to_timestamp>(
						    col_data, current_batch_size, filter_mask, out, output_offset);
						break;
					case ConvertedType::TIMESTAMP_MILLIS:
						fill_timestamp_plain<int64_t, arrow_timestamp_ms_to_timestamp>(col_data, current_batch_size,
						                                                               filter_mask, out, output_offset);
						break;
					default:
						throw InternalException("Unsupported converted type for timestamp");
					}
					break;
				case Type::INT96:
					// impala timestamp
					fill_timestamp_plain<Int96, impala_timestamp_to_timestamp_t>(col_data, current_batch_size,
					                                                             filter_mask, out, output_offset);
					break;
				default:
					throw InternalException("Unsupported type for timestamp");
				}
				break;
			case LogicalTypeId::BLOB:
			case LogicalTypeId::VARCHAR: {
				for (idx_t i = 0; i < current_batch_size; i++) {
					if (!col_data.has_nulls || col_data.defined_buf.ptr[i]) {
						uint32_t str_len = col_data.payload.read<uint32_t>();

						if (!filter_mask[i + output_offset]) {
							col_data.payload.inc(str_len);
							continue; // early out if this value is skipped
						}

						col_data.payload.available(str_len);
						VerifyString(return_types[file_col_idx].id(), col_data.payload.ptr, str_len);
						FlatVector::GetData<string_t>(out)[i + output_offset] =
						    StringVector::AddStringOrBlob(out, string_t(col_data.payload.ptr, str_len));
						col_data.payload.inc(str_len);
					} else {
						FlatVector::SetNull(out, i + output_offset, true);
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

template <class T, class OP>
void templated_filter_operation2(Vector &v, T constant, parquet_filter_t &filter_mask, idx_t count) {
	D_ASSERT(v.vector_type == VectorType::FLAT_VECTOR); // we just created the damn thing it better be

	auto v_ptr = FlatVector::GetData<T>(v);
	auto &nullmask = FlatVector::Nullmask(v);

	if (nullmask.any()) {
		for (idx_t i = 0; i < count; i++) {
			filter_mask[i] = filter_mask[i] && !(nullmask)[i] && OP::Operation(v_ptr[i], constant);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			filter_mask[i] = filter_mask[i] && OP::Operation(v_ptr[i], constant);
		}
	}
}

template <class OP>
static void templated_filter_operation(Vector &v, Value &constant, parquet_filter_t &filter_mask, idx_t count) {
	if (filter_mask.none() || count == 0) {
		return;
	}
	switch (v.type.id()) {
	case LogicalTypeId::BOOLEAN:
		templated_filter_operation2<bool, OP>(v, constant.value_.boolean, filter_mask, count);
		break;

	case LogicalTypeId::INTEGER:
		templated_filter_operation2<int32_t, OP>(v, constant.value_.integer, filter_mask, count);
		break;

	case LogicalTypeId::BIGINT:
		templated_filter_operation2<int64_t, OP>(v, constant.value_.bigint, filter_mask, count);
		break;

	case LogicalTypeId::FLOAT:
		templated_filter_operation2<float, OP>(v, constant.value_.float_, filter_mask, count);
		break;

	case LogicalTypeId::DOUBLE:
		templated_filter_operation2<double, OP>(v, constant.value_.double_, filter_mask, count);
		break;

	case LogicalTypeId::TIMESTAMP:
		templated_filter_operation2<timestamp_t, OP>(v, constant.value_.bigint, filter_mask, count);
		break;

	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		templated_filter_operation2<string_t, OP>(v, string_t(constant.str_value), filter_mask, count);
		break;

	default:
		throw NotImplementedException("Unsupported type for filter %s", v.ToString());
	}
}

void ParquetReader::Scan(ParquetReaderScanState &state, DataChunk &result) {
	while (ScanInternal(state, result)) {
		if (result.size() > 0) {
			break;
		}
		result.Reset();
	}
}

bool ParquetReader::ScanInternal(ParquetReaderScanState &state, DataChunk &result) {
	if (state.finished) {
		return false;
	}

	// see if we have to switch to the next row group in the parquet file
	if (state.current_group < 0 || (int64_t)state.group_offset >= GetGroup(state).num_rows) {
		state.current_group++;
		state.group_offset = 0;

		if ((idx_t)state.current_group == state.group_idx_list.size()) {
			state.finished = true;
			return false;
		}

		for (idx_t out_col_idx = 0; out_col_idx < result.ColumnCount(); out_col_idx++) {
			auto file_col_idx = state.column_ids[out_col_idx];

			// this is a special case where we are not interested in the actual contents of the file
			if (file_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}

			PrepareRowGroupBuffer(state, file_col_idx, result.GetTypes()[out_col_idx]);
			// trigger the reading of a new page in FillColumn
			state.column_data[file_col_idx]->page_value_count = 0;
		}
		return true;
	}

	auto this_output_chunk_rows = MinValue<idx_t>(STANDARD_VECTOR_SIZE, GetGroup(state).num_rows - state.group_offset);
	result.SetCardinality(this_output_chunk_rows);

	if (this_output_chunk_rows == 0) {
		state.finished = true;
		return false; // end of last group, we are done
	}

	// we evaluate simple table filters directly in this scan so we can skip decoding column data that's never going to
	// be relevant
	parquet_filter_t filter_mask;
	filter_mask.set();

	if (state.filters) {
		vector<bool> need_to_read(result.ColumnCount(), true);

		// first load the columns that are used in filters
		for (auto &filter_col : state.filters->filters) {
			if (filter_mask.none()) { // if no rows are left we can stop checking filters
				break;
			}
			ScanColumn(state, filter_mask, result.size(), filter_col.first, result.data[filter_col.first]);
			need_to_read[filter_col.first] = false;

			for (auto &filter : filter_col.second) {
				switch (filter.comparison_type) {
				case ExpressionType::COMPARE_EQUAL:
					templated_filter_operation<Equals>(result.data[filter_col.first], filter.constant, filter_mask,
					                                   this_output_chunk_rows);
					break;
				case ExpressionType::COMPARE_LESSTHAN:
					templated_filter_operation<LessThan>(result.data[filter_col.first], filter.constant, filter_mask,
					                                     this_output_chunk_rows);
					break;
				case ExpressionType::COMPARE_LESSTHANOREQUALTO:
					templated_filter_operation<LessThanEquals>(result.data[filter_col.first], filter.constant,
					                                           filter_mask, this_output_chunk_rows);
					break;
				case ExpressionType::COMPARE_GREATERTHAN:
					templated_filter_operation<GreaterThan>(result.data[filter_col.first], filter.constant, filter_mask,
					                                        this_output_chunk_rows);
					break;
				case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
					templated_filter_operation<GreaterThanEquals>(result.data[filter_col.first], filter.constant,
					                                              filter_mask, this_output_chunk_rows);
					break;
				default:
					D_ASSERT(0);
				}
			}
		}

		// we still may have to read some cols
		for (idx_t out_col_idx = 0; out_col_idx < result.ColumnCount(); out_col_idx++) {
			if (need_to_read[out_col_idx]) {
				ScanColumn(state, filter_mask, result.size(), out_col_idx, result.data[out_col_idx]);
			}
		}

		idx_t sel_size = 0;
		for (idx_t i = 0; i < this_output_chunk_rows; i++) {
			if (filter_mask[i]) {
				state.sel.set_index(sel_size++, i);
			}
		}

		result.Slice(state.sel, sel_size);
		result.Verify();

	} else { // just fricking load the data
		for (idx_t out_col_idx = 0; out_col_idx < result.ColumnCount(); out_col_idx++) {
			ScanColumn(state, filter_mask, result.size(), out_col_idx, result.data[out_col_idx]);
		}
	}

	state.group_offset += this_output_chunk_rows;
	return true; // thank you scan again
}

} // namespace duckdb
