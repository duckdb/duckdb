#include "column_reader.hpp"
#include "parquet_timestamp.hpp"
#include "utf8proc_wrapper.hpp"
#include "parquet_reader.hpp"

#include "boolean_column_reader.hpp"
#include "callback_column_reader.hpp"
#include "decimal_column_reader.hpp"
#include "list_column_reader.hpp"
#include "string_column_reader.hpp"
#include "struct_column_reader.hpp"
#include "templated_column_reader.hpp"

#include "snappy.h"
#include "miniz_wrapper.hpp"
#include "zstd.h"
#include <iostream>

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#endif

namespace duckdb {

using duckdb_parquet::format::CompressionCodec;
using duckdb_parquet::format::ConvertedType;
using duckdb_parquet::format::Encoding;
using duckdb_parquet::format::PageType;
using duckdb_parquet::format::Type;

const uint32_t RleBpDecoder::BITPACK_MASKS[] = {
    0,       1,       3,        7,        15,       31,        63,        127,       255,        511,       1023,
    2047,    4095,    8191,     16383,    32767,    65535,     131071,    262143,    524287,     1048575,   2097151,
    4194303, 8388607, 16777215, 33554431, 67108863, 134217727, 268435455, 536870911, 1073741823, 2147483647};

const uint8_t RleBpDecoder::BITPACK_DLEN = 8;

ColumnReader::ColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t file_idx_p,
                           idx_t max_define_p, idx_t max_repeat_p)
    : schema(schema_p), file_idx(file_idx_p), max_define(max_define_p), max_repeat(max_repeat_p), reader(reader),
      type(move(type_p)), page_rows_available(0) {

	// dummies for Skip()
	none_filter.none();
	dummy_define.resize(reader.allocator, STANDARD_VECTOR_SIZE);
	dummy_repeat.resize(reader.allocator, STANDARD_VECTOR_SIZE);
}

ColumnReader::~ColumnReader() {
}

template <class T>
unique_ptr<ColumnReader> CreateDecimalReader(ParquetReader &reader, const LogicalType &type_p,
                                             const SchemaElement &schema_p, idx_t file_idx_p, idx_t max_define,
                                             idx_t max_repeat) {
	switch (type_p.InternalType()) {
	case PhysicalType::INT16:
		return make_unique<TemplatedColumnReader<int16_t, TemplatedParquetValueConversion<T>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case PhysicalType::INT32:
		return make_unique<TemplatedColumnReader<int32_t, TemplatedParquetValueConversion<T>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case PhysicalType::INT64:
		return make_unique<TemplatedColumnReader<int64_t, TemplatedParquetValueConversion<T>>>(
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
		return make_unique<BooleanColumnReader>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::UTINYINT:
		return make_unique<TemplatedColumnReader<uint8_t, TemplatedParquetValueConversion<uint32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::USMALLINT:
		return make_unique<TemplatedColumnReader<uint16_t, TemplatedParquetValueConversion<uint32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::UINTEGER:
		return make_unique<TemplatedColumnReader<uint32_t, TemplatedParquetValueConversion<uint32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::UBIGINT:
		return make_unique<TemplatedColumnReader<uint64_t, TemplatedParquetValueConversion<uint64_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::TINYINT:
		return make_unique<TemplatedColumnReader<int8_t, TemplatedParquetValueConversion<int32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::SMALLINT:
		return make_unique<TemplatedColumnReader<int16_t, TemplatedParquetValueConversion<int32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::INTEGER:
		return make_unique<TemplatedColumnReader<int32_t, TemplatedParquetValueConversion<int32_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::BIGINT:
		return make_unique<TemplatedColumnReader<int64_t, TemplatedParquetValueConversion<int64_t>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::FLOAT:
		return make_unique<TemplatedColumnReader<float, TemplatedParquetValueConversion<float>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::DOUBLE:
		return make_unique<TemplatedColumnReader<double, TemplatedParquetValueConversion<double>>>(
		    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::TIMESTAMP:
		switch (schema_p.type) {
		case Type::INT96:
			return make_unique<CallbackColumnReader<Int96, timestamp_t, ImpalaTimestampToTimestamp>>(
			    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
		case Type::INT64:
			switch (schema_p.converted_type) {
			case ConvertedType::TIMESTAMP_MICROS:
				return make_unique<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampMicrosToTimestamp>>(
				    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
			case ConvertedType::TIMESTAMP_MILLIS:
				return make_unique<CallbackColumnReader<int64_t, timestamp_t, ParquetTimestampMsToTimestamp>>(
				    reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
			default:
				break;
			}
		default:
			break;
		}
		break;
	case LogicalTypeId::DATE:
		return make_unique<CallbackColumnReader<int32_t, date_t, ParquetIntToDate>>(reader, type_p, schema_p,
		                                                                            file_idx_p, max_define, max_repeat);
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return make_unique<StringColumnReader>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	case LogicalTypeId::DECIMAL:
		// we have to figure out what kind of int we need
		switch (schema_p.type) {
		case Type::INT32:
			return CreateDecimalReader<int32_t>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
		case Type::INT64:
			return CreateDecimalReader<int64_t>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
		case Type::FIXED_LEN_BYTE_ARRAY:
			switch (type_p.InternalType()) {
			case PhysicalType::INT16:
				return make_unique<DecimalColumnReader<int16_t>>(reader, type_p, schema_p, file_idx_p, max_define,
				                                                 max_repeat);
			case PhysicalType::INT32:
				return make_unique<DecimalColumnReader<int32_t>>(reader, type_p, schema_p, file_idx_p, max_define,
				                                                 max_repeat);
			case PhysicalType::INT64:
				return make_unique<DecimalColumnReader<int64_t>>(reader, type_p, schema_p, file_idx_p, max_define,
				                                                 max_repeat);
			case PhysicalType::INT128:
				return make_unique<DecimalColumnReader<hugeint_t>>(reader, type_p, schema_p, file_idx_p, max_define,
				                                                   max_repeat);
			default:
				throw InternalException("Unrecognized type for Decimal");
			}
		default:
			throw NotImplementedException("Unrecognized type for Decimal");
		}
		break;
	default:
		break;
	}
	throw NotImplementedException(type_p.ToString());
}

void ColumnReader::PrepareRead(parquet_filter_t &filter) {
	dict_decoder.reset();
	defined_decoder.reset();
	block.reset();

	PageHeader page_hdr;
	page_hdr.read(protocol);

	//	page_hdr.printTo(std::cout);
	//	std::cout << '\n';

	PreparePage(page_hdr.compressed_page_size, page_hdr.uncompressed_page_size);

	switch (page_hdr.type) {
	case PageType::DATA_PAGE_V2:
	case PageType::DATA_PAGE:
		PrepareDataPage(page_hdr);
		break;
	case PageType::DICTIONARY_PAGE:
		Dictionary(move(block), page_hdr.dictionary_page_header.num_values);
		break;
	default:
		break; // ignore INDEX page type and any other custom extensions
	}
}

void ColumnReader::PreparePage(idx_t compressed_page_size, idx_t uncompressed_page_size) {
	auto &trans = (ThriftFileTransport &)*protocol->getTransport();

	block = make_shared<ResizeableBuffer>(reader.allocator, compressed_page_size + 1);
	trans.read((uint8_t *)block->ptr, compressed_page_size);

	shared_ptr<ResizeableBuffer> unpacked_block;
	if (chunk->meta_data.codec != CompressionCodec::UNCOMPRESSED) {
		unpacked_block = make_shared<ResizeableBuffer>(reader.allocator, uncompressed_page_size + 1);
	}

	switch (chunk->meta_data.codec) {
	case CompressionCodec::UNCOMPRESSED:
		break;
	case CompressionCodec::GZIP: {
		MiniZStream s;

		s.Decompress((const char *)block->ptr, compressed_page_size, (char *)unpacked_block->ptr,
		             uncompressed_page_size);
		block = move(unpacked_block);

		break;
	}
	case CompressionCodec::SNAPPY: {
		auto res = snappy::RawUncompress((const char *)block->ptr, compressed_page_size, (char *)unpacked_block->ptr);
		if (!res) {
			throw std::runtime_error("Decompression failure");
		}
		block = move(unpacked_block);
		break;
	}
	case CompressionCodec::ZSTD: {
		auto res = duckdb_zstd::ZSTD_decompress((char *)unpacked_block->ptr, uncompressed_page_size,
		                                        (const char *)block->ptr, compressed_page_size);
		if (duckdb_zstd::ZSTD_isError(res) || res != (size_t)uncompressed_page_size) {
			throw std::runtime_error("ZSTD Decompression failure");
		}
		block = move(unpacked_block);
		break;
	}

	default: {
		std::stringstream codec_name;
		codec_name << chunk->meta_data.codec;
		throw std::runtime_error("Unsupported compression codec \"" + codec_name.str() +
		                         "\". Supported options are uncompressed, gzip or snappy");
		break;
	}
	}
}

static uint8_t ComputeBitWidth(idx_t val) {
	if (val == 0) {
		return 0;
	}
	uint8_t ret = 1;
	while (((idx_t)(1 << ret) - 1) < val) {
		ret++;
	}
	return ret;
}

void ColumnReader::PrepareDataPage(PageHeader &page_hdr) {
	if (page_hdr.type == PageType::DATA_PAGE && !page_hdr.__isset.data_page_header) {
		throw std::runtime_error("Missing data page header from data page");
	}
	if (page_hdr.type == PageType::DATA_PAGE_V2 && !page_hdr.__isset.data_page_header_v2) {
		throw std::runtime_error("Missing data page header from data page v2");
	}

	page_rows_available = page_hdr.type == PageType::DATA_PAGE ? page_hdr.data_page_header.num_values
	                                                           : page_hdr.data_page_header_v2.num_values;
	auto page_encoding = page_hdr.type == PageType::DATA_PAGE ? page_hdr.data_page_header.encoding
	                                                          : page_hdr.data_page_header_v2.encoding;

	if (HasRepeats()) {
		uint32_t rep_length = page_hdr.type == PageType::DATA_PAGE
		                          ? block->read<uint32_t>()
		                          : page_hdr.data_page_header_v2.repetition_levels_byte_length;
		block->available(rep_length);
		repeated_decoder =
		    make_unique<RleBpDecoder>((const uint8_t *)block->ptr, rep_length, ComputeBitWidth(max_repeat));
		block->inc(rep_length);
	}

	if (HasDefines()) {
		uint32_t def_length = page_hdr.type == PageType::DATA_PAGE
		                          ? block->read<uint32_t>()
		                          : page_hdr.data_page_header_v2.definition_levels_byte_length;
		block->available(def_length);
		defined_decoder =
		    make_unique<RleBpDecoder>((const uint8_t *)block->ptr, def_length, ComputeBitWidth(max_define));
		block->inc(def_length);
	}

	switch (page_encoding) {
	case Encoding::RLE_DICTIONARY:
	case Encoding::PLAIN_DICTIONARY: {
		// TODO there seems to be some confusion whether this is in the bytes for v2
		// where is it otherwise??
		auto dict_width = block->read<uint8_t>();
		// TODO somehow dict_width can be 0 ?
		dict_decoder = make_unique<RleBpDecoder>((const uint8_t *)block->ptr, block->len, dict_width);
		block->inc(block->len);
		break;
	}
	case Encoding::PLAIN:
		// nothing to do here, will be read directly below
		break;

	default:
		throw std::runtime_error("Unsupported page encoding");
	}
}

idx_t ColumnReader::Read(uint64_t num_values, parquet_filter_t &filter, uint8_t *define_out, uint8_t *repeat_out,
                         Vector &result) {
	// we need to reset the location because multiple column readers share the same protocol
	auto &trans = (ThriftFileTransport &)*protocol->getTransport();
	trans.SetLocation(chunk_read_offset);

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
			repeated_decoder->GetBatch<uint8_t>((char *)repeat_out + result_offset, read_now);
		}

		if (HasDefines()) {
			D_ASSERT(defined_decoder);
			defined_decoder->GetBatch<uint8_t>((char *)define_out + result_offset, read_now);
		}

		if (dict_decoder) {
			// we need the null count because the offsets and plain values have no entries for nulls
			idx_t null_count = 0;
			if (HasDefines()) {
				for (idx_t i = 0; i < read_now; i++) {
					if (define_out[i + result_offset] != max_define) {
						null_count++;
					}
				}
			}

			offset_buffer.resize(reader.allocator, sizeof(uint32_t) * (read_now - null_count));
			dict_decoder->GetBatch<uint32_t>(offset_buffer.ptr, read_now - null_count);
			DictReference(result);
			Offsets((uint32_t *)offset_buffer.ptr, define_out, read_now, filter, result_offset, result);
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
	dummy_define.zero();
	dummy_repeat.zero();

	// TODO this can be optimized, for example we dont actually have to bitunpack offsets
	Vector dummy_result(type, nullptr);
	auto values_read =
	    Read(num_values, none_filter, (uint8_t *)dummy_define.ptr, (uint8_t *)dummy_repeat.ptr, dummy_result);
	if (values_read != num_values) {
		throw std::runtime_error("Row count mismatch when skipping rows");
	}
}

uint32_t StringColumnReader::VerifyString(const char *str_data, uint32_t str_len) {
	if (Type() != LogicalTypeId::VARCHAR) {
		return str_len;
	}
	// verify if a string is actually UTF8, and if there are no null bytes in the middle of the string
	// technically Parquet should guarantee this, but reality is often disappointing
	UnicodeInvalidReason reason;
	size_t pos;
	auto utf_type = Utf8Proc::Analyze(str_data, str_len, &reason, &pos);
	if (utf_type == UnicodeType::INVALID) {
		if (reason == UnicodeInvalidReason::NULL_BYTE) {
			// for null bytes we just truncate the string
			return pos;
		}
		throw InvalidInputException("Invalid string encoding found in Parquet file: value \"" +
		                            Blob::ToString(string_t(str_data, str_len)) + "\" is not valid UTF8!");
	}
	return str_len;
}

void StringColumnReader::Dictionary(shared_ptr<ByteBuffer> data, idx_t num_entries) {
	dict = move(data);
	dict_strings = unique_ptr<string_t[]>(new string_t[num_entries]);
	for (idx_t dict_idx = 0; dict_idx < num_entries; dict_idx++) {
		uint32_t str_len = dict->read<uint32_t>();
		dict->available(str_len);

		auto actual_str_len = VerifyString(dict->ptr, str_len);
		dict_strings[dict_idx] = string_t(dict->ptr, actual_str_len);
		dict->inc(str_len);
	}
}

class ParquetStringVectorBuffer : public VectorBuffer {
public:
	explicit ParquetStringVectorBuffer(shared_ptr<ByteBuffer> buffer_p)
	    : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), buffer(move(buffer_p)) {
	}

private:
	shared_ptr<ByteBuffer> buffer;
};

void StringColumnReader::DictReference(Vector &result) {
	StringVector::AddBuffer(result, make_buffer<ParquetStringVectorBuffer>(dict));
}
void StringColumnReader::PlainReference(shared_ptr<ByteBuffer> plain_data, Vector &result) {
	StringVector::AddBuffer(result, make_buffer<ParquetStringVectorBuffer>(move(plain_data)));
}

string_t StringParquetValueConversion::DictRead(ByteBuffer &dict, uint32_t &offset, ColumnReader &reader) {
	auto &dict_strings = ((StringColumnReader &)reader).dict_strings;
	return dict_strings[offset];
}

string_t StringParquetValueConversion::PlainRead(ByteBuffer &plain_data, ColumnReader &reader) {
	auto &scr = ((StringColumnReader &)reader);
	uint32_t str_len = scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() : scr.fixed_width_string_length;
	plain_data.available(str_len);
	auto actual_str_len = ((StringColumnReader &)reader).VerifyString(plain_data.ptr, str_len);
	auto ret_str = string_t(plain_data.ptr, actual_str_len);
	plain_data.inc(str_len);
	return ret_str;
}

void StringParquetValueConversion::PlainSkip(ByteBuffer &plain_data, ColumnReader &reader) {
	auto &scr = ((StringColumnReader &)reader);
	uint32_t str_len = scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() : scr.fixed_width_string_length;
	plain_data.available(str_len);
	plain_data.inc(str_len);
}

idx_t ListColumnReader::Read(uint64_t num_values, parquet_filter_t &filter, uint8_t *define_out, uint8_t *repeat_out,
                             Vector &result_out) {
	idx_t result_offset = 0;
	auto result_ptr = FlatVector::GetData<list_entry_t>(result_out);
	auto &result_mask = FlatVector::Validity(result_out);

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
			read_vector.Slice(read_vector, child_idx);
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
    : ColumnReader(reader, move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p),
      child_column_reader(move(child_column_reader_p)), read_cache(ListType::GetChildType(Type())),
      read_vector(read_cache), overflow_child_count(0) {

	child_defines.resize(reader.allocator, STANDARD_VECTOR_SIZE);
	child_repeats.resize(reader.allocator, STANDARD_VECTOR_SIZE);
	child_defines_ptr = (uint8_t *)child_defines.ptr;
	child_repeats_ptr = (uint8_t *)child_repeats.ptr;

	child_filter.set();
}

} // namespace duckdb
