#include "parquet_writer.hpp"
#include "parquet_timestamp.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#endif

#include "snappy.h"
#include "miniz_wrapper.hpp"
#include "zstd.h"

namespace duckdb {

using namespace duckdb_parquet;                   // NOLINT
using namespace duckdb_apache::thrift;            // NOLINT
using namespace duckdb_apache::thrift::protocol;  // NOLINT
using namespace duckdb_apache::thrift::transport; // NOLINT
using namespace duckdb_miniz;                     // NOLINT

using duckdb_parquet::format::CompressionCodec;
using duckdb_parquet::format::ConvertedType;
using duckdb_parquet::format::Encoding;
using duckdb_parquet::format::FieldRepetitionType;
using duckdb_parquet::format::FileMetaData;
using duckdb_parquet::format::PageHeader;
using duckdb_parquet::format::PageType;
using ParquetRowGroup = duckdb_parquet::format::RowGroup;
using duckdb_parquet::format::Type;

class MyTransport : public TTransport {
public:
	explicit MyTransport(Serializer &serializer) : serializer(serializer) {
	}

	bool isOpen() const override {
		return true;
	}

	void open() override {
	}

	void close() override {
	}

	void write_virt(const uint8_t *buf, uint32_t len) override {
		serializer.WriteData((const_data_ptr_t)buf, len);
	}

private:
	Serializer &serializer;
};

static Type::type DuckDBTypeToParquetType(const LogicalType &duckdb_type) {
	switch (duckdb_type.id()) {
	case LogicalTypeId::BOOLEAN:
		return Type::BOOLEAN;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return Type::INT32;
	case LogicalTypeId::BIGINT:
		return Type::INT64;
	case LogicalTypeId::FLOAT:
		return Type::FLOAT;
	case LogicalTypeId::DECIMAL: // for now...
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::HUGEINT:
		return Type::DOUBLE;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return Type::BYTE_ARRAY;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
		return Type::INT64;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return Type::INT32;
	case LogicalTypeId::UBIGINT:
		return Type::INT64;
	default:
		throw NotImplementedException(duckdb_type.ToString());
	}
}

static bool DuckDBTypeToConvertedType(const LogicalType &duckdb_type, ConvertedType::type &result) {
	switch (duckdb_type.id()) {
	case LogicalTypeId::TINYINT:
		result = ConvertedType::INT_8;
		return true;
	case LogicalTypeId::SMALLINT:
		result = ConvertedType::INT_16;
		return true;
	case LogicalTypeId::INTEGER:
		result = ConvertedType::INT_32;
		return true;
	case LogicalTypeId::BIGINT:
		result = ConvertedType::INT_64;
		return true;
	case LogicalTypeId::UTINYINT:
		result = ConvertedType::UINT_8;
		return true;
	case LogicalTypeId::USMALLINT:
		result = ConvertedType::UINT_16;
		return true;
	case LogicalTypeId::UINTEGER:
		result = ConvertedType::UINT_32;
		return true;
	case LogicalTypeId::UBIGINT:
		result = ConvertedType::UINT_64;
		return true;
	case LogicalTypeId::DATE:
		result = ConvertedType::DATE;
		return true;
	case LogicalTypeId::VARCHAR:
		result = ConvertedType::UTF8;
		return true;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
		result = ConvertedType::TIMESTAMP_MICROS;
		return true;
	case LogicalTypeId::TIMESTAMP_MS:
		result = ConvertedType::TIMESTAMP_MILLIS;
		return true;
	default:
		return false;
	}
}

static void VarintEncode(uint32_t val, Serializer &ser) {
	do {
		uint8_t byte = val & 127;
		val >>= 7;
		if (val != 0) {
			byte |= 128;
		}
		ser.Write<uint8_t>(byte);
	} while (val != 0);
}

static uint8_t GetVarintSize(uint32_t val) {
	uint8_t res = 0;
	do {
		uint8_t byte = val & 127;
		val >>= 7;
		if (val != 0) {
			byte |= 128;
		}
		res++;
	} while (val != 0);
	return res;
}

struct ParquetCastOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return TGT(input);
	}
};

struct ParquetTimestampNSOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Timestamp::FromEpochNanoSeconds(input).value;
	}
};

struct ParquetTimestampSOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Timestamp::FromEpochSeconds(input).value;
	}
};

struct ParquetHugeintOperator {
	template <class SRC, class TGT>
	static TGT Operation(SRC input) {
		return Hugeint::Cast<double>(input);
	}
};

template <class SRC, class TGT, class OP = ParquetCastOperator>
static void TemplatedWritePlain(Vector &col, idx_t chunk_start, idx_t chunk_end, ValidityMask &mask, Serializer &ser) {
	auto *ptr = FlatVector::GetData<SRC>(col);
	for (idx_t r = chunk_start; r < chunk_end; r++) {
		if (mask.RowIsValid(r)) {
			ser.Write<TGT>(OP::template Operation<SRC, TGT>(ptr[r]));
		}
	}
}

ParquetWriter::ParquetWriter(FileSystem &fs, string file_name_p, FileOpener *file_opener_p, vector<LogicalType> types_p,
                             vector<string> names_p, CompressionCodec::type codec)
    : file_name(move(file_name_p)), sql_types(move(types_p)), column_names(move(names_p)), codec(codec) {
	// initialize the file writer
	writer = make_unique<BufferedFileWriter>(
	    fs, file_name.c_str(), FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW, file_opener_p);
	// parquet files start with the string "PAR1"
	writer->WriteData((const_data_ptr_t) "PAR1", 4);
	TCompactProtocolFactoryT<MyTransport> tproto_factory;
	protocol = tproto_factory.getProtocol(make_shared<MyTransport>(*writer));

	file_meta_data.num_rows = 0;
	file_meta_data.version = 1;

	file_meta_data.__isset.created_by = true;
	file_meta_data.created_by = "DuckDB";

	file_meta_data.schema.resize(sql_types.size() + 1);

	// populate root schema object
	file_meta_data.schema[0].name = "duckdb_schema";
	file_meta_data.schema[0].num_children = sql_types.size();
	file_meta_data.schema[0].__isset.num_children = true;

	for (idx_t i = 0; i < sql_types.size(); i++) {
		auto &schema_element = file_meta_data.schema[i + 1];

		schema_element.type = DuckDBTypeToParquetType(sql_types[i]);
		schema_element.repetition_type = FieldRepetitionType::OPTIONAL;
		schema_element.num_children = 0;
		schema_element.__isset.num_children = true;
		schema_element.__isset.type = true;
		schema_element.__isset.repetition_type = true;
		schema_element.name = column_names[i];
		schema_element.__isset.converted_type = DuckDBTypeToConvertedType(sql_types[i], schema_element.converted_type);
	}
}

idx_t ParquetWriter::MaxWriteCount(ChunkCollection &buffer, idx_t col_idx, idx_t chunk_idx, idx_t index_in_chunk,
                                   idx_t &out_max_chunk, idx_t &out_max_index_in_chunk) {
	D_ASSERT(buffer.ChunkCount() > 0);
	if (buffer.Types()[col_idx].InternalType() != PhysicalType::VARCHAR) {
		// for non-variable length types we know 100K rows will fit within 2GB (the max page size in Parquet)
		// so checking this is unnecessary
		out_max_chunk = buffer.ChunkCount();
		out_max_index_in_chunk = 0;
		return buffer.Count();
	}
	idx_t current_size = 0;
	idx_t write_count = 0;
	auto &chunks = buffer.Chunks();
	for (; chunk_idx < chunks.size();) {
		auto &input = *chunks[chunk_idx];
		auto &input_column = input.data[col_idx];
		auto &mask = FlatVector::Validity(input_column);
		auto *ptr = FlatVector::GetData<string_t>(input_column);
		for (; index_in_chunk < input.size(); index_in_chunk++) {
			if (mask.RowIsValid(index_in_chunk)) {
				current_size += ptr[index_in_chunk].GetSize() + sizeof(uint32_t);
				if (current_size >= MAX_UNCOMPRESSED_PAGE_SIZE) {
					index_in_chunk++;
					break;
				}
			}
			write_count++;
		}
		if (index_in_chunk >= input.size()) {
			index_in_chunk = 0;
			chunk_idx++;
		}
		if (current_size >= MAX_UNCOMPRESSED_PAGE_SIZE) {
			break;
		}
	}
	out_max_chunk = chunk_idx;
	out_max_index_in_chunk = index_in_chunk;
	return write_count;
}

void ParquetWriter::WriteVector(Serializer &temp_writer, DataChunk &input, idx_t col_idx, idx_t chunk_start,
                                idx_t chunk_end) {
	auto &input_column = input.data[col_idx];
	auto &mask = FlatVector::Validity(input_column);

	// write actual payload data
	switch (sql_types[col_idx].id()) {
	case LogicalTypeId::BOOLEAN: {
#if STANDARD_VECTOR_SIZE < 64
		throw InternalException("Writing booleans to Parquet not supported for vsize < 64");
#endif
		auto *ptr = FlatVector::GetData<bool>(input_column);
		uint8_t byte = 0;
		uint8_t byte_pos = 0;
		for (idx_t r = chunk_start; r < chunk_end; r++) {
			if (mask.RowIsValid(r)) { // only encode if non-null
				byte |= (ptr[r] & 1) << byte_pos;
				byte_pos++;

				if (byte_pos == 8) {
					temp_writer.Write<uint8_t>(byte);
					byte = 0;
					byte_pos = 0;
				}
			}
		}
		// flush last byte if req
		if (byte_pos > 0) {
			temp_writer.Write<uint8_t>(byte);
		}
		break;
	}
	case LogicalTypeId::TINYINT:
		TemplatedWritePlain<int8_t, int32_t>(input_column, chunk_start, chunk_end, mask, temp_writer);
		break;
	case LogicalTypeId::SMALLINT:
		TemplatedWritePlain<int16_t, int32_t>(input_column, chunk_start, chunk_end, mask, temp_writer);
		break;
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		TemplatedWritePlain<int32_t, int32_t>(input_column, chunk_start, chunk_end, mask, temp_writer);
		break;
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
		TemplatedWritePlain<int64_t, int64_t>(input_column, chunk_start, chunk_end, mask, temp_writer);
		break;
	case LogicalTypeId::HUGEINT:
		TemplatedWritePlain<hugeint_t, double, ParquetHugeintOperator>(input_column, chunk_start, chunk_end, mask,
		                                                               temp_writer);
		break;
	case LogicalTypeId::TIMESTAMP_NS:
		TemplatedWritePlain<int64_t, int64_t, ParquetTimestampNSOperator>(input_column, chunk_start, chunk_end, mask,
		                                                                  temp_writer);
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
		TemplatedWritePlain<int64_t, int64_t, ParquetTimestampSOperator>(input_column, chunk_start, chunk_end, mask,
		                                                                 temp_writer);
		break;
	case LogicalTypeId::UTINYINT:
		TemplatedWritePlain<uint8_t, int32_t>(input_column, chunk_start, chunk_end, mask, temp_writer);
		break;
	case LogicalTypeId::USMALLINT:
		TemplatedWritePlain<uint16_t, int32_t>(input_column, chunk_start, chunk_end, mask, temp_writer);
		break;
	case LogicalTypeId::UINTEGER:
		TemplatedWritePlain<uint32_t, uint32_t>(input_column, chunk_start, chunk_end, mask, temp_writer);
		break;
	case LogicalTypeId::UBIGINT:
		TemplatedWritePlain<uint64_t, uint64_t>(input_column, chunk_start, chunk_end, mask, temp_writer);
		break;
	case LogicalTypeId::FLOAT:
		TemplatedWritePlain<float, float>(input_column, chunk_start, chunk_end, mask, temp_writer);
		break;
	case LogicalTypeId::DECIMAL: {
		// FIXME: fixed length byte array...
		Vector double_vec(LogicalType::DOUBLE);
		VectorOperations::Cast(input_column, double_vec, input.size());
		TemplatedWritePlain<double, double>(double_vec, chunk_start, chunk_end, mask, temp_writer);
		break;
	}
	case LogicalTypeId::DOUBLE:
		TemplatedWritePlain<double, double>(input_column, chunk_start, chunk_end, mask, temp_writer);
		break;
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR: {
		auto *ptr = FlatVector::GetData<string_t>(input_column);
		for (idx_t r = chunk_start; r < chunk_end; r++) {
			if (mask.RowIsValid(r)) {
				temp_writer.Write<uint32_t>(ptr[r].GetSize());
				temp_writer.WriteData((const_data_ptr_t)ptr[r].GetDataUnsafe(), ptr[r].GetSize());
			}
		}
		break;
	}
	default:
		throw NotImplementedException((sql_types[col_idx].ToString()));
	}
}

void ParquetWriter::WriteColumn(ChunkCollection &buffer, idx_t col_idx, idx_t chunk_idx, idx_t index_in_chunk,
                                idx_t max_chunk, idx_t max_index_in_chunk, idx_t write_count) {
	// we start off by writing everything into a temporary buffer
	// this is necessary to (1) know the total written size, and (2) to compress it afterwards
	BufferedSerializer temp_writer;

	// set up some metadata
	PageHeader hdr;
	hdr.compressed_page_size = 0;
	hdr.uncompressed_page_size = 0;
	hdr.type = PageType::DATA_PAGE;
	hdr.__isset.data_page_header = true;

	hdr.data_page_header.num_values = write_count;
	hdr.data_page_header.encoding = Encoding::PLAIN;
	hdr.data_page_header.definition_level_encoding = Encoding::RLE;
	hdr.data_page_header.repetition_level_encoding = Encoding::BIT_PACKED;

	// write the definition levels (i.e. the inverse of the nullmask)
	// we always bit pack everything

	// first figure out how many bytes we need (1 byte per 8 rows, rounded up)
	auto define_byte_count = (write_count + 7) / 8;
	// we need to set up the count as a varint, plus an added marker for the RLE scheme
	// for this marker we shift the count left 1 and set low bit to 1 to indicate bit packed literals
	uint32_t define_header = (define_byte_count << 1) | 1;
	uint32_t define_size = GetVarintSize(define_header) + define_byte_count;

	// write the number of defines as a varint
	temp_writer.Write<uint32_t>(define_size);
	VarintEncode(define_header, temp_writer);

	auto &chunks = buffer.Chunks();
	if (max_index_in_chunk == 0) {
		max_chunk--;
		max_index_in_chunk = chunks[max_chunk]->size();
	}
	// construct a large validity mask holding all the validity bits we need to write
	ValidityMask result_mask(write_count);
	idx_t current_index = 0;
	for (idx_t i = chunk_idx; i <= max_chunk; i++) {
		auto &input = *chunks[i];
		auto start_row = i == chunk_idx ? index_in_chunk : 0;
		auto end_row = i == max_chunk ? max_index_in_chunk : input.size();
		auto &validity = FlatVector::Validity(input.data[col_idx]);
		for (idx_t i = start_row; i < end_row; i++) {
			result_mask.Set(current_index++, validity.RowIsValid(i));
		}
	}
	// actually write out the validity bits
	temp_writer.WriteData((const_data_ptr_t)result_mask.GetData(), define_byte_count);

	// now write the actual payload: we always write this as PLAIN values for now
	for (idx_t i = chunk_idx; i <= max_chunk; i++) {
		auto &input = *chunks[i];
		auto start_row = i == chunk_idx ? index_in_chunk : 0;
		auto end_row = i == max_chunk ? max_index_in_chunk : input.size();
		WriteVector(temp_writer, input, col_idx, start_row, end_row);
	}

	// now that we have finished writing the data we know the uncompressed size
	if (temp_writer.blob.size > idx_t(NumericLimits<int32_t>::Maximum())) {
		throw InternalException("Parquet writer: %d uncompressed page size out of range for type integer",
		                        temp_writer.blob.size);
	}
	hdr.uncompressed_page_size = temp_writer.blob.size;

	// compress the data based
	size_t compressed_size;
	data_ptr_t compressed_data;
	unique_ptr<data_t[]> compressed_buf;
	switch (codec) {
	case CompressionCodec::UNCOMPRESSED:
		compressed_size = temp_writer.blob.size;
		compressed_data = temp_writer.blob.data.get();
		break;
	case CompressionCodec::SNAPPY: {
		compressed_size = duckdb_snappy::MaxCompressedLength(temp_writer.blob.size);
		compressed_buf = unique_ptr<data_t[]>(new data_t[compressed_size]);
		duckdb_snappy::RawCompress((const char *)temp_writer.blob.data.get(), temp_writer.blob.size,
		                           (char *)compressed_buf.get(), &compressed_size);
		compressed_data = compressed_buf.get();
		D_ASSERT(compressed_size <= duckdb_snappy::MaxCompressedLength(temp_writer.blob.size));
		break;
	}
	case CompressionCodec::GZIP: {
		MiniZStream s;
		compressed_size = s.MaxCompressedLength(temp_writer.blob.size);
		compressed_buf = unique_ptr<data_t[]>(new data_t[compressed_size]);
		s.Compress((const char *)temp_writer.blob.data.get(), temp_writer.blob.size, (char *)compressed_buf.get(),
		           &compressed_size);
		compressed_data = compressed_buf.get();
		break;
	}
	case CompressionCodec::ZSTD: {
		compressed_size = duckdb_zstd::ZSTD_compressBound(temp_writer.blob.size);
		compressed_buf = unique_ptr<data_t[]>(new data_t[compressed_size]);
		compressed_size = duckdb_zstd::ZSTD_compress((void *)compressed_buf.get(), compressed_size,
		                                             (const void *)temp_writer.blob.data.get(), temp_writer.blob.size,
		                                             ZSTD_CLEVEL_DEFAULT);
		compressed_data = compressed_buf.get();
		break;
	}
	default:
		throw InternalException("Unsupported codec for Parquet Writer");
	}

	if (compressed_size > idx_t(NumericLimits<int32_t>::Maximum())) {
		throw InternalException("Parquet writer: %d compressed page size out of range for type integer",
		                        temp_writer.blob.size);
	}
	hdr.compressed_page_size = compressed_size;
	// now finally write the data to the actual file
	hdr.write(protocol.get());
	writer->WriteData(compressed_data, compressed_size);
}

void ParquetWriter::Flush(ChunkCollection &buffer) {
	if (buffer.Count() == 0) {
		return;
	}
	lock_guard<mutex> glock(lock);

	// set up a new row group for this chunk collection
	ParquetRowGroup row_group;
	row_group.num_rows = 0;
	row_group.file_offset = writer->GetTotalWritten();
	row_group.__isset.file_offset = true;
	row_group.columns.resize(buffer.ColumnCount());

	// iterate over each of the columns of the chunk collection and write them
	for (idx_t col_idx = 0; col_idx < buffer.ColumnCount(); col_idx++) {
		// record the current offset of the writer into the file
		// this is the starting position of the current column
		auto start_offset = writer->GetTotalWritten();

		idx_t chunk_idx = 0;
		idx_t index_in_chunk = 0;
		while (chunk_idx < buffer.ChunkCount()) {
			idx_t max_chunk_idx;
			idx_t max_index_in_chunk;
			// check how many rows we can write in a single page
			idx_t write_count =
			    MaxWriteCount(buffer, col_idx, chunk_idx, index_in_chunk, max_chunk_idx, max_index_in_chunk);
			D_ASSERT(write_count > 0);
			D_ASSERT(max_chunk_idx >= chunk_idx);
			D_ASSERT(max_chunk_idx > chunk_idx || max_index_in_chunk > index_in_chunk);

			// now write that many rows to the file as a single page
			WriteColumn(buffer, col_idx, chunk_idx, index_in_chunk, max_chunk_idx, max_index_in_chunk, write_count);
			chunk_idx = max_chunk_idx;
			index_in_chunk = max_index_in_chunk;
		}

		auto &column_chunk = row_group.columns[col_idx];
		column_chunk.__isset.meta_data = true;
		column_chunk.meta_data.data_page_offset = start_offset;
		column_chunk.meta_data.total_compressed_size = writer->GetTotalWritten() - start_offset;
		column_chunk.meta_data.codec = codec;
		column_chunk.meta_data.path_in_schema.push_back(file_meta_data.schema[col_idx + 1].name);
		column_chunk.meta_data.num_values = buffer.Count();
		column_chunk.meta_data.type = file_meta_data.schema[col_idx + 1].type;
	}
	row_group.num_rows += buffer.Count();

	// append the row group to the file meta data
	file_meta_data.row_groups.push_back(row_group);
	file_meta_data.num_rows += buffer.Count();
}

void ParquetWriter::Finalize() {
	auto start_offset = writer->GetTotalWritten();
	file_meta_data.write(protocol.get());

	writer->Write<uint32_t>(writer->GetTotalWritten() - start_offset);

	// parquet files also end with the string "PAR1"
	writer->WriteData((const_data_ptr_t) "PAR1", 4);

	// flush to disk
	writer->Sync();
	writer.reset();
}

} // namespace duckdb
