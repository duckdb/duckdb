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
		return Type::INT32;
	case LogicalTypeId::BIGINT:
		return Type::INT64;
	case LogicalTypeId::FLOAT:
		return Type::FLOAT;
	case LogicalTypeId::DECIMAL: // for now...
	case LogicalTypeId::DOUBLE:
		return Type::DOUBLE;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return Type::BYTE_ARRAY;
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
		return Type::INT96;
	default:
		throw NotImplementedException(duckdb_type.ToString());
	}
}

static bool DuckDBTypeToConvertedType(const LogicalType &duckdb_type, ConvertedType::type &result) {
	switch (duckdb_type.id()) {
	case LogicalTypeId::VARCHAR:
		result = ConvertedType::UTF8;
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

template <class SRC, class TGT>
static void TemplatedWritePlain(Vector &col, idx_t length, ValidityMask &mask, Serializer &ser) {
	auto *ptr = FlatVector::GetData<SRC>(col);
	for (idx_t r = 0; r < length; r++) {
		if (mask.RowIsValid(r)) {
			ser.Write<TGT>((TGT)ptr[r]);
		}
	}
}

ParquetWriter::ParquetWriter(FileSystem &fs, string file_name_p, vector<LogicalType> types_p, vector<string> names_p,
                             CompressionCodec::type codec)
    : file_name(move(file_name_p)), sql_types(move(types_p)), column_names(move(names_p)), codec(codec) {
	// initialize the file writer
	writer = make_unique<BufferedFileWriter>(fs, file_name.c_str(),
	                                         FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	// parquet files start with the string "PAR1"
	writer->WriteData((const_data_ptr_t) "PAR1", 4);
	TCompactProtocolFactoryT<MyTransport> tproto_factory;
	protocol = tproto_factory.getProtocol(make_shared<MyTransport>(*writer));
	file_meta_data.num_rows = 0;
	file_meta_data.schema.resize(sql_types.size() + 1);

	file_meta_data.schema[0].num_children = sql_types.size();
	file_meta_data.schema[0].__isset.num_children = true;
	file_meta_data.version = 1;

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

void ParquetWriter::Flush(ChunkCollection &buffer) {
	if (buffer.Count() == 0) {
		return;
	}
	lock_guard<mutex> glock(lock);

	// set up a new row group for this chunk collection
	RowGroup row_group;
	row_group.num_rows = 0;
	row_group.file_offset = writer->GetTotalWritten();
	row_group.__isset.file_offset = true;
	row_group.columns.resize(buffer.ColumnCount());

	// iterate over each of the columns of the chunk collection and write them
	for (idx_t i = 0; i < buffer.ColumnCount(); i++) {
		// we start off by writing everything into a temporary buffer
		// this is necessary to (1) know the total written size, and (2) to compress it afterwards
		BufferedSerializer temp_writer;

		// set up some metadata
		PageHeader hdr;
		hdr.compressed_page_size = 0;
		hdr.uncompressed_page_size = 0;
		hdr.type = PageType::DATA_PAGE;
		hdr.__isset.data_page_header = true;

		hdr.data_page_header.num_values = buffer.Count();
		hdr.data_page_header.encoding = Encoding::PLAIN;
		hdr.data_page_header.definition_level_encoding = Encoding::RLE;
		hdr.data_page_header.repetition_level_encoding = Encoding::BIT_PACKED;

		// record the current offset of the writer into the file
		// this is the starting position of the current page
		auto start_offset = writer->GetTotalWritten();

		// write the definition levels (i.e. the inverse of the nullmask)
		// we always bit pack everything

		// first figure out how many bytes we need (1 byte per 8 rows, rounded up)
		auto define_byte_count = (buffer.Count() + 7) / 8;
		// we need to set up the count as a varint, plus an added marker for the RLE scheme
		// for this marker we shift the count left 1 and set low bit to 1 to indicate bit packed literals
		uint32_t define_header = (define_byte_count << 1) | 1;
		uint32_t define_size = GetVarintSize(define_header) + define_byte_count;

		// we write the actual definitions into the temp_writer for now
		temp_writer.Write<uint32_t>(define_size);
		VarintEncode(define_header, temp_writer);

		for (auto &chunk : buffer.Chunks()) {
			auto &validity = FlatVector::Validity(chunk->data[i]);
			auto validity_data = validity.GetData();
			auto chunk_define_byte_count = (chunk->size() + 7) / 8;
			if (!validity_data) {
				ValidityMask nop_mask(chunk->size());
				temp_writer.WriteData((const_data_ptr_t)nop_mask.GetData(), chunk_define_byte_count);
			} else {
				// write the bits of the nullmask
				temp_writer.WriteData((const_data_ptr_t)validity_data, chunk_define_byte_count);
			}
		}

		// now write the actual payload: we write this as PLAIN values (for now? possibly for ever?)
		for (auto &chunk : buffer.Chunks()) {
			auto &input = *chunk;
			auto &input_column = input.data[i];
			auto &mask = FlatVector::Validity(input_column);

			// write actual payload data
			switch (sql_types[i].id()) {
			case LogicalTypeId::BOOLEAN: {
				auto *ptr = FlatVector::GetData<bool>(input_column);
				uint8_t byte = 0;
				uint8_t byte_pos = 0;
				for (idx_t r = 0; r < input.size(); r++) {
					if (mask.RowIsValid(r)) { // only encode if non-null
						byte |= (ptr[r] & 1) << byte_pos;
						byte_pos++;

						temp_writer.Write<uint8_t>(byte);
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
				TemplatedWritePlain<int8_t, int32_t>(input_column, input.size(), mask, temp_writer);
				break;
			case LogicalTypeId::SMALLINT:
				TemplatedWritePlain<int16_t, int32_t>(input_column, input.size(), mask, temp_writer);
				break;
			case LogicalTypeId::INTEGER:
				TemplatedWritePlain<int32_t, int32_t>(input_column, input.size(), mask, temp_writer);
				break;
			case LogicalTypeId::BIGINT:
				TemplatedWritePlain<int64_t, int64_t>(input_column, input.size(), mask, temp_writer);
				break;
			case LogicalTypeId::FLOAT:
				TemplatedWritePlain<float, float>(input_column, input.size(), mask, temp_writer);
				break;
			case LogicalTypeId::DECIMAL: {
				// FIXME: fixed length byte array...
				Vector double_vec(LogicalType::DOUBLE);
				VectorOperations::Cast(input_column, double_vec, input.size());
				TemplatedWritePlain<double, double>(double_vec, input.size(), mask, temp_writer);
				break;
			}
			case LogicalTypeId::DOUBLE:
				TemplatedWritePlain<double, double>(input_column, input.size(), mask, temp_writer);
				break;
			case LogicalTypeId::DATE: {
				auto *ptr = FlatVector::GetData<date_t>(input_column);
				for (idx_t r = 0; r < input.size(); r++) {
					if (mask.RowIsValid(r)) {
						auto ts = Timestamp::FromDatetime(ptr[r], dtime_t(0));
						temp_writer.Write<Int96>(TimestampToImpalaTimestamp(ts));
					}
				}
				break;
			}
			case LogicalTypeId::TIMESTAMP: {
				auto *ptr = FlatVector::GetData<timestamp_t>(input_column);
				for (idx_t r = 0; r < input.size(); r++) {
					if (mask.RowIsValid(r)) {
						temp_writer.Write<Int96>(TimestampToImpalaTimestamp(ptr[r]));
					}
				}
				break;
			}
			case LogicalTypeId::BLOB:
			case LogicalTypeId::VARCHAR: {
				auto *ptr = FlatVector::GetData<string_t>(input_column);
				for (idx_t r = 0; r < input.size(); r++) {
					if (mask.RowIsValid(r)) {
						temp_writer.Write<uint32_t>(ptr[r].GetSize());
						temp_writer.WriteData((const_data_ptr_t)ptr[r].GetDataUnsafe(), ptr[r].GetSize());
					}
				}
				break;
			}
			default:
				throw NotImplementedException((sql_types[i].ToString()));
			}
		}

		// now that we have finished writing the data we know the uncompressed size
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
			compressed_size = snappy::MaxCompressedLength(temp_writer.blob.size);
			compressed_buf = unique_ptr<data_t[]>(new data_t[compressed_size]);
			snappy::RawCompress((const char *)temp_writer.blob.data.get(), temp_writer.blob.size,
			                    (char *)compressed_buf.get(), &compressed_size);
			compressed_data = compressed_buf.get();
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
			                                             (const void *)temp_writer.blob.data.get(),
			                                             temp_writer.blob.size, ZSTD_CLEVEL_DEFAULT);
			compressed_data = compressed_buf.get();
			break;
		}
		default:
			throw InternalException("Unsupported codec for Parquet Writer");
		}

		hdr.compressed_page_size = compressed_size;
		// now finally write the data to the actual file
		hdr.write(protocol.get());
		writer->WriteData(compressed_data, compressed_size);

		auto &column_chunk = row_group.columns[i];
		column_chunk.__isset.meta_data = true;
		column_chunk.meta_data.data_page_offset = start_offset;
		column_chunk.meta_data.total_compressed_size = writer->GetTotalWritten() - start_offset;
		column_chunk.meta_data.codec = codec;
		column_chunk.meta_data.path_in_schema.push_back(file_meta_data.schema[i + 1].name);
		column_chunk.meta_data.num_values = buffer.Count();
		column_chunk.meta_data.type = file_meta_data.schema[i + 1].type;
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
