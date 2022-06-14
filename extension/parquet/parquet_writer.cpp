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
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#endif

namespace duckdb {

using namespace duckdb_apache::thrift;            // NOLINT
using namespace duckdb_apache::thrift::protocol;  // NOLINT
using namespace duckdb_apache::thrift::transport; // NOLINT

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

Type::type ParquetWriter::DuckDBTypeToParquetType(const LogicalType &duckdb_type) {
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
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::HUGEINT:
		return Type::DOUBLE;
	case LogicalTypeId::ENUM:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
	case LogicalTypeId::BLOB:
		return Type::BYTE_ARRAY;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
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
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::UUID:
		return Type::FIXED_LEN_BYTE_ARRAY;
	case LogicalTypeId::DECIMAL:
		switch (duckdb_type.InternalType()) {
		case PhysicalType::INT16:
		case PhysicalType::INT32:
			return Type::INT32;
		case PhysicalType::INT64:
			return Type::INT64;
		case PhysicalType::INT128:
			return Type::FIXED_LEN_BYTE_ARRAY;
		default:
			throw InternalException("Unsupported internal decimal type");
		}
	default:
		throw NotImplementedException("Unimplemented type for Parquet \"%s\"", duckdb_type.ToString());
	}
}

void ParquetWriter::SetSchemaProperties(const LogicalType &duckdb_type,
                                        duckdb_parquet::format::SchemaElement &schema_ele) {
	switch (duckdb_type.id()) {
	case LogicalTypeId::TINYINT:
		schema_ele.converted_type = ConvertedType::INT_8;
		schema_ele.__isset.converted_type = true;
		break;
	case LogicalTypeId::SMALLINT:
		schema_ele.converted_type = ConvertedType::INT_16;
		schema_ele.__isset.converted_type = true;
		break;
	case LogicalTypeId::INTEGER:
		schema_ele.converted_type = ConvertedType::INT_32;
		schema_ele.__isset.converted_type = true;
		break;
	case LogicalTypeId::BIGINT:
		schema_ele.converted_type = ConvertedType::INT_64;
		schema_ele.__isset.converted_type = true;
		break;
	case LogicalTypeId::UTINYINT:
		schema_ele.converted_type = ConvertedType::UINT_8;
		schema_ele.__isset.converted_type = true;
		break;
	case LogicalTypeId::USMALLINT:
		schema_ele.converted_type = ConvertedType::UINT_16;
		schema_ele.__isset.converted_type = true;
		break;
	case LogicalTypeId::UINTEGER:
		schema_ele.converted_type = ConvertedType::UINT_32;
		schema_ele.__isset.converted_type = true;
		break;
	case LogicalTypeId::UBIGINT:
		schema_ele.converted_type = ConvertedType::UINT_64;
		schema_ele.__isset.converted_type = true;
		break;
	case LogicalTypeId::DATE:
		schema_ele.converted_type = ConvertedType::DATE;
		schema_ele.__isset.converted_type = true;
		break;
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIME:
		schema_ele.converted_type = ConvertedType::TIME_MICROS;
		schema_ele.__isset.converted_type = true;
		schema_ele.logicalType.__isset.TIME = true;
		schema_ele.logicalType.TIME.isAdjustedToUTC = (duckdb_type.id() == LogicalTypeId::TIME_TZ);
		schema_ele.logicalType.TIME.unit.__isset.MICROS = true;
		break;
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
		schema_ele.converted_type = ConvertedType::TIMESTAMP_MICROS;
		schema_ele.__isset.converted_type = true;
		schema_ele.__isset.logicalType = true;
		schema_ele.logicalType.__isset.TIMESTAMP = true;
		schema_ele.logicalType.TIMESTAMP.isAdjustedToUTC = (duckdb_type.id() == LogicalTypeId::TIMESTAMP_TZ);
		schema_ele.logicalType.TIMESTAMP.unit.__isset.MICROS = true;
		break;
	case LogicalTypeId::TIMESTAMP_MS:
		schema_ele.converted_type = ConvertedType::TIMESTAMP_MILLIS;
		schema_ele.__isset.converted_type = true;
		schema_ele.__isset.logicalType = true;
		schema_ele.logicalType.__isset.TIMESTAMP = true;
		schema_ele.logicalType.TIMESTAMP.isAdjustedToUTC = false;
		schema_ele.logicalType.TIMESTAMP.unit.__isset.MILLIS = true;
		break;
	case LogicalTypeId::ENUM:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		schema_ele.converted_type = ConvertedType::UTF8;
		schema_ele.__isset.converted_type = true;
		break;
	case LogicalTypeId::INTERVAL:
		schema_ele.type_length = 12;
		schema_ele.converted_type = ConvertedType::INTERVAL;
		schema_ele.__isset.type_length = true;
		schema_ele.__isset.converted_type = true;
		break;
	case LogicalTypeId::UUID:
		schema_ele.type_length = 16;
		schema_ele.__isset.type_length = true;
		schema_ele.__isset.logicalType = true;
		schema_ele.logicalType.__isset.UUID = true;
		break;
	case LogicalTypeId::DECIMAL:
		schema_ele.converted_type = ConvertedType::DECIMAL;
		schema_ele.precision = DecimalType::GetWidth(duckdb_type);
		schema_ele.scale = DecimalType::GetScale(duckdb_type);
		schema_ele.__isset.converted_type = true;
		schema_ele.__isset.precision = true;
		schema_ele.__isset.scale = true;
		if (duckdb_type.InternalType() == PhysicalType::INT128) {
			schema_ele.type_length = 16;
			schema_ele.__isset.type_length = true;
		}
		schema_ele.__isset.logicalType = true;
		schema_ele.logicalType.__isset.DECIMAL = true;
		schema_ele.logicalType.DECIMAL.precision = schema_ele.precision;
		schema_ele.logicalType.DECIMAL.scale = schema_ele.scale;
		break;
	default:
		break;
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

	file_meta_data.schema.resize(1);

	// populate root schema object
	file_meta_data.schema[0].name = "duckdb_schema";
	file_meta_data.schema[0].num_children = sql_types.size();
	file_meta_data.schema[0].__isset.num_children = true;
	file_meta_data.schema[0].repetition_type = duckdb_parquet::format::FieldRepetitionType::REQUIRED;
	file_meta_data.schema[0].__isset.repetition_type = true;

	vector<string> schema_path;
	for (idx_t i = 0; i < sql_types.size(); i++) {
		column_writers.push_back(ColumnWriter::CreateWriterRecursive(file_meta_data.schema, *this, sql_types[i],
		                                                             column_names[i], schema_path));
	}
}

void ParquetWriter::Flush(ChunkCollection &buffer) {
	if (buffer.Count() == 0) {
		return;
	}
	lock_guard<mutex> glock(lock);

	// set up a new row group for this chunk collection
	ParquetRowGroup row_group;
	row_group.num_rows = buffer.Count();
	row_group.file_offset = writer->GetTotalWritten();
	row_group.__isset.file_offset = true;

	// iterate over each of the columns of the chunk collection and write them
	auto &chunks = buffer.Chunks();
	D_ASSERT(buffer.ColumnCount() == column_writers.size());
	for (idx_t col_idx = 0; col_idx < buffer.ColumnCount(); col_idx++) {
		auto write_state = column_writers[col_idx]->InitializeWriteState(row_group);
		for (idx_t chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
			column_writers[col_idx]->Prepare(*write_state, nullptr, chunks[chunk_idx]->data[col_idx],
			                                 chunks[chunk_idx]->size());
		}
		column_writers[col_idx]->BeginWrite(*write_state);
		for (idx_t chunk_idx = 0; chunk_idx < chunks.size(); chunk_idx++) {
			column_writers[col_idx]->Write(*write_state, chunks[chunk_idx]->data[col_idx], chunks[chunk_idx]->size());
		}
		column_writers[col_idx]->FinalizeWrite(*write_state);
	}

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
