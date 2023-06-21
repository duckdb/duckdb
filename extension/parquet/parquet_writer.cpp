#include "parquet_writer.hpp"

#include "duckdb.hpp"
#include "parquet_timestamp.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
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
		serializer.WriteData(const_data_ptr_cast(buf), len);
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
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BIT:
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
		schema_ele.__isset.logicalType = true;
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

void VerifyUniqueNames(const vector<string> &names) {
#ifdef DEBUG
	unordered_set<string> name_set;
	name_set.reserve(names.size());
	for (auto &column : names) {
		auto res = name_set.insert(column);
		D_ASSERT(res.second == true);
	}
	// If there would be duplicates, these sizes would differ
	D_ASSERT(name_set.size() == names.size());
#endif
}

ParquetWriter::ParquetWriter(FileSystem &fs, string file_name_p, vector<LogicalType> types_p, vector<string> names_p,
                             CompressionCodec::type codec)
    : file_name(std::move(file_name_p)), sql_types(std::move(types_p)), column_names(std::move(names_p)), codec(codec) {
	// initialize the file writer
	writer = make_uniq<BufferedFileWriter>(fs, file_name.c_str(),
	                                       FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	// parquet files start with the string "PAR1"
	writer->WriteData(const_data_ptr_cast("PAR1"), 4);
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

	auto &unique_names = column_names;
	VerifyUniqueNames(unique_names);

	vector<string> schema_path;
	for (idx_t i = 0; i < sql_types.size(); i++) {
		column_writers.push_back(ColumnWriter::CreateWriterRecursive(file_meta_data.schema, *this, sql_types[i],
		                                                             unique_names[i], schema_path));
	}
}

void ParquetWriter::PrepareRowGroup(ColumnDataCollection &buffer, PreparedRowGroup &result) {
	// set up a new row group for this chunk collection
	auto &row_group = result.row_group;
	row_group.num_rows = buffer.Count();
	row_group.__isset.file_offset = true;

	auto &states = result.states;
	// iterate over each of the columns of the chunk collection and write them
	D_ASSERT(buffer.ColumnCount() == column_writers.size());
	for (idx_t col_idx = 0; col_idx < buffer.ColumnCount(); col_idx++) {
		const auto &col_writer = column_writers[col_idx];
		auto write_state = col_writer->InitializeWriteState(row_group, buffer.GetAllocator());
		if (col_writer->HasAnalyze()) {
			for (auto &chunk : buffer.Chunks()) {
				col_writer->Analyze(*write_state, nullptr, chunk.data[col_idx], chunk.size());
			}
			col_writer->FinalizeAnalyze(*write_state);
		}
		for (auto &chunk : buffer.Chunks()) {
			col_writer->Prepare(*write_state, nullptr, chunk.data[col_idx], chunk.size());
		}
		col_writer->BeginWrite(*write_state);
		for (auto &chunk : buffer.Chunks()) {
			col_writer->Write(*write_state, chunk.data[col_idx], chunk.size());
		}
		states.push_back(std::move(write_state));
	}
}

void ParquetWriter::FlushRowGroup(PreparedRowGroup &prepared) {
	lock_guard<mutex> glock(lock);
	auto &row_group = prepared.row_group;
	auto &states = prepared.states;
	if (states.empty()) {
		throw InternalException("Attempting to flush a row group with no rows");
	}
	row_group.file_offset = writer->GetTotalWritten();
	for (idx_t col_idx = 0; col_idx < states.size(); col_idx++) {
		const auto &col_writer = column_writers[col_idx];
		auto write_state = std::move(states[col_idx]);
		col_writer->FinalizeWrite(*write_state);
	}

	// append the row group to the file meta data
	file_meta_data.row_groups.push_back(row_group);
	file_meta_data.num_rows += row_group.num_rows;
}

void ParquetWriter::Flush(ColumnDataCollection &buffer) {
	if (buffer.Count() == 0) {
		return;
	}

	PreparedRowGroup prepared_row_group;
	PrepareRowGroup(buffer, prepared_row_group);

	FlushRowGroup(prepared_row_group);
}

void ParquetWriter::Finalize() {
	auto start_offset = writer->GetTotalWritten();
	file_meta_data.write(protocol.get());

	writer->Write<uint32_t>(writer->GetTotalWritten() - start_offset);

	// parquet files also end with the string "PAR1"
	writer->WriteData(const_data_ptr_cast("PAR1"), 4);

	// flush to disk
	writer->Sync();
	writer.reset();
}

} // namespace duckdb
