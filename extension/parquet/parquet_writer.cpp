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
#include "duckdb/common/serializer/write_stream.hpp"
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

ChildFieldIDs::ChildFieldIDs() {
	ids = make_uniq<case_insensitive_map_t<FieldID>>();
}

ChildFieldIDs ChildFieldIDs::Copy() const {
	ChildFieldIDs result;
	for (const auto &id : *ids) {
		result.ids->emplace(id.first, id.second.Copy());
	}
	return result;
}

FieldID::FieldID() : set(false) {
}

FieldID::FieldID(int32_t field_id_p) : set(true), field_id(field_id_p) {
}

FieldID FieldID::Copy() const {
	auto result = set ? FieldID(field_id) : FieldID();
	result.child_field_ids = child_field_ids.Copy();
	return result;
}

class MyTransport : public TTransport {
public:
	explicit MyTransport(WriteStream &serializer) : serializer(serializer) {
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
	WriteStream &serializer;
};

CopyTypeSupport ParquetWriter::DuckDBTypeToParquetTypeInternal(const LogicalType &duckdb_type,
                                                               Type::type &parquet_type) {
	switch (duckdb_type.id()) {
	case LogicalTypeId::BOOLEAN:
		parquet_type = Type::BOOLEAN;
		break;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		parquet_type = Type::INT32;
		break;
	case LogicalTypeId::BIGINT:
		parquet_type = Type::INT64;
		break;
	case LogicalTypeId::FLOAT:
		parquet_type = Type::FLOAT;
		break;
	case LogicalTypeId::DOUBLE:
		parquet_type = Type::DOUBLE;
		break;
	case LogicalTypeId::HUGEINT:
		parquet_type = Type::DOUBLE;
		return CopyTypeSupport::LOSSY;
	case LogicalTypeId::ENUM:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		parquet_type = Type::BYTE_ARRAY;
		break;
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
		parquet_type = Type::INT64;
		break;
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		parquet_type = Type::INT32;
		break;
	case LogicalTypeId::UBIGINT:
		parquet_type = Type::INT64;
		break;
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::UUID:
		parquet_type = Type::FIXED_LEN_BYTE_ARRAY;
		break;
	case LogicalTypeId::DECIMAL:
		switch (duckdb_type.InternalType()) {
		case PhysicalType::INT16:
		case PhysicalType::INT32:
			parquet_type = Type::INT32;
			break;
		case PhysicalType::INT64:
			parquet_type = Type::INT64;
			break;
		case PhysicalType::INT128:
			parquet_type = Type::FIXED_LEN_BYTE_ARRAY;
			break;
		default:
			throw InternalException("Unsupported internal decimal type");
		}
		break;
	default:
		// Anything that is not supported
		return CopyTypeSupport::UNSUPPORTED;
	}
	return CopyTypeSupport::SUPPORTED;
}

Type::type ParquetWriter::DuckDBTypeToParquetType(const LogicalType &duckdb_type) {
	Type::type result;
	auto type_supports = DuckDBTypeToParquetTypeInternal(duckdb_type, result);
	if (type_supports == CopyTypeSupport::UNSUPPORTED) {
		throw NotImplementedException("Unimplemented type for Parquet \"%s\"", duckdb_type.ToString());
	}
	return result;
}

CopyTypeSupport ParquetWriter::TypeIsSupported(const LogicalType &type) {
	Type::type unused;
	auto id = type.id();
	if (id == LogicalTypeId::LIST) {
		auto &child_type = ListType::GetChildType(type);
		return TypeIsSupported(child_type);
	}
	if (id == LogicalTypeId::UNION) {
		auto count = UnionType::GetMemberCount(type);
		for (idx_t i = 0; i < count; i++) {
			auto &member_type = UnionType::GetMemberType(type, i);
			auto type_support = TypeIsSupported(member_type);
			if (type_support != CopyTypeSupport::SUPPORTED) {
				return type_support;
			}
		}
		return CopyTypeSupport::SUPPORTED;
	}
	if (id == LogicalTypeId::STRUCT) {
		auto &children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			auto &child_type = child.second;
			auto type_support = TypeIsSupported(child_type);
			if (type_support != CopyTypeSupport::SUPPORTED) {
				return type_support;
			}
		}
		return CopyTypeSupport::SUPPORTED;
	}
	if (id == LogicalTypeId::MAP) {
		auto &key_type = MapType::KeyType(type);
		auto &value_type = MapType::ValueType(type);
		auto key_type_support = TypeIsSupported(key_type);
		if (key_type_support != CopyTypeSupport::SUPPORTED) {
			return key_type_support;
		}
		auto value_type_support = TypeIsSupported(value_type);
		if (value_type_support != CopyTypeSupport::SUPPORTED) {
			return value_type_support;
		}
		return CopyTypeSupport::SUPPORTED;
	}
	return DuckDBTypeToParquetTypeInternal(type, unused);
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
                             CompressionCodec::type codec, ChildFieldIDs field_ids_p)
    : file_name(std::move(file_name_p)), sql_types(std::move(types_p)), column_names(std::move(names_p)), codec(codec),
      field_ids(std::move(field_ids_p)) {
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
		                                                             unique_names[i], schema_path, &field_ids));
	}
}

void ParquetWriter::PrepareRowGroup(ColumnDataCollection &buffer, PreparedRowGroup &result) {
	// We write 8 columns at a time so that iterating over ColumnDataCollection is more efficient
	static constexpr idx_t COLUMNS_PER_PASS = 8;

	// We want these to be in-memory/hybrid so we don't have to copy over strings to the dictionary
	D_ASSERT(buffer.GetAllocatorType() == ColumnDataAllocatorType::IN_MEMORY_ALLOCATOR ||
	         buffer.GetAllocatorType() == ColumnDataAllocatorType::HYBRID);

	// set up a new row group for this chunk collection
	auto &row_group = result.row_group;
	row_group.num_rows = buffer.Count();
	row_group.total_byte_size = buffer.SizeInBytes();
	row_group.__isset.file_offset = true;

	auto &states = result.states;
	// iterate over each of the columns of the chunk collection and write them
	D_ASSERT(buffer.ColumnCount() == column_writers.size());
	for (idx_t col_idx = 0; col_idx < buffer.ColumnCount(); col_idx += COLUMNS_PER_PASS) {
		const auto next = MinValue<idx_t>(buffer.ColumnCount() - col_idx, COLUMNS_PER_PASS);
		vector<column_t> column_ids;
		vector<reference<ColumnWriter>> col_writers;
		vector<unique_ptr<ColumnWriterState>> write_states;
		for (idx_t i = 0; i < next; i++) {
			column_ids.emplace_back(col_idx + i);
			col_writers.emplace_back(*column_writers[column_ids.back()]);
			write_states.emplace_back(col_writers.back().get().InitializeWriteState(row_group));
		}

		for (auto &chunk : buffer.Chunks({column_ids})) {
			for (idx_t i = 0; i < next; i++) {
				if (col_writers[i].get().HasAnalyze()) {
					col_writers[i].get().Analyze(*write_states[i], nullptr, chunk.data[i], chunk.size());
				}
			}
		}

		for (idx_t i = 0; i < next; i++) {
			if (col_writers[i].get().HasAnalyze()) {
				col_writers[i].get().FinalizeAnalyze(*write_states[i]);
			}
		}

		for (auto &chunk : buffer.Chunks({column_ids})) {
			for (idx_t i = 0; i < next; i++) {
				col_writers[i].get().Prepare(*write_states[i], nullptr, chunk.data[i], chunk.size());
			}
		}

		for (idx_t i = 0; i < next; i++) {
			col_writers[i].get().BeginWrite(*write_states[i]);
		}

		for (auto &chunk : buffer.Chunks({column_ids})) {
			for (idx_t i = 0; i < next; i++) {
				col_writers[i].get().Write(*write_states[i], chunk.data[i], chunk.size());
			}
		}

		for (auto &write_state : write_states) {
			states.push_back(std::move(write_state));
		}
	}
	result.heaps = buffer.GetHeapReferences();
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

	prepared.heaps.clear();
}

void ParquetWriter::Flush(ColumnDataCollection &buffer) {
	if (buffer.Count() == 0) {
		return;
	}

	PreparedRowGroup prepared_row_group;
	PrepareRowGroup(buffer, prepared_row_group);
	buffer.Reset();

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
