#include "parquet_writer.hpp"

#include "duckdb.hpp"
#include "mbedtls_wrapper.hpp"
#include "parquet_crypto.hpp"
#include "parquet_timestamp.hpp"
#include "resizable_buffer.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
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

using duckdb_parquet::CompressionCodec;
using duckdb_parquet::ConvertedType;
using duckdb_parquet::Encoding;
using duckdb_parquet::FieldRepetitionType;
using duckdb_parquet::FileCryptoMetaData;
using duckdb_parquet::FileMetaData;
using duckdb_parquet::PageHeader;
using duckdb_parquet::PageType;
using ParquetRowGroup = duckdb_parquet::RowGroup;
using duckdb_parquet::Type;

ChildFieldIDs::ChildFieldIDs() : ids(make_uniq<case_insensitive_map_t<FieldID>>()) {
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

bool ParquetWriter::TryGetParquetType(const LogicalType &duckdb_type, optional_ptr<Type::type> parquet_type_ptr) {
	Type::type parquet_type;
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
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::HUGEINT:
		parquet_type = Type::DOUBLE;
		break;
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
		return false;
	}
	if (parquet_type_ptr) {
		*parquet_type_ptr = parquet_type;
	}
	return true;
}

Type::type ParquetWriter::DuckDBTypeToParquetType(const LogicalType &duckdb_type) {
	Type::type result;
	if (TryGetParquetType(duckdb_type, &result)) {
		return result;
	}
	throw NotImplementedException("Unimplemented type for Parquet \"%s\"", duckdb_type.ToString());
}

void ParquetWriter::SetSchemaProperties(const LogicalType &duckdb_type, duckdb_parquet::SchemaElement &schema_ele) {
	if (duckdb_type.IsJSONType()) {
		schema_ele.converted_type = ConvertedType::JSON;
		schema_ele.__isset.converted_type = true;
		schema_ele.__isset.logicalType = true;
		schema_ele.logicalType.__set_JSON(duckdb_parquet::JsonType());
		return;
	}
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
	case LogicalTypeId::TIMESTAMP_SEC:
		schema_ele.converted_type = ConvertedType::TIMESTAMP_MICROS;
		schema_ele.__isset.converted_type = true;
		schema_ele.__isset.logicalType = true;
		schema_ele.logicalType.__isset.TIMESTAMP = true;
		schema_ele.logicalType.TIMESTAMP.isAdjustedToUTC = (duckdb_type.id() == LogicalTypeId::TIMESTAMP_TZ);
		schema_ele.logicalType.TIMESTAMP.unit.__isset.MICROS = true;
		break;
	case LogicalTypeId::TIMESTAMP_NS:
		schema_ele.__isset.converted_type = false;
		schema_ele.__isset.logicalType = true;
		schema_ele.logicalType.__isset.TIMESTAMP = true;
		schema_ele.logicalType.TIMESTAMP.isAdjustedToUTC = false;
		schema_ele.logicalType.TIMESTAMP.unit.__isset.NANOS = true;
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

uint32_t ParquetWriter::Write(const duckdb_apache::thrift::TBase &object) {
	if (encryption_config) {
		return ParquetCrypto::Write(object, *protocol, encryption_config->GetFooterKey(), *encryption_util);
	} else {
		return object.write(protocol.get());
	}
}

uint32_t ParquetWriter::WriteData(const const_data_ptr_t buffer, const uint32_t buffer_size) {
	if (encryption_config) {
		return ParquetCrypto::WriteData(*protocol, buffer, buffer_size, encryption_config->GetFooterKey(),
		                                *encryption_util);
	} else {
		protocol->getTransport()->write(buffer, buffer_size);
		return buffer_size;
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

ParquetWriter::ParquetWriter(ClientContext &context, FileSystem &fs, string file_name_p, vector<LogicalType> types_p,
                             vector<string> names_p, CompressionCodec::type codec, ChildFieldIDs field_ids_p,
                             const vector<pair<string, string>> &kv_metadata,
                             shared_ptr<ParquetEncryptionConfig> encryption_config_p, idx_t dictionary_size_limit_p,
                             double bloom_filter_false_positive_ratio_p, int64_t compression_level_p,
                             bool debug_use_openssl_p, ParquetVersion parquet_version)
    : context(context), file_name(std::move(file_name_p)), sql_types(std::move(types_p)),
      column_names(std::move(names_p)), codec(codec), field_ids(std::move(field_ids_p)),
      encryption_config(std::move(encryption_config_p)), dictionary_size_limit(dictionary_size_limit_p),
      bloom_filter_false_positive_ratio(bloom_filter_false_positive_ratio_p), compression_level(compression_level_p),
      debug_use_openssl(debug_use_openssl_p), parquet_version(parquet_version) {

	// initialize the file writer
	writer = make_uniq<BufferedFileWriter>(fs, file_name.c_str(),
	                                       FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	if (encryption_config) {
		auto &config = DBConfig::GetConfig(context);
		if (config.encryption_util && debug_use_openssl) {
			// Use OpenSSL
			encryption_util = config.encryption_util;
		} else {
			encryption_util = make_shared_ptr<duckdb_mbedtls::MbedTlsWrapper::AESGCMStateMBEDTLSFactory>();
		}
		// encrypted parquet files start with the string "PARE"
		writer->WriteData(const_data_ptr_cast("PARE"), 4);
		// we only support this one for now, not "AES_GCM_CTR_V1"
		file_meta_data.encryption_algorithm.__isset.AES_GCM_V1 = true;
	} else {
		// parquet files start with the string "PAR1"
		writer->WriteData(const_data_ptr_cast("PAR1"), 4);
	}
	TCompactProtocolFactoryT<MyTransport> tproto_factory;
	protocol = tproto_factory.getProtocol(std::make_shared<MyTransport>(*writer));

	file_meta_data.num_rows = 0;
	file_meta_data.version = 1;

	file_meta_data.__isset.created_by = true;
	file_meta_data.created_by =
	    StringUtil::Format("DuckDB version %s (build %s)", DuckDB::LibraryVersion(), DuckDB::SourceID());

	file_meta_data.schema.resize(1);

	for (auto &kv_pair : kv_metadata) {
		duckdb_parquet::KeyValue kv;
		kv.__set_key(kv_pair.first);
		kv.__set_value(kv_pair.second);
		file_meta_data.key_value_metadata.push_back(kv);
		file_meta_data.__isset.key_value_metadata = true;
	}

	// populate root schema object
	file_meta_data.schema[0].name = "duckdb_schema";
	file_meta_data.schema[0].num_children = NumericCast<int32_t>(sql_types.size());
	file_meta_data.schema[0].__isset.num_children = true;
	file_meta_data.schema[0].repetition_type = duckdb_parquet::FieldRepetitionType::REQUIRED;
	file_meta_data.schema[0].__isset.repetition_type = true;

	auto &unique_names = column_names;
	VerifyUniqueNames(unique_names);

	vector<string> schema_path;
	for (idx_t i = 0; i < sql_types.size(); i++) {
		column_writers.push_back(ColumnWriter::CreateWriterRecursive(
		    context, file_meta_data.schema, *this, sql_types[i], unique_names[i], schema_path, &field_ids));
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
	row_group.num_rows = NumericCast<int64_t>(buffer.Count());
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

		// Reserving these once at the start really pays off
		for (auto &write_state : write_states) {
			write_state->definition_levels.reserve(buffer.Count());
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

// Validation code adapted from Impala
static void ValidateOffsetInFile(const string &filename, idx_t col_idx, idx_t file_length, idx_t offset,
                                 const string &offset_name) {
	if (offset >= file_length) {
		throw IOException("File '%s': metadata is corrupt. Column %d has invalid "
		                  "%s (offset=%llu file_size=%llu).",
		                  filename, col_idx, offset_name, offset, file_length);
	}
}

static void ValidateColumnOffsets(const string &filename, idx_t file_length, const ParquetRowGroup &row_group) {
	for (idx_t i = 0; i < row_group.columns.size(); ++i) {
		const auto &col_chunk = row_group.columns[i];
		ValidateOffsetInFile(filename, i, file_length, col_chunk.meta_data.data_page_offset, "data page offset");
		auto col_start = NumericCast<idx_t>(col_chunk.meta_data.data_page_offset);
		// The file format requires that if a dictionary page exists, it be before data pages.
		if (col_chunk.meta_data.__isset.dictionary_page_offset) {
			ValidateOffsetInFile(filename, i, file_length, col_chunk.meta_data.dictionary_page_offset,
			                     "dictionary page offset");
			if (NumericCast<idx_t>(col_chunk.meta_data.dictionary_page_offset) >= col_start) {
				throw IOException("Parquet file '%s': metadata is corrupt. Dictionary "
				                  "page (offset=%llu) must come before any data pages (offset=%llu).",
				                  filename, col_chunk.meta_data.dictionary_page_offset, col_start);
			}
			col_start = col_chunk.meta_data.dictionary_page_offset;
		}
		auto col_len = NumericCast<idx_t>(col_chunk.meta_data.total_compressed_size);
		auto col_end = col_start + col_len;
		if (col_end <= 0 || col_end > file_length) {
			throw IOException("Parquet file '%s': metadata is corrupt. Column %llu has "
			                  "invalid column offsets (offset=%llu, size=%llu, file_size=%llu).",
			                  filename, i, col_start, col_len, file_length);
		}
	}
}

void ParquetWriter::FlushRowGroup(PreparedRowGroup &prepared) {
	lock_guard<mutex> glock(lock);
	auto &row_group = prepared.row_group;
	auto &states = prepared.states;
	if (states.empty()) {
		throw InternalException("Attempting to flush a row group with no rows");
	}
	row_group.file_offset = NumericCast<int64_t>(writer->GetTotalWritten());
	for (idx_t col_idx = 0; col_idx < states.size(); col_idx++) {
		const auto &col_writer = column_writers[col_idx];
		auto write_state = std::move(states[col_idx]);
		col_writer->FinalizeWrite(*write_state);
	}
	// let's make sure all offsets are ay-okay
	ValidateColumnOffsets(file_name, writer->GetTotalWritten(), row_group);

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

	// dump the bloom filters right before footer, not if stuff is encrypted

	for (auto &bloom_filter_entry : bloom_filters) {
		D_ASSERT(!encryption_config);
		// write nonsense bloom filter header
		duckdb_parquet::BloomFilterHeader filter_header;
		auto bloom_filter_bytes = bloom_filter_entry.bloom_filter->Get();
		filter_header.numBytes = NumericCast<int32_t>(bloom_filter_bytes->len);
		filter_header.algorithm.__set_BLOCK(duckdb_parquet::SplitBlockAlgorithm());
		filter_header.compression.__set_UNCOMPRESSED(duckdb_parquet::Uncompressed());
		filter_header.hash.__set_XXHASH(duckdb_parquet::XxHash());

		// set metadata flags
		auto &column_chunk =
		    file_meta_data.row_groups[bloom_filter_entry.row_group_idx].columns[bloom_filter_entry.column_idx];

		column_chunk.meta_data.__isset.bloom_filter_offset = true;
		column_chunk.meta_data.bloom_filter_offset = NumericCast<int64_t>(writer->GetTotalWritten());

		auto bloom_filter_header_size = Write(filter_header);
		// write actual data
		WriteData(bloom_filter_bytes->ptr, bloom_filter_bytes->len);

		column_chunk.meta_data.__isset.bloom_filter_length = true;
		column_chunk.meta_data.bloom_filter_length =
		    NumericCast<int32_t>(bloom_filter_header_size + bloom_filter_bytes->len);
	}

	const auto metadata_start_offset = writer->GetTotalWritten();
	if (encryption_config) {
		// Crypto metadata is written unencrypted
		FileCryptoMetaData crypto_metadata;
		duckdb_parquet::AesGcmV1 aes_gcm_v1;
		duckdb_parquet::EncryptionAlgorithm alg;
		alg.__set_AES_GCM_V1(aes_gcm_v1);
		crypto_metadata.__set_encryption_algorithm(alg);
		crypto_metadata.write(protocol.get());
	}

	// Add geoparquet metadata to the file metadata
	if (geoparquet_data) {
		geoparquet_data->Write(file_meta_data);
	}

	Write(file_meta_data);

	writer->Write<uint32_t>(writer->GetTotalWritten() - metadata_start_offset);

	if (encryption_config) {
		// encrypted parquet files also end with the string "PARE"
		writer->WriteData(const_data_ptr_cast("PARE"), 4);
	} else {
		// parquet files also end with the string "PAR1"
		writer->WriteData(const_data_ptr_cast("PAR1"), 4);
	}

	// flush to disk
	writer->Close();
	writer.reset();
}

GeoParquetFileMetadata &ParquetWriter::GetGeoParquetData() {
	if (!geoparquet_data) {
		geoparquet_data = make_uniq<GeoParquetFileMetadata>();
	}
	return *geoparquet_data;
}

void ParquetWriter::BufferBloomFilter(idx_t col_idx, unique_ptr<ParquetBloomFilter> bloom_filter) {
	if (encryption_config) {
		return;
	}
	ParquetBloomFilterEntry new_entry;
	new_entry.bloom_filter = std::move(bloom_filter);
	new_entry.column_idx = col_idx;
	new_entry.row_group_idx = file_meta_data.row_groups.size();
	bloom_filters.push_back(std::move(new_entry));
}

} // namespace duckdb
