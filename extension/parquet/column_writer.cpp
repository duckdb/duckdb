#include "column_writer.hpp"
#include "parquet_writer.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/buffered_file_writer.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/hugeint.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#endif

#include "snappy.h"
#include "miniz_wrapper.hpp"
#include "zstd.h"

namespace duckdb {

using namespace duckdb_parquet; // NOLINT
using namespace duckdb_miniz;   // NOLINT

using duckdb_parquet::format::CompressionCodec;
using duckdb_parquet::format::ConvertedType;
using duckdb_parquet::format::Encoding;
using duckdb_parquet::format::FieldRepetitionType;
using duckdb_parquet::format::FileMetaData;
using duckdb_parquet::format::PageHeader;
using duckdb_parquet::format::PageType;
using ParquetRowGroup = duckdb_parquet::format::RowGroup;
using duckdb_parquet::format::Type;

//===--------------------------------------------------------------------===//
// ColumnWriter
//===--------------------------------------------------------------------===//
ColumnWriter::ColumnWriter(ParquetWriter &writer, idx_t schema_idx) : writer(writer), schema_idx(schema_idx) {
}
ColumnWriter::~ColumnWriter() {
}

ColumnWriterState::~ColumnWriterState() {
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

void ColumnWriter::CompressPage(BufferedSerializer &temp_writer, size_t &compressed_size, data_ptr_t &compressed_data,
                                unique_ptr<data_t[]> &compressed_buf) {
	switch (writer.codec) {
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
}

struct PageInformation {
	idx_t row_count = 0;
	idx_t estimated_page_size = 0;
	vector<uint16_t> definition_levels;
	vector<uint16_t> repetition_levels;
};

struct PageWriteInformation {
	PageHeader page_header;
	unique_ptr<BufferedSerializer> temp_writer;
	idx_t write_page_idx = 0;
	idx_t write_count = 0;
	idx_t max_write_count = 0;
};

class StandardColumnWriterState : public ColumnWriterState {
public:
	StandardColumnWriterState(duckdb_parquet::format::RowGroup &row_group, idx_t col_idx)
	    : row_group(row_group), col_idx(col_idx) {
		page_info.push_back(PageInformation());
	}
	~StandardColumnWriterState() override = default;

	duckdb_parquet::format::RowGroup &row_group;
	idx_t col_idx;
	vector<PageInformation> page_info;
	PageWriteInformation write_info;
};

unique_ptr<ColumnWriterState> ColumnWriter::InitializeWriteState(duckdb_parquet::format::RowGroup &row_group,
                                                                 idx_t total_values) {
	auto result = make_unique<StandardColumnWriterState>(row_group, row_group.columns.size());

	duckdb_parquet::format::ColumnChunk column_chunk;
	column_chunk.__isset.meta_data = true;
	column_chunk.meta_data.codec = writer.codec;
	column_chunk.meta_data.path_in_schema.push_back(writer.file_meta_data.schema[schema_idx].name);
	column_chunk.meta_data.num_values = total_values;
	column_chunk.meta_data.type = writer.file_meta_data.schema[schema_idx].type;
	row_group.columns.push_back(move(column_chunk));

	return move(result);
}

void ColumnWriter::Prepare(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	auto &state = (StandardColumnWriterState &)state_p;

	auto &validity = FlatVector::Validity(vector);
	for (idx_t i = 0; i < count; i++) {
		auto &page_info = state.page_info.back();
		page_info.row_count++;

		if (validity.RowIsValid(i)) {
			page_info.definition_levels.push_back(1);
			page_info.estimated_page_size += GetRowSize(vector, i);
			if (page_info.estimated_page_size >= MAX_UNCOMPRESSED_PAGE_SIZE) {
				state.page_info.push_back(PageInformation());
			}
		} else {
			page_info.definition_levels.push_back(0);
		}
	}
}

void ColumnWriter::BeginWrite(ColumnWriterState &state_p) {
	auto &state = (StandardColumnWriterState &)state_p;
	auto &column_chunk = state.row_group.columns[state.col_idx];
	column_chunk.meta_data.data_page_offset = writer.writer->GetTotalWritten();

	// start writing the first page
	NextPage(state_p);
}

void ColumnWriter::NextPage(ColumnWriterState &state_p) {
	auto &state = (StandardColumnWriterState &)state_p;

	if (state.write_info.write_page_idx > 0) {
		// need to flush the current page
		FlushPage(state_p);
	}
	if (state.write_info.write_page_idx == state.page_info.size()) {
		state.write_info.write_count = 0;
		state.write_info.max_write_count = 0;
		return;
	}
	D_ASSERT(state.write_info.write_page_idx < state.page_info.size());
	auto &page_info = state.page_info[state.write_info.write_page_idx];
	idx_t num_values = page_info.row_count;
	if (num_values == 0) {
		// nothing to write
		return;
	}

	// reset the serializer
	state.write_info.temp_writer = make_unique<BufferedSerializer>();

	// set up the page header struct
	auto &hdr = state.write_info.page_header;
	hdr.compressed_page_size = 0;
	hdr.uncompressed_page_size = 0;
	hdr.type = PageType::DATA_PAGE;
	hdr.__isset.data_page_header = true;

	hdr.data_page_header.num_values = num_values;
	hdr.data_page_header.encoding = Encoding::PLAIN;
	hdr.data_page_header.definition_level_encoding = Encoding::RLE;
	hdr.data_page_header.repetition_level_encoding = Encoding::BIT_PACKED;

	state.write_info.write_count = 0;
	state.write_info.max_write_count = num_values;
	state.write_info.write_page_idx++;

	// write repetition levels
	// TODO

	auto &temp_writer = *state.write_info.temp_writer;
	// write the definition levels (i.e. the inverse of the nullmask)
	// we always bit pack everything (for now)

	// first figure out how many bytes we need (1 byte per 8 rows, rounded up)
	auto define_byte_count = (num_values + 7) / 8;
	// we need to set up the count as a varint, plus an added marker for the RLE scheme
	// for this marker we shift the count left 1 and set low bit to 1 to indicate bit packed literals
	uint32_t define_header = (define_byte_count << 1) | 1;
	uint32_t define_size = GetVarintSize(define_header) + define_byte_count;

	// write the number of defines as a varint
	temp_writer.Write<uint32_t>(define_size);
	VarintEncode(define_header, temp_writer);

	// construct a large validity mask holding all the validity bits we need to write
	ValidityMask result_mask(num_values);
	for (idx_t i = 0; i < page_info.definition_levels.size(); i++) {
		result_mask.Set(i, page_info.definition_levels[i] > 0);
	}
	// actually write out the validity bits
	temp_writer.WriteData((const_data_ptr_t)result_mask.GetData(), define_byte_count);
}

void ColumnWriter::FlushPage(ColumnWriterState &state_p) {
	auto &state = (StandardColumnWriterState &)state_p;
	D_ASSERT(state.write_info.write_page_idx > 0);

	// flush the page info to disk
	auto &temp_writer = *state.write_info.temp_writer;
	auto &hdr = state.write_info.page_header;

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
	CompressPage(temp_writer, compressed_size, compressed_data, compressed_buf);

	hdr.compressed_page_size = compressed_size;
	// now finally write the data to the actual file
	hdr.write(writer.protocol.get());
	writer.writer->WriteData(compressed_data, compressed_size);
}

void ColumnWriter::Write(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	auto &state = (StandardColumnWriterState &)state_p;

	idx_t remaining = count;
	idx_t offset = 0;
	while (remaining > 0) {
		auto &temp_writer = *state.write_info.temp_writer;
		idx_t write_count = MinValue<idx_t>(remaining, state.write_info.max_write_count - state.write_info.write_count);
		D_ASSERT(write_count > 0);

		WriteVector(temp_writer, vector, offset, offset + write_count);

		state.write_info.write_count += write_count;
		if (state.write_info.write_count == state.write_info.max_write_count) {
			NextPage(state_p);
		}
		offset += write_count;
		remaining -= write_count;
	}
}

void ColumnWriter::FinalizeWrite(ColumnWriterState &state_p) {
	auto &state = (StandardColumnWriterState &)state_p;
	auto &column_chunk = state.row_group.columns[state.col_idx];
	column_chunk.meta_data.total_compressed_size =
	    writer.writer->GetTotalWritten() - column_chunk.meta_data.data_page_offset;
	// verify that we wrote all the pages
	D_ASSERT(state.write_info.write_page_idx == state.page_info.size());
}

//===--------------------------------------------------------------------===//
// Standard Column Writer
//===--------------------------------------------------------------------===//
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

template <class SRC, class TGT, class OP = ParquetCastOperator>
class StandardColumnWriter : public ColumnWriter {
public:
	StandardColumnWriter(ParquetWriter &writer, idx_t schema_idx) : ColumnWriter(writer, schema_idx) {
	}
	~StandardColumnWriter() override = default;

public:
	void WriteVector(Serializer &temp_writer, Vector &input_column, idx_t chunk_start, idx_t chunk_end) override {
		auto &mask = FlatVector::Validity(input_column);
		TemplatedWritePlain<SRC, TGT, OP>(input_column, chunk_start, chunk_end, mask, temp_writer);
	}

	idx_t GetRowSize(Vector &vector, idx_t index) override {
		return sizeof(TGT);
	}
};

//===--------------------------------------------------------------------===//
// Boolean Column Writer
//===--------------------------------------------------------------------===//
class BooleanColumnWriter : public ColumnWriter {
public:
	BooleanColumnWriter(ParquetWriter &writer, idx_t schema_idx) : ColumnWriter(writer, schema_idx) {
	}
	~BooleanColumnWriter() override = default;

public:
	void WriteVector(Serializer &temp_writer, Vector &input_column, idx_t chunk_start, idx_t chunk_end) override {
		auto &mask = FlatVector::Validity(input_column);

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
	}

	idx_t GetRowSize(Vector &vector, idx_t index) override {
		return sizeof(bool);
	}
};

//===--------------------------------------------------------------------===//
// Decimal Column Writer
//===--------------------------------------------------------------------===//
class DecimalColumnWriter : public ColumnWriter {
public:
	DecimalColumnWriter(ParquetWriter &writer, idx_t schema_idx) : ColumnWriter(writer, schema_idx) {
	}
	~DecimalColumnWriter() override = default;

public:
	void WriteVector(Serializer &temp_writer, Vector &input_column, idx_t chunk_start, idx_t chunk_end) override {
		auto &mask = FlatVector::Validity(input_column);

		// FIXME: fixed length byte array...
		Vector double_vec(LogicalType::DOUBLE);
		VectorOperations::Cast(input_column, double_vec, chunk_end);
		TemplatedWritePlain<double, double>(double_vec, chunk_start, chunk_end, mask, temp_writer);
	}

	idx_t GetRowSize(Vector &vector, idx_t index) override {
		return sizeof(double);
	}
};

//===--------------------------------------------------------------------===//
// String Column Writer
//===--------------------------------------------------------------------===//
class StringColumnWriter : public ColumnWriter {
public:
	StringColumnWriter(ParquetWriter &writer, idx_t schema_idx) : ColumnWriter(writer, schema_idx) {
	}
	~StringColumnWriter() override = default;

public:
	void WriteVector(Serializer &temp_writer, Vector &input_column, idx_t chunk_start, idx_t chunk_end) override {
		auto &mask = FlatVector::Validity(input_column);

		auto *ptr = FlatVector::GetData<string_t>(input_column);
		for (idx_t r = chunk_start; r < chunk_end; r++) {
			if (mask.RowIsValid(r)) {
				temp_writer.Write<uint32_t>(ptr[r].GetSize());
				temp_writer.WriteData((const_data_ptr_t)ptr[r].GetDataUnsafe(), ptr[r].GetSize());
			}
		}
	}

	idx_t GetRowSize(Vector &vector, idx_t index) override {
		auto strings = FlatVector::GetData<string_t>(vector);
		return strings[index].GetSize();
	}
};

//===--------------------------------------------------------------------===//
// Create Column Writer
//===--------------------------------------------------------------------===//
unique_ptr<ColumnWriter> ColumnWriter::CreateWriterRecursive(vector<duckdb_parquet::format::SchemaElement> &schemas,
                                                             ParquetWriter &writer, const LogicalType &type,
                                                             const string &name) {
	idx_t schema_idx = schemas.size();
	if (type.id() == LogicalTypeId::STRUCT) {
		auto &child_types = StructType::GetChildTypes(type);
		// set up the schema element for this struct
		duckdb_parquet::format::SchemaElement schema_element;
		schema_element.repetition_type = FieldRepetitionType::OPTIONAL;
		schema_element.num_children = child_types.size();
		schema_element.__isset.num_children = true;
		schema_element.__isset.type = false;
		schema_element.__isset.repetition_type = true;
		schema_element.name = name;
		schemas.push_back(move(schema_element));
		// construct the child types recursively
		vector<unique_ptr<ColumnWriter>> child_writers;
		child_writers.reserve(child_types.size());
		for (auto &child_type : child_types) {
			child_writers.push_back(CreateWriterRecursive(schemas, writer, child_type.second, child_type.first));
		}
		throw InternalException("eek");
	}
	duckdb_parquet::format::SchemaElement schema_element;
	schema_element.type = ParquetWriter::DuckDBTypeToParquetType(type);
	schema_element.repetition_type = FieldRepetitionType::OPTIONAL;
	schema_element.num_children = 0;
	schema_element.__isset.num_children = true;
	schema_element.__isset.type = true;
	schema_element.__isset.repetition_type = true;
	schema_element.name = name;
	schema_element.__isset.converted_type =
	    ParquetWriter::DuckDBTypeToConvertedType(type, schema_element.converted_type);
	schemas.push_back(move(schema_element));

	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return make_unique<BooleanColumnWriter>(writer, schema_idx);
	case LogicalTypeId::TINYINT:
		return make_unique<StandardColumnWriter<int8_t, int32_t>>(writer, schema_idx);
	case LogicalTypeId::SMALLINT:
		return make_unique<StandardColumnWriter<int16_t, int32_t>>(writer, schema_idx);
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return make_unique<StandardColumnWriter<int32_t, int32_t>>(writer, schema_idx);
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
		return make_unique<StandardColumnWriter<int64_t, int64_t>>(writer, schema_idx);
	case LogicalTypeId::HUGEINT:
		return make_unique<StandardColumnWriter<hugeint_t, double, ParquetHugeintOperator>>(writer, schema_idx);
	case LogicalTypeId::TIMESTAMP_NS:
		return make_unique<StandardColumnWriter<int64_t, int64_t, ParquetTimestampNSOperator>>(writer, schema_idx);
	case LogicalTypeId::TIMESTAMP_SEC:
		return make_unique<StandardColumnWriter<int64_t, int64_t, ParquetTimestampSOperator>>(writer, schema_idx);
	case LogicalTypeId::UTINYINT:
		return make_unique<StandardColumnWriter<uint8_t, int32_t>>(writer, schema_idx);
	case LogicalTypeId::USMALLINT:
		return make_unique<StandardColumnWriter<uint16_t, int32_t>>(writer, schema_idx);
	case LogicalTypeId::UINTEGER:
		return make_unique<StandardColumnWriter<uint32_t, uint32_t>>(writer, schema_idx);
	case LogicalTypeId::UBIGINT:
		return make_unique<StandardColumnWriter<uint64_t, uint64_t>>(writer, schema_idx);
	case LogicalTypeId::FLOAT:
		return make_unique<StandardColumnWriter<float, float>>(writer, schema_idx);
	case LogicalTypeId::DOUBLE:
		return make_unique<StandardColumnWriter<double, double>>(writer, schema_idx);
	case LogicalTypeId::DECIMAL:
		return make_unique<DecimalColumnWriter>(writer, schema_idx);
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return make_unique<StringColumnWriter>(writer, schema_idx);
	default:
		throw InternalException("Unsupported type in Parquet writer");
	}
}

} // namespace duckdb
