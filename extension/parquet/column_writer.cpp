#include "column_writer.hpp"
#include "parquet_writer.hpp"
#include "parquet_rle_bp_decoder.hpp"

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
ColumnWriter::ColumnWriter(ParquetWriter &writer, idx_t schema_idx, idx_t max_repeat, idx_t max_define)
    : writer(writer), schema_idx(schema_idx), max_repeat(max_repeat), max_define(max_define) {
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
	idx_t offset = 0;
	idx_t row_count = 0;
	idx_t estimated_page_size = 0;
};

struct PageWriteInformation {
	PageHeader page_header;
	unique_ptr<BufferedSerializer> temp_writer;
	idx_t write_page_idx = 0;
	idx_t write_count = 0;
	idx_t max_write_count = 0;
	size_t compressed_size;
	data_ptr_t compressed_data;
	unique_ptr<data_t[]> compressed_buf;
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
	vector<PageWriteInformation> write_info;
	idx_t current_page = 0;
};

unique_ptr<ColumnWriterState> ColumnWriter::InitializeWriteState(duckdb_parquet::format::RowGroup &row_group,
                                                                 vector<string> schema_path) {
	auto result = make_unique<StandardColumnWriterState>(row_group, row_group.columns.size());

	duckdb_parquet::format::ColumnChunk column_chunk;
	column_chunk.__isset.meta_data = true;
	column_chunk.meta_data.codec = writer.codec;
	column_chunk.meta_data.path_in_schema = move(schema_path);
	column_chunk.meta_data.path_in_schema.push_back(writer.file_meta_data.schema[schema_idx].name);
	column_chunk.meta_data.num_values = 0;
	column_chunk.meta_data.type = writer.file_meta_data.schema[schema_idx].type;
	row_group.columns.push_back(move(column_chunk));

	return move(result);
}

void ColumnWriter::HandleDefineLevels(ColumnWriterState &state, ColumnWriterState *parent, ValidityMask &validity,
                                      idx_t count, uint16_t define_value, uint16_t null_value) {
	for (idx_t i = 0; i < count; i++) {
		if (parent && parent->definition_levels[state.definition_levels.size()] != ColumnWriterState::DEFINE_VALID) {
			state.definition_levels.push_back(parent->definition_levels[state.definition_levels.size()]);
		} else if (validity.RowIsValid(i)) {
			state.definition_levels.push_back(define_value);
		} else {
			state.definition_levels.push_back(null_value);
		}
	}
}

void ColumnWriter::Prepare(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	auto &state = (StandardColumnWriterState &)state_p;
	auto &col_chunk = state.row_group.columns[state.col_idx];

	auto &validity = FlatVector::Validity(vector);
	HandleDefineLevels(state_p, parent, validity, count, max_define, max_define - 1);
	for (idx_t i = 0; i < count; i++) {
		auto &page_info = state.page_info.back();
		page_info.row_count++;
		col_chunk.meta_data.num_values++;
		if (validity.RowIsValid(i)) {
			page_info.estimated_page_size += GetRowSize(vector, i);
			if (page_info.estimated_page_size >= MAX_UNCOMPRESSED_PAGE_SIZE) {
				PageInformation new_info;
				new_info.offset = page_info.offset + page_info.row_count;
				state.page_info.push_back(new_info);
			}
		}
	}
}

void ColumnWriter::BeginWrite(ColumnWriterState &state_p) {
	auto &state = (StandardColumnWriterState &)state_p;

	// set up the page write info
	for (idx_t page_idx = 0; page_idx < state.page_info.size(); page_idx++) {
		auto &page_info = state.page_info[page_idx];
		if (page_info.row_count == 0) {
			D_ASSERT(page_idx + 1 == state.page_info.size());
			state.page_info.erase(state.page_info.begin() + page_idx);
			break;
		}
		PageWriteInformation write_info;
		// set up the header
		auto &hdr = write_info.page_header;
		hdr.compressed_page_size = 0;
		hdr.uncompressed_page_size = 0;
		hdr.type = PageType::DATA_PAGE;
		hdr.__isset.data_page_header = true;

		hdr.data_page_header.num_values = page_info.row_count;
		hdr.data_page_header.encoding = Encoding::PLAIN;
		hdr.data_page_header.definition_level_encoding = Encoding::RLE;
		hdr.data_page_header.repetition_level_encoding = Encoding::RLE;

		write_info.temp_writer = make_unique<BufferedSerializer>();
		write_info.write_count = 0;
		write_info.max_write_count = page_info.row_count;

		write_info.compressed_size = 0;
		write_info.compressed_data = nullptr;

		state.write_info.push_back(move(write_info));
	}

	// start writing the first page
	NextPage(state_p);
}

void ColumnWriter::WriteLevels(Serializer &temp_writer, const vector<uint16_t> &levels, idx_t max_value, idx_t offset,
                               idx_t count) {
	if (levels.empty() || count == 0) {
		return;
	}

	// write the levels
	// we always RLE everything (for now)
	auto bit_width = RleBpDecoder::ComputeBitWidth((max_value));
	auto byte_width = (bit_width + 7) / 8;

	// figure out how many bytes we are going to need
	idx_t byte_count = 0;
	idx_t run_count = 1;
	idx_t current_run_count = 1;
	for (idx_t i = offset + 1; i <= offset + count; i++) {
		if (i == offset + count || levels[i] != levels[i - 1]) {
			// last value, or value has changed
			// write out the current run
			byte_count += GetVarintSize(current_run_count << 1) + byte_width;
			current_run_count = 1;
			run_count++;
		} else {
			current_run_count++;
		}
	}
	temp_writer.Write<uint32_t>(byte_count);

	// now actually write the values
	current_run_count = 1;
	for (idx_t i = offset + 1; i <= offset + count; i++) {
		if (i == offset + count || levels[i] != levels[i - 1]) {
			// new run: write out the old run
			// first write the header
			VarintEncode(current_run_count << 1, temp_writer);
			// now write hte value
			switch (byte_width) {
			case 1:
				temp_writer.Write<uint8_t>(levels[i - 1]);
				break;
			case 2:
				temp_writer.Write<uint16_t>(levels[i - 1]);
				break;
			default:
				throw InternalException("unsupported byte width for RLE encoding");
			}
			current_run_count = 1;
		} else {
			current_run_count++;
		}
	}
}

void ColumnWriter::NextPage(ColumnWriterState &state_p) {
	auto &state = (StandardColumnWriterState &)state_p;

	if (state.current_page > 0) {
		// need to flush the current page
		FlushPage(state_p);
	}
	if (state.current_page >= state.write_info.size()) {
		return;
	}
	auto &page_info = state.page_info[state.current_page];
	auto &write_info = state.write_info[state.current_page];
	state.current_page++;

	auto &temp_writer = *write_info.temp_writer;

	// write the repetition levels
	WriteLevels(temp_writer, state.repetition_levels, max_repeat, page_info.offset, page_info.row_count);

	// write the definition levels
	WriteLevels(temp_writer, state.definition_levels, max_define, page_info.offset, page_info.row_count);
}

void ColumnWriter::FlushPage(ColumnWriterState &state_p) {
	auto &state = (StandardColumnWriterState &)state_p;
	D_ASSERT(state.current_page > 0);

	// compress the page info
	auto &write_info = state.write_info[state.current_page - 1];
	auto &temp_writer = *write_info.temp_writer;
	auto &hdr = write_info.page_header;

	// now that we have finished writing the data we know the uncompressed size
	if (temp_writer.blob.size > idx_t(NumericLimits<int32_t>::Maximum())) {
		throw InternalException("Parquet writer: %d uncompressed page size out of range for type integer",
		                        temp_writer.blob.size);
	}
	hdr.uncompressed_page_size = temp_writer.blob.size;

	// compress the data
	CompressPage(temp_writer, write_info.compressed_size, write_info.compressed_data, write_info.compressed_buf);
	hdr.compressed_page_size = write_info.compressed_size;

	if (write_info.compressed_buf) {
		// if the data has been compressed, we no longer need the compressed data
		D_ASSERT(write_info.compressed_buf.get() == write_info.compressed_data);
		write_info.temp_writer.reset();
	}
}

void ColumnWriter::Write(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	auto &state = (StandardColumnWriterState &)state_p;

	idx_t remaining = count;
	idx_t offset = 0;
	while (remaining > 0) {
		auto &write_info = state.write_info[state.current_page - 1];
		auto &temp_writer = *write_info.temp_writer;
		idx_t write_count = MinValue<idx_t>(remaining, write_info.max_write_count - write_info.write_count);
		D_ASSERT(write_count > 0);

		WriteVector(temp_writer, vector, offset, offset + write_count);

		write_info.write_count += write_count;
		if (write_info.write_count == write_info.max_write_count) {
			NextPage(state_p);
		}
		offset += write_count;
		remaining -= write_count;
	}
}

void ColumnWriter::FinalizeWrite(ColumnWriterState &state_p) {
	auto &state = (StandardColumnWriterState &)state_p;
	auto &column_chunk = state.row_group.columns[state.col_idx];

	// record the start position of the pages for this column
	column_chunk.meta_data.data_page_offset = writer.writer->GetTotalWritten();
	// write the individual pages to disk
	for (auto &write_info : state.write_info) {
		write_info.page_header.write(writer.protocol.get());
		writer.writer->WriteData(write_info.compressed_data, write_info.compressed_size);
	}
	column_chunk.meta_data.total_compressed_size =
	    writer.writer->GetTotalWritten() - column_chunk.meta_data.data_page_offset;
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
	StandardColumnWriter(ParquetWriter &writer, idx_t schema_idx, idx_t max_repeat, idx_t max_define)
	    : ColumnWriter(writer, schema_idx, max_repeat, max_define) {
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
	BooleanColumnWriter(ParquetWriter &writer, idx_t schema_idx, idx_t max_repeat, idx_t max_define)
	    : ColumnWriter(writer, schema_idx, max_repeat, max_define) {
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
	DecimalColumnWriter(ParquetWriter &writer, idx_t schema_idx, idx_t max_repeat, idx_t max_define)
	    : ColumnWriter(writer, schema_idx, max_repeat, max_define) {
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
	StringColumnWriter(ParquetWriter &writer, idx_t schema_idx, idx_t max_repeat, idx_t max_define)
	    : ColumnWriter(writer, schema_idx, max_repeat, max_define) {
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
// Struct Column Writer
//===--------------------------------------------------------------------===//
class StructColumnWriter : public ColumnWriter {
public:
	StructColumnWriter(ParquetWriter &writer, idx_t schema_idx, idx_t max_repeat, idx_t max_define,
	                   vector<unique_ptr<ColumnWriter>> child_writers_p)
	    : ColumnWriter(writer, schema_idx, max_repeat, max_define), child_writers(move(child_writers_p)) {
	}
	~StructColumnWriter() override = default;

	vector<unique_ptr<ColumnWriter>> child_writers;

public:
	void WriteVector(Serializer &temp_writer, Vector &input_column, idx_t chunk_start, idx_t chunk_end) override {
		throw InternalException("Cannot write vector of type struct");
	}

	idx_t GetRowSize(Vector &vector, idx_t index) override {
		throw InternalException("Cannot get row size of struct");
	}

	unique_ptr<ColumnWriterState> InitializeWriteState(duckdb_parquet::format::RowGroup &row_group,
	                                                   vector<string> schema_path) override;
	void Prepare(ColumnWriterState &state, ColumnWriterState *parent, Vector &vector, idx_t count) override;

	void BeginWrite(ColumnWriterState &state) override;
	void Write(ColumnWriterState &state, Vector &vector, idx_t count) override;
	void FinalizeWrite(ColumnWriterState &state) override;
};

class StructColumnWriterState : public ColumnWriterState {
public:
	StructColumnWriterState(duckdb_parquet::format::RowGroup &row_group, idx_t col_idx)
	    : row_group(row_group), col_idx(col_idx) {
	}
	~StructColumnWriterState() override = default;

	duckdb_parquet::format::RowGroup &row_group;
	idx_t col_idx;
	vector<unique_ptr<ColumnWriterState>> child_states;
};

unique_ptr<ColumnWriterState> StructColumnWriter::InitializeWriteState(duckdb_parquet::format::RowGroup &row_group,
                                                                       vector<string> schema_path) {
	auto result = make_unique<StructColumnWriterState>(row_group, row_group.columns.size());
	schema_path.push_back(writer.file_meta_data.schema[schema_idx].name);

	result->child_states.reserve(child_writers.size());
	for (auto &child_writer : child_writers) {
		result->child_states.push_back(child_writer->InitializeWriteState(row_group, schema_path));
	}
	return move(result);
}

void StructColumnWriter::Prepare(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	auto &state = (StructColumnWriterState &)state_p;

	auto &validity = FlatVector::Validity(vector);
	HandleDefineLevels(state_p, parent, validity, count, ColumnWriterState::DEFINE_VALID, max_define - 1);
	auto &child_vectors = StructVector::GetEntries(vector);
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		child_writers[child_idx]->Prepare(*state.child_states[child_idx], &state_p, *child_vectors[child_idx], count);
	}
}

void StructColumnWriter::BeginWrite(ColumnWriterState &state_p) {
	auto &state = (StructColumnWriterState &)state_p;
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		child_writers[child_idx]->BeginWrite(*state.child_states[child_idx]);
	}
}

void StructColumnWriter::Write(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	auto &state = (StructColumnWriterState &)state_p;
	auto &child_vectors = StructVector::GetEntries(vector);
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		child_writers[child_idx]->Write(*state.child_states[child_idx], *child_vectors[child_idx], count);
	}
}

void StructColumnWriter::FinalizeWrite(ColumnWriterState &state_p) {
	auto &state = (StructColumnWriterState &)state_p;
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		child_writers[child_idx]->FinalizeWrite(*state.child_states[child_idx]);
	}
}

//===--------------------------------------------------------------------===//
// Create Column Writer
//===--------------------------------------------------------------------===//
unique_ptr<ColumnWriter> ColumnWriter::CreateWriterRecursive(vector<duckdb_parquet::format::SchemaElement> &schemas,
                                                             ParquetWriter &writer, const LogicalType &type,
                                                             const string &name, idx_t max_repeat, idx_t max_define) {
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
			child_writers.push_back(CreateWriterRecursive(schemas, writer, child_type.second, child_type.first,
			                                              max_repeat, max_define + 1));
		}
		return make_unique<StructColumnWriter>(writer, schema_idx, max_repeat, max_define, move(child_writers));
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
		return make_unique<BooleanColumnWriter>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::TINYINT:
		return make_unique<StandardColumnWriter<int8_t, int32_t>>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::SMALLINT:
		return make_unique<StandardColumnWriter<int16_t, int32_t>>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return make_unique<StandardColumnWriter<int32_t, int32_t>>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
		return make_unique<StandardColumnWriter<int64_t, int64_t>>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::HUGEINT:
		return make_unique<StandardColumnWriter<hugeint_t, double, ParquetHugeintOperator>>(writer, schema_idx,
		                                                                                    max_repeat, max_define);
	case LogicalTypeId::TIMESTAMP_NS:
		return make_unique<StandardColumnWriter<int64_t, int64_t, ParquetTimestampNSOperator>>(writer, schema_idx,
		                                                                                       max_repeat, max_define);
	case LogicalTypeId::TIMESTAMP_SEC:
		return make_unique<StandardColumnWriter<int64_t, int64_t, ParquetTimestampSOperator>>(writer, schema_idx,
		                                                                                      max_repeat, max_define);
	case LogicalTypeId::UTINYINT:
		return make_unique<StandardColumnWriter<uint8_t, int32_t>>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::USMALLINT:
		return make_unique<StandardColumnWriter<uint16_t, int32_t>>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::UINTEGER:
		return make_unique<StandardColumnWriter<uint32_t, uint32_t>>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::UBIGINT:
		return make_unique<StandardColumnWriter<uint64_t, uint64_t>>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::FLOAT:
		return make_unique<StandardColumnWriter<float, float>>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::DOUBLE:
		return make_unique<StandardColumnWriter<double, double>>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::DECIMAL:
		return make_unique<DecimalColumnWriter>(writer, schema_idx, max_repeat, max_define);
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return make_unique<StringColumnWriter>(writer, schema_idx, max_repeat, max_define);
	default:
		throw InternalException("Unsupported type in Parquet writer");
	}
}

} // namespace duckdb
