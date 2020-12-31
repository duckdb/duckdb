#pragma once

#include "parquet_types.h"
#include "thrift_tools.hpp"
#include "parquet_rle_bp_decoder.hpp"

#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

using apache::thrift::protocol::TProtocol;

using parquet::format::ColumnChunk;
using parquet::format::FieldRepetitionType;
using parquet::format::PageHeader;
using parquet::format::SchemaElement;

typedef nullmask_t parquet_filter_t;

class ColumnReader {

public:
	ColumnReader(LogicalType type_p, const ColumnChunk &chunk_p, const SchemaElement &schema_p, TProtocol &protocol_p)
	    : type(type_p), chunk(chunk_p), protocol(protocol_p), rows_available(0) {

		// ugh. sometimes there is an extra offset for the dict. sometimes it's wrong.
		chunk_read_offset = chunk.meta_data.data_page_offset;
		if (chunk.meta_data.__isset.dictionary_page_offset && chunk.meta_data.dictionary_page_offset >= 4) {
			// this assumes the data pages follow the dict pages directly.
			chunk_read_offset = chunk.meta_data.dictionary_page_offset;
		}
		can_have_nulls = schema_p.repetition_type == FieldRepetitionType::OPTIONAL;
	};

	virtual ~ColumnReader();

	virtual void Read(uint64_t num_values, parquet_filter_t &filter, Vector &result);

	virtual void Skip(idx_t num_values) = 0;

protected:
	virtual void Plain(shared_ptr<ByteBuffer> plain_data, uint8_t *defines, idx_t num_values, parquet_filter_t &filter,
	                   idx_t result_offset, Vector &result) = 0;

	virtual void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries) = 0;

	virtual void Offsets(uint32_t *offsets, uint8_t *defines, idx_t num_values, parquet_filter_t &filter,
	                     idx_t result_offset, Vector &result) = 0;

	virtual void DictReference(Vector &result) {
	}
	virtual void PlainReference(shared_ptr<ByteBuffer>, Vector &result) {
	}

	LogicalType type;
	const parquet::format::ColumnChunk &chunk;

	bool can_have_nulls;

private:
	void PrepareRead(parquet_filter_t &filter);
	void PreparePage(idx_t compressed_page_size, idx_t uncompressed_page_size);
	void PrepareDataPage(PageHeader &page_hdr);

	apache::thrift::protocol::TProtocol &protocol;
	idx_t rows_available;
	idx_t chunk_read_offset;

	shared_ptr<ResizeableBuffer> block;

	ResizeableBuffer offset_buffer;
	ResizeableBuffer defined_buffer;

	unique_ptr<RleBpDecoder> dict_decoder;
	unique_ptr<RleBpDecoder> defined_decoder;
};

template <class VALUE_TYPE> class TemplatedColumnReader : public ColumnReader {

public:
	TemplatedColumnReader(LogicalType type_p, const ColumnChunk &chunk_p, const SchemaElement &schema_p,
	                      TProtocol &protocol_p)
	    : ColumnReader(type_p, chunk_p, schema_p, protocol_p){};

	void Dictionary(shared_ptr<ByteBuffer> data, idx_t num_entries) override {
		dict = move(data);
	}

	void Offsets(uint32_t *offsets, uint8_t *defines, uint64_t num_values, parquet_filter_t &filter,
	             idx_t result_offset, Vector &result) override {
		auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);

		idx_t offset_idx = 0;
		for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
			if (defines && !defines[row_idx]) {
				FlatVector::SetNull(result, row_idx + result_offset, true);
				continue;
			}
			if (filter[row_idx + result_offset]) {
				result_ptr[row_idx + result_offset] = DictRead(offsets[offset_idx++]);
			}
		}
	}

	void Plain(shared_ptr<ByteBuffer> plain_data, uint8_t *defines, uint64_t num_values, parquet_filter_t &filter,
	           idx_t result_offset, Vector &result) override {
		auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
		for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
			if (defines && !defines[row_idx]) {
				FlatVector::SetNull(result, row_idx + result_offset, true);
				continue;
			}
			if (filter[row_idx + result_offset]) {
				result_ptr[row_idx + result_offset] = PlainRead(*plain_data);
			} else { // there is still some data there that we have to skip over
				PlainSkip(*plain_data);
			}
		}
	}

	void Skip(idx_t num_values) override {
		D_ASSERT(0);
	}

protected:
	virtual VALUE_TYPE DictRead(uint32_t &offset) {
		return ((VALUE_TYPE *)dict->ptr)[offset];
	}

	virtual VALUE_TYPE PlainRead(ByteBuffer &plain_data) {
		return plain_data.read<VALUE_TYPE>();
	}

	virtual void PlainSkip(ByteBuffer &plain_data) {
		plain_data.inc(sizeof(VALUE_TYPE));
	}

	shared_ptr<ByteBuffer> dict;
};

class StringColumnReader : public TemplatedColumnReader<string_t> {

public:
	StringColumnReader(LogicalType type_p, const ColumnChunk &chunk_p, const SchemaElement &schema_p,
	                   TProtocol &protocol_p)
	    : TemplatedColumnReader<string_t>(type_p, chunk_p, schema_p, protocol_p){};

	void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries) override;
	void Skip(idx_t num_values) override;

protected:
	string_t PlainRead(ByteBuffer &plain_data) override;
	void PlainSkip(ByteBuffer &plain_data) override;
	string_t DictRead(uint32_t &offset) override;

	void DictReference(Vector &result) override;
	void PlainReference(shared_ptr<ByteBuffer> plain_data, Vector &result) override;

private:
	unique_ptr<string_t[]> dict_strings;
	void VerifyString(LogicalTypeId id, const char *str_data, idx_t str_len);
};

template <class PARQUET_PHYSICAL_TYPE, timestamp_t (*FUNC)(const PARQUET_PHYSICAL_TYPE &input)>
class TimestampColumnReader : public TemplatedColumnReader<timestamp_t> {

public:
	TimestampColumnReader(LogicalType type_p, const ColumnChunk &chunk_p, const SchemaElement &schema_p,
	                      TProtocol &protocol_p)
	    : TemplatedColumnReader<timestamp_t>(type_p, chunk_p, schema_p, protocol_p){};

protected:
	void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries) {
		dict = make_shared<ResizeableBuffer>(num_entries * sizeof(timestamp_t));
		auto dict_ptr = (timestamp_t *)dict->ptr;
		for (idx_t i = 0; i < num_entries; i++) {
			dict_ptr[i] = FUNC(dictionary_data->read<PARQUET_PHYSICAL_TYPE>());
		}
	}

	void Skip(idx_t num_values) {
		D_ASSERT(0);
	}

	timestamp_t PlainRead(ByteBuffer &plain_data) {
		return FUNC(plain_data.read<PARQUET_PHYSICAL_TYPE>());
	}

	void PlainSkip(ByteBuffer &plain_data) {
		plain_data.inc(sizeof(PARQUET_PHYSICAL_TYPE));
	}
};

class BooleanColumnReader : public TemplatedColumnReader<bool> {
public:
	BooleanColumnReader(LogicalType type_p, const ColumnChunk &chunk_p, const SchemaElement &schema_p,
	                    TProtocol &protocol_p)
	    : TemplatedColumnReader<bool>(type_p, chunk_p, schema_p, protocol_p), byte_pos(0){};
	bool PlainRead(ByteBuffer &plain_data) override {
		plain_data.available(1);
		bool ret = (*plain_data.ptr >> byte_pos) & 1;
		byte_pos++;
		if (byte_pos == 8) {
			byte_pos = 0;
			plain_data.inc(1);
		}
		return ret;
	}
	void PlainSkip(ByteBuffer &plain_data) override {
		PlainRead(plain_data);
	}

private:
	uint8_t byte_pos;
};

class ListColumnReader : public ColumnReader {
public:
	ListColumnReader(LogicalType type_p, const ColumnChunk &chunk_p, const SchemaElement &schema_p,
	                 TProtocol &protocol_p)
	    : ColumnReader(type_p, chunk_p, schema_p, protocol_p) {
		auto child_type = type_p.child_types()[0].second;
		// TODO extract this into separate method, also used in ParquetReader::PrepareRowGroupBuffer
		switch (child_type.id()) {
		case LogicalTypeId::BIGINT:
			child_column_reader = make_unique_base<ColumnReader, TemplatedColumnReader<int64_t>>(child_type, chunk_p,
			                                                                                     schema_p, protocol_p);
			break;
		case LogicalTypeId::VARCHAR:
			child_column_reader =
			    make_unique_base<ColumnReader, StringColumnReader>(child_type, chunk_p, schema_p, protocol_p);

			break;
		default:
			D_ASSERT(0);
		}
	};

	void Read(uint64_t num_values, parquet_filter_t &filter, Vector &result) override {
		D_ASSERT(0);
	}

	virtual void Skip(idx_t num_values) override {
		D_ASSERT(0);
	}

protected
	// TODO this is ugly, need to have more abstract super class

	void Plain(shared_ptr<ByteBuffer> plain_data, uint8_t *defines, idx_t num_values, parquet_filter_t &filter,
	           idx_t result_offset, Vector &result) override {
	}

	void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries) override {
	}

	void Offsets(uint32_t *offsets, uint8_t *defines, idx_t num_values, parquet_filter_t &filter, idx_t result_offset,
	             Vector &result) override {
	}

private:
	unique_ptr<ColumnReader> child_column_reader;
};

} // namespace duckdb
