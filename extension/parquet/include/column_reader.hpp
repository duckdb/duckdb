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
	ColumnReader(LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p)
	    : type(type_p), rows_available(0), schema_idx(schema_idx_p) {
		can_have_nulls = true; // FIXME schema_p.repetition_type == FieldRepetitionType::OPTIONAL;
	};

	virtual void IntializeRead(const std::vector<ColumnChunk> &columns, TProtocol &protocol_p) {
		chunk = &columns[schema_idx];
		protocol = &protocol_p;
		D_ASSERT(chunk);
		// ugh. sometimes there is an extra offset for the dict. sometimes it's wrong.
		chunk_read_offset = chunk->meta_data.data_page_offset;
		if (chunk->meta_data.__isset.dictionary_page_offset && chunk->meta_data.dictionary_page_offset >= 4) {
			// this assumes the data pages follow the dict pages directly.
			chunk_read_offset = chunk->meta_data.dictionary_page_offset;
		}
	}

	virtual ~ColumnReader();

	virtual void Read(uint64_t num_values, parquet_filter_t &filter, Vector &result);

	virtual void Skip(idx_t num_values) = 0;
	// FIXME
	bool is_list = false;

	static unique_ptr<ColumnReader> CreateReader(LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p);

	LogicalType Type() {
		return type;
	}

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

	bool IsNull(idx_t row_idx) {
		if (!is_list) {
			return !defined_buffer.ptr[row_idx];
		}
		// FIXME D_ASSERT(defined_buffer.len < row_idx);
		return defined_buffer.ptr[row_idx] != defined_here_val;
	}

	LogicalType type;
	const parquet::format::ColumnChunk *chunk;

	bool can_have_nulls;
	idx_t defined_here_val = 3; // FIXME

	idx_t schema_idx;

private:
	void PrepareRead(parquet_filter_t &filter);
	void PreparePage(idx_t compressed_page_size, idx_t uncompressed_page_size);
	void PrepareDataPage(PageHeader &page_hdr);

	apache::thrift::protocol::TProtocol *protocol;
	idx_t rows_available;
	idx_t chunk_read_offset;

	shared_ptr<ResizeableBuffer> block;

	ResizeableBuffer offset_buffer;
	ResizeableBuffer defined_buffer;
	ResizeableBuffer repeated_buffer;

	unique_ptr<RleBpDecoder> dict_decoder;
	unique_ptr<RleBpDecoder> defined_decoder;
	unique_ptr<RleBpDecoder> repeated_decoder;
};

template <class VALUE_TYPE> class TemplatedColumnReader : public ColumnReader {

public:
	TemplatedColumnReader(LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p)
	    : ColumnReader(type_p, schema_p, schema_idx_p), dict_size(0){};

	void Dictionary(shared_ptr<ByteBuffer> data, idx_t num_entries) override {
		dict = move(data);
		dict_size = num_entries;
	}

	void Offsets(uint32_t *offsets, uint8_t *defines, uint64_t num_values, parquet_filter_t &filter,
	             idx_t result_offset, Vector &result) override {
		auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);

		idx_t offset_idx = 0;
		for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
			if (IsNull(row_idx)) {
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
			if (IsNull(row_idx)) {
				FlatVector::SetNull(result, row_idx + result_offset, true);
				continue;
			}
			// TODO add IsFiltered() method
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
		D_ASSERT(offset < dict_size);
		return ((VALUE_TYPE *)dict->ptr)[offset];
	}

	virtual VALUE_TYPE PlainRead(ByteBuffer &plain_data) {
		return plain_data.read<VALUE_TYPE>();
	}

	virtual void PlainSkip(ByteBuffer &plain_data) {
		plain_data.inc(sizeof(VALUE_TYPE));
	}

	shared_ptr<ByteBuffer> dict;
	idx_t dict_size;
};

class StringColumnReader : public TemplatedColumnReader<string_t> {

public:
	StringColumnReader(LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p)
	    : TemplatedColumnReader<string_t>(type_p, schema_p, schema_idx_p){};

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
	TimestampColumnReader(LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p)
	    : TemplatedColumnReader<timestamp_t>(type_p, schema_p, schema_idx_p){};

protected:
	void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries) {
		dict = make_shared<ResizeableBuffer>(num_entries * sizeof(timestamp_t));
		auto dict_ptr = (timestamp_t *)dict->ptr;
		for (idx_t i = 0; i < num_entries; i++) {
			dict_ptr[i] = FUNC(dictionary_data->read<PARQUET_PHYSICAL_TYPE>());
		}
		dict_size = num_entries;
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
	BooleanColumnReader(LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p)
	    : TemplatedColumnReader<bool>(type_p, schema_p, schema_idx_p), byte_pos(0){};
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

class StructColumnReader : public TemplatedColumnReader<bool> {

public:
	StructColumnReader(LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p)
	    : TemplatedColumnReader<bool>(type_p, schema_p, schema_idx_p) {
		D_ASSERT(type_p.id() == LogicalTypeId::STRUCT);
		D_ASSERT(!type_p.child_types().empty());
		for (auto &child_type : type_p.child_types()) {
			child_readers.push_back(ColumnReader::CreateReader(child_type.second, schema_p, schema_idx_p));
		}
	};

	void Read(uint64_t num_values, parquet_filter_t &filter, Vector &result) override {
		result.Initialize(type);

		for (idx_t i = 0; i < type.child_types().size(); i++) {
			auto child_read = make_unique<Vector>();
			child_read->Initialize(type.child_types()[i].second);
			child_readers[i]->Read(num_values, filter, *child_read);
			StructVector::AddEntry(result, type.child_types()[i].first, move(child_read));
		}
	}

	virtual void Skip(idx_t num_values) override {
		D_ASSERT(0);
	}

protected:
	// TODO this is ugly, need to have more abstract super class

	void Plain(shared_ptr<ByteBuffer> plain_data, uint8_t *defines, idx_t num_values, parquet_filter_t &filter,
	           idx_t result_offset, Vector &result) override {
	}

	void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries) override {
	}

	void Offsets(uint32_t *offsets, uint8_t *defines, idx_t num_values, parquet_filter_t &filter, idx_t result_offset,
	             Vector &result) override {
	}
	// TODO this should perhaps be a map
	vector<unique_ptr<ColumnReader>> child_readers;
};

class ListColumnReader : public TemplatedColumnReader<list_entry_t> {
public:
	ListColumnReader(LogicalType type_p, const SchemaElement &schema_p, idx_t schema_idx_p)
	    : TemplatedColumnReader<list_entry_t>(type_p, schema_p, schema_idx_p) {
		auto child_type = type_p.child_types()[0].second;
		child_column_reader = ColumnReader::CreateReader(child_type, schema_p, schema_idx_p);
		child_column_reader->is_list = true;
	};

	void Read(uint64_t num_values, parquet_filter_t &filter, Vector &result) override {
		child_column_reader->Read(num_values, filter, result);
	}

	virtual void Skip(idx_t num_values) override {
		D_ASSERT(0);
	}

	void IntializeRead(const std::vector<ColumnChunk> &columns, TProtocol &protocol_p) override {
		child_column_reader->IntializeRead(columns, protocol_p);
	}

protected:
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
