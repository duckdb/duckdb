#pragma once

#include "parquet_types.h"
#include "thrift_tools.hpp"
#include "miniz_wrapper.hpp"
#include "parquet_rle_bp_decoder.hpp"

#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

using apache::thrift::protocol::TProtocol;
using parquet::format::ColumnChunk;
using parquet::format::CompressionCodec;
using parquet::format::Encoding;
using parquet::format::FieldRepetitionType;
using parquet::format::PageType;
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

	void Read(uint64_t num_values, parquet_filter_t &filter, Vector &result);

	virtual void Skip(idx_t num_values) = 0;

	virtual unique_ptr<BaseStatistics> GetStatistics() = 0;

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
	void VerifyString(LogicalTypeId id, const char *str_data, idx_t str_len);

	bool can_have_nulls;

private:
	apache::thrift::protocol::TProtocol &protocol;
	idx_t rows_available;
	idx_t chunk_read_offset;

	shared_ptr<ResizeableBuffer> block;

	ResizeableBuffer offset_buffer;
	ResizeableBuffer defined_buffer;

	unique_ptr<RleBpDecoder> dict_decoder;
	unique_ptr<RleBpDecoder> defined_decoder;
};

template <Value (*FUNC)(const_data_ptr_t input)>
static unique_ptr<BaseStatistics> templated_get_numeric_stats(const LogicalType &type,
                                                              const parquet::format::Statistics &parquet_stats) {
	auto stats = make_unique<NumericStatistics>(type);

	// for reasons unknown to science, Parquet defines *both* `min` and `min_value` as well as `max` and
	// `max_value`. All are optional. such elegance.
	if (parquet_stats.__isset.min) {
		stats->min = FUNC((const_data_ptr_t)parquet_stats.min.data());
	} else if (parquet_stats.__isset.min_value) {
		stats->min = FUNC((const_data_ptr_t)parquet_stats.min_value.data());
	} else {
		stats->min.is_null = true;
	}
	if (parquet_stats.__isset.max) {
		stats->max = FUNC((const_data_ptr_t)parquet_stats.max.data());
	} else if (parquet_stats.__isset.max_value) {
		stats->max = FUNC((const_data_ptr_t)parquet_stats.max_value.data());
	} else {
		stats->max.is_null = true;
	}
	// GCC 4.x insists on a move() here
	return move(stats);
}

template <class T> static Value transform_statistics_plain(const_data_ptr_t input) {
	return Value(Load<T>(input));
}
// TODO conversion operator template param for timestamps / decimals etc.
template <class VALUE_TYPE> class TemplatedColumnReader : public ColumnReader {

public:
	TemplatedColumnReader(LogicalType type_p, const ColumnChunk &chunk_p, const SchemaElement &schema_p,
	                      TProtocol &protocol_p)
	    : ColumnReader(type_p, chunk_p, schema_p, protocol_p){};

	unique_ptr<BaseStatistics> GetStatistics() override {
		if (!chunk.__isset.meta_data || !chunk.meta_data.__isset.statistics) {
			return nullptr;
		}
		return templated_get_numeric_stats<transform_statistics_plain<VALUE_TYPE>>(type, chunk.meta_data.statistics);
	}

	void Dictionary(shared_ptr<ByteBuffer> data, idx_t num_entries) override {
		dict = move(data);
	}

	void Offsets(uint32_t *offsets, uint8_t *defines, uint64_t num_values, parquet_filter_t &filter,
	             idx_t result_offset, Vector &result) override {
		auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);

		for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
			if (!defines[row_idx]) {
				FlatVector::SetNull(result, row_idx + result_offset, true);
				continue;
			}
			if (filter[row_idx + result_offset]) {
				result_ptr[row_idx + result_offset] = DictRead(offsets[row_idx], result);
			}
		}
	}

	void Plain(shared_ptr<ByteBuffer> plain_data, uint8_t *defines, uint64_t num_values, parquet_filter_t &filter,
	           idx_t result_offset, Vector &result) override {
		auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
		for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
			if (!defines[row_idx]) {
				FlatVector::SetNull(result, row_idx + result_offset, true);
				continue;
			}
			if (filter[row_idx + result_offset]) {
				result_ptr[row_idx + result_offset] = PlainRead(*plain_data, result);
			} else { // there is still some data there that we have to skip over
				PlainSkip(*plain_data);
			}
		}
	}

	void Skip(idx_t num_values) override {
		D_ASSERT(0);
	}

protected:
	virtual VALUE_TYPE DictRead(uint32_t &offset, Vector &result) {
		return ((VALUE_TYPE *)dict->ptr)[offset];
	}

	virtual VALUE_TYPE PlainRead(ByteBuffer &plain_data, Vector &result) {
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

	unique_ptr<BaseStatistics> GetStatistics() override;
	void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries) override;
	void Skip(idx_t num_values) override;

protected:
	string_t PlainRead(ByteBuffer &plain_data, Vector &result) override;
	void PlainSkip(ByteBuffer &plain_data) override;
	string_t DictRead(uint32_t &offset, Vector &result) override;

	void DictReference(Vector &result) override;
	void PlainReference(shared_ptr<ByteBuffer> plain_data, Vector &result) override;

private:
	unique_ptr<string_t[]> dict_strings;
};

} // namespace duckdb
