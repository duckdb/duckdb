#pragma once

#include "parquet_types.h"
#include "thrift_tools.hpp"
#include "miniz_wrapper.hpp"
#include "parquet_rle_bp_decoder.hpp"

#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/common/types/vector.hpp"

namespace duckdb {

using apache::thrift::protocol::TProtocol;
using parquet::format::CompressionCodec;
using parquet::format::Encoding;
using parquet::format::PageType;

// class ParquetDecoder {
//    TProtocol& protocol_p;
//	unique_ptr<FileHandle> handle
//};

class ColumnReader {

public:
	ColumnReader(LogicalType type_p, const parquet::format::ColumnChunk &chunk_p, TProtocol &protocol_p)
	    : type(type_p), chunk(chunk_p), protocol(protocol_p), rows_available(0) {

		// ugh. sometimes there is an extra offset for the dict. sometimes it's wrong.
		chunk_read_offset = chunk.meta_data.data_page_offset;
		if (chunk.meta_data.__isset.dictionary_page_offset && chunk.meta_data.dictionary_page_offset >= 4) {
			// this assumes the data pages follow the dict pages directly.
			chunk_read_offset = chunk.meta_data.dictionary_page_offset;
		}
		chunk_len = chunk.meta_data.total_compressed_size;

		auto trans = (DuckdbFileTransport *)protocol.getTransport().get();
		trans->SetLocation(chunk_read_offset);
	};

	virtual ~ColumnReader();

	void Read(uint64_t num_values, Vector &result);

	virtual void Skip(idx_t num_values) = 0;

	virtual unique_ptr<BaseStatistics> GetStatistics() = 0;

protected:
	virtual void Plain(ByteBuffer *data, idx_t num_values, idx_t result_offset, Vector &result) = 0;

    virtual void Dictionary(ByteBuffer *data, idx_t num_entries) = 0;

	virtual void Offsets(uint32_t *offsets, idx_t num_values, idx_t result_offset, Vector &result) = 0;

	LogicalType type;
	const parquet::format::ColumnChunk &chunk;

private:
	apache::thrift::protocol::TProtocol &protocol;
	idx_t rows_available;
	idx_t chunk_read_offset;
	idx_t chunk_len;
	ResizeableBuffer unpack_buffer;
	ResizeableBuffer read_buffer;
	ResizeableBuffer offset_buffer;

	ByteBuffer *col_data_buf;
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
template <class VALUE_TYPE> class NumericColumnReader : public ColumnReader {

public:
	NumericColumnReader(LogicalType type_p, const parquet::format::ColumnChunk &chunk_p, TProtocol &protocol_p)
	    : ColumnReader(type_p, chunk_p, protocol_p){};

	unique_ptr<BaseStatistics> GetStatistics() override {
		if (!chunk.__isset.meta_data || !chunk.meta_data.__isset.statistics) {
			return nullptr;
		}
		return templated_get_numeric_stats<transform_statistics_plain<VALUE_TYPE>>(type, chunk.meta_data.statistics);
	}

	void Dictionary(ByteBuffer *data, idx_t num_entries) override {
		dict_buf.resize(num_entries * GetTypeIdSize(type.InternalType()));
		data->copy_to(dict_buf.ptr, dict_buf.len); // TODO HMMM
	}

	// TODO pass NULL mask / skip mask into those functions
	void Offsets(uint32_t *offsets, uint64_t num_values, idx_t result_offset, Vector &result) override {
		auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
		auto dict_ptr = (VALUE_TYPE *)dict_buf.ptr;

		for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
			result_ptr[row_idx + result_offset] = dict_ptr[offsets[row_idx]];
		}
	}

	void Plain(ByteBuffer *data, uint64_t num_values, idx_t result_offset, Vector &result) override {
		auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
		for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
			result_ptr[row_idx + result_offset] = data->read<VALUE_TYPE>();
		}
	}

	void Skip(idx_t num_values) override {
		D_ASSERT(0);
	}

private:
	ResizeableBuffer dict_buf;
};

class StringColumnReader : public ColumnReader {

public:
	StringColumnReader(LogicalType type_p, const parquet::format::ColumnChunk &chunk_p, TProtocol &protocol_p)
	    : ColumnReader(type_p, chunk_p, protocol_p){};

	unique_ptr<BaseStatistics> GetStatistics() override {
		if (!chunk.__isset.meta_data || !chunk.meta_data.__isset.statistics) {
			return nullptr;
		}
		// FIXME   return templated_get_numeric_stats<transform_statistics_plain<VALUE_TYPE>>(type,
		// chunk.meta_data.statistics);
	}

	void Dictionary(ByteBuffer *data, idx_t num_entries) override {
		//        dict_buf.resize(num_entries * GetTypeIdSize(type.InternalType()));
		//        data->copy_to(dict_buf.ptr, dict_buf.len); // TODO HMMM
	}

	// TODO pass NULL mask / skip mask into those functions
	void Offsets(uint32_t *offsets, uint64_t num_values, idx_t result_offset, Vector &result) override {
		//        auto result_ptr = FlatVector::GetData<string_t>(result);
		//        auto dict_ptr = (VALUE_TYPE *)dict_buf.ptr;
		//
		//        for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
		//            result_ptr[row_idx + result_offset] = dict_ptr[offsets[row_idx]];
		//        }
	}

	void Plain(ByteBuffer *data, uint64_t num_values, idx_t result_offset, Vector &result) override {
		//        auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
		//        for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
		//            result_ptr[row_idx + result_offset] = data->read<VALUE_TYPE>();
		//        }
	}

	void Skip(idx_t num_values) override {
		D_ASSERT(0);
	}

private:
	ResizeableBuffer dict_buf;
};

} // namespace duckdb
