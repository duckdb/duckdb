#pragma once

#include "parquet_types.h"
#include "thrift_tools.hpp"

#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

using apache::thrift::protocol::TProtocol;

class ColumnReader {

public:
    ColumnReader(LogicalType type_p, const parquet::format::ColumnChunk& chunk_p,
	             TProtocol& protocol_p) : type(type_p), chunk(chunk_p), protocol(protocol_p), rows_available(0) {

        // ugh. sometimes there is an extra offset for the dict. sometimes it's wrong.
        chunk_start_offset = chunk.meta_data.data_page_offset;
        if (chunk.meta_data.__isset.dictionary_page_offset && chunk.meta_data.dictionary_page_offset >= 4) {
            // this assumes the data pages follow the dict pages directly.
            chunk_start_offset = chunk.meta_data.dictionary_page_offset;
        }
        chunk_len = chunk.meta_data.total_compressed_size;



    };

    virtual ~ColumnReader();

    void Read(
        uint64_t num_values,
        Vector& result
    )  {
		idx_t to_read = num_values;
        while (to_read  > 0) {
			while (rows_available == 0) {
                parquet::format::PageHeader page_hdr;
                thrift_unpack_file(protocol, chunk_start_offset, &page_hdr);
				
				rows_available = page_hdr.data_page_header.num_values;
			}
		}
	}

    virtual void Skip(idx_t num_values) = 0;

	virtual unique_ptr<BaseStatistics>  GetStatistics() = 0;

protected:
    virtual void ReadInternal(
        uint64_t num_values,
        Vector& result
    ) = 0;


	LogicalType type;
    const parquet::format::ColumnChunk &chunk;

private:
    apache::thrift::protocol::TProtocol& protocol;
    idx_t rows_available;
	idx_t chunk_start_offset;
	idx_t chunk_len;

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

template <class VALUE_TYPE> class NumericColumnReader : public ColumnReader {

public:
    NumericColumnReader(LogicalType type_p, const parquet::format::ColumnChunk& chunk_p, TProtocol& protocol_p) : ColumnReader(type_p, chunk_p, protocol_p) {};

    unique_ptr<BaseStatistics> GetStatistics() override {
		if (!chunk.__isset.meta_data || !chunk.meta_data.__isset.statistics) {
			return nullptr;
		}
        return templated_get_numeric_stats<transform_statistics_plain<VALUE_TYPE>>(type, chunk.meta_data.statistics);
    }

    void ReadInternal(
        uint64_t num_values,
        Vector& result
    ) override {


		// do we have a page? no? read one
		// do we have rows left in current page? read that page
		// see a dict page? decode and store


	}
    void Skip(idx_t num_values) override {

	}


};



}