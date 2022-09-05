//===----------------------------------------------------------------------===//
//                         DuckDB
//
// column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parquet_types.h"
#include "thrift_tools.hpp"
#include "resizable_buffer.hpp"

#include "parquet_rle_bp_decoder.hpp"
#include "parquet_dbp_decoder.hpp"
#include "parquet_statistics.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/storage/statistics/string_statistics.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#endif

namespace duckdb {
class ParquetReader;

using duckdb_apache::thrift::protocol::TProtocol;

using duckdb_parquet::format::ColumnChunk;
using duckdb_parquet::format::FieldRepetitionType;
using duckdb_parquet::format::PageHeader;
using duckdb_parquet::format::SchemaElement;
using duckdb_parquet::format::Type;

typedef std::bitset<STANDARD_VECTOR_SIZE> parquet_filter_t;

class ColumnReader {
public:
	ColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t file_idx_p,
	             idx_t max_define_p, idx_t max_repeat_p);
	virtual ~ColumnReader();

public:
	static unique_ptr<ColumnReader> CreateReader(ParquetReader &reader, const LogicalType &type_p,
	                                             const SchemaElement &schema_p, idx_t schema_idx_p, idx_t max_define,
	                                             idx_t max_repeat);
	virtual void InitializeRead(const std::vector<ColumnChunk> &columns, TProtocol &protocol_p);
	virtual idx_t Read(uint64_t num_values, parquet_filter_t &filter, uint8_t *define_out, uint8_t *repeat_out,
	                   Vector &result_out);

	virtual void Skip(idx_t num_values);

	ParquetReader &Reader();
	const LogicalType &Type() const;
	const SchemaElement &Schema() const;
	idx_t FileIdx() const;
	idx_t MaxDefine() const;
	idx_t MaxRepeat() const;

	virtual idx_t FileOffset() const;
	virtual uint64_t TotalCompressedSize();
	virtual idx_t GroupRowsAvailable();

	// register the range this reader will touch for prefetching
	virtual void RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge);

	virtual unique_ptr<BaseStatistics> Stats(const std::vector<ColumnChunk> &columns);

protected:
	// readers that use the default Read() need to implement those
	virtual void Plain(shared_ptr<ByteBuffer> plain_data, uint8_t *defines, idx_t num_values, parquet_filter_t &filter,
	                   idx_t result_offset, Vector &result);
	virtual void Dictionary(shared_ptr<ByteBuffer> dictionary_data, idx_t num_entries);
	virtual void Offsets(uint32_t *offsets, uint8_t *defines, idx_t num_values, parquet_filter_t &filter,
	                     idx_t result_offset, Vector &result);

	// these are nops for most types, but not for strings
	virtual void DictReference(Vector &result);
	virtual void PlainReference(shared_ptr<ByteBuffer>, Vector &result);

	// applies any skips that were registered using Skip()
	virtual void ApplyPendingSkips(idx_t num_values);

	bool HasDefines() {
		return max_define > 0;
	}

	bool HasRepeats() {
		return max_repeat > 0;
	}

protected:
	const SchemaElement &schema;

	idx_t file_idx;
	idx_t max_define;
	idx_t max_repeat;

	ParquetReader &reader;
	LogicalType type;

	idx_t pending_skips = 0;

private:
	void PrepareRead(parquet_filter_t &filter);
	void PreparePage(idx_t compressed_page_size, idx_t uncompressed_page_size);
	void PrepareDataPage(PageHeader &page_hdr);
	void PreparePageV2(PageHeader &page_hdr);

	const duckdb_parquet::format::ColumnChunk *chunk = nullptr;

	duckdb_apache::thrift::protocol::TProtocol *protocol;
	idx_t page_rows_available;
	idx_t group_rows_available;
	idx_t chunk_read_offset;

	shared_ptr<ResizeableBuffer> block;

	ResizeableBuffer offset_buffer;

	unique_ptr<RleBpDecoder> dict_decoder;
	unique_ptr<RleBpDecoder> defined_decoder;
	unique_ptr<RleBpDecoder> repeated_decoder;
	unique_ptr<DbpDecoder> dbp_decoder;

	// dummies for Skip()
	parquet_filter_t none_filter;
	ResizeableBuffer dummy_define;
	ResizeableBuffer dummy_repeat;
};

} // namespace duckdb
