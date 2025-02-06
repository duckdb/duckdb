//===----------------------------------------------------------------------===//
//                         DuckDB
//
// column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "parquet_bss_decoder.hpp"
#include "parquet_statistics.hpp"
#include "parquet_types.h"
#include "resizable_buffer.hpp"
#include "thrift_tools.hpp"
#include "decoder/byte_stream_split_decoder.hpp"
#include "decoder/delta_binary_packed_decoder.hpp"
#include "decoder/dictionary_decoder.hpp"
#include "decoder/rle_decoder.hpp"
#include "decoder/delta_length_byte_array_decoder.hpp"
#include "decoder/delta_byte_array_decoder.hpp"
#ifndef DUCKDB_AMALGAMATION

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#endif

namespace duckdb {
class ParquetReader;

using duckdb_apache::thrift::protocol::TProtocol;

using duckdb_parquet::ColumnChunk;
using duckdb_parquet::CompressionCodec;
using duckdb_parquet::FieldRepetitionType;
using duckdb_parquet::PageHeader;
using duckdb_parquet::SchemaElement;
using duckdb_parquet::Type;

typedef std::bitset<STANDARD_VECTOR_SIZE> parquet_filter_t;

enum class ColumnEncoding {
	INVALID,
	DICTIONARY,
	DELTA_BINARY_PACKED,
	RLE,
	DELTA_LENGTH_BYTE_ARRAY,
	DELTA_BYTE_ARRAY,
	BYTE_STREAM_SPLIT,
	PLAIN
};

class ColumnReader {
	friend class ByteStreamSplitDecoder;
	friend class DeltaBinaryPackedDecoder;
	friend class DeltaByteArrayDecoder;
	friend class DeltaLengthByteArrayDecoder;
	friend class DictionaryDecoder;
	friend class RLEDecoder;

public:
	ColumnReader(ParquetReader &reader, LogicalType type_p, const SchemaElement &schema_p, idx_t file_idx_p,
	             idx_t max_define_p, idx_t max_repeat_p);
	virtual ~ColumnReader();

public:
	static unique_ptr<ColumnReader> CreateReader(ParquetReader &reader, const LogicalType &type_p,
	                                             const SchemaElement &schema_p, idx_t schema_idx_p, idx_t max_define,
	                                             idx_t max_repeat);
	virtual void InitializeRead(idx_t row_group_index, const vector<ColumnChunk> &columns, TProtocol &protocol_p);
	virtual idx_t Read(uint64_t num_values, parquet_filter_t &filter, data_ptr_t define_out, data_ptr_t repeat_out,
	                   Vector &result_out);

	virtual void Skip(idx_t num_values);

	ParquetReader &Reader();
	const LogicalType &Type() const;
	const SchemaElement &Schema() const;
	optional_ptr<const SchemaElement> GetParentSchema() const;
	void SetParentSchema(const SchemaElement &parent_schema);

	idx_t FileIdx() const;
	idx_t MaxDefine() const;
	idx_t MaxRepeat() const;

	virtual idx_t FileOffset() const;
	virtual uint64_t TotalCompressedSize();
	virtual idx_t GroupRowsAvailable();

	// register the range this reader will touch for prefetching
	virtual void RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge);

	virtual unique_ptr<BaseStatistics> Stats(idx_t row_group_idx_p, const vector<ColumnChunk> &columns);

	template <class VALUE_TYPE, class CONVERSION>
	void PlainTemplated(ByteBuffer &plain_data, uint8_t *defines, uint64_t num_values, parquet_filter_t *filter,
	                    idx_t result_offset, Vector &result) {
		if (HasDefines()) {
			if (CONVERSION::PlainAvailable(plain_data, num_values)) {
				PlainTemplatedInternal<VALUE_TYPE, CONVERSION, true, true>(plain_data, defines, num_values, filter,
				                                                           result_offset, result);
			} else {
				PlainTemplatedInternal<VALUE_TYPE, CONVERSION, true, false>(plain_data, defines, num_values, filter,
				                                                            result_offset, result);
			}
		} else {
			if (CONVERSION::PlainAvailable(plain_data, num_values)) {
				PlainTemplatedInternal<VALUE_TYPE, CONVERSION, false, true>(plain_data, defines, num_values, filter,
				                                                            result_offset, result);
			} else {
				PlainTemplatedInternal<VALUE_TYPE, CONVERSION, false, false>(plain_data, defines, num_values, filter,
				                                                             result_offset, result);
			}
		}
	}

private:
	template <class VALUE_TYPE, class CONVERSION, bool HAS_DEFINES, bool UNSAFE>
	void PlainTemplatedInternal(ByteBuffer &plain_data, const uint8_t *__restrict defines, const uint64_t num_values,
	                            const parquet_filter_t *filter, const idx_t result_offset, Vector &result) {
		const auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
		auto &result_mask = FlatVector::Validity(result);
		for (idx_t row_idx = result_offset; row_idx < result_offset + num_values; row_idx++) {
			if (HAS_DEFINES && defines && defines[row_idx] != max_define) {
				result_mask.SetInvalid(row_idx);
			} else if (!filter || filter->test(row_idx)) {
				result_ptr[row_idx] =
				    UNSAFE ? CONVERSION::UnsafePlainRead(plain_data, *this) : CONVERSION::PlainRead(plain_data, *this);
			} else { // there is still some data there that we have to skip over
				if (UNSAFE) {
					CONVERSION::UnsafePlainSkip(plain_data, *this);
				} else {
					CONVERSION::PlainSkip(plain_data, *this);
				}
			}
		}
	}

protected:
	Allocator &GetAllocator();
	// readers that use the default Read() need to implement those
	virtual void Plain(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values, parquet_filter_t *filter,
	                   idx_t result_offset, Vector &result);
	virtual void Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
	                   parquet_filter_t *filter, idx_t result_offset, Vector &result);

	// applies any skips that were registered using Skip()
	virtual void ApplyPendingSkips(idx_t num_values);

	bool HasDefines() const {
		return max_define > 0;
	}

	bool HasRepeats() const {
		return max_repeat > 0;
	}

protected:
	const SchemaElement &schema;
	optional_ptr<const SchemaElement> parent_schema;

	idx_t file_idx;
	idx_t max_define;
	idx_t max_repeat;

	ParquetReader &reader;
	LogicalType type;

	idx_t pending_skips = 0;

	virtual void ResetPage();

private:
	void AllocateBlock(idx_t size);
	void AllocateCompressed(idx_t size);
	void PrepareRead(parquet_filter_t &filter);
	void PreparePage(PageHeader &page_hdr);
	void PrepareDataPage(PageHeader &page_hdr);
	void PreparePageV2(PageHeader &page_hdr);
	void DecompressInternal(CompressionCodec::type codec, const_data_ptr_t src, idx_t src_size, data_ptr_t dst,
	                        idx_t dst_size);
	const ColumnChunk *chunk = nullptr;

	TProtocol *protocol;
	idx_t page_rows_available;
	idx_t group_rows_available;
	idx_t chunk_read_offset;

	shared_ptr<ResizeableBuffer> block;

	ResizeableBuffer compressed_buffer;

	ColumnEncoding encoding = ColumnEncoding::INVALID;
	unique_ptr<RleBpDecoder> defined_decoder;
	unique_ptr<RleBpDecoder> repeated_decoder;
	DictionaryDecoder dictionary_decoder;
	DeltaBinaryPackedDecoder delta_binary_packed_decoder;
	RLEDecoder rle_decoder;
	DeltaLengthByteArrayDecoder delta_length_byte_array_decoder;
	DeltaByteArrayDecoder delta_byte_array_decoder;
	ByteStreamSplitDecoder byte_stream_split_decoder;

	//! Resizeable buffers used for the various encodings above
	ResizeableBuffer encoding_buffers[2];

	// dummies for Skip()
	parquet_filter_t none_filter;
	ResizeableBuffer dummy_define;
	ResizeableBuffer dummy_repeat;

public:
	template <class TARGET>
	TARGET &Cast() {
		if (TARGET::TYPE != PhysicalType::INVALID && type.InternalType() != TARGET::TYPE) {
			throw InternalException("Failed to cast column reader to type - type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (TARGET::TYPE != PhysicalType::INVALID && type.InternalType() != TARGET::TYPE) {
			throw InternalException("Failed to cast column reader to type - type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
