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
#include "parquet_column_schema.hpp"
#ifndef DUCKDB_AMALGAMATION

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/vector_cache.hpp"
#endif

namespace duckdb {
class ParquetReader;
struct TableFilterState;

using duckdb_apache::thrift::protocol::TProtocol;

using duckdb_parquet::ColumnChunk;
using duckdb_parquet::CompressionCodec;
using duckdb_parquet::FieldRepetitionType;
using duckdb_parquet::PageHeader;
using duckdb_parquet::SchemaElement;
using duckdb_parquet::Type;

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
	ColumnReader(ParquetReader &reader, const ParquetColumnSchema &schema_p);
	virtual ~ColumnReader();

public:
	static unique_ptr<ColumnReader> CreateReader(ParquetReader &reader, const ParquetColumnSchema &schema);
	virtual void InitializeRead(idx_t row_group_index, const vector<ColumnChunk> &columns, TProtocol &protocol_p);
	virtual idx_t Read(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result_out);
	virtual void Select(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result_out,
	                    const SelectionVector &sel, idx_t approved_tuple_count);
	virtual void Filter(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result_out,
	                    const TableFilter &filter, TableFilterState &filter_state, SelectionVector &sel,
	                    idx_t &approved_tuple_count, bool is_first_filter);
	static void ApplyFilter(Vector &v, const TableFilter &filter, TableFilterState &filter_state, idx_t scan_count,
	                        SelectionVector &sel, idx_t &approved_tuple_count);
	virtual void Skip(idx_t num_values);

	ParquetReader &Reader();
	const LogicalType &Type() const {
		return column_schema.type;
	}
	const ParquetColumnSchema &Schema() const {
		return column_schema;
	}

	inline idx_t ColumnIndex() const {
		return column_schema.column_index;
	}
	inline idx_t MaxDefine() const {
		return column_schema.max_define;
	}
	idx_t MaxRepeat() const {
		return column_schema.max_repeat;
	}

	virtual idx_t FileOffset() const;
	virtual uint64_t TotalCompressedSize();
	virtual idx_t GroupRowsAvailable();

	// register the range this reader will touch for prefetching
	virtual void RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge);

	unique_ptr<BaseStatistics> Stats(idx_t row_group_idx_p, const vector<ColumnChunk> &columns);

	template <class VALUE_TYPE, class CONVERSION, bool HAS_DEFINES>
	void PlainTemplatedDefines(ByteBuffer &plain_data, const uint8_t *defines, uint64_t num_values, idx_t result_offset,
	                           Vector &result) {
		if (CONVERSION::PlainAvailable(plain_data, num_values)) {
			PlainTemplatedInternal<VALUE_TYPE, CONVERSION, HAS_DEFINES, false>(plain_data, defines, num_values,
			                                                                   result_offset, result);
		} else {
			PlainTemplatedInternal<VALUE_TYPE, CONVERSION, HAS_DEFINES, true>(plain_data, defines, num_values,
			                                                                  result_offset, result);
		}
	}
	template <class VALUE_TYPE, class CONVERSION>
	void PlainTemplated(ByteBuffer &plain_data, const uint8_t *defines, uint64_t num_values, idx_t result_offset,
	                    Vector &result) {
		if (HasDefines() && defines) {
			PlainTemplatedDefines<VALUE_TYPE, CONVERSION, true>(plain_data, defines, num_values, result_offset, result);
		} else {
			PlainTemplatedDefines<VALUE_TYPE, CONVERSION, false>(plain_data, defines, num_values, result_offset,
			                                                     result);
		}
	}

	template <class CONVERSION, bool HAS_DEFINES>
	void PlainSkipTemplatedDefines(ByteBuffer &plain_data, const uint8_t *defines, uint64_t num_values) {
		if (CONVERSION::PlainAvailable(plain_data, num_values)) {
			PlainSkipTemplatedInternal<CONVERSION, HAS_DEFINES, false>(plain_data, defines, num_values);
		} else {
			PlainSkipTemplatedInternal<CONVERSION, HAS_DEFINES, true>(plain_data, defines, num_values);
		}
	}
	template <class CONVERSION>
	void PlainSkipTemplated(ByteBuffer &plain_data, const uint8_t *defines, uint64_t num_values) {
		if (HasDefines() && defines) {
			PlainSkipTemplatedDefines<CONVERSION, true>(plain_data, defines, num_values);
		} else {
			PlainSkipTemplatedDefines<CONVERSION, false>(plain_data, defines, num_values);
		}
	}

	template <class VALUE_TYPE, class CONVERSION>
	void PlainSelectTemplated(ByteBuffer &plain_data, const uint8_t *defines, uint64_t num_values, Vector &result,
	                          const SelectionVector &sel, idx_t approved_tuple_count) {
		if (HasDefines() && defines) {
			PlainSelectTemplatedInternal<VALUE_TYPE, CONVERSION, true, true>(plain_data, defines, num_values, result,
			                                                                 sel, approved_tuple_count);
		} else {
			PlainSelectTemplatedInternal<VALUE_TYPE, CONVERSION, false, true>(plain_data, defines, num_values, result,
			                                                                  sel, approved_tuple_count);
		}
	}

	idx_t GetValidCount(uint8_t *defines, idx_t count, idx_t offset = 0) const {
		if (!defines) {
			return count;
		}
		idx_t valid_count = 0;
		for (idx_t i = offset; i < offset + count; i++) {
			valid_count += defines[i] == MaxDefine();
		}
		return valid_count;
	}

protected:
	virtual bool SupportsDirectFilter() const {
		return false;
	}
	virtual bool SupportsDirectSelect() const {
		return false;
	}
	void DirectFilter(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result_out,
	                  const TableFilter &filter, TableFilterState &filter_state, SelectionVector &sel,
	                  idx_t &approved_tuple_count);
	void DirectSelect(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result,
	                  const SelectionVector &sel, idx_t approved_tuple_count);

private:
	//! Check if a previous table filter has filtered out this page
	bool PageIsFilteredOut(PageHeader &page_hdr);
	void BeginRead(data_ptr_t define_out, data_ptr_t repeat_out);
	void FinishRead(idx_t read_count);
	idx_t ReadPageHeaders(idx_t max_read, optional_ptr<const TableFilter> filter = nullptr,
	                      optional_ptr<TableFilterState> filter_state = nullptr);
	idx_t ReadInternal(uint64_t num_values, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result);
	//! Prepare a read of up to "max_read" rows and read the defines/repeats.
	//! Returns whether all values are valid (i.e., not NULL)
	bool PrepareRead(idx_t read_count, data_ptr_t define_out, data_ptr_t repeat_out, idx_t result_offset);
	void ReadData(idx_t read_now, data_ptr_t define_out, data_ptr_t repeat_out, Vector &result, idx_t result_offset);

	template <class VALUE_TYPE, class CONVERSION, bool HAS_DEFINES, bool CHECKED>
	void PlainTemplatedInternal(ByteBuffer &plain_data, const uint8_t *__restrict defines, const uint64_t num_values,
	                            const idx_t result_offset, Vector &result) {
		const auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
		if (!HAS_DEFINES && !CHECKED && CONVERSION::PlainConstantSize() == sizeof(VALUE_TYPE)) {
			// we can memcpy
			idx_t copy_count = num_values * CONVERSION::PlainConstantSize();
			memcpy(result_ptr + result_offset, plain_data.ptr, copy_count);
			plain_data.unsafe_inc(copy_count);
			return;
		}
		auto &result_mask = FlatVector::Validity(result);
		for (idx_t row_idx = result_offset; row_idx < result_offset + num_values; row_idx++) {
			if (HAS_DEFINES && defines[row_idx] != MaxDefine()) {
				result_mask.SetInvalid(row_idx);
				continue;
			}
			result_ptr[row_idx] = CONVERSION::template PlainRead<CHECKED>(plain_data, *this);
		}
	}

	template <class CONVERSION, bool HAS_DEFINES, bool CHECKED>
	void PlainSkipTemplatedInternal(ByteBuffer &plain_data, const uint8_t *__restrict defines,
	                                const uint64_t num_values, idx_t row_offset = 0) {
		if (!HAS_DEFINES && CONVERSION::PlainConstantSize() > 0) {
			if (CHECKED) {
				plain_data.inc(num_values * CONVERSION::PlainConstantSize());
			} else {
				plain_data.unsafe_inc(num_values * CONVERSION::PlainConstantSize());
			}
			return;
		}
		for (idx_t row_idx = row_offset; row_idx < row_offset + num_values; row_idx++) {
			if (HAS_DEFINES && defines[row_idx] != MaxDefine()) {
				continue;
			}
			CONVERSION::template PlainSkip<CHECKED>(plain_data, *this);
		}
	}

	template <class VALUE_TYPE, class CONVERSION, bool HAS_DEFINES, bool CHECKED>
	void PlainSelectTemplatedInternal(ByteBuffer &plain_data, const uint8_t *__restrict defines,
	                                  const uint64_t num_values, Vector &result, const SelectionVector &sel,
	                                  idx_t approved_tuple_count) {
		const auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
		auto &result_mask = FlatVector::Validity(result);
		idx_t current_entry = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			auto next_entry = sel.get_index(i);
			D_ASSERT(current_entry <= next_entry);
			// perform any skips forward if required
			PlainSkipTemplatedInternal<CONVERSION, HAS_DEFINES, CHECKED>(plain_data, defines,
			                                                             next_entry - current_entry, current_entry);
			// read this row
			if (HAS_DEFINES && defines[next_entry] != MaxDefine()) {
				result_mask.SetInvalid(next_entry);
			} else {
				result_ptr[next_entry] = CONVERSION::template PlainRead<CHECKED>(plain_data, *this);
			}
			current_entry = next_entry + 1;
		}
		if (current_entry < num_values) {
			// skip forward to the end of where we are selecting
			PlainSkipTemplatedInternal<CONVERSION, HAS_DEFINES, CHECKED>(plain_data, defines,
			                                                             num_values - current_entry, current_entry);
		}
	}

protected:
	Allocator &GetAllocator();
	// readers that use the default Read() need to implement those
	virtual void PlainSkip(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values);
	virtual void Plain(ByteBuffer &plain_data, uint8_t *defines, idx_t num_values, idx_t result_offset, Vector &result);
	virtual void Plain(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
	                   idx_t result_offset, Vector &result);
	virtual void PlainSelect(shared_ptr<ResizeableBuffer> &plain_data, uint8_t *defines, idx_t num_values,
	                         Vector &result, const SelectionVector &sel, idx_t count);

	// applies any skips that were registered using Skip()
	virtual void ApplyPendingSkips(data_ptr_t define_out, data_ptr_t repeat_out);

	inline bool HasDefines() const {
		return MaxDefine() > 0;
	}

	inline bool HasRepeats() const {
		return MaxRepeat() > 0;
	}

protected:
	const ParquetColumnSchema &column_schema;

	ParquetReader &reader;
	idx_t pending_skips = 0;
	bool page_is_filtered_out = false;

	virtual void ResetPage();

private:
	void AllocateBlock(idx_t size);
	void PrepareRead(optional_ptr<const TableFilter> filter, optional_ptr<TableFilterState> filter_state);
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

public:
	template <class TARGET>
	TARGET &Cast() {
		if (TARGET::TYPE != PhysicalType::INVALID && Type().InternalType() != TARGET::TYPE) {
			throw InternalException("Failed to cast column reader to type - type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (TARGET::TYPE != PhysicalType::INVALID && Type().InternalType() != TARGET::TYPE) {
			throw InternalException("Failed to cast column reader to type - type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

} // namespace duckdb
