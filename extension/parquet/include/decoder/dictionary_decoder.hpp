//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decoder/dictionary_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "parquet_rle_bp_decoder.hpp"
#include "resizable_buffer.hpp"

namespace duckdb {
class ColumnReader;

class DictionaryDecoder {
public:
	explicit DictionaryDecoder(ColumnReader &reader);

public:
	void InitializeDictionary(idx_t dictionary_size, optional_ptr<const TableFilter> filter, bool has_defines);
	void InitializePage();
	idx_t Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset);
	void Skip(uint8_t *defines, idx_t skip_count);
	bool CanFilter(const TableFilter &filter);
	void Filter(uint8_t *defines, idx_t read_count, Vector &result, const TableFilter &filter, SelectionVector &sel,
	            idx_t &approved_tuple_count);
	bool HasFilter() const {
		return filter_result.get();
	}
	bool HasFilteredOutAllValues() const {
		return HasFilter() && filter_count == 0;
	}

private:
	idx_t GetValidValues(uint8_t *defines, idx_t read_count, idx_t result_offset);
	void ConvertDictToSelVec(uint32_t *offsets, const SelectionVector &rows, idx_t count);

private:
	ColumnReader &reader;
	ResizeableBuffer &offset_buffer;
	unique_ptr<RleBpDecoder> dict_decoder;
	SelectionVector valid_sel;
	SelectionVector dictionary_selection_vector;
	idx_t dictionary_size;
	unique_ptr<Vector> dictionary;
	unsafe_unique_array<bool> filter_result;
	idx_t filter_count;
	bool can_have_nulls;
	string dictionary_id;
};

} // namespace duckdb
