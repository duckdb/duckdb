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
	void InitializeDictionary(idx_t dictionary_size);
	void InitializePage();
	void Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset);

private:
	void ConvertDictToSelVec(uint32_t *offsets, const SelectionVector &rows, idx_t count, idx_t result_offset);

private:
	ColumnReader &reader;
	ResizeableBuffer &offset_buffer;
	unique_ptr<RleBpDecoder> dict_decoder;
	SelectionVector valid_sel;
	SelectionVector dictionary_selection_vector;
	idx_t dictionary_size;
	unique_ptr<Vector> dictionary;
	string dictionary_id;
};

} // namespace duckdb
