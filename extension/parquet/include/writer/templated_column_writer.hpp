//===----------------------------------------------------------------------===//
//                         DuckDB
//
// writer/templated_column_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "writer/primitive_column_writer.hpp"
#include "writer/parquet_write_operators.hpp"
#include "parquet_dbp_encoder.hpp"
#include "parquet_dlba_encoder.hpp"
#include "parquet_rle_bp_encoder.hpp"

namespace duckdb {

template <class SRC, class TGT, class OP = ParquetCastOperator>
static void TemplatedWritePlain(Vector &col, ColumnWriterStatistics *stats, const idx_t chunk_start,
                                const idx_t chunk_end, const ValidityMask &mask, WriteStream &ser) {

	const auto *ptr = FlatVector::GetData<SRC>(col);
	for (idx_t r = chunk_start; r < chunk_end; r++) {
		if (!mask.RowIsValid(r)) {
			continue;
		}
		TGT target_value = OP::template Operation<SRC, TGT>(ptr[r]);
		OP::template HandleStats<SRC, TGT>(stats, target_value);
		OP::template WriteToStream<SRC, TGT>(target_value, ser);
	}
}

template <class T>
class StandardColumnWriterState : public PrimitiveColumnWriterState {
public:
	StandardColumnWriterState(duckdb_parquet::RowGroup &row_group, idx_t col_idx)
	    : PrimitiveColumnWriterState(row_group, col_idx) {
	}
	~StandardColumnWriterState() override = default;

	// analysis state for integer values for DELTA_BINARY_PACKED/DELTA_LENGTH_BYTE_ARRAY
	idx_t total_value_count = 0;
	idx_t total_string_size = 0;
	uint32_t key_bit_width = 0;

	unordered_map<T, uint32_t> dictionary;
	duckdb_parquet::Encoding::type encoding;
};

template <class SRC, class TGT>
class StandardWriterPageState : public ColumnWriterPageState {
public:
	explicit StandardWriterPageState(const idx_t total_value_count, const idx_t total_string_size,
	                                 duckdb_parquet::Encoding::type encoding_p,
	                                 const unordered_map<SRC, uint32_t> &dictionary_p)
	    : encoding(encoding_p), dbp_initialized(false), dbp_encoder(total_value_count), dlba_initialized(false),
	      dlba_encoder(total_value_count, total_string_size), bss_encoder(total_value_count, sizeof(TGT)),
	      dictionary(dictionary_p), dict_written_value(false),
	      dict_bit_width(RleBpDecoder::ComputeBitWidth(dictionary.size())), dict_encoder(dict_bit_width) {
	}
	duckdb_parquet::Encoding::type encoding;

	bool dbp_initialized;
	DbpEncoder dbp_encoder;

	bool dlba_initialized;
	DlbaEncoder dlba_encoder;

	BssEncoder bss_encoder;

	const unordered_map<SRC, uint32_t> &dictionary;
	bool dict_written_value;
	uint32_t dict_bit_width;
	RleBpEncoder dict_encoder;
};

template <class SRC, class TGT, class OP = ParquetCastOperator>
class StandardColumnWriter : public PrimitiveColumnWriter {
public:
	StandardColumnWriter(ParquetWriter &writer, idx_t schema_idx, vector<string> schema_path_p, // NOLINT
	                     idx_t max_repeat, idx_t max_define, bool can_have_nulls)
	    : PrimitiveColumnWriter(writer, schema_idx, std::move(schema_path_p), max_repeat, max_define, can_have_nulls) {
	}
	~StandardColumnWriter() override = default;

public:
	unique_ptr<ColumnWriterState> InitializeWriteState(duckdb_parquet::RowGroup &row_group) override {
		auto result = make_uniq<StandardColumnWriterState<SRC>>(row_group, row_group.columns.size());
		result->encoding = duckdb_parquet::Encoding::RLE_DICTIONARY;
		RegisterToRowGroup(row_group);
		return std::move(result);
	}

	unique_ptr<ColumnWriterPageState> InitializePageState(PrimitiveColumnWriterState &state_p) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();

		auto result = make_uniq<StandardWriterPageState<SRC, TGT>>(state.total_value_count, state.total_string_size,
		                                                           state.encoding, state.dictionary);
		return std::move(result);
	}

	void FlushPageState(WriteStream &temp_writer, ColumnWriterPageState *state_p) override {
		auto &page_state = state_p->Cast<StandardWriterPageState<SRC, TGT>>();
		switch (page_state.encoding) {
		case duckdb_parquet::Encoding::DELTA_BINARY_PACKED:
			if (!page_state.dbp_initialized) {
				dbp_encoder::BeginWrite<int64_t>(page_state.dbp_encoder, temp_writer, 0);
			}
			page_state.dbp_encoder.FinishWrite(temp_writer);
			break;
		case duckdb_parquet::Encoding::RLE_DICTIONARY:
			D_ASSERT(page_state.dict_bit_width != 0);
			if (!page_state.dict_written_value) {
				// all values are null
				// just write the bit width
				temp_writer.Write<uint8_t>(page_state.dict_bit_width);
				return;
			}
			page_state.dict_encoder.FinishWrite(temp_writer);
			break;
		case duckdb_parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
			if (!page_state.dlba_initialized) {
				dlba_encoder::BeginWrite<string_t>(page_state.dlba_encoder, temp_writer, string_t(""));
			}
			page_state.dlba_encoder.FinishWrite(temp_writer);
			break;
		case duckdb_parquet::Encoding::BYTE_STREAM_SPLIT:
			page_state.bss_encoder.FinishWrite(temp_writer);
			break;
		case duckdb_parquet::Encoding::PLAIN:
			break;
		default:
			throw InternalException("Unknown encoding");
		}
	}

	duckdb_parquet::Encoding::type GetEncoding(PrimitiveColumnWriterState &state_p) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();
		return state.encoding;
	}

	bool HasAnalyze() override {
		return true;
	}

	void Analyze(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();

		auto data_ptr = FlatVector::GetData<SRC>(vector);
		idx_t vector_index = 0;
		uint32_t new_value_index = state.dictionary.size();

		const bool check_parent_empty = parent && !parent->is_empty.empty();
		const idx_t parent_index = state.definition_levels.size();

		const idx_t vcount =
		    check_parent_empty ? parent->definition_levels.size() - state.definition_levels.size() : count;

		const auto &validity = FlatVector::Validity(vector);

		for (idx_t i = 0; i < vcount; i++) {
			if (check_parent_empty && parent->is_empty[parent_index + i]) {
				continue;
			}
			if (validity.RowIsValid(vector_index)) {
				const auto &src_value = data_ptr[vector_index];
				if (state.dictionary.size() <= writer.DictionarySizeLimit()) {
					if (state.dictionary.find(src_value) == state.dictionary.end()) {
						state.dictionary[src_value] = new_value_index;
						new_value_index++;
					}
				}
				state.total_value_count++;
				state.total_string_size += dlba_encoder::GetDlbaStringSize(src_value);
			}
			vector_index++;
		}
	}

	void FinalizeAnalyze(ColumnWriterState &state_p) override {
		const auto type = writer.GetType(schema_idx);

		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();
		if (state.dictionary.size() == 0 || state.dictionary.size() > writer.DictionarySizeLimit()) {
			if (writer.GetParquetVersion() == ParquetVersion::V1) {
				// Can't do the cool stuff for V1
				state.encoding = duckdb_parquet::Encoding::PLAIN;
			} else {
				// If we aren't doing dictionary encoding, these encodings are virtually always better than PLAIN
				switch (type) {
				case duckdb_parquet::Type::type::INT32:
				case duckdb_parquet::Type::type::INT64:
					state.encoding = duckdb_parquet::Encoding::DELTA_BINARY_PACKED;
					break;
				case duckdb_parquet::Type::type::BYTE_ARRAY:
					state.encoding = duckdb_parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY;
					break;
				case duckdb_parquet::Type::type::FLOAT:
				case duckdb_parquet::Type::type::DOUBLE:
					state.encoding = duckdb_parquet::Encoding::BYTE_STREAM_SPLIT;
					break;
				default:
					state.encoding = duckdb_parquet::Encoding::PLAIN;
				}
			}
			state.dictionary.clear();
		} else {
			state.key_bit_width = RleBpDecoder::ComputeBitWidth(state.dictionary.size());
		}
	}

	unique_ptr<ColumnWriterStatistics> InitializeStatsState() override {
		return OP::template InitializeStats<SRC, TGT>();
	}

	bool HasDictionary(PrimitiveColumnWriterState &state_p) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();
		return state.encoding == duckdb_parquet::Encoding::RLE_DICTIONARY;
	}

	idx_t DictionarySize(PrimitiveColumnWriterState &state_p) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();
		return state.dictionary.size();
	}

	void WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats, ColumnWriterPageState *page_state_p,
	                 Vector &input_column, idx_t chunk_start, idx_t chunk_end) override {
		auto &page_state = page_state_p->Cast<StandardWriterPageState<SRC, TGT>>();

		const auto &mask = FlatVector::Validity(input_column);
		const auto *data_ptr = FlatVector::GetData<SRC>(input_column);

		switch (page_state.encoding) {
		case duckdb_parquet::Encoding::RLE_DICTIONARY: {
			for (idx_t r = chunk_start; r < chunk_end; r++) {
				if (!mask.RowIsValid(r)) {
					continue;
				}
				auto &src_val = data_ptr[r];
				auto value_index = page_state.dictionary.at(src_val);
				if (!page_state.dict_written_value) {
					// first value
					// write the bit-width as a one-byte entry
					temp_writer.Write<uint8_t>(page_state.dict_bit_width);
					// now begin writing the actual value
					page_state.dict_encoder.BeginWrite(temp_writer, value_index);
					page_state.dict_written_value = true;
				} else {
					page_state.dict_encoder.WriteValue(temp_writer, value_index);
				}
			}
			break;
		}
		case duckdb_parquet::Encoding::DELTA_BINARY_PACKED: {
			idx_t r = chunk_start;
			if (!page_state.dbp_initialized) {
				// find first non-null value
				for (; r < chunk_end; r++) {
					if (!mask.RowIsValid(r)) {
						continue;
					}
					const TGT target_value = OP::template Operation<SRC, TGT>(data_ptr[r]);
					OP::template HandleStats<SRC, TGT>(stats, target_value);
					dbp_encoder::BeginWrite(page_state.dbp_encoder, temp_writer, target_value);
					page_state.dbp_initialized = true;
					r++; // skip over
					break;
				}
			}

			for (; r < chunk_end; r++) {
				if (!mask.RowIsValid(r)) {
					continue;
				}
				const TGT target_value = OP::template Operation<SRC, TGT>(data_ptr[r]);
				OP::template HandleStats<SRC, TGT>(stats, target_value);
				dbp_encoder::WriteValue(page_state.dbp_encoder, temp_writer, target_value);
			}
			break;
		}
		case duckdb_parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY: {
			idx_t r = chunk_start;
			if (!page_state.dlba_initialized) {
				// find first non-null value
				for (; r < chunk_end; r++) {
					if (!mask.RowIsValid(r)) {
						continue;
					}
					const TGT target_value = OP::template Operation<SRC, TGT>(data_ptr[r]);
					OP::template HandleStats<SRC, TGT>(stats, target_value);
					dlba_encoder::BeginWrite(page_state.dlba_encoder, temp_writer, target_value);
					page_state.dlba_initialized = true;
					r++; // skip over
					break;
				}
			}

			for (; r < chunk_end; r++) {
				if (!mask.RowIsValid(r)) {
					continue;
				}
				const TGT target_value = OP::template Operation<SRC, TGT>(data_ptr[r]);
				OP::template HandleStats<SRC, TGT>(stats, target_value);
				dlba_encoder::WriteValue(page_state.dlba_encoder, temp_writer, target_value);
			}
			break;
		}
		case duckdb_parquet::Encoding::BYTE_STREAM_SPLIT: {
			for (idx_t r = chunk_start; r < chunk_end; r++) {
				if (!mask.RowIsValid(r)) {
					continue;
				}
				const TGT target_value = OP::template Operation<SRC, TGT>(data_ptr[r]);
				OP::template HandleStats<SRC, TGT>(stats, target_value);
				bss_encoder::WriteValue(page_state.bss_encoder, target_value);
			}
			break;
		}
		case duckdb_parquet::Encoding::PLAIN: {
			D_ASSERT(page_state.encoding == duckdb_parquet::Encoding::PLAIN);
			TemplatedWritePlain<SRC, TGT, OP>(input_column, stats, chunk_start, chunk_end, mask, temp_writer);
			break;
		}
		default:
			throw InternalException("Unknown encoding");
		}
	}

	void FlushDictionary(PrimitiveColumnWriterState &state_p, ColumnWriterStatistics *stats) override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();

		D_ASSERT(state.encoding == duckdb_parquet::Encoding::RLE_DICTIONARY);

		// first we need to sort the values in index order
		auto values = vector<SRC>(state.dictionary.size());
		for (const auto &entry : state.dictionary) {
			values[entry.second] = entry.first;
		}

		state.bloom_filter =
		    make_uniq<ParquetBloomFilter>(state.dictionary.size(), writer.BloomFilterFalsePositiveRatio());

		// first write the contents of the dictionary page to a temporary buffer
		auto temp_writer = make_uniq<MemoryStream>(
		    Allocator::Get(writer.GetContext()), MaxValue<idx_t>(NextPowerOfTwo(state.dictionary.size() * sizeof(TGT)),
		                                                         MemoryStream::DEFAULT_INITIAL_CAPACITY));
		for (idx_t r = 0; r < values.size(); r++) {
			const TGT target_value = OP::template Operation<SRC, TGT>(values[r]);
			// update the statistics
			OP::template HandleStats<SRC, TGT>(stats, target_value);
			// update the bloom filter
			auto hash = OP::template XXHash64<SRC, TGT>(target_value);
			state.bloom_filter->FilterInsert(hash);
			// actually write the dictionary value
			OP::template WriteToStream<SRC, TGT>(target_value, *temp_writer);
		}
		// flush the dictionary page and add it to the to-be-written pages
		WriteDictionary(state, std::move(temp_writer), values.size());
		// bloom filter will be queued for writing in ParquetWriter::BufferBloomFilter one level up
	}

	idx_t GetRowSize(const Vector &vector, const idx_t index,
	                 const PrimitiveColumnWriterState &state_p) const override {
		auto &state = state_p.Cast<StandardColumnWriterState<SRC>>();
		if (state.encoding == duckdb_parquet::Encoding::RLE_DICTIONARY) {
			return (state.key_bit_width + 7) / 8;
		} else {
			return OP::template GetRowSize<SRC, TGT>(vector, index);
		}
	}
};

} // namespace duckdb
