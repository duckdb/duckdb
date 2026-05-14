#include "writer/enum_column_writer.hpp"

#include <utility>

#include "parquet_rle_bp_decoder.hpp"
#include "parquet_rle_bp_encoder.hpp"
#include "parquet_writer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/serializer/write_stream.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "parquet_column_schema.hpp"

#include <algorithm>
#include <numeric>

namespace duckdb {
class Vector;

using duckdb_parquet::Encoding;

class EnumWriterPageState : public ColumnWriterPageState {
public:
	explicit EnumWriterPageState(uint32_t bit_width) : encoder(bit_width), written_value(false) {
	}

	RleBpEncoder encoder;
	bool written_value;
};

EnumColumnWriter::EnumColumnWriter(ParquetWriter &writer, ParquetColumnSchema &&column_schema,
                                   vector<string> schema_path_p)
    : PrimitiveColumnWriter(writer, std::move(column_schema), std::move(schema_path_p)) {
	bit_width = RleBpDecoder::ComputeBitWidthFromValueCount(EnumType::GetSize(Type()));

	// Precompute lex_rank.
	auto enum_count = EnumType::GetSize(Type());
	auto &enum_values = EnumType::GetValuesInsertOrder(Type());
	auto string_values = FlatVector::GetData<string_t>(enum_values);
	vector<uint32_t> order(enum_count);
	std::iota(order.begin(), order.end(), 0);
	std::sort(order.begin(), order.end(),
	          [&](uint32_t a, uint32_t b) { return LessThan::Operation(string_values[a], string_values[b]); });
	lex_rank.resize(enum_count);
	for (uint32_t rank = 0; rank < enum_count; rank++) {
		lex_rank[order[rank]] = rank;
	}
}

unique_ptr<ColumnWriterStatistics> EnumColumnWriter::InitializeStatsState() {
	return make_uniq<StringStatisticsState>();
}

template <class T>
void EnumColumnWriter::WriteEnumInternal(WriteStream &temp_writer, Vector &input_column, idx_t chunk_start,
                                         idx_t chunk_end, EnumWriterPageState &page_state,
                                         StringStatisticsState &stats) {
	auto &mask = FlatVector::ValidityMutable(input_column);
	auto *ptr = FlatVector::GetData<T>(input_column);

	// Track the lex min/max enum index seen in this chunk.
	uint32_t best_min_idx = 0;
	uint32_t best_max_idx = 0;
	uint32_t best_min_rank = std::numeric_limits<uint32_t>::max();
	uint32_t best_max_rank = 0;
	bool any_seen = false;

	for (idx_t r = chunk_start; r < chunk_end; r++) {
		if (mask.RowIsValid(r)) {
			if (!page_state.written_value) {
				// first value: write the bit-width as a one-byte entry and initialize writer
				temp_writer.Write<uint8_t>(bit_width);
				page_state.encoder.BeginWrite();
				page_state.written_value = true;
			}
			auto idx = ptr[r];
			page_state.encoder.WriteValue(temp_writer, idx);
			auto rank = lex_rank[idx];
			if (!any_seen || rank < best_min_rank) {
				best_min_rank = rank;
				best_min_idx = idx;
			}
			if (!any_seen || rank > best_max_rank) {
				best_max_rank = rank;
				best_max_idx = idx;
			}
			any_seen = true;
		}
	}

	if (any_seen) {
		auto &enum_values = EnumType::GetValuesInsertOrder(Type());
		auto string_values = FlatVector::GetData<string_t>(enum_values);
		stats.Update(string_values[best_min_idx]);
		if (best_max_idx != best_min_idx) {
			stats.Update(string_values[best_max_idx]);
		}
	}
}

void EnumColumnWriter::WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats_p,
                                   ColumnWriterPageState *page_state_p, Vector &input_column, idx_t chunk_start,
                                   idx_t chunk_end) {
	auto &page_state = page_state_p->Cast<EnumWriterPageState>();
	auto &stats = stats_p->Cast<StringStatisticsState>();
	switch (Type().InternalType()) {
	case PhysicalType::UINT8:
		WriteEnumInternal<uint8_t>(temp_writer, input_column, chunk_start, chunk_end, page_state, stats);
		break;
	case PhysicalType::UINT16:
		WriteEnumInternal<uint16_t>(temp_writer, input_column, chunk_start, chunk_end, page_state, stats);
		break;
	case PhysicalType::UINT32:
		WriteEnumInternal<uint32_t>(temp_writer, input_column, chunk_start, chunk_end, page_state, stats);
		break;
	default:
		throw InternalException("Unsupported internal enum type");
	}
}

unique_ptr<ColumnWriterPageState> EnumColumnWriter::InitializePageState(PrimitiveColumnWriterState &state,
                                                                        idx_t page_idx) {
	return make_uniq<EnumWriterPageState>(bit_width);
}

void EnumColumnWriter::FlushPageState(WriteStream &temp_writer, ColumnWriterPageState *state_p) {
	auto &page_state = state_p->Cast<EnumWriterPageState>();
	if (!page_state.written_value) {
		// all values are null
		// just write the bit width
		temp_writer.Write<uint8_t>(bit_width);
		return;
	}
	page_state.encoder.FinishWrite(temp_writer);
}

duckdb_parquet::Encoding::type EnumColumnWriter::GetEncoding(PrimitiveColumnWriterState &state) {
	return Encoding::RLE_DICTIONARY;
}

bool EnumColumnWriter::HasDictionary(PrimitiveColumnWriterState &state) {
	return true;
}

idx_t EnumColumnWriter::DictionarySize(PrimitiveColumnWriterState &state_p) {
	return EnumType::GetSize(Type());
}

void EnumColumnWriter::FlushDictionary(PrimitiveColumnWriterState &state, ColumnWriterStatistics *stats_p) {
	auto &enum_values = EnumType::GetValuesInsertOrder(Type());
	auto enum_count = EnumType::GetSize(Type());
	auto string_values = FlatVector::GetData<string_t>(enum_values);
	// first write the contents of the dictionary page to a temporary buffer
	auto temp_writer = make_uniq<MemoryStream>(BufferAllocator::Get(writer.GetContext()));
	for (idx_t r = 0; r < enum_count; r++) {
		D_ASSERT(!FlatVector::IsNull(enum_values, r));
		// write this string value to the dictionary
		temp_writer->Write<uint32_t>(string_values[r].GetSize());
		temp_writer->WriteData(const_data_ptr_cast(string_values[r].GetData()), string_values[r].GetSize());
	}
	// flush the dictionary page and add it to the to-be-written pages
	WriteDictionary(state, std::move(temp_writer), enum_count);
}

idx_t EnumColumnWriter::GetRowSize(const Vector &vector, const idx_t index,
                                   const PrimitiveColumnWriterState &state) const {
	return (bit_width + 7) / 8;
}

} // namespace duckdb
