#include "writer/enum_column_writer.hpp"
#include "parquet_rle_bp_decoder.hpp"
#include "parquet_rle_bp_encoder.hpp"
#include "parquet_writer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

namespace duckdb {
using duckdb_parquet::Encoding;

class EnumWriterPageState : public ColumnWriterPageState {
public:
	explicit EnumWriterPageState(uint32_t bit_width) : encoder(bit_width), written_value(false) {
	}

	RleBpEncoder encoder;
	bool written_value;
};

EnumColumnWriter::EnumColumnWriter(ParquetWriter &writer, const ParquetColumnSchema &column_schema,
                                   vector<string> schema_path_p, bool can_have_nulls)
    : PrimitiveColumnWriter(writer, column_schema, std::move(schema_path_p), can_have_nulls) {
	bit_width = RleBpDecoder::ComputeBitWidth(EnumType::GetSize(Type()));
}

unique_ptr<ColumnWriterStatistics> EnumColumnWriter::InitializeStatsState() {
	return make_uniq<StringStatisticsState>();
}

template <class T>
void EnumColumnWriter::WriteEnumInternal(WriteStream &temp_writer, Vector &input_column, idx_t chunk_start,
                                         idx_t chunk_end, EnumWriterPageState &page_state) {
	auto &mask = FlatVector::Validity(input_column);
	auto *ptr = FlatVector::GetData<T>(input_column);
	for (idx_t r = chunk_start; r < chunk_end; r++) {
		if (mask.RowIsValid(r)) {
			if (!page_state.written_value) {
				// first value: write the bit-width as a one-byte entry and initialize writer
				temp_writer.Write<uint8_t>(bit_width);
				page_state.encoder.BeginWrite();
				page_state.written_value = true;
			}
			page_state.encoder.WriteValue(temp_writer, ptr[r]);
		}
	}
}

void EnumColumnWriter::WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats_p,
                                   ColumnWriterPageState *page_state_p, Vector &input_column, idx_t chunk_start,
                                   idx_t chunk_end) {
	auto &page_state = page_state_p->Cast<EnumWriterPageState>();
	switch (Type().InternalType()) {
	case PhysicalType::UINT8:
		WriteEnumInternal<uint8_t>(temp_writer, input_column, chunk_start, chunk_end, page_state);
		break;
	case PhysicalType::UINT16:
		WriteEnumInternal<uint16_t>(temp_writer, input_column, chunk_start, chunk_end, page_state);
		break;
	case PhysicalType::UINT32:
		WriteEnumInternal<uint32_t>(temp_writer, input_column, chunk_start, chunk_end, page_state);
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
	auto &stats = stats_p->Cast<StringStatisticsState>();
	// write the enum values to a dictionary page
	auto &enum_values = EnumType::GetValuesInsertOrder(Type());
	auto enum_count = EnumType::GetSize(Type());
	auto string_values = FlatVector::GetData<string_t>(enum_values);
	// first write the contents of the dictionary page to a temporary buffer
	auto temp_writer = make_uniq<MemoryStream>(Allocator::Get(writer.GetContext()));
	for (idx_t r = 0; r < enum_count; r++) {
		D_ASSERT(!FlatVector::IsNull(enum_values, r));
		// update the statistics
		stats.Update(string_values[r]);
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
