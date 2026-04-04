#include "writer/decimal_column_writer.hpp"
#include "duckdb/common/bswap.hpp"

namespace duckdb {

static void WriteParquetDecimal(hugeint_t input, data_ptr_t result) {
	// C++ stores negative numbers in two's complement, so we just need to
	// byte-swap each 64-bit half from little-endian to big-endian (Parquet order).
	uint64_t high = BSWAP64(static_cast<uint64_t>(input.upper));
	uint64_t low = BSWAP64(input.lower);
	memcpy(result, &high, sizeof(uint64_t));
	memcpy(result + sizeof(uint64_t), &low, sizeof(uint64_t));
}

class FixedDecimalStatistics : public ColumnWriterStatistics {
public:
	FixedDecimalStatistics() : min(NumericLimits<hugeint_t>::Maximum()), max(NumericLimits<hugeint_t>::Minimum()) {
	}

	hugeint_t min;
	hugeint_t max;

public:
	string GetStats(hugeint_t &input) {
		data_t buffer[16];
		WriteParquetDecimal(input, buffer);
		return string(const_char_ptr_cast(buffer), 16);
	}

	bool HasStats() override {
		return min <= max;
	}

	void Update(hugeint_t &val) {
		if (LessThan::Operation(val, min)) {
			min = val;
		}
		if (GreaterThan::Operation(val, max)) {
			max = val;
		}
	}

	string GetMin() override {
		return GetMinValue();
	}
	string GetMax() override {
		return GetMaxValue();
	}
	string GetMinValue() override {
		return HasStats() ? GetStats(min) : string();
	}
	string GetMaxValue() override {
		return HasStats() ? GetStats(max) : string();
	}
};

FixedDecimalColumnWriter::FixedDecimalColumnWriter(ParquetWriter &writer, ParquetColumnSchema &&column_schema,
                                                   vector<string> schema_path_p)
    : PrimitiveColumnWriter(writer, std::move(column_schema), std::move(schema_path_p)) {
}

unique_ptr<ColumnWriterStatistics> FixedDecimalColumnWriter::InitializeStatsState() {
	return make_uniq<FixedDecimalStatistics>();
}

void FixedDecimalColumnWriter::WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats_p,
                                           ColumnWriterPageState *page_state, Vector &input_column, idx_t chunk_start,
                                           idx_t chunk_end) {
	auto &mask = FlatVector::Validity(input_column);
	auto *ptr = FlatVector::GetData<hugeint_t>(input_column);
	auto &stats = stats_p->Cast<FixedDecimalStatistics>();

	data_t temp_buffer[16];
	for (idx_t r = chunk_start; r < chunk_end; r++) {
		if (mask.RowIsValid(r)) {
			stats.Update(ptr[r]);
			WriteParquetDecimal(ptr[r], temp_buffer);
			temp_writer.WriteData(temp_buffer, 16);
		}
	}
}

idx_t FixedDecimalColumnWriter::GetRowSize(const Vector &vector, const idx_t index,
                                           const PrimitiveColumnWriterState &state) const {
	return sizeof(hugeint_t);
}

} // namespace duckdb
