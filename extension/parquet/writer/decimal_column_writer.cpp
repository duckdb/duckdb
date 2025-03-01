#include "writer/decimal_column_writer.hpp"

namespace duckdb {

static void WriteParquetDecimal(hugeint_t input, data_ptr_t result) {
	bool positive = input >= 0;
	// numbers are stored as two's complement so some muckery is required
	if (!positive) {
		input = NumericLimits<hugeint_t>::Maximum() + input + 1;
	}
	uint64_t high_bytes = uint64_t(input.upper);
	uint64_t low_bytes = input.lower;

	for (idx_t i = 0; i < sizeof(uint64_t); i++) {
		auto shift_count = (sizeof(uint64_t) - i - 1) * 8;
		result[i] = (high_bytes >> shift_count) & 0xFF;
	}
	for (idx_t i = 0; i < sizeof(uint64_t); i++) {
		auto shift_count = (sizeof(uint64_t) - i - 1) * 8;
		result[sizeof(uint64_t) + i] = (low_bytes >> shift_count) & 0xFF;
	}
	if (!positive) {
		result[0] |= 0x80;
	}
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

FixedDecimalColumnWriter::FixedDecimalColumnWriter(ParquetWriter &writer, idx_t schema_idx,
                                                   vector<string> schema_path_p, idx_t max_repeat, idx_t max_define,
                                                   bool can_have_nulls)
    : PrimitiveColumnWriter(writer, schema_idx, std::move(schema_path_p), max_repeat, max_define, can_have_nulls) {
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
