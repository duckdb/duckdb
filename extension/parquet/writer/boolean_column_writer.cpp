#include "writer/boolean_column_writer.hpp"

namespace duckdb {

class BooleanStatisticsState : public ColumnWriterStatistics {
public:
	BooleanStatisticsState() : min(true), max(false) {
	}

	bool min;
	bool max;

public:
	bool HasStats() override {
		return !(min && !max);
	}

	string GetMin() override {
		return GetMinValue();
	}
	string GetMax() override {
		return GetMaxValue();
	}
	string GetMinValue() override {
		return HasStats() ? string(const_char_ptr_cast(&min), sizeof(bool)) : string();
	}
	string GetMaxValue() override {
		return HasStats() ? string(const_char_ptr_cast(&max), sizeof(bool)) : string();
	}
};

class BooleanWriterPageState : public ColumnWriterPageState {
public:
	uint8_t byte = 0;
	uint8_t byte_pos = 0;
};

BooleanColumnWriter::BooleanColumnWriter(ParquetWriter &writer, const ParquetColumnSchema &column_schema,
                                         vector<string> schema_path_p, bool can_have_nulls)
    : PrimitiveColumnWriter(writer, column_schema, std::move(schema_path_p), can_have_nulls) {
}

unique_ptr<ColumnWriterStatistics> BooleanColumnWriter::InitializeStatsState() {
	return make_uniq<BooleanStatisticsState>();
}

void BooleanColumnWriter::WriteVector(WriteStream &temp_writer, ColumnWriterStatistics *stats_p,
                                      ColumnWriterPageState *state_p, Vector &input_column, idx_t chunk_start,
                                      idx_t chunk_end) {
	auto &stats = stats_p->Cast<BooleanStatisticsState>();
	auto &state = state_p->Cast<BooleanWriterPageState>();
	const auto &mask = FlatVector::Validity(input_column);

	const auto *const ptr = FlatVector::GetData<bool>(input_column);
	if (stats.max && !stats.min && mask.AllValid()) {
		// Fast path: stats have already been set, and there's no NULLs
		for (idx_t r = chunk_start; r < chunk_end; r++) {
			const auto &val = ptr[r];
			state.byte |= val << state.byte_pos;
			if (++state.byte_pos == 8) {
				temp_writer.Write(state.byte);
				state.byte = 0;
				state.byte_pos = 0;
			}
		}
	} else {
		for (idx_t r = chunk_start; r < chunk_end; r++) {
			if (!mask.RowIsValid(r)) {
				continue;
			}
			const auto &val = ptr[r];

			stats.max |= val;
			stats.min &= val;
			state.byte |= val << state.byte_pos;

			if (++state.byte_pos == 8) {
				temp_writer.Write(state.byte);
				state.byte = 0;
				state.byte_pos = 0;
			}
		}
	}
}

unique_ptr<ColumnWriterPageState> BooleanColumnWriter::InitializePageState(PrimitiveColumnWriterState &state,
                                                                           idx_t page_idx) {
	return make_uniq<BooleanWriterPageState>();
}

void BooleanColumnWriter::FlushPageState(WriteStream &temp_writer, ColumnWriterPageState *state_p) {
	auto &state = state_p->Cast<BooleanWriterPageState>();
	if (state.byte_pos > 0) {
		temp_writer.Write<uint8_t>(state.byte);
		state.byte = 0;
		state.byte_pos = 0;
	}
}

idx_t BooleanColumnWriter::GetRowSize(const Vector &vector, const idx_t index,
                                      const PrimitiveColumnWriterState &state) const {
	return sizeof(bool);
}

} // namespace duckdb
