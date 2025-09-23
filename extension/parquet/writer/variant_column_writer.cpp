#include "writer/variant_column_writer.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

namespace {

class VariantColumnWriterState : public ColumnWriterState {
public:
	VariantColumnWriterState(duckdb_parquet::RowGroup &row_group, idx_t col_idx)
	    : row_group(row_group), col_idx(col_idx) {
	}
	~VariantColumnWriterState() override = default;

	duckdb_parquet::RowGroup &row_group;
	idx_t col_idx;
	vector<unique_ptr<ColumnWriterState>> child_states;
};

} // namespace

unique_ptr<ColumnWriterState> VariantColumnWriter::InitializeWriteState(duckdb_parquet::RowGroup &row_group) {
	auto result = make_uniq<VariantColumnWriterState>(row_group, row_group.columns.size());

	result->child_states.reserve(child_writers.size());
	for (auto &child_writer : child_writers) {
		result->child_states.push_back(child_writer->InitializeWriteState(row_group));
	}
	return std::move(result);
}

bool VariantColumnWriter::HasAnalyze() {
	for (auto &child_writer : child_writers) {
		if (child_writer->HasAnalyze()) {
			return true;
		}
	}
	return false;
}

void VariantColumnWriter::Analyze(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<VariantColumnWriterState>();
	auto &child_vectors = StructVector::GetEntries(vector);
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		// Need to check again. It might be that just one child needs it but the rest not
		if (child_writers[child_idx]->HasAnalyze()) {
			child_writers[child_idx]->Analyze(*state.child_states[child_idx], &state_p, *child_vectors[child_idx],
			                                  count);
		}
	}
}

void VariantColumnWriter::FinalizeAnalyze(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<VariantColumnWriterState>();
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		// Need to check again. It might be that just one child needs it but the rest not
		if (child_writers[child_idx]->HasAnalyze()) {
			child_writers[child_idx]->FinalizeAnalyze(*state.child_states[child_idx]);
		}
	}
}

void VariantColumnWriter::Prepare(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count,
                                  bool vector_can_span_multiple_pages) {
	D_ASSERT(child_writers.size() == 2);
	auto &metadata_writer = *child_writers[0];
	auto &value_writer = *child_writers[1];

	auto &state = state_p.Cast<VariantColumnWriterState>();
	auto &metadata_state = *state.child_states[0];
	auto &value_state = *state.child_states[1];

	auto &validity = FlatVector::Validity(vector);
	if (parent) {
		// propagate empty entries from the parent
		if (state.is_empty.size() < parent->is_empty.size()) {
			state.is_empty.insert(state.is_empty.end(), parent->is_empty.begin() + state.is_empty.size(),
			                      parent->is_empty.end());
		}
	}
	HandleRepeatLevels(state_p, parent, count);
	HandleDefineLevels(state_p, parent, validity, count, PARQUET_DEFINE_VALID, MaxDefine() - 1);

	metadata_writer.Prepare(metadata_state, &state_p, vector, count, vector_can_span_multiple_pages);
	value_writer.Prepare(value_state, &state_p, vector, count, vector_can_span_multiple_pages);
}

void VariantColumnWriter::BeginWrite(ColumnWriterState &state_p) {
	D_ASSERT(child_writers.size() == 2);
	auto &metadata_writer = *child_writers[0];
	auto &value_writer = *child_writers[1];

	auto &state = state_p.Cast<VariantColumnWriterState>();
	auto &metadata_state = *state.child_states[0];
	auto &value_state = *state.child_states[1];

	metadata_writer.BeginWrite(metadata_state);
	value_writer.BeginWrite(value_state);
}

void VariantColumnWriter::Write(ColumnWriterState &state_p, Vector &input, idx_t count) {
	D_ASSERT(child_writers.size() == 2);

	auto &metadata_writer = *child_writers[0];
	auto &value_writer = *child_writers[1];

	auto &state = state_p.Cast<VariantColumnWriterState>();
	auto &metadata_state = *state.child_states[0];
	auto &value_state = *state.child_states[1];

	auto &child_vectors = StructVector::GetEntries(input);
	metadata_writer.Write(metadata_state, *child_vectors[0], count);
	value_writer.Write(value_state, *child_vectors[1], count);
}

void VariantColumnWriter::FinalizeWrite(ColumnWriterState &state_p) {
	D_ASSERT(child_writers.size() == 2);
	auto &metadata_writer = *child_writers[0];
	auto &value_writer = *child_writers[1];

	auto &state = state_p.Cast<VariantColumnWriterState>();
	auto &metadata_state = *state.child_states[0];
	auto &value_state = *state.child_states[1];

	metadata_writer.FinalizeWrite(metadata_state);
	value_writer.FinalizeWrite(value_state);
}

} // namespace duckdb
