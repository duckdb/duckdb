#include "writer/struct_column_writer.hpp"

namespace duckdb {

class StructColumnWriterState : public ColumnWriterState {
public:
	StructColumnWriterState(duckdb_parquet::RowGroup &row_group, idx_t col_idx)
	    : row_group(row_group), col_idx(col_idx) {
	}
	~StructColumnWriterState() override = default;

	duckdb_parquet::RowGroup &row_group;
	idx_t col_idx;
	vector<unique_ptr<ColumnWriterState>> child_states;
};

unique_ptr<ColumnWriterState> StructColumnWriter::InitializeWriteState(duckdb_parquet::RowGroup &row_group) {
	auto result = make_uniq<StructColumnWriterState>(row_group, row_group.columns.size());

	result->child_states.reserve(child_writers.size());
	for (auto &child_writer : child_writers) {
		result->child_states.push_back(child_writer->InitializeWriteState(row_group));
	}
	return std::move(result);
}

bool StructColumnWriter::HasAnalyze() {
	for (auto &child_writer : child_writers) {
		if (child_writer->HasAnalyze()) {
			return true;
		}
	}
	return false;
}

void StructColumnWriter::Analyze(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<StructColumnWriterState>();
	auto &child_vectors = StructVector::GetEntries(vector);
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		// Need to check again. It might be that just one child needs it but the rest not
		if (child_writers[child_idx]->HasAnalyze()) {
			child_writers[child_idx]->Analyze(*state.child_states[child_idx], &state_p, *child_vectors[child_idx],
			                                  count);
		}
	}
}

void StructColumnWriter::FinalizeAnalyze(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<StructColumnWriterState>();
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		// Need to check again. It might be that just one child needs it but the rest not
		if (child_writers[child_idx]->HasAnalyze()) {
			child_writers[child_idx]->FinalizeAnalyze(*state.child_states[child_idx]);
		}
	}
}

void StructColumnWriter::Prepare(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<StructColumnWriterState>();

	auto &validity = FlatVector::Validity(vector);
	if (parent) {
		// propagate empty entries from the parent
		while (state.is_empty.size() < parent->is_empty.size()) {
			state.is_empty.push_back(parent->is_empty[state.is_empty.size()]);
		}
	}
	HandleRepeatLevels(state_p, parent, count, MaxRepeat());
	HandleDefineLevels(state_p, parent, validity, count, PARQUET_DEFINE_VALID, MaxDefine() - 1);
	auto &child_vectors = StructVector::GetEntries(vector);
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		child_writers[child_idx]->Prepare(*state.child_states[child_idx], &state_p, *child_vectors[child_idx], count);
	}
}

void StructColumnWriter::BeginWrite(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<StructColumnWriterState>();
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		child_writers[child_idx]->BeginWrite(*state.child_states[child_idx]);
	}
}

void StructColumnWriter::Write(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<StructColumnWriterState>();
	auto &child_vectors = StructVector::GetEntries(vector);
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		child_writers[child_idx]->Write(*state.child_states[child_idx], *child_vectors[child_idx], count);
	}
}

void StructColumnWriter::FinalizeWrite(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<StructColumnWriterState>();
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		// we add the null count of the struct to the null count of the children
		state.child_states[child_idx]->null_count += state_p.null_count;
		child_writers[child_idx]->FinalizeWrite(*state.child_states[child_idx]);
	}
}

} // namespace duckdb
