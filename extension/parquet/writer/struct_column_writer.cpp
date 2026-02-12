#include "writer/struct_column_writer.hpp"

namespace duckdb {

using namespace duckdb_parquet; // NOLINT

using duckdb_parquet::ConvertedType;
using duckdb_parquet::FieldRepetitionType;

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

void StructColumnWriter::Prepare(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count,
                                 bool vector_can_span_multiple_pages) {
	auto &state = state_p.Cast<StructColumnWriterState>();

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
	auto &child_vectors = StructVector::GetEntries(vector);
	for (idx_t child_idx = 0; child_idx < child_writers.size(); child_idx++) {
		child_writers[child_idx]->Prepare(*state.child_states[child_idx], &state_p, *child_vectors[child_idx], count,
		                                  vector_can_span_multiple_pages);
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

idx_t StructColumnWriter::FinalizeSchema(vector<duckdb_parquet::SchemaElement> &schemas) {
	idx_t schema_idx = schemas.size();

	auto &schema = column_schema;
	schema.SetSchemaIndex(schema_idx);

	auto &repetition_type = schema.repetition_type;
	auto &name = schema.name;
	auto &field_id = schema.field_id;

	// set up the schema element for this struct
	duckdb_parquet::SchemaElement schema_element;
	schema_element.repetition_type = repetition_type;
	schema_element.num_children = child_writers.size();
	schema_element.__isset.num_children = true;
	schema_element.__isset.type = false;
	schema_element.__isset.repetition_type = true;
	schema_element.name = name;
	if (field_id.IsValid()) {
		schema_element.__isset.field_id = true;
		schema_element.field_id = field_id.GetIndex();
	}
	schemas.push_back(std::move(schema_element));

	idx_t unique_columns = 0;
	for (auto &child_writer : child_writers) {
		unique_columns += child_writer->FinalizeSchema(schemas);
	}
	return unique_columns;
}

} // namespace duckdb
