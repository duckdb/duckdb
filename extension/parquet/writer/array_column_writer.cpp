#include "writer/array_column_writer.hpp"

namespace duckdb {

void ArrayColumnWriter::Analyze(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	auto &array_child = ArrayVector::GetEntry(vector);
	auto array_size = ArrayType::GetSize(vector.GetType());
	child_writer->Analyze(*state.child_state, &state_p, array_child, array_size * count);
}

void ArrayColumnWriter::Prepare(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<ListColumnWriterState>();

	auto array_size = ArrayType::GetSize(vector.GetType());
	auto &validity = FlatVector::Validity(vector);

	// write definition levels and repeats
	// the main difference between this and ListColumnWriter::Prepare is that we need to make sure to write out
	// repetition levels and definitions for the child elements of the array even if the array itself is NULL.
	idx_t start = 0;
	idx_t vcount = parent ? parent->definition_levels.size() - state.parent_index : count;
	idx_t vector_index = 0;
	for (idx_t i = start; i < vcount; i++) {
		idx_t parent_index = state.parent_index + i;
		if (parent && !parent->is_empty.empty() && parent->is_empty[parent_index]) {
			state.definition_levels.push_back(parent->definition_levels[parent_index]);
			state.repetition_levels.push_back(parent->repetition_levels[parent_index]);
			state.is_empty.push_back(true);
			continue;
		}
		auto first_repeat_level =
		    parent && !parent->repetition_levels.empty() ? parent->repetition_levels[parent_index] : MaxRepeat();
		if (parent && parent->definition_levels[parent_index] != PARQUET_DEFINE_VALID) {
			state.definition_levels.push_back(parent->definition_levels[parent_index]);
			state.repetition_levels.push_back(first_repeat_level);
			state.is_empty.push_back(false);
			for (idx_t k = 1; k < array_size; k++) {
				state.repetition_levels.push_back(MaxRepeat() + 1);
				state.definition_levels.push_back(parent->definition_levels[parent_index]);
				state.is_empty.push_back(false);
			}
		} else if (validity.RowIsValid(vector_index)) {
			// push the repetition levels
			state.definition_levels.push_back(PARQUET_DEFINE_VALID);
			state.is_empty.push_back(false);

			state.repetition_levels.push_back(first_repeat_level);
			for (idx_t k = 1; k < array_size; k++) {
				state.repetition_levels.push_back(MaxRepeat() + 1);
				state.definition_levels.push_back(PARQUET_DEFINE_VALID);
				state.is_empty.push_back(false);
			}
		} else {
			state.definition_levels.push_back(MaxDefine() - 1);
			state.repetition_levels.push_back(first_repeat_level);
			state.is_empty.push_back(false);
			for (idx_t k = 1; k < array_size; k++) {
				state.repetition_levels.push_back(MaxRepeat() + 1);
				state.definition_levels.push_back(MaxDefine() - 1);
				state.is_empty.push_back(false);
			}
		}
		vector_index++;
	}
	state.parent_index += vcount;

	auto &array_child = ArrayVector::GetEntry(vector);
	child_writer->Prepare(*state.child_state, &state_p, array_child, count * array_size);
}

void ArrayColumnWriter::Write(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	auto array_size = ArrayType::GetSize(vector.GetType());
	auto &array_child = ArrayVector::GetEntry(vector);
	child_writer->Write(*state.child_state, array_child, count * array_size);
}

} // namespace duckdb
