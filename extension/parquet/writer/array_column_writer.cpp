#include "writer/array_column_writer.hpp"

namespace duckdb {

void ArrayColumnWriter::Analyze(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	auto &array_child = ArrayVector::GetEntry(vector);
	auto array_size = ArrayType::GetSize(vector.GetType());
	child_writer->Analyze(*state.child_state, &state_p, array_child, array_size * count);
}

void ArrayColumnWriter::WriteArrayState(ListColumnWriterState &state, idx_t array_size, uint16_t first_repeat_level,
                                        idx_t define_value, const bool is_empty) {
	state.definition_levels.push_back(define_value);
	state.repetition_levels.push_back(first_repeat_level);
	state.is_empty.push_back(is_empty);

	if (is_empty) {
		return;
	}
	for (idx_t k = 1; k < array_size; k++) {
		state.repetition_levels.push_back(MaxRepeat() + 1);
		state.definition_levels.push_back(define_value);
		state.is_empty.push_back(false);
	}
}

void ArrayColumnWriter::Prepare(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count,
                                bool vector_can_span_multiple_pages) {
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
			WriteArrayState(state, array_size, parent->repetition_levels[parent_index],
			                parent->definition_levels[parent_index], true);
			continue;
		}
		auto first_repeat_level =
		    parent && !parent->repetition_levels.empty() ? parent->repetition_levels[parent_index] : MaxRepeat();
		if (parent && parent->definition_levels[parent_index] != PARQUET_DEFINE_VALID) {
			WriteArrayState(state, array_size, first_repeat_level, parent->definition_levels[parent_index]);
		} else if (validity.RowIsValid(vector_index)) {
			// push the repetition levels
			WriteArrayState(state, array_size, first_repeat_level, PARQUET_DEFINE_VALID);
		} else {
			//! Produce a null
			WriteArrayState(state, array_size, first_repeat_level, MaxDefine() - 1);
		}
		vector_index++;
	}
	state.parent_index += vcount;

	auto &array_child = ArrayVector::GetEntry(vector);
	// The elements of a single array should not span multiple Parquet pages
	// So, we force the entire vector to fit on a single page by setting "vector_can_span_multiple_pages=false"
	child_writer->Prepare(*state.child_state, &state_p, array_child, count * array_size, false);
}

void ArrayColumnWriter::Write(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	auto array_size = ArrayType::GetSize(vector.GetType());
	auto &array_child = ArrayVector::GetEntry(vector);
	child_writer->Write(*state.child_state, array_child, count * array_size);
}

} // namespace duckdb
