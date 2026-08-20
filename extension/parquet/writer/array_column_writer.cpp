#include <stdint.h>

#include "duckdb/common/vector/array_vector.hpp"
#include "writer/array_column_writer.hpp"
#include "column_writer.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "writer/list_column_writer.hpp"

namespace duckdb {

static idx_t GetConsecutiveChildArray(Vector &array, Vector &result, idx_t count) {
	auto &validity = FlatVector::ValidityMutable(array);
	auto array_size = ArrayType::GetSize(array.GetType());
	bool is_consecutive = true;
	idx_t total_length = 0;
	for (idx_t array_idx = 0; array_idx < count; array_idx++) {
		if (!validity.RowIsValid(array_idx)) {
			continue;
		}
		if (array_idx * array_size != total_length) {
			is_consecutive = false;
		}
		total_length += array_size;
	}
	if (is_consecutive) {
		return total_length;
	}

	SelectionVector sel(total_length);
	idx_t result_idx = 0;
	for (idx_t array_idx = 0; array_idx < count; array_idx++) {
		if (!validity.RowIsValid(array_idx)) {
			continue;
		}
		for (idx_t child_idx = 0; child_idx < array_size; child_idx++) {
			sel.set_index(result_idx++, array_idx * array_size + child_idx);
		}
	}
	result.Slice(sel, total_length);
	result.Flatten();
	return total_length;
}

void ArrayColumnWriter::Analyze(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	auto &array_child = ArrayVector::GetChildMutable(vector);
	Vector child_array(Vector::Ref(array_child));
	auto child_count = GetConsecutiveChildArray(vector, child_array, count);
	GetChildWriter().Analyze(*state.child_state, &state_p, child_array, child_count);
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
	auto &validity = FlatVector::ValidityMutable(vector);

	// write definition levels and repeats
	// The main difference between this and ListColumnWriter::Prepare is that valid arrays always have array_size
	// child elements.
	idx_t vcount = parent ? parent->definition_levels.size() - state.parent_index : count;
	idx_t vector_index = 0;
	for (idx_t i = 0; i < vcount; i++) {
		idx_t parent_index = state.parent_index + i;
		if (parent && !parent->is_empty.empty() && parent->is_empty[parent_index]) {
			WriteArrayState(state, array_size, parent->repetition_levels[parent_index],
			                parent->definition_levels[parent_index], true);
			continue;
		}
		auto first_repeat_level =
		    parent && !parent->repetition_levels.empty() ? parent->repetition_levels[parent_index] : MaxRepeat();
		if (parent && parent->definition_levels[parent_index] != PARQUET_DEFINE_VALID) {
			WriteArrayState(state, array_size, first_repeat_level, parent->definition_levels[parent_index], true);
		} else if (validity.RowIsValid(vector_index)) {
			// push the repetition levels
			WriteArrayState(state, array_size, first_repeat_level, PARQUET_DEFINE_VALID);
		} else {
			//! Produce a null
			WriteArrayState(state, array_size, first_repeat_level, MaxDefine() - 1, true);
		}
		vector_index++;
	}
	state.parent_index += vcount;

	auto &array_child = ArrayVector::GetChildMutable(vector);
	Vector child_array(Vector::Ref(array_child));
	auto child_count = GetConsecutiveChildArray(vector, child_array, count);
	// The elements of a single array should not span multiple Parquet pages
	// So, we force the entire vector to fit on a single page by setting "vector_can_span_multiple_pages=false"
	GetChildWriter().Prepare(*state.child_state, &state_p, child_array, child_count, false);
}

void ArrayColumnWriter::Write(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	auto &array_child = ArrayVector::GetChildMutable(vector);
	Vector child_array(Vector::Ref(array_child));
	auto child_count = GetConsecutiveChildArray(vector, child_array, count);
	GetChildWriter().Write(*state.child_state, child_array, child_count);
}

} // namespace duckdb
