#include "writer/variant_column_writer.hpp"

namespace duckdb {

unique_ptr<ColumnWriterState> VariantColumnWriter::InitializeWriteState(duckdb_parquet::RowGroup &row_group) {
	throw NotImplementedException("VariantColumnWriter::InitializeWriteState");
	// auto result = make_uniq<VariantColumnWriterState>(row_group, row_group.columns.size());
	// result->child_state = child_writer->InitializeWriteState(row_group);
	// return std::move(result);
}

bool VariantColumnWriter::HasAnalyze() {
	throw NotImplementedException("VariantColumnWriter::HasAnalyze");
}
void VariantColumnWriter::Analyze(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	throw NotImplementedException("VariantColumnWriter::Analyze");
	// auto &state = state_p.Cast<VariantColumnWriterState>();
	// auto &list_child = ListVector::GetEntry(vector);
	// auto list_count = ListVector::GetListSize(vector);
	// child_writer->Analyze(*state.child_state, &state_p, list_child, list_count);
}

void VariantColumnWriter::FinalizeAnalyze(ColumnWriterState &state_p) {
	throw NotImplementedException("VariantColumnWriter::FinalizeAnalyze");
	// auto &state = state_p.Cast<VariantColumnWriterState>();
	// child_writer->FinalizeAnalyze(*state.child_state);
}

void VariantColumnWriter::Prepare(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count,
                                  bool vector_can_span_multiple_pages) {
	throw NotImplementedException("VariantColumnWriter::Prepare");
	// auto &state = state_p.Cast<VariantColumnWriterState>();

	// auto list_data = FlatVector::GetData<list_entry_t>(vector);
	// auto &validity = FlatVector::Validity(vector);

	//// write definition levels and repeats
	// idx_t start = 0;
	// idx_t vcount = parent ? parent->definition_levels.size() - state.parent_index : count;
	// idx_t vector_index = 0;
	// for (idx_t i = start; i < vcount; i++) {
	//	idx_t parent_index = state.parent_index + i;
	//	if (parent && !parent->is_empty.empty() && parent->is_empty[parent_index]) {
	//		state.definition_levels.push_back(parent->definition_levels[parent_index]);
	//		state.repetition_levels.push_back(parent->repetition_levels[parent_index]);
	//		state.is_empty.push_back(true);
	//		continue;
	//	}
	//	auto first_repeat_level =
	//	    parent && !parent->repetition_levels.empty() ? parent->repetition_levels[parent_index] : MaxRepeat();
	//	if (parent && parent->definition_levels[parent_index] != PARQUET_DEFINE_VALID) {
	//		state.definition_levels.push_back(parent->definition_levels[parent_index]);
	//		state.repetition_levels.push_back(first_repeat_level);
	//		state.is_empty.push_back(true);
	//	} else if (validity.RowIsValid(vector_index)) {
	//		// push the repetition levels
	//		if (list_data[vector_index].length == 0) {
	//			state.definition_levels.push_back(MaxDefine());
	//			state.is_empty.push_back(true);
	//		} else {
	//			state.definition_levels.push_back(PARQUET_DEFINE_VALID);
	//			state.is_empty.push_back(false);
	//		}
	//		state.repetition_levels.push_back(first_repeat_level);
	//		for (idx_t k = 1; k < list_data[vector_index].length; k++) {
	//			state.repetition_levels.push_back(MaxRepeat() + 1);
	//			state.definition_levels.push_back(PARQUET_DEFINE_VALID);
	//			state.is_empty.push_back(false);
	//		}
	//	} else {
	//		if (!can_have_nulls) {
	//			throw IOException("Parquet writer: map key column is not allowed to contain NULL values");
	//		}
	//		state.definition_levels.push_back(MaxDefine() - 1);
	//		state.repetition_levels.push_back(first_repeat_level);
	//		state.is_empty.push_back(true);
	//	}
	//	vector_index++;
	//}
	// state.parent_index += vcount;

	// auto &list_child = ListVector::GetEntry(vector);
	// Vector child_list(list_child);
	// auto child_length = GetConsecutiveChildList(vector, child_list, 0, count);
	//// The elements of a single list should not span multiple Parquet pages
	//// So, we force the entire vector to fit on a single page by setting "vector_can_span_multiple_pages=false"
	// child_writer->Prepare(*state.child_state, &state_p, child_list, child_length, false);
}

void VariantColumnWriter::BeginWrite(ColumnWriterState &state_p) {
	throw NotImplementedException("VariantColumnWriter::BeginWrite");
	// auto &state = state_p.Cast<VariantColumnWriterState>();
	// child_writer->BeginWrite(*state.child_state);
}

void VariantColumnWriter::Write(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	throw NotImplementedException("VariantColumnWriter::Write");
	// auto &state = state_p.Cast<VariantColumnWriterState>();

	// auto &list_child = ListVector::GetEntry(vector);
	// Vector child_list(list_child);
	// auto child_length = GetConsecutiveChildList(vector, child_list, 0, count);
	// child_writer->Write(*state.child_state, child_list, child_length);
}

void VariantColumnWriter::FinalizeWrite(ColumnWriterState &state_p) {
	throw NotImplementedException("VariantColumnWriter::FinalizeWrite");
	// auto &state = state_p.Cast<VariantColumnWriterState>();
	// child_writer->FinalizeWrite(*state.child_state);
}

} // namespace duckdb
