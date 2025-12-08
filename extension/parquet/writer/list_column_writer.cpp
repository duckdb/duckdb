#include "writer/list_column_writer.hpp"

namespace duckdb {

using namespace duckdb_parquet; // NOLINT

using duckdb_parquet::ConvertedType;
using duckdb_parquet::FieldRepetitionType;

unique_ptr<ColumnWriterState> ListColumnWriter::InitializeWriteState(duckdb_parquet::RowGroup &row_group) {
	auto result = make_uniq<ListColumnWriterState>(row_group, row_group.columns.size());
	result->child_state = GetChildWriter().InitializeWriteState(row_group);
	return std::move(result);
}

bool ListColumnWriter::HasAnalyze() {
	return GetChildWriter().HasAnalyze();
}
void ListColumnWriter::Analyze(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	auto &list_child = ListVector::GetEntry(vector);
	auto list_count = ListVector::GetListSize(vector);
	GetChildWriter().Analyze(*state.child_state, &state_p, list_child, list_count);
}

void ListColumnWriter::FinalizeAnalyze(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	GetChildWriter().FinalizeAnalyze(*state.child_state);
}

static idx_t GetConsecutiveChildList(Vector &list, Vector &result, idx_t offset, idx_t count) {
	// returns a consecutive child list that fully flattens and repeats all required elements
	auto &validity = FlatVector::Validity(list);
	auto list_entries = FlatVector::GetData<list_entry_t>(list);
	bool is_consecutive = true;
	idx_t total_length = 0;
	for (idx_t c = offset; c < offset + count; c++) {
		if (!validity.RowIsValid(c)) {
			continue;
		}
		if (list_entries[c].offset != total_length) {
			is_consecutive = false;
		}
		total_length += list_entries[c].length;
	}
	if (is_consecutive) {
		// already consecutive - leave it as-is
		return total_length;
	}
	SelectionVector sel(total_length);
	idx_t index = 0;
	for (idx_t c = offset; c < offset + count; c++) {
		if (!validity.RowIsValid(c)) {
			continue;
		}
		for (idx_t k = 0; k < list_entries[c].length; k++) {
			sel.set_index(index++, list_entries[c].offset + k);
		}
	}
	result.Slice(sel, total_length);
	result.Flatten(total_length);
	return total_length;
}

void ListColumnWriter::Prepare(ColumnWriterState &state_p, ColumnWriterState *parent, Vector &vector, idx_t count,
                               bool vector_can_span_multiple_pages) {
	auto &state = state_p.Cast<ListColumnWriterState>();

	auto list_data = FlatVector::GetData<list_entry_t>(vector);
	auto &validity = FlatVector::Validity(vector);

	// write definition levels and repeats
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
			state.is_empty.push_back(true);
		} else if (validity.RowIsValid(vector_index)) {
			// push the repetition levels
			if (list_data[vector_index].length == 0) {
				state.definition_levels.push_back(MaxDefine());
				state.is_empty.push_back(true);
			} else {
				state.definition_levels.push_back(PARQUET_DEFINE_VALID);
				state.is_empty.push_back(false);
			}
			state.repetition_levels.push_back(first_repeat_level);
			for (idx_t k = 1; k < list_data[vector_index].length; k++) {
				state.repetition_levels.push_back(MaxRepeat() + 1);
				state.definition_levels.push_back(PARQUET_DEFINE_VALID);
				state.is_empty.push_back(false);
			}
		} else {
			if (!can_have_nulls) {
				throw IOException("Parquet writer: map key column is not allowed to contain NULL values");
			}
			state.definition_levels.push_back(MaxDefine() - 1);
			state.repetition_levels.push_back(first_repeat_level);
			state.is_empty.push_back(true);
		}
		vector_index++;
	}
	state.parent_index += vcount;

	auto &list_child = ListVector::GetEntry(vector);
	Vector child_list(list_child);
	auto child_length = GetConsecutiveChildList(vector, child_list, 0, count);
	// The elements of a single list should not span multiple Parquet pages
	// So, we force the entire vector to fit on a single page by setting "vector_can_span_multiple_pages=false"
	GetChildWriter().Prepare(*state.child_state, &state_p, child_list, child_length, false);
}

void ListColumnWriter::BeginWrite(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	GetChildWriter().BeginWrite(*state.child_state);
}

void ListColumnWriter::Write(ColumnWriterState &state_p, Vector &vector, idx_t count) {
	auto &state = state_p.Cast<ListColumnWriterState>();

	auto &list_child = ListVector::GetEntry(vector);
	Vector child_list(list_child);
	auto child_length = GetConsecutiveChildList(vector, child_list, 0, count);
	GetChildWriter().Write(*state.child_state, child_list, child_length);
}

void ListColumnWriter::FinalizeWrite(ColumnWriterState &state_p) {
	auto &state = state_p.Cast<ListColumnWriterState>();
	GetChildWriter().FinalizeWrite(*state.child_state);
}

ColumnWriter &ListColumnWriter::GetChildWriter() {
	D_ASSERT(child_writers.size() == 1);
	return *child_writers[0];
}

void ListColumnWriter::FinalizeSchema(vector<duckdb_parquet::SchemaElement> &schemas) {
	idx_t schema_idx = schemas.size();

	auto &schema = column_schema;
	schema.SetSchemaIndex(schema_idx);

	auto null_type = schema.repetition_type;
	auto &name = schema.name;
	auto &field_id = schema.field_id;
	auto &type = schema.type;

	// set up the two schema elements for the list
	// for some reason we only set the converted type in the OPTIONAL element
	// first an OPTIONAL element
	duckdb_parquet::SchemaElement optional_element;
	optional_element.repetition_type = null_type;
	optional_element.num_children = 1;
	optional_element.converted_type = (type.id() == LogicalTypeId::MAP) ? ConvertedType::MAP : ConvertedType::LIST;
	optional_element.__isset.num_children = true;
	optional_element.__isset.type = false;
	optional_element.__isset.repetition_type = true;
	optional_element.__isset.converted_type = true;
	optional_element.name = name;
	if (field_id.IsValid()) {
		optional_element.__isset.field_id = true;
		optional_element.field_id = field_id.GetIndex();
	}
	schemas.push_back(std::move(optional_element));

	if (type.id() != LogicalTypeId::MAP) {
		duckdb_parquet::SchemaElement repeated_element;
		repeated_element.repetition_type = FieldRepetitionType::REPEATED;
		repeated_element.__isset.num_children = true;
		repeated_element.__isset.type = false;
		repeated_element.__isset.repetition_type = true;
		repeated_element.num_children = 1;
		repeated_element.name = "list";
		schemas.push_back(std::move(repeated_element));
	} else {
		//! When we're describing a MAP, we skip the dummy "list" element
		//! Instead, the "key_value" struct will be marked as REPEATED
		D_ASSERT(GetChildWriter().Schema().repetition_type == FieldRepetitionType::REPEATED);
	}
	GetChildWriter().FinalizeSchema(schemas);
}

} // namespace duckdb
