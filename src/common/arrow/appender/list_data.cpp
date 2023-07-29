#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/appender/list_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Lists
//===--------------------------------------------------------------------===//
void ArrowListData::AppendOffsets(ArrowAppendData &append_data, UnifiedVectorFormat &format, idx_t from, idx_t to,
                                  vector<sel_t> &child_sel) {
	// resize the offset buffer - the offset buffer holds the offsets into the child array
	idx_t size = to - from;
	append_data.main_buffer.resize(append_data.main_buffer.size() + sizeof(uint32_t) * (size + 1));
	auto data = UnifiedVectorFormat::GetData<list_entry_t>(format);
	auto offset_data = append_data.main_buffer.GetData<uint32_t>();
	if (append_data.row_count == 0) {
		// first entry
		offset_data[0] = 0;
	}
	// set up the offsets using the list entries
	auto last_offset = offset_data[append_data.row_count];
	for (idx_t i = from; i < to; i++) {
		auto source_idx = format.sel->get_index(i);
		auto offset_idx = append_data.row_count + i + 1 - from;

		if (!format.validity.RowIsValid(source_idx)) {
			offset_data[offset_idx] = last_offset;
			continue;
		}

		// append the offset data
		auto list_length = data[source_idx].length;
		last_offset += list_length;
		offset_data[offset_idx] = last_offset;

		for (idx_t k = 0; k < list_length; k++) {
			child_sel.push_back(data[source_idx].offset + k);
		}
	}
}

void ArrowListData::Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
	auto &child_type = ListType::GetChildType(type);
	result.main_buffer.reserve((capacity + 1) * sizeof(uint32_t));
	auto child_buffer = ArrowAppender::InitializeChild(child_type, capacity, result.options);
	result.child_data.push_back(std::move(child_buffer));
}

void ArrowListData::Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(input_size, format);
	idx_t size = to - from;
	vector<sel_t> child_indices;
	AppendValidity(append_data, format, from, to);
	ArrowListData::AppendOffsets(append_data, format, from, to, child_indices);

	// append the child vector of the list
	SelectionVector child_sel(child_indices.data());
	auto &child = ListVector::GetEntry(input);
	auto child_size = child_indices.size();
	Vector child_copy(child.GetType());
	child_copy.Slice(child, child_sel, child_size);
	append_data.child_data[0]->append_vector(*append_data.child_data[0], child_copy, 0, child_size, child_size);
	append_data.row_count += size;
}

void ArrowListData::Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
	result->n_buffers = 2;
	result->buffers[1] = append_data.main_buffer.data();

	auto &child_type = ListType::GetChildType(type);
	append_data.child_pointers.resize(1);
	result->children = append_data.child_pointers.data();
	result->n_children = 1;
	append_data.child_pointers[0] = ArrowAppender::FinalizeChild(child_type, *append_data.child_data[0]);
}

} // namespace duckdb
