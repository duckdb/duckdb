#pragma once

#include "duckdb/common/arrow/appender/append_data.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"

namespace duckdb {

template <class BUFTYPE = int64_t>
struct ArrowListData {
public:
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		auto &child_type = ListType::GetChildType(type);
		result.GetMainBuffer().reserve((capacity + 1) * sizeof(BUFTYPE));
		auto child_buffer = ArrowAppender::InitializeChild(child_type, capacity, result.options);
		result.child_data.push_back(std::move(child_buffer));
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);
		idx_t size = to - from;
		vector<sel_t> child_indices;
		AppendValidity(append_data, format, from, to);
		AppendOffsets(append_data, format, from, to, child_indices);

		// append the child vector of the list
		SelectionVector child_sel(child_indices.data());
		auto &child = ListVector::GetEntry(input);
		auto child_size = child_indices.size();
		Vector child_copy(child.GetType());
		child_copy.Slice(child, child_sel, child_size);
		append_data.child_data[0]->append_vector(*append_data.child_data[0], child_copy, 0, child_size, child_size);
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 2;
		result->buffers[1] = append_data.GetMainBuffer().data();

		auto &child_type = ListType::GetChildType(type);
		ArrowAppender::AddChildren(append_data, 1);
		result->children = append_data.child_pointers.data();
		result->n_children = 1;
		append_data.child_arrays[0] = *ArrowAppender::FinalizeChild(child_type, std::move(append_data.child_data[0]));
	}

public:
	static void AppendOffsets(ArrowAppendData &append_data, UnifiedVectorFormat &format, idx_t from, idx_t to,
	                          vector<sel_t> &child_sel) {
		// resize the offset buffer - the offset buffer holds the offsets into the child array
		idx_t size = to - from;
		auto &main_buffer = append_data.GetMainBuffer();

		main_buffer.resize(main_buffer.size() + sizeof(BUFTYPE) * (size + 1));
		auto data = UnifiedVectorFormat::GetData<list_entry_t>(format);
		auto offset_data = main_buffer.GetData<BUFTYPE>();
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
			if (std::is_same<BUFTYPE, int32_t>::value == true &&
			    (uint64_t)last_offset + list_length > NumericLimits<int32_t>::Maximum()) {
				throw InvalidInputException(
				    "Arrow Appender: The maximum combined list offset for regular list buffers is "
				    "%u but the offset of %lu exceeds this.\n* SET arrow_large_buffer_size=true to use large list "
				    "buffers",
				    NumericLimits<int32_t>::Maximum(), last_offset);
			}
			last_offset += list_length;
			offset_data[offset_idx] = last_offset;

			for (idx_t k = 0; k < list_length; k++) {
				child_sel.push_back(UnsafeNumericCast<sel_t>(data[source_idx].offset + k));
			}
		}
	}
};

} // namespace duckdb
