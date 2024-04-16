#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/appender/union_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Unions
//===--------------------------------------------------------------------===//
void ArrowUnionData::Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
	result.GetMainBuffer().reserve(capacity * sizeof(int8_t));

	for (auto &child : UnionType::CopyMemberTypes(type)) {
		auto child_buffer = ArrowAppender::InitializeChild(child.second, capacity, result.options);
		result.child_data.push_back(std::move(child_buffer));
	}
}

void ArrowUnionData::Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(input_size, format);
	idx_t size = to - from;

	auto &types_buffer = append_data.GetMainBuffer();

	duckdb::vector<Vector> child_vectors;
	for (const auto &child : UnionType::CopyMemberTypes(input.GetType())) {
		child_vectors.emplace_back(child.second, size);
	}

	for (idx_t input_idx = from; input_idx < to; input_idx++) {
		const auto &val = input.GetValue(input_idx);

		idx_t tag = 0;
		Value resolved_value(nullptr);
		if (!val.IsNull()) {
			tag = UnionValue::GetTag(val);

			resolved_value = UnionValue::GetValue(val);
		}

		for (idx_t child_idx = 0; child_idx < child_vectors.size(); child_idx++) {
			child_vectors[child_idx].SetValue(input_idx, child_idx == tag ? resolved_value : Value(nullptr));
		}

		types_buffer.data()[input_idx] = NumericCast<data_t>(tag);
	}

	for (idx_t child_idx = 0; child_idx < child_vectors.size(); child_idx++) {
		auto &child_buffer = append_data.child_data[child_idx];
		auto &child = child_vectors[child_idx];
		child_buffer->append_vector(*child_buffer, child, from, to, size);
	}
	append_data.row_count += size;
}

void ArrowUnionData::Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
	result->n_buffers = 1;
	result->buffers[0] = append_data.GetMainBuffer().data();

	auto &child_types = UnionType::CopyMemberTypes(type);
	ArrowAppender::AddChildren(append_data, child_types.size());
	result->children = append_data.child_pointers.data();
	result->n_children = child_types.size();
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto &child_type = child_types[i].second;
		append_data.child_arrays[i] = *ArrowAppender::FinalizeChild(child_type, std::move(append_data.child_data[i]));
	}
}

} // namespace duckdb
