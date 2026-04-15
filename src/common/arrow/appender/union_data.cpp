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
		types_buffer.push_back<data_t>(NumericCast<data_t>(tag));
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
	result->n_children = NumericCast<int64_t>(child_types.size());
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto &child_type = child_types[i].second;
		append_data.child_arrays[i] = *ArrowAppender::FinalizeChild(child_type, std::move(append_data.child_data[i]));
	}
}

//===--------------------------------------------------------------------===//
// Dense Unions
//===--------------------------------------------------------------------===//
void ArrowDenseUnionData::Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
	// Main buffer = type_ids (int8_t per row)
	result.GetMainBuffer().reserve(capacity * sizeof(int8_t));
	// Aux buffer = offsets (int32_t per row)
	result.GetAuxBuffer().reserve(capacity * sizeof(int32_t));

	for (auto &child : UnionType::CopyMemberTypes(type)) {
		auto child_buffer = ArrowAppender::InitializeChild(child.second, capacity, result.options);
		result.child_data.push_back(std::move(child_buffer));
	}
}

void ArrowDenseUnionData::Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
	auto &types_buffer = append_data.GetMainBuffer();
	auto &offsets_buffer = append_data.GetAuxBuffer();

	auto member_types = UnionType::CopyMemberTypes(input.GetType());
	idx_t n_children = member_types.size();

	// Track per-child row counts to compute offsets
	duckdb::vector<idx_t> child_counts(n_children, 0);
	// Count existing rows in each child to get the starting offset
	for (idx_t child_idx = 0; child_idx < n_children; child_idx++) {
		child_counts[child_idx] = append_data.child_data[child_idx]->row_count;
	}

	// Build per-child vectors containing only the values for that child
	duckdb::vector<duckdb::vector<Value>> child_values(n_children);

	idx_t append_count = to - from;
	// Pre-allocate space for offsets buffer (int32_t per row, managed at byte level)
	idx_t offsets_byte_start = offsets_buffer.size();
	offsets_buffer.resize(offsets_byte_start + append_count * sizeof(int32_t));
	auto offsets_data = reinterpret_cast<int32_t *>(offsets_buffer.data() + offsets_byte_start);

	for (idx_t input_idx = from; input_idx < to; input_idx++) {
		const auto &val = input.GetValue(input_idx);

		idx_t tag = 0;
		Value resolved_value(nullptr);
		if (!val.IsNull()) {
			tag = UnionValue::GetTag(val);
			resolved_value = UnionValue::GetValue(val);
		}

		types_buffer.push_back<data_t>(NumericCast<data_t>(tag));
		offsets_data[input_idx - from] = NumericCast<int32_t>(child_counts[tag]);
		child_values[tag].push_back(std::move(resolved_value));
		child_counts[tag]++;
	}

	// Append each child's collected values
	for (idx_t child_idx = 0; child_idx < n_children; child_idx++) {
		auto &values = child_values[child_idx];
		if (values.empty()) {
			continue;
		}
		idx_t child_size = values.size();
		Vector child_vector(member_types[child_idx].second, child_size);
		for (idx_t i = 0; i < child_size; i++) {
			child_vector.SetValue(i, values[i]);
		}
		auto &child_buffer = append_data.child_data[child_idx];
		child_buffer->append_vector(*child_buffer, child_vector, 0, child_size, child_size);
	}
	append_data.row_count += to - from;
}

void ArrowDenseUnionData::Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
	// Arrow C Data Interface: dense union has 3 buffers = [null_bitmap (always null), type_ids, offsets]
	result->n_buffers = 3;
	result->buffers[0] = nullptr;
	result->buffers[1] = append_data.GetMainBuffer().data();
	result->buffers[2] = append_data.GetAuxBuffer().data();

	auto &child_types = UnionType::CopyMemberTypes(type);
	ArrowAppender::AddChildren(append_data, child_types.size());
	result->children = append_data.child_pointers.data();
	result->n_children = NumericCast<int64_t>(child_types.size());
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto &child_type = child_types[i].second;
		append_data.child_arrays[i] = *ArrowAppender::FinalizeChild(child_type, std::move(append_data.child_data[i]));
	}
}

} // namespace duckdb
