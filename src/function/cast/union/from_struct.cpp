#include "duckdb/function/cast/bound_cast_data.hpp"

namespace duckdb {

bool StructToUnionCast::AllowImplicitCastFromStruct(const LogicalType &source, const LogicalType &target) {
	if (source.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	auto member_count = UnionType::GetMemberCount(target);
	auto fields = StructType::GetChildTypes(source);
	if (member_count != fields.size()) {
		// Struct should have the same amount of fields as the union has members
		return false;
	}
	for (idx_t i = 0; i < member_count; i++) {
		auto &member = UnionType::GetMemberType(target, i);
		auto &member_name = UnionType::GetMemberName(target, i);
		auto &field = fields[i].second;
		auto &field_name = fields[i].first;
		if (member != field && field != LogicalType::VARCHAR) {
			// We allow the field to be VARCHAR, since unsupported types get cast to VARCHAR by EXPORT DATABASE (format
			// PARQUET) i.e UNION(a BIT) becomes STRUCT(a VARCHAR)
			return false;
		}
		if (member_name != field_name) {
			return false;
		}
	}
	return true;
}

// Physical Cast execution

void ReconstructTagVector(Vector &result, idx_t count) {
	auto &type = result.GetType();
	auto member_count = static_cast<union_tag_t>(UnionType::GetMemberCount(type));

	// Keep track for every row if the tag is (already) set
	vector<bool> tag_is_set(count, false);

	vector<LogicalType> types;
	types.push_back(LogicalType::BOOLEAN);
	DataChunk tag_chunk;
	auto &allocator = Allocator::DefaultAllocator();
	tag_chunk.Initialize(allocator, types, count);

	auto &tags = UnionVector::GetTags(result);
	UnifiedVectorFormat tag_format;
	tags.ToUnifiedFormat(count, tag_format);
	auto tag_data = UnifiedVectorFormat::GetDataNoConst<union_tag_t>(tag_format);
	auto &tag_validity = FlatVector::Validity(tags);

	for (union_tag_t member_idx = 0; member_idx < member_count; member_idx++) {
		tag_chunk.Reset();
		auto &result_child_vector = UnionVector::GetMember(result, member_idx);
		UnifiedVectorFormat format;
		result_child_vector.ToUnifiedFormat(count, format);
		if (format.validity.AllValid()) {
			// Fast pass, this means every value has this tag
			for (idx_t i = 0; i < count; i++) {
				tag_data[i] = member_idx;
			}
			// Verify that no tag has been set yet
			for (idx_t i = 0; i < count; i++) {
				if (tag_is_set[i]) {
					throw InvalidInputException(
					    "STRUCT -> UNION cast could not be performed because multiple fields are none-null");
				}
			}
			for (idx_t i = 0; i < count; i++) {
				tag_is_set[i] = true;
			}
			// Don't return yet, have to verify that no other tags are non-null
		} else {
			for (idx_t i = 0; i < count; i++) {
				if (!format.validity.RowIsValidUnsafe(i)) {
					continue;
				}
				if (tag_is_set[i]) {
					throw InvalidInputException(
					    "STRUCT -> UNION cast could not be performed because multiple fields are none-null");
				}
				tag_is_set[i] = true;
				tag_data[i] = member_idx;
			}
		}
	}

	// Set the validity mask for the tag vector
	for (idx_t i = 0; i < count; i++) {
		if (!tag_is_set[i]) {
			tag_validity.SetInvalid(i);
		} else {
			tag_validity.SetValid(i);
		}
	}
}

bool StructToUnionCast::Cast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<StructBoundCastData>();
	auto &lstate = parameters.local_state->Cast<StructCastLocalState>();

	D_ASSERT(source.GetType().id() == LogicalTypeId::STRUCT);
	D_ASSERT(result.GetType().id() == LogicalTypeId::UNION);
	D_ASSERT(cast_data.target.id() == LogicalTypeId::UNION);

	auto &source_children = StructVector::GetEntries(source);
	D_ASSERT(source_children.size() == UnionType::GetMemberCount(result.GetType()));

	bool all_converted = true;
	for (idx_t i = 0; i < source_children.size(); i++) {
		auto &result_child_vector = UnionVector::GetMember(result, i);
		auto &source_child_vector = *source_children[i];
		CastParameters child_parameters(parameters, cast_data.child_cast_info[i].cast_data, lstate.local_states[i]);
		if (!cast_data.child_cast_info[i].function(source_child_vector, result_child_vector, count, child_parameters)) {
			all_converted = false;
		}
	}

	ReconstructTagVector(result, count);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, ConstantVector::IsNull(source));
	} else {
		source.Flatten(count);
		FlatVector::Validity(result) = FlatVector::Validity(source);
	}
	result.Verify(count);
	return all_converted;
}

// Bind cast

unique_ptr<BoundCastData> StructToUnionCast::BindData(BindCastInput &input, const LogicalType &source,
                                                      const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	D_ASSERT(source.id() == LogicalTypeId::STRUCT);
	D_ASSERT(target.id() == LogicalTypeId::UNION);

	auto source_child_count = StructType::GetChildCount(source);
	(void)source_child_count;
	auto result_child_count = UnionType::GetMemberCount(target);
	D_ASSERT(source_child_count == result_child_count);

	for (idx_t i = 0; i < result_child_count; i++) {
		auto &source_child = StructType::GetChildType(source, i);
		auto &target_child = UnionType::GetMemberType(target, i);

		auto child_cast = input.GetCastFunction(source_child, target_child);
		child_cast_info.push_back(std::move(child_cast));
	}
	return make_uniq<StructBoundCastData>(std::move(child_cast_info), target);
}

BoundCastInfo StructToUnionCast::Bind(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	auto cast_data = StructToUnionCast::BindData(input, source, target);
	return BoundCastInfo(&StructToUnionCast::Cast, std::move(cast_data), StructToUnionCast::InitLocalState);
}

// Initialize local state

unique_ptr<FunctionLocalState> StructToUnionCast::InitLocalState(CastLocalStateParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<StructBoundCastData>();
	auto result = make_uniq<StructCastLocalState>();

	for (auto &entry : cast_data.child_cast_info) {
		unique_ptr<FunctionLocalState> child_state;
		if (entry.init_local_state) {
			CastLocalStateParameters child_params(parameters, entry.cast_data);
			child_state = entry.init_local_state(child_params);
		}
		result->local_states.push_back(std::move(child_state));
	}
	return std::move(result);
}

} // namespace duckdb
