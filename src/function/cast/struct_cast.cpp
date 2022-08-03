#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {

unique_ptr<BoundCastData> BindStructToStructCast(BindCastInput &input, const LogicalType &source, const LogicalType &target)  {

}

static bool StructToStructCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &source_child_types = StructType::GetChildTypes(source.GetType());
	auto &result_child_types = StructType::GetChildTypes(result.GetType());
	if (source_child_types.size() != result_child_types.size()) {
		throw TypeMismatchException(source.GetType(), result.GetType(), "Cannot cast STRUCTs of different size");
	}
	auto &source_children = StructVector::GetEntries(source);
	D_ASSERT(source_children.size() == source_child_types.size());

	auto &result_children = StructVector::GetEntries(result);
	for (idx_t c_idx = 0; c_idx < result_child_types.size(); c_idx++) {
		auto &result_child_vector = result_children[c_idx];
		auto &source_child_vector = *source_children[c_idx];
		if (result_child_vector->GetType() != source_child_vector.GetType()) {
			VectorOperations::Cast(source_child_vector, *result_child_vector, count, false);
		} else {
			result_child_vector->Reference(source_child_vector);
		}
	}
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, ConstantVector::IsNull(source));
	} else {
		source.Flatten(count);
		FlatVector::Validity(result) = FlatVector::Validity(source);
	}
	return true;
}

static bool StructToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(source.GetVectorType());
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);
	}
	for (idx_t i = 0; i < count; i++) {
		auto src_val = source.GetValue(i);
		auto str_val = src_val.ToString();
		result.SetValue(i, Value(str_val));
	}
	return true;
}

BoundCastInfo DefaultCasts::StructCastSwitch(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
		return BoundCastInfo(StructToStructCast, BindStructToStructCast(input, source, target));
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR:
		return StructToVarcharCast;
	default:
		return TryVectorNullCast;
	}
}

}
