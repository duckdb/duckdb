#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"

namespace duckdb {

struct StructBoundCastData : public BoundCastData {
	StructBoundCastData(vector<BoundCastInfo> child_casts) :
		child_cast_info(move(child_casts)) {}

	vector<BoundCastInfo> child_cast_info;

public:
	unique_ptr<BoundCastData> Copy() const override {
		vector<BoundCastInfo> copy_info;
		for(auto &info : child_cast_info) {
			copy_info.push_back(info.Copy());
		}
		return make_unique<StructBoundCastData>(move(copy_info));
	}

};

unique_ptr<BoundCastData> BindStructToStructCast(BindCastInput &input, const LogicalType &source, const LogicalType &target)  {
	vector<BoundCastInfo> child_cast_info;
	auto &source_child_types = StructType::GetChildTypes(source);
	auto &result_child_types = StructType::GetChildTypes(target);
	if (source_child_types.size() != result_child_types.size()) {
		throw TypeMismatchException(source, target, "Cannot cast STRUCTs of different size");
	}
	for(idx_t i = 0; i < source_child_types.size(); i++) {
		auto child_cast = input.function_set.GetCastFunction(source_child_types[i].second, result_child_types[i].second);
		child_cast_info.push_back(move(child_cast));

	}
	return make_unique<StructBoundCastData>(move(child_cast_info));
}

static bool StructToStructCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = (StructBoundCastData &) *parameters.cast_data;
	auto &source_child_types = StructType::GetChildTypes(source.GetType());
	auto &source_children = StructVector::GetEntries(source);
	D_ASSERT(source_children.size() == StructType::GetChildTypes(result.GetType()).size());

	auto &result_children = StructVector::GetEntries(result);
	for (idx_t c_idx = 0; c_idx < source_child_types.size(); c_idx++) {
		auto &result_child_vector = *result_children[c_idx];
		auto &source_child_vector = *source_children[c_idx];
		CastParameters child_parameters(parameters, cast_data.child_cast_info[c_idx].cast_data.get());
		cast_data.child_cast_info[c_idx].function(source_child_vector, result_child_vector, count, child_parameters);
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
