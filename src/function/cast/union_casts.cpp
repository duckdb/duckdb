#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {


//--------------------------------------------------------------------------------------------------
// ??? -> UNION
//--------------------------------------------------------------------------------------------------

struct ToUnionBoundCastData : public BoundCastData {
	ToUnionBoundCastData(union_tag_t child_idx, BoundCastInfo child_cast, LogicalType target)
	    : child_idx(child_idx), child_cast_info(move(child_cast)), target(move(target)) {
	}
	union_tag_t child_idx;
	BoundCastInfo child_cast_info;
	LogicalType target;

	public:
	unique_ptr<BoundCastData> Copy() const override {
		return make_unique<ToUnionBoundCastData>(child_idx, child_cast_info.Copy(), target);
	}
};

unique_ptr<BoundCastData> BindToUnionCast(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	D_ASSERT(target.id() == LogicalTypeId::UNION);
	auto child_types = UnionType::GetMemberTypes(target);
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto &child_type = child_types[i];
		if (input.function_set.ImplicitCastCost(source, child_type.second) > -1) {
			auto child_cast = input.function_set.GetCastFunction(source, child_type.second);
			return make_unique<ToUnionBoundCastData>(i, move(child_cast), target);
		}
	}
	throw TypeMismatchException(source, target, "UNION does not contain a member of the source type");
};


static bool ToUnionCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters){
	D_ASSERT(result.GetType().id() == LogicalTypeId::UNION);
	auto &cast_data = (ToUnionBoundCastData &)*parameters.cast_data;
	auto &child_vector = UnionVector::GetMember(result, cast_data.child_idx);

	CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data.get());
	if (!cast_data.child_cast_info.function(source, child_vector, count, child_parameters)) {
		return false;
	}
	
	// cast succeeded, update union tags
	auto tags = UnionVector::GetTags(result);
	for (idx_t i = 0; i < count; i++) {
		tags[i] = cast_data.child_idx;
	}
	
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, ConstantVector::IsNull(source));
	} else {
		source.Flatten(count);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, false);
	}
	return true;
};

BoundCastInfo DefaultCasts::PromoteToUnionCast(BindCastInput &input, const LogicalType &source, const LogicalType &target){
	return BoundCastInfo(&ToUnionCast, BindToUnionCast(input, source, target));
}

//--------------------------------------------------------------------------------------------------
// UNION -> ???
//--------------------------------------------------------------------------------------------------

struct FromUnionBoundCastData : public BoundCastData {
	FromUnionBoundCastData(idx_t child_idx, BoundCastInfo child_cast, LogicalType target)
	    : child_idx(child_idx), child_cast_info(move(child_cast)), target(move(target)) {
	}
	idx_t child_idx;
	BoundCastInfo child_cast_info;
	LogicalType target;
	public:
	unique_ptr<BoundCastData> Copy() const override {
		return make_unique<FromUnionBoundCastData>(child_idx, child_cast_info.Copy(), target);
	}
};

unique_ptr<BoundCastData> BindFromUnionCast(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::UNION);
	auto &child_types = UnionType::GetChildTypes(source);
	for(idx_t i = 0; i < child_types.size(); i++) {
		auto &child_type = child_types[i];
		if (input.function_set.ImplicitCastCost(child_type.second, target) > -1) {
			auto child_cast = input.function_set.GetCastFunction(child_type.second, target);
			return make_unique<FromUnionBoundCastData>(i, move(child_cast), target);
		}
	}
	throw TypeMismatchException(source, target, "UNION does not contain a member of the target type");
};

static bool FromUnionCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(source.GetType().id() == LogicalTypeId::UNION);
	auto &cast_data = (FromUnionBoundCastData &)*parameters.cast_data;
	auto &child_vector = *UnionVector::GetEntries(source)[cast_data.child_idx];

	CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data.get());
	if(!cast_data.child_cast_info.function(child_vector, result, count, child_parameters)) {
		return false;
	}
	// cast succeeded

	if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		source.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(source, ConstantVector::IsNull(result));
	} else {
		result.Flatten(count);
		source.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(source, false);
	}
	return true;
};

//--------------------------------------------------------------------------------------------------
// UNION -> UNION
//--------------------------------------------------------------------------------------------------

struct UnionToUnionBoundCastData : public BoundCastData {
	UnionToUnionBoundCastData(vector<std::tuple<idx_t, idx_t, BoundCastInfo>> child_casts, LogicalType target_p)
	    : child_cast_info(move(child_casts)), target(move(target_p)) {
	}

	vector<std::tuple<idx_t, idx_t, BoundCastInfo>> child_cast_info;
	LogicalType target;

public:
	unique_ptr<BoundCastData> Copy() const override {
		vector<std::tuple<idx_t, idx_t, BoundCastInfo>> copy_info;
		for (auto &info : child_cast_info) {
			copy_info.push_back(std::make_tuple(std::get<0>(info), std::get<1>(info), std::get<2>(info).Copy()));
		}
		return make_unique<UnionToUnionBoundCastData>(move(copy_info), target);
	}
};

unique_ptr<BoundCastData> BindUnionToUnionCast(BindCastInput &input, const LogicalType &source,
                                                 const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::UNION);
	D_ASSERT(target.id() == LogicalTypeId::UNION);
	vector<std::tuple<idx_t, idx_t, BoundCastInfo>> child_cast_info;
	auto source_child_types = UnionType::GetMemberTypes(source);
	auto result_child_types = UnionType::GetMemberTypes(target);

	auto children_common = child_list_t<LogicalType>();

	for(idx_t i = 0; i < source_child_types.size(); i++) {
		for(idx_t j = 0; j < result_child_types.size(); j++) {
			if(source_child_types[i].first == result_child_types[j].first) {
				auto child_cast =
				    input.function_set.GetCastFunction(source_child_types[i].second, result_child_types[j].second);
				child_cast_info.push_back(std::make_tuple(i, j, move(child_cast)));
			}
		}
	}
	if(child_cast_info.size() == 0) {
		throw TypeMismatchException(source, target, "Cannot cast disjoint UNIONs types");
	}

	return make_unique<UnionToUnionBoundCastData>(move(child_cast_info), target);
}

static bool UnionToUnionCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = (UnionToUnionBoundCastData &)*parameters.cast_data;
	auto &source_children = UnionVector::GetEntries(source);
	auto &result_children = UnionVector::GetEntries(result);
	
	for (auto &info : cast_data.child_cast_info) {
		auto source_idx = std::get<0>(info);
		auto result_idx = std::get<1>(info);
		auto &child_cast = std::get<2>(info);

		auto &source_child_vector = *source_children[source_idx];
		auto &result_child_vector = *result_children[result_idx];

		CastParameters child_parameters(parameters, child_cast.cast_data.get());
		if(!child_cast.function(source_child_vector, result_child_vector, count, child_parameters)) {
			return false;
		}

		// TODO: what about extra children in target thats not present in source? test this!
	}

	// cast succeeded, update union tags
	// TODO: use a selection vector here instead of our wacky tuple mapping
	auto source_tags = UnionVector::GetTags(source);
	auto result_tags = UnionVector::GetTags(result);
	for (idx_t i = 0; i < count; i++) {
		for(idx_t j = 0; j < cast_data.child_cast_info.size(); j++) {
			auto source_idx = std::get<0>(cast_data.child_cast_info[j]);
			auto result_idx = std::get<1>(cast_data.child_cast_info[j]);
			if(source_tags[i] == source_idx) {
				result_tags[i] = result_idx;
			}
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

static bool UnionToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;
	// first cast all child elements to varchar
	auto &cast_data = (UnionToUnionBoundCastData &)*parameters.cast_data;
	Vector varchar_struct(cast_data.target, count);
	UnionToUnionCast(source, varchar_struct, count, parameters);

	// now construct the actual varchar vector
	varchar_struct.Flatten(count);
	auto &child_types = UnionType::GetChildTypes(source.GetType());
	auto &children = UnionVector::GetEntries(varchar_struct);
	auto tags = UnionVector::GetData(source);

	auto &validity = FlatVector::Validity(varchar_struct);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		auto tag = tags[i].tag;
		auto &child = *children[tag];
		auto child_str = FlatVector::GetData<string_t>(child)[i];
		result_data[i] = StringVector::AddString(result, child_str);
	}

	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

BoundCastInfo DefaultCasts::UnionCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {	
    switch (target.id()) {
	case LogicalTypeId::UNION:
		return BoundCastInfo(UnionToUnionCast, BindUnionToUnionCast(input, source, target));
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR: {
		// bind a cast in which we convert all child entries to VARCHAR entries
		auto &struct_children = UnionType::GetChildTypes(source);
		child_list_t<LogicalType> varchar_children;
		for (auto &child_entry : struct_children) {
			varchar_children.push_back(make_pair(child_entry.first, LogicalType::VARCHAR));
		}
		auto varchar_type = LogicalType::UNION(move(varchar_children));
		return BoundCastInfo(UnionToVarcharCast, BindUnionToUnionCast(input, source, varchar_type));
	}
	default:
		if(UnionType::GetChildrenOfType(source, target).size() != 0){
			return BoundCastInfo(FromUnionCast, BindFromUnionCast(input, source, target));
		}

		return TryVectorNullCast;
	}
}

} // namespace duckdb
