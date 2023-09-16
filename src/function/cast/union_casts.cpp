#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"

#include <algorithm> // for std::sort

namespace duckdb {

//--------------------------------------------------------------------------------------------------
// ??? -> UNION
//--------------------------------------------------------------------------------------------------
// if the source can be implicitly cast to a member of the target union, the cast is valid

unique_ptr<BoundCastData> BindToUnionCast(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	D_ASSERT(target.id() == LogicalTypeId::UNION);

	vector<UnionBoundCastData> candidates;

	for (idx_t member_idx = 0; member_idx < UnionType::GetMemberCount(target); member_idx++) {
		auto member_type = UnionType::GetMemberType(target, member_idx);
		auto member_name = UnionType::GetMemberName(target, member_idx);
		auto member_cast_cost = input.function_set.ImplicitCastCost(source, member_type);
		if (member_cast_cost != -1) {
			auto member_cast_info = input.GetCastFunction(source, member_type);
			candidates.emplace_back(member_idx, member_name, member_type, member_cast_cost,
			                        std::move(member_cast_info));
		}
	};

	// no possible casts found!
	if (candidates.empty()) {
		auto message = StringUtil::Format(
		    "Type %s can't be cast as %s. %s can't be implicitly cast to any of the union member types: ",
		    source.ToString(), target.ToString(), source.ToString());

		auto member_count = UnionType::GetMemberCount(target);
		for (idx_t member_idx = 0; member_idx < member_count; member_idx++) {
			auto member_type = UnionType::GetMemberType(target, member_idx);
			message += member_type.ToString();
			if (member_idx < member_count - 1) {
				message += ", ";
			}
		}
		throw CastException(message);
	}

	// sort the candidate casts by cost
	std::sort(candidates.begin(), candidates.end(), UnionBoundCastData::SortByCostAscending);

	// select the lowest possible cost cast
	auto &selected_cast = candidates[0];
	auto selected_cost = candidates[0].cost;

	// check if the cast is ambiguous (2 or more casts have the same cost)
	if (candidates.size() > 1 && candidates[1].cost == selected_cost) {

		// collect all the ambiguous types
		auto message = StringUtil::Format(
		    "Type %s can't be cast as %s. The cast is ambiguous, multiple possible members in target: ", source,
		    target);
		for (size_t i = 0; i < candidates.size(); i++) {
			if (candidates[i].cost == selected_cost) {
				message += StringUtil::Format("'%s (%s)'", candidates[i].name, candidates[i].type.ToString());
				if (i < candidates.size() - 1) {
					message += ", ";
				}
			}
		}
		message += ". Disambiguate the target type by using the 'union_value(<tag> := <arg>)' function to promote the "
		           "source value to a single member union before casting.";
		throw CastException(message);
	}

	// otherwise, return the selected cast
	return make_uniq<UnionBoundCastData>(std::move(selected_cast));
}

unique_ptr<FunctionLocalState> InitToUnionLocalState(CastLocalStateParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<UnionBoundCastData>();
	if (!cast_data.member_cast_info.init_local_state) {
		return nullptr;
	}
	CastLocalStateParameters child_parameters(parameters, cast_data.member_cast_info.cast_data);
	return cast_data.member_cast_info.init_local_state(child_parameters);
}

static bool ToUnionCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::UNION);
	auto &cast_data = parameters.cast_data->Cast<UnionBoundCastData>();
	auto &selected_member_vector = UnionVector::GetMember(result, cast_data.tag);

	CastParameters child_parameters(parameters, cast_data.member_cast_info.cast_data, parameters.local_state);
	if (!cast_data.member_cast_info.function(source, selected_member_vector, count, child_parameters)) {
		return false;
	}

	// cast succeeded, create union vector
	UnionVector::SetToMember(result, cast_data.tag, selected_member_vector, count, true);

	result.Verify(count);

	return true;
}

BoundCastInfo DefaultCasts::ImplicitToUnionCast(BindCastInput &input, const LogicalType &source,
                                                const LogicalType &target) {

	D_ASSERT(target.id() == LogicalTypeId::UNION);
	if (StructToUnionCast::AllowImplicitCastFromStruct(source, target)) {
		return StructToUnionCast::Bind(input, source, target);
	}
	auto cast_data = BindToUnionCast(input, source, target);
	return BoundCastInfo(&ToUnionCast, std::move(cast_data), InitToUnionLocalState);
}

//--------------------------------------------------------------------------------------------------
// UNION -> UNION
//--------------------------------------------------------------------------------------------------
// if the source member tags is a subset of the target member tags, and all the source members can be
// implicitly cast to the corresponding target members, the cast is valid.
//
// VALID: 	UNION(A, B) 	-> 	UNION(A, B, C)
// VALID: 	UNION(A, B) 	-> 	UNION(A, C)		if B can be implicitly cast to C
//
// INVALID: UNION(A, B, C)	->	UNION(A, B)
// INVALID:	UNION(A, B) 	->	UNION(A, C)		if B can't be implicitly cast to C
// INVALID:	UNION(A, B, D) 	->	UNION(A, B, C)

struct UnionUnionBoundCastData : public BoundCastData {

	// mapping from source member index to target member index
	// these are always the same size as the source member count
	// (since all source members must be present in the target)
	vector<idx_t> tag_map;
	vector<BoundCastInfo> member_casts;

	LogicalType target_type;

	UnionUnionBoundCastData(vector<idx_t> tag_map, vector<BoundCastInfo> member_casts, LogicalType target_type)
	    : tag_map(std::move(tag_map)), member_casts(std::move(member_casts)), target_type(std::move(target_type)) {
	}

public:
	unique_ptr<BoundCastData> Copy() const override {
		vector<BoundCastInfo> member_casts_copy;
		for (auto &member_cast : member_casts) {
			member_casts_copy.push_back(member_cast.Copy());
		}
		return make_uniq<UnionUnionBoundCastData>(tag_map, std::move(member_casts_copy), target_type);
	}
};

unique_ptr<BoundCastData> BindUnionToUnionCast(BindCastInput &input, const LogicalType &source,
                                               const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::UNION);
	D_ASSERT(target.id() == LogicalTypeId::UNION);

	auto source_member_count = UnionType::GetMemberCount(source);

	auto tag_map = vector<idx_t>(source_member_count);
	auto member_casts = vector<BoundCastInfo>();

	for (idx_t source_idx = 0; source_idx < source_member_count; source_idx++) {
		auto &source_member_type = UnionType::GetMemberType(source, source_idx);
		auto &source_member_name = UnionType::GetMemberName(source, source_idx);

		bool found = false;
		for (idx_t target_idx = 0; target_idx < UnionType::GetMemberCount(target); target_idx++) {
			auto &target_member_name = UnionType::GetMemberName(target, target_idx);

			// found a matching member
			if (source_member_name == target_member_name) {
				auto &target_member_type = UnionType::GetMemberType(target, target_idx);
				tag_map[source_idx] = target_idx;
				member_casts.push_back(input.GetCastFunction(source_member_type, target_member_type));
				found = true;
				break;
			}
		}
		if (!found) {
			// no matching member tag found in the target set
			auto message =
			    StringUtil::Format("Type %s can't be cast as %s. The member '%s' is not present in target union",
			                       source.ToString(), target.ToString(), source_member_name);
			throw CastException(message);
		}
	}

	return make_uniq<UnionUnionBoundCastData>(tag_map, std::move(member_casts), target);
}

unique_ptr<FunctionLocalState> InitUnionToUnionLocalState(CastLocalStateParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<UnionUnionBoundCastData>();
	auto result = make_uniq<StructCastLocalState>();

	for (auto &entry : cast_data.member_casts) {
		unique_ptr<FunctionLocalState> child_state;
		if (entry.init_local_state) {
			CastLocalStateParameters child_params(parameters, entry.cast_data);
			child_state = entry.init_local_state(child_params);
		}
		result->local_states.push_back(std::move(child_state));
	}
	return std::move(result);
}

static bool UnionToUnionCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<UnionUnionBoundCastData>();
	auto &lstate = parameters.local_state->Cast<StructCastLocalState>();

	auto source_member_count = UnionType::GetMemberCount(source.GetType());
	auto target_member_count = UnionType::GetMemberCount(result.GetType());

	auto target_member_is_mapped = vector<bool>(target_member_count);

	// Perform the casts from source to target members
	for (idx_t member_idx = 0; member_idx < source_member_count; member_idx++) {
		auto target_member_idx = cast_data.tag_map[member_idx];

		auto &source_member_vector = UnionVector::GetMember(source, member_idx);
		auto &target_member_vector = UnionVector::GetMember(result, target_member_idx);
		auto &member_cast = cast_data.member_casts[member_idx];

		CastParameters child_parameters(parameters, member_cast.cast_data, lstate.local_states[member_idx]);
		if (!member_cast.function(source_member_vector, target_member_vector, count, child_parameters)) {
			return false;
		}

		target_member_is_mapped[target_member_idx] = true;
	}

	// All member casts succeeded!

	// Set the unmapped target members to constant NULL.
	// If we cast UNION(A, B) -> UNION(A, B, C) we need to invalidate C so that
	// the invariants of the result union hold. (only member columns "selected"
	// by the rowwise corresponding tag in the tag vector should be valid)
	for (idx_t target_member_idx = 0; target_member_idx < target_member_count; target_member_idx++) {
		if (!target_member_is_mapped[target_member_idx]) {
			auto &target_member_vector = UnionVector::GetMember(result, target_member_idx);
			target_member_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(target_member_vector, true);
		}
	}

	// Update the tags in the result vector
	auto &source_tag_vector = UnionVector::GetTags(source);
	auto &result_tag_vector = UnionVector::GetTags(result);

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// Constant vector case optimization
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		if (ConstantVector::IsNull(source)) {
			ConstantVector::SetNull(result, true);
		} else {
			// map the tag
			auto source_tag = ConstantVector::GetData<union_tag_t>(source_tag_vector)[0];
			auto mapped_tag = cast_data.tag_map[source_tag];
			ConstantVector::GetData<union_tag_t>(result_tag_vector)[0] = mapped_tag;
		}
	} else {
		// Otherwise, use the unified vector format to access the source vector.

		// Ensure that all the result members are flat vectors
		// This is not always the case, e.g. when a member is cast using the default TryNullCast function
		// the resulting member vector will be a constant null vector.
		for (idx_t target_member_idx = 0; target_member_idx < target_member_count; target_member_idx++) {
			UnionVector::GetMember(result, target_member_idx).Flatten(count);
		}

		// We assume that a union tag vector validity matches the union vector validity.
		UnifiedVectorFormat source_tag_format;
		source_tag_vector.ToUnifiedFormat(count, source_tag_format);

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto source_row_idx = source_tag_format.sel->get_index(row_idx);
			if (source_tag_format.validity.RowIsValid(source_row_idx)) {
				// map the tag
				auto source_tag = (UnifiedVectorFormat::GetData<union_tag_t>(source_tag_format))[source_row_idx];
				auto target_tag = cast_data.tag_map[source_tag];
				FlatVector::GetData<union_tag_t>(result_tag_vector)[row_idx] = target_tag;
			} else {

				// Issue: The members of the result is not always flatvectors
				// In the case of TryNullCast, the result member is constant.
				FlatVector::SetNull(result, row_idx, true);
			}
		}
	}

	result.Verify(count);

	return true;
}

static bool UnionToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;
	// first cast all union members to varchar
	auto &cast_data = parameters.cast_data->Cast<UnionUnionBoundCastData>();
	Vector varchar_union(cast_data.target_type, count);

	UnionToUnionCast(source, varchar_union, count, parameters);

	// now construct the actual varchar vector
	varchar_union.Flatten(count);
	auto &tag_vector = UnionVector::GetTags(source);
	auto tags = FlatVector::GetData<union_tag_t>(tag_vector);

	auto &validity = FlatVector::Validity(varchar_union);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto &member = UnionVector::GetMember(varchar_union, tags[i]);
		UnifiedVectorFormat member_vdata;
		member.ToUnifiedFormat(count, member_vdata);

		auto mapped_idx = member_vdata.sel->get_index(i);
		auto member_valid = member_vdata.validity.RowIsValid(mapped_idx);
		if (member_valid) {
			auto member_str = (UnifiedVectorFormat::GetData<string_t>(member_vdata))[mapped_idx];
			result_data[i] = StringVector::AddString(result, member_str);
		} else {
			result_data[i] = StringVector::AddString(result, "NULL");
		}
	}

	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	result.Verify(count);
	return true;
}

BoundCastInfo DefaultCasts::UnionCastSwitch(BindCastInput &input, const LogicalType &source,
                                            const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::UNION);
	switch (target.id()) {
	case LogicalTypeId::VARCHAR: {
		// bind a cast in which we convert all members to VARCHAR first
		child_list_t<LogicalType> varchar_members;
		for (idx_t member_idx = 0; member_idx < UnionType::GetMemberCount(source); member_idx++) {
			varchar_members.push_back(make_pair(UnionType::GetMemberName(source, member_idx), LogicalType::VARCHAR));
		}
		auto varchar_type = LogicalType::UNION(std::move(varchar_members));
		return BoundCastInfo(UnionToVarcharCast, BindUnionToUnionCast(input, source, varchar_type),
		                     InitUnionToUnionLocalState);
	}
	case LogicalTypeId::UNION:
		return BoundCastInfo(UnionToUnionCast, BindUnionToUnionCast(input, source, target), InitUnionToUnionLocalState);
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
