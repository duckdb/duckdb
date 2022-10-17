#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

static void CopyValidity(Vector &source, Vector &result, idx_t count) {

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, ConstantVector::IsNull(source));
	} else {
		source.Flatten(count);
		FlatVector::Validity(result) = FlatVector::Validity(source);
	}
}

//--------------------------------------------------------------------------------------------------
// ??? -> UNION
//--------------------------------------------------------------------------------------------------
// if the source can be implicitly cast to a member of the target union, the cast is valid

struct UnionMemberBoundCastData : public BoundCastData {
	UnionMemberBoundCastData(union_tag_t member_idx, string name, LogicalType type, int64_t cost,
	                         BoundCastInfo member_cast_info)
	    : tag(member_idx), name(name), type(type), cost(cost), member_cast_info(move(member_cast_info)) {
	}

	union_tag_t tag;
	string name;
	LogicalType type;
	int64_t cost;
	BoundCastInfo member_cast_info;

public:
	unique_ptr<BoundCastData> Copy() const override {
		return make_unique<UnionMemberBoundCastData>(tag, name, type, cost, member_cast_info.Copy());
	}

	static bool SortByCostAscending(const UnionMemberBoundCastData &left, const UnionMemberBoundCastData &right) {
		return left.cost < right.cost;
	}
};

unique_ptr<BoundCastData> BindToUnionCast(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	D_ASSERT(target.id() == LogicalTypeId::UNION);

	vector<UnionMemberBoundCastData> candidates;

	for (idx_t member_idx = 0; member_idx < UnionType::GetMemberCount(target); member_idx++) {
		auto member_type = UnionType::GetMemberType(target, member_idx);
		auto member_name = UnionType::GetMemberName(target, member_idx);
		auto member_cast_cost = input.function_set.ImplicitCastCost(source, member_type);
		if (member_cast_cost != -1) {
			auto member_cast_info = input.GetCastFunction(source, member_type);
			candidates.push_back(UnionMemberBoundCastData(member_idx, member_name, member_type, member_cast_cost,
			                                              move(member_cast_info)));
		}
	};

	// no possible casts found!
	if (candidates.size() == 0) {
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
	sort(candidates.begin(), candidates.end(), UnionMemberBoundCastData::SortByCostAscending);

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
	return make_unique<UnionMemberBoundCastData>(move(selected_cast));
};

static bool ToUnionCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(result.GetType().id() == LogicalTypeId::UNION);
	auto &cast_data = (UnionMemberBoundCastData &)*parameters.cast_data;
	auto &selected_member_vector = UnionVector::GetMember(result, cast_data.tag);

	CastParameters child_parameters(parameters, cast_data.member_cast_info.cast_data.get());
	if (!cast_data.member_cast_info.function(source, selected_member_vector, count, child_parameters)) {
		return false;
	}

	// cast succeeded, update union tags
	UnionVector::SetTags(result, cast_data.tag);

	CopyValidity(source, result, count);

	return true;
};

BoundCastInfo DefaultCasts::ImplicitToUnionCast(BindCastInput &input, const LogicalType &source,
                                                const LogicalType &target) {
	return BoundCastInfo(&ToUnionCast, BindToUnionCast(input, source, target));
}

//--------------------------------------------------------------------------------------------------
// UNION -> ???
//--------------------------------------------------------------------------------------------------
// if the source contains a member that can be implicitly cast the target, the cast is valid

unique_ptr<BoundCastData> BindFromUnionCast(BindCastInput &input, const LogicalType &source,
                                            const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::UNION);

	vector<UnionMemberBoundCastData> candidates;

	for (idx_t member_idx = 0; member_idx < UnionType::GetMemberCount(source); member_idx++) {
		auto member_type = UnionType::GetMemberType(source, member_idx);
		auto member_name = UnionType::GetMemberName(source, member_idx);
		auto member_cast_cost = input.function_set.ImplicitCastCost(member_type, target);
		if (member_cast_cost != -1) {
			auto member_cast_info = input.GetCastFunction(member_type, target);
			candidates.push_back(UnionMemberBoundCastData(member_idx, member_name, member_type, member_cast_cost,
			                                              move(member_cast_info)));
		}
	};

	// no possible casts found!
	if (candidates.size() == 0) {
		auto message = StringUtil::Format(
		    "Type %s can't be cast as %s. %s can't be implicitly cast from any of the union member types: ",
		    target.ToString(), source.ToString(), target.ToString());

		auto member_count = UnionType::GetMemberCount(source);
		for (idx_t member_idx = 0; member_idx < member_count; member_idx++) {
			auto member_type = UnionType::GetMemberType(source, member_idx);
			message += member_type.ToString();
			if (member_idx < member_count - 1) {
				message += ", ";
			}
		}
		throw CastException(message);
	}

	// sort the candidate casts by cost
	sort(candidates.begin(), candidates.end(), UnionMemberBoundCastData::SortByCostAscending);

	// select the lowest possible cost cast
	auto &selected_cast = candidates[0];
	auto selected_cost = candidates[0].cost;

	// check if the cast is ambiguous (2 or more casts have the same cost)
	if (candidates.size() > 1 && candidates[1].cost == selected_cost) {

		// collect all the ambiguous types
		auto message = StringUtil::Format(
		    "Type %s can't be cast as %s. The cast is ambiguous, multiple possible members in source: ", source,
		    target);
		for (size_t i = 0; i < candidates.size(); i++) {
			if (candidates[i].cost == selected_cost) {
				message += StringUtil::Format("'%s (%s)'", candidates[i].name, candidates[i].type.ToString());
				if (i < candidates.size() - 1) {
					message += ", ";
				}
			}
		}
		message += ". Select the desired union member type by using the 'union_extract(<tag>)' function, dot or "
		           "bracket notation before casting.";
		throw CastException(message);
	}

	// otherwise, return the selected cast
	return make_unique<UnionMemberBoundCastData>(move(selected_cast));
};

static bool FromUnionCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(source.GetType().id() == LogicalTypeId::UNION);

	auto &cast_data = (UnionMemberBoundCastData &)*parameters.cast_data;
	auto &selected_member_vector = UnionVector::GetMember(source, cast_data.tag);

	CastParameters child_parameters(parameters, cast_data.member_cast_info.cast_data.get());
	if (!cast_data.member_cast_info.function(selected_member_vector, result, count, child_parameters)) {
		return false;
	}

	CopyValidity(source, result, count);

	return true;
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

struct UnionToUnionBoundCastData : public BoundCastData {

	// mapping from source member index to target member index
	// these are always the same size as the source member count
	// (since all source members must be present in the target)
	vector<idx_t> tag_map;
	vector<BoundCastInfo> member_casts;

	LogicalType target_type;

	UnionToUnionBoundCastData(vector<idx_t> tag_map, vector<BoundCastInfo> member_casts, LogicalType target_type)
	    : tag_map(move(tag_map)), member_casts(move(member_casts)), target_type(target_type) {
	}

public:
	unique_ptr<BoundCastData> Copy() const override {
		vector<BoundCastInfo> member_casts_copy;
		for (auto &member_cast : member_casts) {
			member_casts_copy.push_back(member_cast.Copy());
		}
		return make_unique<UnionToUnionBoundCastData>(tag_map, move(member_casts_copy), target_type);
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

			// found a matching member, check if the types are castable
			if (source_member_name == target_member_name) {
				auto &target_member_type = UnionType::GetMemberType(target, target_idx);

				if (input.function_set.ImplicitCastCost(source_member_type, target_member_type) < 0) {

					auto message = StringUtil::Format(
					    "Type %s can't be cast as %s. The member %s can't be implicitly cast from %s to %s",
					    source.ToString(), target.ToString(), source_member_name, source_member_type.ToString(),
					    target_member_type.ToString());
					throw CastException(message);
				}

				tag_map[source_idx] = target_idx;
				member_casts.push_back(input.GetCastFunction(source_member_type, target_member_type));
				found = true;
				break;
			}
		}
		if (!found) {
			// no matching member tag found in the target set
			auto message = StringUtil::Format(
			    "Type %s can't be cast as %s. The member %s is not present in target union", source.ToString(),
			    target.ToString(), source_member_name, source_member_type.ToString());
			throw CastException(message);
		}
	}

	return make_unique<UnionToUnionBoundCastData>(tag_map, move(member_casts), target);
}

static bool UnionToUnionCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = (UnionToUnionBoundCastData &)*parameters.cast_data;

	auto source_member_count = UnionType::GetMemberCount(source.GetType());
	for (idx_t member_idx = 0; member_idx < source_member_count; member_idx++) {
		auto target_member_idx = cast_data.tag_map[member_idx];

		auto &source_member_vector = UnionVector::GetMember(source, member_idx);
		auto &target_member_vector = UnionVector::GetMember(result, target_member_idx);
		auto &member_cast = cast_data.member_casts[member_idx];

		CastParameters child_parameters(parameters, member_cast.cast_data.get());
		if (!member_cast.function(source_member_vector, target_member_vector, count, child_parameters)) {
			return false;
		}
	}

	// cast succeeded, update union tags
	UnifiedVectorFormat f;
	source.ToUnifiedFormat(count, f);
	auto validity = f.validity;

	auto source_tags = UnionVector::GetTags(source);
	auto result_tags = UnionVector::GetTags(result);

	// fast path
	if (validity.AllValid()) {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			result_tags[row_idx] = cast_data.tag_map[source_tags[row_idx]];
		}
	} else {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {

			// map source tag to target tag
			if (f.validity.RowIsValid(row_idx)) {
				result_tags[row_idx] = cast_data.tag_map[source_tags[row_idx]];
			}
		}
	}

	CopyValidity(source, result, count);

	return true;
}

static bool UnionToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;
	// first cast all union members to varchar
	auto &cast_data = (UnionToUnionBoundCastData &)*parameters.cast_data;
	Vector varchar_union(cast_data.target_type, count);

	UnionToUnionCast(source, varchar_union, count, parameters);

	// now construct the actual varchar vector
	varchar_union.Flatten(count);
	auto tags = UnionVector::GetTags(source);

	auto &validity = FlatVector::Validity(varchar_union);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto &member = UnionVector::GetMember(varchar_union, tags[i]);
		auto member_str = FlatVector::GetData<string_t>(member)[i];
		auto member_valid = FlatVector::Validity(member).RowIsValid(i);
		if (member_valid) {
			result_data[i] = StringVector::AddString(result, member_str);
		} else {
			result_data[i] = StringVector::AddString(result, "NULL");
		}
	}

	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return true;
}

BoundCastInfo DefaultCasts::UnionCastSwitch(BindCastInput &input, const LogicalType &source,
                                            const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR: {
		// bind a cast in which we convert all members to VARCHAR first
		child_list_t<LogicalType> varchar_members;
		for (idx_t member_idx = 0; member_idx < UnionType::GetMemberCount(source); member_idx++) {
			varchar_members.push_back(make_pair(UnionType::GetMemberName(source, member_idx), LogicalType::VARCHAR));
		}
		auto varchar_type = LogicalType::UNION(move(varchar_members));
		return BoundCastInfo(UnionToVarcharCast, BindUnionToUnionCast(input, source, varchar_type));
	} break;
	case LogicalTypeId::UNION:
		return BoundCastInfo(UnionToUnionCast, BindUnionToUnionCast(input, source, target));
	default:
		return BoundCastInfo(FromUnionCast, BindFromUnionCast(input, source, target));
	}
}

} // namespace duckdb
