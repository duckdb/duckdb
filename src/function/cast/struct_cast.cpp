#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"

namespace duckdb {

unique_ptr<BoundCastData> StructBoundCastData::BindStructToStructCast(BindCastInput &input, const LogicalType &source,
                                                                      const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto &source_children = StructType::GetChildTypes(source);
	auto &target_children = StructType::GetChildTypes(target);

	auto target_is_unnamed = StructType::IsUnnamed(target);
	auto source_is_unnamed = StructType::IsUnnamed(source);

	auto is_unnamed = target_is_unnamed || source_is_unnamed;
	if (is_unnamed && source_children.size() != target_children.size()) {
		throw TypeMismatchException(input.query_location, source, target, "Cannot cast STRUCTs of different size");
	}

	case_insensitive_map_t<idx_t> target_children_map;
	if (!is_unnamed) {
		for (idx_t i = 0; i < target_children.size(); i++) {
			auto &name = target_children[i].first;
			if (target_children_map.find(name) != target_children_map.end()) {
				throw NotImplementedException("Error while casting - duplicate name \"%s\" in struct", name);
			}
			target_children_map[name] = i;
		}
	}

	vector<idx_t> source_indexes;
	vector<idx_t> target_indexes;
	vector<idx_t> target_null_indexes;
	bool has_any_match = is_unnamed;

	for (idx_t i = 0; i < source_children.size(); i++) {
		auto &source_child = source_children[i];
		auto target_idx = i;

		// Map to the correct index for names structs.
		if (!is_unnamed) {
			auto target_child = target_children_map.find(source_child.first);
			if (target_child == target_children_map.end()) {
				// Skip any children that have no target.
				continue;
			}
			target_idx = target_child->second;
			target_children_map.erase(target_child);
			has_any_match = true;
		}

		source_indexes.push_back(i);
		target_indexes.push_back(target_idx);
		auto child_cast = input.GetCastFunction(source_child.second, target_children[target_idx].second);
		child_cast_info.push_back(std::move(child_cast));
	}

	if (!has_any_match) {
		throw BinderException("STRUCT to STRUCT cast must have at least one matching member");
	}

	// The remaining target children have no match in the source struct.
	// Thus, they become NULL.
	for (const auto &target_child : target_children_map) {
		target_null_indexes.push_back(target_child.second);
	}

	return make_uniq<StructBoundCastData>(std::move(child_cast_info), target, std::move(source_indexes),
	                                      std::move(target_indexes), std::move(target_null_indexes));
}

unique_ptr<FunctionLocalState> StructBoundCastData::InitStructCastLocalState(CastLocalStateParameters &parameters) {
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

static bool StructToStructCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<StructBoundCastData>();
	auto &l_state = parameters.local_state->Cast<StructCastLocalState>();

	auto &source_vectors = StructVector::GetEntries(source);
	auto &target_children = StructVector::GetEntries(result);

	bool all_converted = true;
	for (idx_t i = 0; i < cast_data.source_indexes.size(); i++) {
		auto source_idx = cast_data.source_indexes[i];
		auto target_idx = cast_data.target_indexes[i];

		auto &source_vector = *source_vectors[source_idx];
		auto &target_vector = *target_children[target_idx];

		auto &child_cast_info = cast_data.child_cast_info[i];
		CastParameters child_parameters(parameters, child_cast_info.cast_data, l_state.local_states[i]);
		auto success = child_cast_info.function(source_vector, target_vector, count, child_parameters);
		if (!success) {
			all_converted = false;
		}
	}

	if (!cast_data.target_null_indexes.empty()) {
		for (idx_t i = 0; i < cast_data.target_null_indexes.size(); i++) {
			auto target_idx = cast_data.target_null_indexes[i];
			auto &target_vector = *target_children[target_idx];

			target_vector.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(target_vector, true);
		}
	}

	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, ConstantVector::IsNull(source));
		return all_converted;
	}

	source.Flatten(count);
	auto &result_validity = FlatVector::Validity(result);
	result_validity = FlatVector::Validity(source);
	result.Verify(count);
	return all_converted;
}

static bool StructToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;
	// first cast all child elements to varchar
	auto &cast_data = parameters.cast_data->Cast<StructBoundCastData>();
	Vector varchar_struct(cast_data.target, count);
	StructToStructCast(source, varchar_struct, count, parameters);

	// now construct the actual varchar vector
	varchar_struct.Flatten(count);
	bool is_unnamed = StructType::IsUnnamed(source.GetType());
	auto &child_types = StructType::GetChildTypes(source.GetType());
	auto &children = StructVector::GetEntries(varchar_struct);
	auto &validity = FlatVector::Validity(varchar_struct);
	auto result_data = FlatVector::GetData<string_t>(result);
	static constexpr const idx_t SEP_LENGTH = 2;
	static constexpr const idx_t NAME_SEP_LENGTH = 4;
	static constexpr const idx_t NULL_LENGTH = 4;
	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		idx_t string_length = 2; // {}
		for (idx_t c = 0; c < children.size(); c++) {
			if (c > 0) {
				string_length += SEP_LENGTH;
			}
			children[c]->Flatten(count);
			auto &child_validity = FlatVector::Validity(*children[c]);
			auto data = FlatVector::GetData<string_t>(*children[c]);
			auto &name = child_types[c].first;
			if (!is_unnamed) {
				string_length += name.size() + NAME_SEP_LENGTH; // "'{name}': "
			}
			string_length += child_validity.RowIsValid(i) ? data[i].GetSize() : NULL_LENGTH;
		}
		result_data[i] = StringVector::EmptyString(result, string_length);
		auto dataptr = result_data[i].GetDataWriteable();
		idx_t offset = 0;
		dataptr[offset++] = is_unnamed ? '(' : '{';
		for (idx_t c = 0; c < children.size(); c++) {
			if (c > 0) {
				memcpy(dataptr + offset, ", ", SEP_LENGTH);
				offset += SEP_LENGTH;
			}
			auto &child_validity = FlatVector::Validity(*children[c]);
			auto data = FlatVector::GetData<string_t>(*children[c]);
			if (!is_unnamed) {
				auto &name = child_types[c].first;
				// "'{name}': "
				dataptr[offset++] = '\'';
				memcpy(dataptr + offset, name.c_str(), name.size());
				offset += name.size();
				dataptr[offset++] = '\'';
				dataptr[offset++] = ':';
				dataptr[offset++] = ' ';
			}
			// value
			if (child_validity.RowIsValid(i)) {
				auto len = data[i].GetSize();
				memcpy(dataptr + offset, data[i].GetData(), len);
				offset += len;
			} else {
				memcpy(dataptr + offset, "NULL", NULL_LENGTH);
				offset += NULL_LENGTH;
			}
		}
		dataptr[offset++] = is_unnamed ? ')' : '}';
		result_data[i].Finalize();
	}

	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

BoundCastInfo DefaultCasts::StructCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::STRUCT:
		return BoundCastInfo(StructToStructCast, StructBoundCastData::BindStructToStructCast(input, source, target),
		                     StructBoundCastData::InitStructCastLocalState);
	case LogicalTypeId::VARCHAR: {
		// bind a cast in which we convert all child entries to VARCHAR entries
		auto &struct_children = StructType::GetChildTypes(source);
		child_list_t<LogicalType> varchar_children;
		for (auto &child_entry : struct_children) {
			varchar_children.push_back(make_pair(child_entry.first, LogicalType::VARCHAR));
		}
		auto varchar_type = LogicalType::STRUCT(varchar_children);
		return BoundCastInfo(StructToVarcharCast,
		                     StructBoundCastData::BindStructToStructCast(input, source, varchar_type),
		                     StructBoundCastData::InitStructCastLocalState);
	}
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
