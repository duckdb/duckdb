#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

unique_ptr<BoundCastData> StructBoundCastData::BindStructToStructCast(BindCastInput &input, const LogicalType &source,
                                                                      const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto &source_children = StructType::GetChildTypes(source);
	auto &target_children = StructType::GetChildTypes(target);

	auto target_is_unnamed = target_children.empty() || StructType::IsUnnamed(target);
	auto source_is_unnamed = source_children.empty() || StructType::IsUnnamed(source);

	auto is_unnamed = target_is_unnamed || source_is_unnamed;
	if (is_unnamed && source_children.size() != target_children.size()) {
		throw TypeMismatchException(input.query_location, source, target, "Cannot cast STRUCTs of different size");
	}

	InsertionOrderPreservingMap<idx_t> target_children_map;
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
	auto &base_children = StructVector::GetEntries(source);

	// now construct the actual varchar vector
	varchar_struct.Flatten(count);
	bool is_unnamed = StructType::IsUnnamed(source.GetType());
	auto &child_types = StructType::GetChildTypes(source.GetType());
	auto &children = StructVector::GetEntries(varchar_struct);
	auto &validity = FlatVector::Validity(varchar_struct);
	auto result_data = FlatVector::GetData<string_t>(result);
	static constexpr const idx_t SEP_LENGTH = 2;
	static constexpr const idx_t NAME_SEP_LENGTH = 2;
	static constexpr const idx_t NULL_LENGTH = 4;
	auto key_needs_quotes = make_unsafe_uniq_array_uninitialized<bool>(children.size());
	auto value_needs_quotes = make_unsafe_uniq_array_uninitialized<bool>(children.size());

	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		//! Calculate the total length of the row
		idx_t string_length = 2; // {}
		for (idx_t c = 0; c < children.size(); c++) {
			if (c > 0) {
				string_length += SEP_LENGTH;
			}
			auto add_escapes = !base_children[c]->GetType().IsNested();
			auto string_length_func = add_escapes ? VectorCastHelpers::CalculateEscapedStringLength<false>
			                                      : VectorCastHelpers::CalculateStringLength;

			children[c]->Flatten(count);
			auto &child_validity = FlatVector::Validity(*children[c]);
			auto data = FlatVector::GetData<string_t>(*children[c]);
			auto &name = child_types[c].first;
			if (!is_unnamed) {
				string_length += VectorCastHelpers::CalculateEscapedStringLength<true>(name, key_needs_quotes[c]);
				string_length += NAME_SEP_LENGTH; // ": "
			}
			if (child_validity.RowIsValid(i)) {
				//! Skip the `\`, not a special character outside quotes
				string_length += string_length_func(data[i], value_needs_quotes[c]);
			} else {
				string_length += NULL_LENGTH;
			}
		}

		result_data[i] = StringVector::EmptyString(result, string_length);
		auto dataptr = result_data[i].GetDataWriteable();

		//! Serialize the struct to the string
		idx_t offset = 0;
		dataptr[offset++] = is_unnamed ? '(' : '{';
		for (idx_t c = 0; c < children.size(); c++) {
			if (c > 0) {
				memcpy(dataptr + offset, ", ", SEP_LENGTH);
				offset += SEP_LENGTH;
			}
			auto add_escapes = !base_children[c]->GetType().IsNested();
			auto write_string_func =
			    add_escapes ? VectorCastHelpers::WriteEscapedString<false> : VectorCastHelpers::WriteString;

			auto &child_validity = FlatVector::Validity(*children[c]);
			auto data = FlatVector::GetData<string_t>(*children[c]);
			if (!is_unnamed) {
				auto &name = child_types[c].first;
				// "{<name>: <value>}"
				offset += VectorCastHelpers::WriteEscapedString<true>(dataptr + offset, name, key_needs_quotes[c]);
				dataptr[offset++] = ':';
				dataptr[offset++] = ' ';
			}
			// value
			if (child_validity.RowIsValid(i)) {
				//! Skip the `\`, not a special character outside quotes
				offset += write_string_func(dataptr + offset, data[i], value_needs_quotes[c]);
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

unique_ptr<BoundCastData> StructToMapBoundCastData::BindStructToMapCast(BindCastInput &input, const LogicalType &source,
                                                                        const LogicalType &target) {
	if (StructType::IsUnnamed(source)) {
		throw TypeMismatchException(input.query_location, source, target, "Cannot cast unnamed STRUCTs to MAP");
	}
	// Get a cast function for the keys (struct has varchar keys, map has single-type keys)
	auto key_cast = input.GetCastFunction(LogicalType::VARCHAR, MapType::KeyType(target));
	// Get cast function for the values (struct may have different value types per key, map has single-type values)
	vector<BoundCastInfo> value_casts;
	auto target_value_type = MapType::ValueType(target);
	for (idx_t i = 0; i < StructType::GetChildCount(source); i++) {
		auto value_cast = input.GetCastFunction(StructType::GetChildType(source, i), target_value_type);
		value_casts.push_back(std::move(value_cast));
	}
	return make_uniq<StructToMapBoundCastData>(std::move(key_cast), std::move(value_casts));
}

unique_ptr<FunctionLocalState>
StructToMapBoundCastData::InitStructToMapCastLocalState(CastLocalStateParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<StructToMapBoundCastData>();
	auto result = make_uniq<StructToMapCastLocalState>();

	if (cast_data.key_cast.init_local_state) {
		CastLocalStateParameters child_params(parameters, cast_data.key_cast.cast_data);
		result->key_state = cast_data.key_cast.init_local_state(child_params);
	}

	for (auto &entry : cast_data.value_casts) {
		unique_ptr<FunctionLocalState> child_state;
		if (entry.init_local_state) {
			CastLocalStateParameters child_params(parameters, entry.cast_data);
			child_state = entry.init_local_state(child_params);
		}
		result->value_states.push_back(std::move(child_state));
	}
	return std::move(result);
}

static bool StructToMapCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// Optimization: if the source vector is constant, we only have a single physical element, so we can set the
		// result vectortype to ConstantVector as well and set the (logical) count to 1
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		count = 1;
		if (ConstantVector::IsNull(source)) {
			// If there's only a null in there we don't need to cast anything
			ConstantVector::SetNull(result, true);
			return true;
		}
	}

	auto &cast_data = parameters.cast_data->Cast<StructToMapBoundCastData>();
	auto &local_state = parameters.local_state->Cast<StructToMapCastLocalState>();

	// Grab all struct columns
	auto &source_children = StructVector::GetEntries(source);
	idx_t field_count = source_children.size();
	idx_t total_count = count * field_count;

	// Allocate result
	ListVector::Reserve(result, total_count);

	// Create key vector with VARCHAR keys (could make this a dictionary vector as optimization)
	Vector varchar_keys(LogicalType::VARCHAR, total_count);
	auto key_data = FlatVector::GetData<string_t>(varchar_keys);
	auto &field_types = StructType::GetChildTypes(source.GetType());
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		for (idx_t field_idx = 0; field_idx < field_count; field_idx++) {
			auto global_idx = row_idx * field_count + field_idx;
			auto &field_name = field_types[field_idx].first;
			key_data[global_idx] = StringVector::AddString(varchar_keys, field_name);
		}
	}

	// Cast keys to result
	auto &map_keys = MapVector::GetKeys(result);
	CastParameters key_parameters(parameters, cast_data.key_cast.cast_data, local_state.key_state);
	auto keys_converted = cast_data.key_cast.function(varchar_keys, map_keys, total_count, key_parameters);

	// Fill the values vector
	bool values_converted = true;
	auto &map_values = MapVector::GetValues(result);
	for (idx_t field_idx = 0; field_idx < field_count; field_idx++) {
		auto &source_field = *source_children[field_idx];
		Vector temp_converted(MapType::ValueType(result.GetType()), count);
		CastParameters child_params(parameters, cast_data.value_casts[field_idx].cast_data,
		                            local_state.value_states[field_idx]);
		auto success = cast_data.value_casts[field_idx].function(source_field, temp_converted, count, child_params);
		if (!success) {
			values_converted = false;
		}

		// Interleave results
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto target_idx = row_idx * field_count + field_idx;
			// Copy also does a validity check
			VectorOperations::Copy(temp_converted, map_values, row_idx + 1, row_idx, target_idx);
		}
	}

	// Check for nulls in the source rows, and set the list data
	UnifiedVectorFormat format;
	source.ToUnifiedFormat(count, format);
	auto &validity = format.validity;
	auto list_data = ListVector::GetData(result);
	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(format.sel->get_index(i))) { // is row null?
			// Note: this must be a FlatVector because if we set it to be a ConstantVector and that was null then we've
			// already returned
			FlatVector::SetNull(result, i, true);
		} else {
			list_data[i] = list_entry_t(i * field_count, field_count);
		}
	}
	// Set the size
	ListVector::SetListSize(result, total_count);

	return keys_converted && values_converted;
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
	case LogicalTypeId::MAP: {
		return BoundCastInfo(StructToMapCast, StructToMapBoundCastData::BindStructToMapCast(input, source, target),
		                     StructToMapBoundCastData::InitStructToMapCastLocalState);
	}
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
