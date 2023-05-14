#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"

namespace duckdb {

unique_ptr<BoundCastData> StructBoundCastData::BindStructToStructCast(BindCastInput &input, const LogicalType &source,
                                                                      const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto &source_child_types = StructType::GetChildTypes(source);
	auto &result_child_types = StructType::GetChildTypes(target);
	if (source_child_types.size() != result_child_types.size()) {
		throw TypeMismatchException(source, target, "Cannot cast STRUCTs of different size");
	}
	for (idx_t i = 0; i < source_child_types.size(); i++) {
		auto child_cast = input.GetCastFunction(source_child_types[i].second, result_child_types[i].second);
		child_cast_info.push_back(std::move(child_cast));
	}
	return make_uniq<StructBoundCastData>(std::move(child_cast_info), target);
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
	auto &lstate = parameters.local_state->Cast<StructCastLocalState>();
	auto &source_child_types = StructType::GetChildTypes(source.GetType());
	auto &source_children = StructVector::GetEntries(source);
	D_ASSERT(source_children.size() == StructType::GetChildTypes(result.GetType()).size());

	auto &result_children = StructVector::GetEntries(result);
	bool all_converted = true;
	for (idx_t c_idx = 0; c_idx < source_child_types.size(); c_idx++) {
		auto &result_child_vector = *result_children[c_idx];
		auto &source_child_vector = *source_children[c_idx];
		CastParameters child_parameters(parameters, cast_data.child_cast_info[c_idx].cast_data,
		                                lstate.local_states[c_idx]);
		if (!cast_data.child_cast_info[c_idx].function(source_child_vector, result_child_vector, count,
		                                               child_parameters)) {
			all_converted = false;
		}
	}
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, ConstantVector::IsNull(source));
	} else {
		source.Flatten(count);
		FlatVector::Validity(result) = FlatVector::Validity(source);
	}
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
			string_length += name.size() + NAME_SEP_LENGTH; // "'{name}': "
			string_length += child_validity.RowIsValid(i) ? data[i].GetSize() : NULL_LENGTH;
		}
		result_data[i] = StringVector::EmptyString(result, string_length);
		auto dataptr = result_data[i].GetDataWriteable();
		idx_t offset = 0;
		dataptr[offset++] = '{';
		for (idx_t c = 0; c < children.size(); c++) {
			if (c > 0) {
				memcpy(dataptr + offset, ", ", SEP_LENGTH);
				offset += SEP_LENGTH;
			}
			auto &child_validity = FlatVector::Validity(*children[c]);
			auto data = FlatVector::GetData<string_t>(*children[c]);
			auto &name = child_types[c].first;
			// "'{name}': "
			dataptr[offset++] = '\'';
			memcpy(dataptr + offset, name.c_str(), name.size());
			offset += name.size();
			dataptr[offset++] = '\'';
			dataptr[offset++] = ':';
			dataptr[offset++] = ' ';
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
		dataptr[offset++] = '}';
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
