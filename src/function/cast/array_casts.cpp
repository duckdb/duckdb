#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

unique_ptr<BoundCastData> ArrayBoundCastData::BindArrayToArrayCast(BindCastInput &input, const LogicalType &source,
                                                                   const LogicalType &target) {
	vector<BoundCastInfo> child_cast_info;
	auto &source_child_type = ArrayType::GetChildType(source);
	auto &result_child_type = ArrayType::GetChildType(target);
	auto child_cast = input.GetCastFunction(source_child_type, result_child_type);
	return make_uniq<ArrayBoundCastData>(std::move(child_cast));
}

static unique_ptr<BoundCastData> BindArrayToListCast(BindCastInput &input, const LogicalType &source,
                                                     const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::ARRAY);
	D_ASSERT(target.id() == LogicalTypeId::LIST);

	vector<BoundCastInfo> child_cast_info;
	auto &source_child_type = ArrayType::GetChildType(source);
	auto &result_child_type = ListType::GetChildType(target);
	auto child_cast = input.GetCastFunction(source_child_type, result_child_type);
	return make_uniq<ArrayBoundCastData>(std::move(child_cast));
}

unique_ptr<FunctionLocalState> ArrayBoundCastData::InitArrayLocalState(CastLocalStateParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<ArrayBoundCastData>();
	if (!cast_data.child_cast_info.init_local_state) {
		return nullptr;
	}
	CastLocalStateParameters child_parameters(parameters, cast_data.child_cast_info.cast_data);
	return cast_data.child_cast_info.init_local_state(child_parameters);
}

//------------------------------------------------------------------------------
// ARRAY -> ARRAY
//------------------------------------------------------------------------------
static bool ArrayToArrayCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {

	auto source_array_size = ArrayType::GetSize(source.GetType());
	auto target_array_size = ArrayType::GetSize(result.GetType());
	if (source_array_size != target_array_size) {
		// Cant cast between arrays of different sizes
		auto msg = StringUtil::Format("Cannot cast array of size %u to array of size %u", source_array_size,
		                              target_array_size);
		HandleCastError::AssignError(msg, parameters);
		if (!parameters.strict) {
			// if this was a TRY_CAST, we know every row will fail, so just return null
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
			return false;
		}
	}

	auto &cast_data = parameters.cast_data->Cast<ArrayBoundCastData>();
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		if (ConstantVector::IsNull(source)) {
			ConstantVector::SetNull(result, true);
		}

		auto &source_cc = ArrayVector::GetEntry(source);
		auto &result_cc = ArrayVector::GetEntry(result);

		// If the array vector is constant, the child vector must be flat (or constant if array size is 1)
		D_ASSERT(source_cc.GetVectorType() == VectorType::FLAT_VECTOR || source_array_size == 1);

		CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data, parameters.local_state);
		bool all_ok = cast_data.child_cast_info.function(source_cc, result_cc, source_array_size, child_parameters);
		return all_ok;
	} else {
		// Flatten if not constant
		source.Flatten(count);
		result.SetVectorType(VectorType::FLAT_VECTOR);

		FlatVector::SetValidity(result, FlatVector::Validity(source));
		auto &source_cc = ArrayVector::GetEntry(source);
		auto &result_cc = ArrayVector::GetEntry(result);

		CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data, parameters.local_state);
		bool all_ok =
		    cast_data.child_cast_info.function(source_cc, result_cc, count * source_array_size, child_parameters);
		return all_ok;
	}
}

//------------------------------------------------------------------------------
// ARRAY -> VARCHAR
//------------------------------------------------------------------------------
static bool ArrayToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto is_constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;

	auto size = ArrayType::GetSize(source.GetType());
	Vector varchar_list(LogicalType::ARRAY(LogicalType::VARCHAR, size), count);
	ArrayToArrayCast(source, varchar_list, count, parameters);

	varchar_list.Flatten(count);
	auto &validity = FlatVector::Validity(varchar_list);
	auto &child = ArrayVector::GetEntry(varchar_list);

	child.Flatten(count);
	auto &child_validity = FlatVector::Validity(child);

	auto in_data = FlatVector::GetData<string_t>(child);
	auto out_data = FlatVector::GetData<string_t>(result);

	static constexpr const idx_t SEP_LENGTH = 2;
	static constexpr const idx_t NULL_LENGTH = 4;

	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		// First pass, compute the length
		idx_t array_varchar_length = 2;
		for (idx_t j = 0; j < size; j++) {
			auto elem_idx = (i * size) + j;
			auto elem = in_data[elem_idx];
			if (j > 0) {
				array_varchar_length += SEP_LENGTH;
			}
			array_varchar_length += child_validity.RowIsValid(elem_idx) ? elem.GetSize() : NULL_LENGTH;
		}

		out_data[i] = StringVector::EmptyString(result, array_varchar_length);
		auto dataptr = out_data[i].GetDataWriteable();
		idx_t offset = 0;
		dataptr[offset++] = '[';

		// Second pass, write the actual data
		for (idx_t j = 0; j < size; j++) {
			auto elem_idx = (i * size) + j;
			auto elem = in_data[elem_idx];
			if (j > 0) {
				memcpy(dataptr + offset, ", ", SEP_LENGTH);
				offset += SEP_LENGTH;
			}
			if (child_validity.RowIsValid(elem_idx)) {
				auto len = elem.GetSize();
				memcpy(dataptr + offset, elem.GetData(), len);
				offset += len;
			} else {
				memcpy(dataptr + offset, "NULL", NULL_LENGTH);
				offset += NULL_LENGTH;
			}
		}
		dataptr[offset++] = ']';
		out_data[i].Finalize();
	}

	if (is_constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return true;
}

//------------------------------------------------------------------------------
// ARRAY -> LIST
//------------------------------------------------------------------------------
static bool ArrayToListCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<ArrayBoundCastData>();

	// FIXME: dont flatten
	source.Flatten(count);

	auto array_size = ArrayType::GetSize(source.GetType());
	auto child_count = count * array_size;

	ListVector::Reserve(result, child_count);
	ListVector::SetListSize(result, child_count);

	auto &source_child = ArrayVector::GetEntry(source);
	auto &result_child = ListVector::GetEntry(result);

	CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data, parameters.local_state);
	bool all_ok = cast_data.child_cast_info.function(source_child, result_child, child_count, child_parameters);

	auto list_data = ListVector::GetData(result);
	for (idx_t i = 0; i < count; i++) {
		if (FlatVector::IsNull(source, i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		list_data[i].offset = i * array_size;
		list_data[i].length = array_size;
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return all_ok;
}

BoundCastInfo DefaultCasts::ArrayCastSwitch(BindCastInput &input, const LogicalType &source,
                                            const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::VARCHAR: {
		auto size = ArrayType::GetSize(source);
		return BoundCastInfo(
		    ArrayToVarcharCast,
		    ArrayBoundCastData::BindArrayToArrayCast(input, source, LogicalType::ARRAY(LogicalType::VARCHAR, size)),
		    ArrayBoundCastData::InitArrayLocalState);
	}
	case LogicalTypeId::ARRAY:
		return BoundCastInfo(ArrayToArrayCast, ArrayBoundCastData::BindArrayToArrayCast(input, source, target),
		                     ArrayBoundCastData::InitArrayLocalState);
	case LogicalTypeId::LIST:
		return BoundCastInfo(ArrayToListCast, BindArrayToListCast(input, source, target),
		                     ArrayBoundCastData::InitArrayLocalState);
	default:
		return DefaultCasts::TryVectorNullCast;
	};
}

} // namespace duckdb
