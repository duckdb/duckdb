#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "duckdb/function/cast/bound_cast_data.hpp"

namespace duckdb {

template <class T>
bool StringEnumCastLoop(const string_t *source_data, ValidityMask &source_mask, const LogicalType &source_type,
                        T *result_data, ValidityMask &result_mask, const LogicalType &result_type, idx_t count,
                        string *error_message, const SelectionVector *sel) {
	bool all_converted = true;
	for (idx_t i = 0; i < count; i++) {
		idx_t source_idx = i;
		if (sel) {
			source_idx = sel->get_index(i);
		}
		if (source_mask.RowIsValid(source_idx)) {
			auto pos = EnumType::GetPos(result_type, source_data[source_idx]);
			if (pos == -1) {
				result_data[i] =
				    HandleVectorCastError::Operation<T>(CastExceptionText<string_t, T>(source_data[source_idx]),
				                                        result_mask, i, error_message, all_converted);
			} else {
				result_data[i] = pos;
			}
		} else {
			result_mask.SetInvalid(i);
		}
	}
	return all_converted;
}

template <class T>
bool StringEnumCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(source.GetType().id() == LogicalTypeId::VARCHAR);
	switch (source.GetVectorType()) {
	case VectorType::CONSTANT_VECTOR: {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		auto source_data = ConstantVector::GetData<string_t>(source);
		auto source_mask = ConstantVector::Validity(source);
		auto result_data = ConstantVector::GetData<T>(result);
		auto &result_mask = ConstantVector::Validity(result);

		return StringEnumCastLoop(source_data, source_mask, source.GetType(), result_data, result_mask,
		                          result.GetType(), 1, parameters.error_message, nullptr);
	}
	default: {
		UnifiedVectorFormat vdata;
		source.ToUnifiedFormat(count, vdata);

		result.SetVectorType(VectorType::FLAT_VECTOR);

		auto source_data = UnifiedVectorFormat::GetData<string_t>(vdata);
		auto source_sel = vdata.sel;
		auto source_mask = vdata.validity;
		auto result_data = FlatVector::GetData<T>(result);
		auto &result_mask = FlatVector::Validity(result);

		return StringEnumCastLoop(source_data, source_mask, source.GetType(), result_data, result_mask,
		                          result.GetType(), count, parameters.error_message, source_sel);
	}
	}
}

static BoundCastInfo VectorStringCastNumericSwitch(BindCastInput &input, const LogicalType &source,
                                                   const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::ENUM: {
		switch (target.InternalType()) {
		case PhysicalType::UINT8:
			return StringEnumCast<uint8_t>;
		case PhysicalType::UINT16:
			return StringEnumCast<uint16_t>;
		case PhysicalType::UINT32:
			return StringEnumCast<uint32_t>;
		default:
			throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
		}
	}
	case LogicalTypeId::BOOLEAN:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, bool, duckdb::TryCast>);
	case LogicalTypeId::TINYINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, int8_t, duckdb::TryCast>);
	case LogicalTypeId::SMALLINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, int16_t, duckdb::TryCast>);
	case LogicalTypeId::INTEGER:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, int32_t, duckdb::TryCast>);
	case LogicalTypeId::BIGINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, int64_t, duckdb::TryCast>);
	case LogicalTypeId::UTINYINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, uint8_t, duckdb::TryCast>);
	case LogicalTypeId::USMALLINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, uint16_t, duckdb::TryCast>);
	case LogicalTypeId::UINTEGER:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, uint32_t, duckdb::TryCast>);
	case LogicalTypeId::UBIGINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, uint64_t, duckdb::TryCast>);
	case LogicalTypeId::HUGEINT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, hugeint_t, duckdb::TryCast>);
	case LogicalTypeId::FLOAT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, float, duckdb::TryCast>);
	case LogicalTypeId::DOUBLE:
		return BoundCastInfo(&VectorCastHelpers::TryCastStrictLoop<string_t, double, duckdb::TryCast>);
	case LogicalTypeId::INTERVAL:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, interval_t, duckdb::TryCastErrorMessage>);
	case LogicalTypeId::DECIMAL:
		return BoundCastInfo(&VectorCastHelpers::ToDecimalCast<string_t>);
	default:
		return DefaultCasts::TryVectorNullCast;
	}
}

//===--------------------------------------------------------------------===//
// string -> list casting
//===--------------------------------------------------------------------===//
bool VectorStringToList::StringToNestedTypeCastLoop(const string_t *source_data, ValidityMask &source_mask,
                                                    Vector &result, ValidityMask &result_mask, idx_t count,
                                                    CastParameters &parameters, const SelectionVector *sel) {
	idx_t total_list_size = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = i;
		if (sel) {
			idx = sel->get_index(i);
		}
		if (!source_mask.RowIsValid(idx)) {
			continue;
		}
		total_list_size += VectorStringToList::CountPartsList(source_data[idx]);
	}

	Vector varchar_vector(LogicalType::VARCHAR, total_list_size);

	ListVector::Reserve(result, total_list_size);
	ListVector::SetListSize(result, total_list_size);

	auto list_data = ListVector::GetData(result);
	auto child_data = FlatVector::GetData<string_t>(varchar_vector);

	bool all_converted = true;
	idx_t total = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = i;
		if (sel) {
			idx = sel->get_index(i);
		}
		if (!source_mask.RowIsValid(idx)) {
			result_mask.SetInvalid(i);
			continue;
		}

		list_data[i].offset = total;
		if (!VectorStringToList::SplitStringList(source_data[idx], child_data, total, varchar_vector)) {
			string text = "Type VARCHAR with value '" + source_data[idx].GetString() +
			              "' can't be cast to the destination type LIST";
			HandleVectorCastError::Operation<string_t>(text, result_mask, idx, parameters.error_message, all_converted);
		}
		list_data[i].length = total - list_data[i].offset; // length is the amount of parts coming from this string
	}
	D_ASSERT(total_list_size == total);

	auto &result_child = ListVector::GetEntry(result);
	auto &cast_data = parameters.cast_data->Cast<ListBoundCastData>();
	CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data, parameters.local_state);
	return cast_data.child_cast_info.function(varchar_vector, result_child, total_list_size, child_parameters) &&
	       all_converted;
}

static LogicalType InitVarcharStructType(const LogicalType &target) {
	child_list_t<LogicalType> child_types;
	for (auto &child : StructType::GetChildTypes(target)) {
		child_types.push_back(make_pair(child.first, LogicalType::VARCHAR));
	}

	return LogicalType::STRUCT(child_types);
}

//===--------------------------------------------------------------------===//
// string -> struct casting
//===--------------------------------------------------------------------===//
bool VectorStringToStruct::StringToNestedTypeCastLoop(const string_t *source_data, ValidityMask &source_mask,
                                                      Vector &result, ValidityMask &result_mask, idx_t count,
                                                      CastParameters &parameters, const SelectionVector *sel) {
	auto varchar_struct_type = InitVarcharStructType(result.GetType());
	Vector varchar_vector(varchar_struct_type, count);
	auto &child_vectors = StructVector::GetEntries(varchar_vector);
	auto &result_children = StructVector::GetEntries(result);

	string_map_t<idx_t> child_names;
	vector<ValidityMask *> child_masks;
	for (idx_t child_idx = 0; child_idx < result_children.size(); child_idx++) {
		child_names.insert({StructType::GetChildName(result.GetType(), child_idx), child_idx});
		child_masks.emplace_back(&FlatVector::Validity(*child_vectors[child_idx]));
		child_masks[child_idx]->SetAllInvalid(count);
	}

	bool all_converted = true;
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = i;
		if (sel) {
			idx = sel->get_index(i);
		}
		if (!source_mask.RowIsValid(idx)) {
			result_mask.SetInvalid(i);
			continue;
		}
		if (!VectorStringToStruct::SplitStruct(source_data[idx], child_vectors, i, child_names, child_masks)) {
			string text = "Type VARCHAR with value '" + source_data[idx].GetString() +
			              "' can't be cast to the destination type STRUCT";
			for (auto &child_mask : child_masks) {
				child_mask->SetInvalid(idx); // some values may have already been found and set valid
			}
			HandleVectorCastError::Operation<string_t>(text, result_mask, idx, parameters.error_message, all_converted);
		}
	}

	auto &cast_data = parameters.cast_data->Cast<StructBoundCastData>();
	auto &lstate = parameters.local_state->Cast<StructCastLocalState>();
	D_ASSERT(cast_data.child_cast_info.size() == result_children.size());

	for (idx_t child_idx = 0; child_idx < result_children.size(); child_idx++) {
		auto &child_varchar_vector = *child_vectors[child_idx];
		auto &result_child_vector = *result_children[child_idx];
		auto &child_cast_info = cast_data.child_cast_info[child_idx];
		CastParameters child_parameters(parameters, child_cast_info.cast_data, lstate.local_states[child_idx]);
		if (!child_cast_info.function(child_varchar_vector, result_child_vector, count, child_parameters)) {
			all_converted = false;
		}
	}
	return all_converted;
}

//===--------------------------------------------------------------------===//
// string -> map casting
//===--------------------------------------------------------------------===//
unique_ptr<FunctionLocalState> InitMapCastLocalState(CastLocalStateParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<MapBoundCastData>();
	auto result = make_uniq<MapCastLocalState>();

	if (cast_data.key_cast.init_local_state) {
		CastLocalStateParameters child_params(parameters, cast_data.key_cast.cast_data);
		result->key_state = cast_data.key_cast.init_local_state(child_params);
	}
	if (cast_data.value_cast.init_local_state) {
		CastLocalStateParameters child_params(parameters, cast_data.value_cast.cast_data);
		result->value_state = cast_data.value_cast.init_local_state(child_params);
	}
	return std::move(result);
}

bool VectorStringToMap::StringToNestedTypeCastLoop(const string_t *source_data, ValidityMask &source_mask,
                                                   Vector &result, ValidityMask &result_mask, idx_t count,
                                                   CastParameters &parameters, const SelectionVector *sel) {
	idx_t total_elements = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = i;
		if (sel) {
			idx = sel->get_index(i);
		}
		if (!source_mask.RowIsValid(idx)) {
			continue;
		}
		total_elements += (VectorStringToMap::CountPartsMap(source_data[idx]) + 1) / 2;
	}

	Vector varchar_key_vector(LogicalType::VARCHAR, total_elements);
	Vector varchar_val_vector(LogicalType::VARCHAR, total_elements);
	auto child_key_data = FlatVector::GetData<string_t>(varchar_key_vector);
	auto child_val_data = FlatVector::GetData<string_t>(varchar_val_vector);

	ListVector::Reserve(result, total_elements);
	ListVector::SetListSize(result, total_elements);
	auto list_data = ListVector::GetData(result);

	bool all_converted = true;
	idx_t total = 0;
	for (idx_t i = 0; i < count; i++) {
		idx_t idx = i;
		if (sel) {
			idx = sel->get_index(i);
		}
		if (!source_mask.RowIsValid(idx)) {
			result_mask.SetInvalid(idx);
			continue;
		}

		list_data[i].offset = total;
		if (!VectorStringToMap::SplitStringMap(source_data[idx], child_key_data, child_val_data, total,
		                                       varchar_key_vector, varchar_val_vector)) {
			string text = "Type VARCHAR with value '" + source_data[idx].GetString() +
			              "' can't be cast to the destination type MAP";
			FlatVector::SetNull(result, idx, true);
			HandleVectorCastError::Operation<string_t>(text, result_mask, idx, parameters.error_message, all_converted);
		}
		list_data[i].length = total - list_data[i].offset;
	}
	D_ASSERT(total_elements == total);

	auto &result_key_child = MapVector::GetKeys(result);
	auto &result_val_child = MapVector::GetValues(result);
	auto &cast_data = parameters.cast_data->Cast<MapBoundCastData>();
	auto &lstate = parameters.local_state->Cast<MapCastLocalState>();

	CastParameters key_params(parameters, cast_data.key_cast.cast_data, lstate.key_state);
	if (!cast_data.key_cast.function(varchar_key_vector, result_key_child, total_elements, key_params)) {
		all_converted = false;
	}
	CastParameters val_params(parameters, cast_data.value_cast.cast_data, lstate.value_state);
	if (!cast_data.value_cast.function(varchar_val_vector, result_val_child, total_elements, val_params)) {
		all_converted = false;
	}

	auto &key_validity = FlatVector::Validity(result_key_child);
	if (!all_converted) {
		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			if (!result_mask.RowIsValid(row_idx)) {
				continue;
			}
			auto list = list_data[row_idx];
			for (idx_t list_idx = 0; list_idx < list.length; list_idx++) {
				auto idx = list.offset + list_idx;
				if (!key_validity.RowIsValid(idx)) {
					result_mask.SetInvalid(row_idx);
				}
			}
		}
	}
	MapVector::MapConversionVerify(result, count);
	return all_converted;
}

template <class T>
bool StringToNestedTypeCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(source.GetType().id() == LogicalTypeId::VARCHAR);

	switch (source.GetVectorType()) {
	case VectorType::CONSTANT_VECTOR: {
		auto source_data = ConstantVector::GetData<string_t>(source);
		auto &source_mask = ConstantVector::Validity(source);
		auto &result_mask = FlatVector::Validity(result);
		auto ret = T::StringToNestedTypeCastLoop(source_data, source_mask, result, result_mask, 1, parameters, nullptr);
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		return ret;
	}
	default: {
		UnifiedVectorFormat unified_source;

		source.ToUnifiedFormat(count, unified_source);
		auto source_sel = unified_source.sel;
		auto source_data = UnifiedVectorFormat::GetData<string_t>(unified_source);
		auto &source_mask = unified_source.validity;
		auto &result_mask = FlatVector::Validity(result);

		return T::StringToNestedTypeCastLoop(source_data, source_mask, result, result_mask, count, parameters,
		                                     source_sel);
	}
	}
}

BoundCastInfo DefaultCasts::StringCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	switch (target.id()) {
	case LogicalTypeId::DATE:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, date_t, duckdb::TryCastErrorMessage>);
	case LogicalTypeId::TIME:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, dtime_t, duckdb::TryCastErrorMessage>);
	case LogicalTypeId::TIME_TZ:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, dtime_tz_t, duckdb::TryCastErrorMessage>);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, timestamp_t, duckdb::TryCastErrorMessage>);
	case LogicalTypeId::TIMESTAMP_NS:
		return BoundCastInfo(
		    &VectorCastHelpers::TryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampNS>);
	case LogicalTypeId::TIMESTAMP_SEC:
		return BoundCastInfo(
		    &VectorCastHelpers::TryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampSec>);
	case LogicalTypeId::TIMESTAMP_MS:
		return BoundCastInfo(
		    &VectorCastHelpers::TryCastStrictLoop<string_t, timestamp_t, duckdb::TryCastToTimestampMS>);
	case LogicalTypeId::BLOB:
		return BoundCastInfo(&VectorCastHelpers::TryCastStringLoop<string_t, string_t, duckdb::TryCastToBlob>);
	case LogicalTypeId::BIT:
		return BoundCastInfo(&VectorCastHelpers::TryCastStringLoop<string_t, string_t, duckdb::TryCastToBit>);
	case LogicalTypeId::UUID:
		return BoundCastInfo(&VectorCastHelpers::TryCastStringLoop<string_t, hugeint_t, duckdb::TryCastToUUID>);
	case LogicalTypeId::SQLNULL:
		return &DefaultCasts::TryVectorNullCast;
	case LogicalTypeId::VARCHAR:
		return &DefaultCasts::ReinterpretCast;
	case LogicalTypeId::LIST:
		// the second argument allows for a secondary casting function to be passed in the CastParameters
		return BoundCastInfo(
		    &StringToNestedTypeCast<VectorStringToList>,
		    ListBoundCastData::BindListToListCast(input, LogicalType::LIST(LogicalType::VARCHAR), target),
		    ListBoundCastData::InitListLocalState);
	case LogicalTypeId::STRUCT:
		return BoundCastInfo(&StringToNestedTypeCast<VectorStringToStruct>,
		                     StructBoundCastData::BindStructToStructCast(input, InitVarcharStructType(target), target),
		                     StructBoundCastData::InitStructCastLocalState);
	case LogicalTypeId::MAP:
		return BoundCastInfo(&StringToNestedTypeCast<VectorStringToMap>,
		                     MapBoundCastData::BindMapToMapCast(
		                         input, LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR), target),
		                     InitMapCastLocalState);
	default:
		return VectorStringCastNumericSwitch(input, source, target);
	}
}

} // namespace duckdb
