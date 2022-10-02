#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

template <class T>
bool StringEnumCastLoop(string_t *source_data, ValidityMask &source_mask, const LogicalType &source_type,
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
	auto enum_name = EnumType::GetTypeName(result.GetType());
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

		auto source_data = (string_t *)vdata.data;
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

bool StringListCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(source.GetType().id() == LogicalTypeId::VARCHAR);
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	auto source_data = FlatVector::GetData<string_t>(source);

	Vector varchar_list(LogicalType::LIST(LogicalType::VARCHAR), count);
	varchar_list.SetVectorType(source.GetVectorType());
	ListVector::Reserve(varchar_list, 10); // TODO hardcoded now, needs to have exact count of parts

    result.SetVectorType(source.GetVectorType());

    auto list_data = ListVector::GetData(varchar_list);
    // list_data contains for each row an offset and length that reference indexes of the child vector

    auto list_data_result = ListVector::GetData(result);
    // list_data contains for each row an offset and length that reference indexes of the child vector

    auto &child = ListVector::GetEntry(varchar_list);
	// Child contains the actual raw values in the varchar_list ListVector
	auto child_data = FlatVector::GetData<string_t>(child);

	//	auto &validity = FlatVector::Validity(result); // TODO

	idx_t total = 0;
	for (idx_t i = 0; i < count; i++) { // loop over source strings
		auto parts = VectorSplitStringifiedList(source_data[i].GetString()).parts;
		//SplitWithStateMachine(source_data[i].GetString(), parts);

		list_data[i].offset = total;        // offset (start of list in child vector) is current total count
		list_data[i].length = parts.size(); // length is the amount of parts coming from this string
		list_data_result[i].offset = total;
		list_data_result[i].length = parts.size();

		idx_t child_start = total;
		for (string part : parts) {
			child_data[child_start] = string_t(part); // TODO convert string to string_t?
			child_start++;
		}
		total += parts.size();
		D_ASSERT(child_start == total);
	}
	ListVector::SetListSize(varchar_list, total); // sets child size
	ListVector::SetListSize(result, total);

	auto &result_child = ListVector::GetEntry(result);

	auto &cast_data = (ListBoundCastData &)*parameters.cast_data;
    CastParameters child_parameters(parameters, cast_data.child_cast_info.cast_data.get());
	return cast_data.child_cast_info.function(child, result_child, total, child_parameters);
}

bool StringToStringList(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(source.GetType().id() == LogicalTypeId::VARCHAR);
	D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

	auto source_data = FlatVector::GetData<string_t>(source);

	auto &child = ListVector::GetEntry(result);

	auto child_data = FlatVector::GetData<string_t>(child);

	auto list_data = ListVector::GetData(result);
	// list_data contains for each row an offset and length that reference indexes of the child vector

	auto &validity = FlatVector::Validity(result); // TODO

	idx_t total = 0;                    // counter for child vector
	for (idx_t i = 0; i < count; i++) { // loop over source strings
//		vector<string> parts;
//		SplitWithStateMachine(source_data[i].GetString(), parts);
        auto parts = VectorSplitStringifiedList(source_data[i].GetString()).parts;
		list_data[i].offset = total;
		list_data[i].length = parts.size();

		idx_t child_start = total;
		for (string part : parts) {
			child_data[child_start] = part;
			child_start++;
		}
		total += parts.size();
		D_ASSERT(child_start == total);
	}
	ListVector::SetListSize(result, total);
	return true;
}

BoundCastInfo StringToListCast(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	if (target == LogicalType::LIST(LogicalType::VARCHAR)) { // TODO look into alternative ways to check this
		return BoundCastInfo(&StringToStringList);
	}

	return BoundCastInfo(&StringListCast,
	                     BindListToListCast(input, LogicalType::LIST(LogicalType::VARCHAR), target));
    // second argument allows for a secondary casting function to be passed in the CastParameters
}

BoundCastInfo DefaultCasts::StringCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	// now switch on the target type
	switch (target.id()) {
	case LogicalTypeId::DATE:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, date_t, duckdb::TryCastErrorMessage>);
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return BoundCastInfo(&VectorCastHelpers::TryCastErrorLoop<string_t, dtime_t, duckdb::TryCastErrorMessage>);
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
	case LogicalTypeId::UUID:
		return BoundCastInfo(&VectorCastHelpers::TryCastStringLoop<string_t, hugeint_t, duckdb::TryCastToUUID>);
	case LogicalTypeId::SQLNULL:
		return &DefaultCasts::TryVectorNullCast;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::JSON:
		return &DefaultCasts::ReinterpretCast;
	case LogicalTypeId::LIST: // my case
		return StringToListCast(input, source, target);
	default:
		return VectorStringCastNumericSwitch(input, source, target);
	}
}

} // namespace duckdb
