#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

template <class SRC_TYPE, class RES_TYPE>
bool FillEnum(Vector &source, Vector &result, idx_t count, string *error_message) {
	bool all_converted = true;
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto &str_vec = EnumType::GetValuesInsertOrder(source.GetType());
	auto str_vec_ptr = FlatVector::GetData<string_t>(str_vec);

	auto res_enum_type = result.GetType();

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto source_data = (SRC_TYPE *)vdata.data;
	auto source_sel = vdata.sel;
	auto source_mask = vdata.validity;

	auto result_data = FlatVector::GetData<RES_TYPE>(result);
	auto &result_mask = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto src_idx = source_sel->get_index(i);
		if (!source_mask.RowIsValid(src_idx)) {
			result_mask.SetInvalid(i);
			continue;
		}
		auto str = str_vec_ptr[source_data[src_idx]].GetString();
		auto key = EnumType::GetPos(res_enum_type, str);
		if (key == -1) {
			// key doesn't exist on result enum
			if (!error_message) {
				result_data[i] = HandleVectorCastError::Operation<RES_TYPE>(
				    CastExceptionText<SRC_TYPE, RES_TYPE>(source_data[src_idx]), result_mask, i, error_message,
				    all_converted);
			} else {
				result_mask.SetInvalid(i);
			}
			continue;
		}
		result_data[i] = key;
	}
	return all_converted;
}

template <class SRC_TYPE>
cast_function_t FillEnumResultTemplate(Vector &source, Vector &result, idx_t count, string *error_message) {
	switch (source.GetType().InternalType()) {
	case PhysicalType::UINT8:
		return FillEnum<SRC_TYPE, uint8_t>(source, result, count, error_message);
	case PhysicalType::UINT16:
		return FillEnum<SRC_TYPE, uint16_t>(source, result, count, error_message);
	case PhysicalType::UINT32:
		return FillEnum<SRC_TYPE, uint32_t>(source, result, count, error_message);
	default:
		throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
	}
}

void EnumToVarchar(Vector &source, Vector &result, idx_t count, PhysicalType enum_physical_type) {
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(source.GetVectorType());
	} else {
		result.SetVectorType(VectorType::FLAT_VECTOR);
	}
	auto &str_vec = EnumType::GetValuesInsertOrder(source.GetType());
	auto str_vec_ptr = FlatVector::GetData<string_t>(str_vec);
	auto res_vec_ptr = FlatVector::GetData<string_t>(result);

	// TODO remove value api from this loop
	for (idx_t i = 0; i < count; i++) {
		auto src_val = source.GetValue(i);
		if (src_val.IsNull()) {
			result.SetValue(i, Value());
			continue;
		}

		uint64_t enum_idx;
		switch (enum_physical_type) {
		case PhysicalType::UINT8:
			enum_idx = UTinyIntValue::Get(src_val);
			break;
		case PhysicalType::UINT16:
			enum_idx = USmallIntValue::Get(src_val);
			break;
		case PhysicalType::UINT32:
			enum_idx = UIntegerValue::Get(src_val);
			break;
		case PhysicalType::UINT64: //  DEDUP_POINTER_ENUM
		{
			res_vec_ptr[i] = (const char *)UBigIntValue::Get(src_val);
			continue;
		}

		default:
			throw InternalException("ENUM can only have unsigned integers as physical types");
		}
		res_vec_ptr[i] = str_vec_ptr[enum_idx];
	}
}

static cast_function_t EnumCastSwitch(const LogicalType &source, const LogicalType &target) {
	auto enum_physical_type = source.GetType().InternalType();
	switch (result.GetType().id()) {
	case LogicalTypeId::ENUM: {
		// This means they are both ENUMs, but of different types.
		switch (enum_physical_type) {
		case PhysicalType::UINT8:
			return FillEnumResultTemplate<uint8_t>(source, result, count, error_message);
		case PhysicalType::UINT16:
			return FillEnumResultTemplate<uint16_t>(source, result, count, error_message);
		case PhysicalType::UINT32:
			return FillEnumResultTemplate<uint32_t>(source, result, count, error_message);
		default:
			throw InternalException("ENUM can only have unsigned integers (except UINT64) as physical types");
		}
	}
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR: {
		EnumToVarchar(source, result, count, enum_physical_type);
		break;
	}
	default: {
		// Cast to varchar
		Vector varchar_cast(LogicalType::VARCHAR, count);
		EnumToVarchar(source, varchar_cast, count, enum_physical_type);
		// Try to cast from varchar to whatever we wanted before
		VectorOperations::TryCast(varchar_cast, result, count, error_message, strict);
		break;
	}
	}
	return true;
}

template <class T>
bool FillEnum(string_t *source_data, ValidityMask &source_mask, const LogicalType &source_type, T *result_data,
              ValidityMask &result_mask, const LogicalType &result_type, idx_t count, string *error_message,
              const SelectionVector *sel) {
	bool all_converted = true;
	for (idx_t i = 0; i < count; i++) {
		idx_t source_idx = i;
		if (sel) {
			source_idx = sel->get_index(i);
		}
		if (source_mask.RowIsValid(source_idx)) {
			auto string_value = source_data[source_idx].GetString();
			auto pos = EnumType::GetPos(result_type, string_value);
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
cast_function_t TransformEnum(Vector &source, Vector &result, idx_t count, string *error_message) {
	D_ASSERT(source.GetType().id() == LogicalTypeId::VARCHAR);
	auto enum_name = EnumType::GetTypeName(result.GetType());
	switch (source.GetVectorType()) {
	case VectorType::CONSTANT_VECTOR: {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);

		auto source_data = ConstantVector::GetData<string_t>(source);
		auto source_mask = ConstantVector::Validity(source);
		auto result_data = ConstantVector::GetData<T>(result);
		auto &result_mask = ConstantVector::Validity(result);

		return FillEnum(source_data, source_mask, source.GetType(), result_data, result_mask, result.GetType(), 1,
		                error_message, nullptr);
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

		return FillEnum(source_data, source_mask, source.GetType(), result_data, result_mask, result.GetType(), count,
		                error_message, source_sel);
	}
	}
}

}