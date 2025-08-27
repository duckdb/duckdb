#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/serializer/varint.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

namespace duckdb {

namespace {

struct FromVariantConversionData {
	//! The input Variant column
	RecursiveUnifiedVectorFormat unified_format;
	//! If unsuccessful - the error of the conversion
	string error;
};

struct EmptyConversionPayloadFromVariant {};

//! string data
struct StringConversionPayload {
public:
	explicit StringConversionPayload(Vector &vec) : vec(vec) {
	}

public:
	//! The string vector that needs to own the non-inlined data
	Vector &vec;
};

//! decimal
struct DecimalConversionPayloadFromVariant {
public:
	DecimalConversionPayloadFromVariant(idx_t width, idx_t scale) : width(width), scale(scale) {
	}

public:
	idx_t width;
	idx_t scale;
};

} // namespace

//===--------------------------------------------------------------------===//
// VARIANT -> ANY
//===--------------------------------------------------------------------===//

static bool FinalizeErrorMessage(FromVariantConversionData &conversion_data, Vector &result,
                                 CastParameters &parameters) {
	auto conversion_error = StringUtil::Format("%s to '%s'", conversion_data.error, result.GetType().ToString());
	if (parameters.error_message) {
		*parameters.error_message = conversion_error;
		return false;
	}
	throw ConversionException(conversion_error);
}

//! ------- Primitive Conversion Methods -------

//! bool
struct VariantBooleanConversion {
	using type = bool;
	static bool Convert(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value, bool &ret,
	                    const EmptyConversionPayloadFromVariant &payload, string &error) {
		if (type_id != VariantLogicalType::BOOL_FALSE && type_id != VariantLogicalType::BOOL_TRUE) {
			error = StringUtil::Format("Can't convert from VARIANT(%s)", EnumUtil::ToString(type_id));
			return false;
		}
		ret = type_id == VariantLogicalType::BOOL_TRUE;
		return true;
	}
};

//! any direct conversion (int8, date_t, dtime_t, timestamp, etc..)
template <class T, VariantLogicalType TYPE_ID>
struct VariantDirectConversion {
	using type = T;
	static bool Convert(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value, T &ret,
	                    const EmptyConversionPayloadFromVariant &payload, string &error) {
		if (type_id != TYPE_ID) {
			error = StringUtil::Format("Can't convert from VARIANT(%s)", EnumUtil::ToString(type_id));
			return false;
		}
		ret = Load<T>(value + byte_offset);
		return true;
	}

	static bool Convert(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value, T &ret,
	                    const StringConversionPayload &payload, string &error) {
		if (type_id != TYPE_ID) {
			error = StringUtil::Format("Can't convert from VARIANT(%s)", EnumUtil::ToString(type_id));
			return false;
		}
		auto ptr = value + byte_offset;
		auto length = VarintDecode<idx_t>(ptr);
		ret = StringVector::AddStringOrBlob(payload.vec, reinterpret_cast<const char *>(ptr), length);
		return true;
	}
};

//! decimal
template <class T>
struct VariantDecimalConversion {
	using type = T;
	static constexpr VariantLogicalType TYPE_ID = VariantLogicalType::DECIMAL;
	static bool Convert(const VariantLogicalType type_id, uint32_t byte_offset, const_data_ptr_t value, T &ret,
	                    const DecimalConversionPayloadFromVariant &payload, string &error) {
		if (type_id != TYPE_ID) {
			error = StringUtil::Format("Can't convert from VARIANT(%s)", EnumUtil::ToString(type_id));
			return false;
		}
		auto ptr = value + byte_offset;
		auto width = VarintDecode<idx_t>(ptr);
		auto scale = VarintDecode<idx_t>(ptr);

		if (width != payload.width || scale != payload.scale) {
			error = StringUtil::Format("Can't convert from VARIANT(DECIMAL(%d, %d))", width, scale);
			return false;
		}
		ret = Load<T>(ptr);
		return true;
	}
};

template <class OP, class T = typename OP::type, class PAYLOAD_CLASS>
static bool CastVariantToPrimitive(FromVariantConversionData &conversion_data, Vector &result,
                                   const SelectionVector &sel, idx_t offset, idx_t count, optional_idx row,
                                   PAYLOAD_CLASS payload) {
	auto &variant = conversion_data.unified_format;

	auto &target_type = result.GetType();

	auto result_data = FlatVector::GetData<T>(result);
	auto &result_validity = FlatVector::Validity(result);
	auto &values_format = UnifiedVariantVector::GetValues(variant);

	auto &type_id_format = UnifiedVariantVector::GetValuesTypeId(variant);
	auto &byte_offset_format = UnifiedVariantVector::GetValuesByteOffset(variant);
	auto &value_format = UnifiedVariantVector::GetData(variant);

	auto type_id_data = type_id_format.GetData<uint8_t>(type_id_format);
	auto byte_offset_data = byte_offset_format.GetData<uint32_t>(byte_offset_format);
	auto value_data = value_format.GetData<string_t>(value_format);

	auto values_data = values_format.GetData<list_entry_t>(values_format);
	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;

		auto index = variant.unified.sel->get_index(row_index);
		if (!variant.unified.validity.RowIsValid(index)) {
			result_validity.SetInvalid(offset + i);
			continue;
		}
		if (!result_validity.RowIsValid(offset + i)) {
			continue;
		}
		auto &values_list_entry = values_data[values_format.sel->get_index(row_index)];
		auto blob_index = value_format.sel->get_index(row_index);

		auto value_index = values_list_entry.offset + sel[i];
		auto type_id_index = type_id_format.sel->get_index(value_index);
		auto byte_offset_index = byte_offset_format.sel->get_index(value_index);

		auto type_id = static_cast<VariantLogicalType>(type_id_data[type_id_index]);
		auto byte_offset = byte_offset_data[byte_offset_index];
		auto value_blob_data = const_data_ptr_cast(value_data[blob_index].GetData());
		bool converted = false;
		if (type_id != VariantLogicalType::OBJECT && type_id != VariantLogicalType::ARRAY) {
			if (OP::Convert(type_id, byte_offset, value_blob_data, result_data[i + offset], payload,
			                conversion_data.error)) {
				converted = true;
			}
		}
		if (!converted) {
			auto value = VariantUtils::ConvertVariantToValue(conversion_data.unified_format, row_index, sel[i]);
			result.SetValue(i + offset, value.DefaultCastAs(target_type, true));
			converted = true;
		}
		if (!converted) {
			return false;
		}
	}
	return true;
}

static bool FindValues(FromVariantConversionData &conversion_data, idx_t row_index, SelectionVector &sel,
                       VariantNestedData &nested_data_entry) {
	auto &source = conversion_data.unified_format;

	//! children
	auto &children = UnifiedVariantVector::GetChildren(source);
	auto children_data = children.GetData<list_entry_t>(children);

	//! value_ids
	auto &value_ids = UnifiedVariantVector::GetChildrenValueId(source);
	auto value_ids_data = value_ids.GetData<uint32_t>(value_ids);

	auto &children_list_entry = children_data[children.sel->get_index(row_index)];
	for (idx_t child_idx = 0; child_idx < nested_data_entry.child_count; child_idx++) {
		auto children_index = children_list_entry.offset + nested_data_entry.children_idx + child_idx;
		auto value_id = value_ids_data[value_ids.sel->get_index(children_index)];
		sel[child_idx] = value_id;
	}
	return true;
}

static bool CastVariant(FromVariantConversionData &conversion_data, Vector &result, const SelectionVector &sel,
                        idx_t offset, idx_t count, optional_idx row);

static bool ConvertVariantToList(FromVariantConversionData &conversion_data, Vector &result, const SelectionVector &sel,
                                 idx_t offset, idx_t count, optional_idx row) {
	auto &allocator = Allocator::DefaultAllocator();

	AllocatedData owned_child_data;
	VariantNestedData *child_data = nullptr;
	if (count) {
		owned_child_data = allocator.Allocate(sizeof(VariantNestedData) * count);
		child_data = reinterpret_cast<VariantNestedData *>(owned_child_data.get());
	}

	if (!VariantUtils::CollectNestedData(conversion_data.unified_format, VariantLogicalType::ARRAY, sel, count, row,
	                                     offset, child_data, FlatVector::Validity(result), conversion_data.error)) {
		return false;
	}
	idx_t total_children = 0;
	idx_t max_children = 0;
	for (idx_t i = 0; i < count; i++) {
		auto &child_data_entry = child_data[i];
		if (child_data_entry.is_null) {
			continue;
		}
		if (child_data_entry.child_count > max_children) {
			max_children = child_data_entry.child_count;
		}
		total_children += child_data_entry.child_count;
	}

	SelectionVector new_sel;
	new_sel.Initialize(max_children);
	idx_t total_offset = 0;
	if (offset) {
		total_offset += ListVector::GetListSize(result);
	}

	ListVector::Reserve(result, total_offset + total_children);
	auto &child = ListVector::GetEntry(result);
	auto list_data = ListVector::GetData(result);
	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;
		auto &child_data_entry = child_data[i];

		if (child_data_entry.is_null) {
			FlatVector::SetNull(result, offset + i, true);
			continue;
		}

		auto &entry = list_data[i + offset];
		entry.offset = total_offset;
		entry.length = child_data_entry.child_count;
		total_offset += entry.length;

		FindValues(conversion_data, row_index, new_sel, child_data_entry);
		if (!CastVariant(conversion_data, child, new_sel, entry.offset, child_data_entry.child_count, row_index)) {
			return false;
		}
	}
	ListVector::SetListSize(result, total_offset);
	return true;
}

static bool ConvertVariantToArray(FromVariantConversionData &conversion_data, Vector &result,
                                  const SelectionVector &sel, idx_t offset, idx_t count, optional_idx row) {
	auto &allocator = Allocator::DefaultAllocator();

	AllocatedData owned_child_data;
	VariantNestedData *child_data = nullptr;
	if (count) {
		owned_child_data = allocator.Allocate(sizeof(VariantNestedData) * count);
		child_data = reinterpret_cast<VariantNestedData *>(owned_child_data.get());
	}

	if (!VariantUtils::CollectNestedData(conversion_data.unified_format, VariantLogicalType::ARRAY, sel, count, row,
	                                     offset, child_data, FlatVector::Validity(result), conversion_data.error)) {
		return false;
	}

	const auto array_size = ArrayType::GetSize(result.GetType());
	for (idx_t i = 0; i < count; i++) {
		auto &child_data_entry = child_data[i];
		if (child_data_entry.is_null) {
			continue;
		}
		if (child_data_entry.child_count != array_size) {
			conversion_data.error =
			    StringUtil::Format("Array size '%d' was expected, found '%d', can't convert VARIANT", array_size,
			                       child_data_entry.child_count);
			return false;
		}
	}

	SelectionVector new_sel;
	new_sel.Initialize(array_size);

	auto &child = ArrayVector::GetEntry(result);
	idx_t total_offset = offset * array_size;
	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;
		auto &child_data_entry = child_data[i];

		if (child_data_entry.is_null) {
			FlatVector::SetNull(result, offset + i, true);
			total_offset += array_size;
			continue;
		}

		FindValues(conversion_data, row_index, new_sel, child_data_entry);
		CastVariant(conversion_data, child, new_sel, total_offset, array_size, row_index);
		total_offset += array_size;
	}
	return true;
}

static bool ConvertVariantToStruct(FromVariantConversionData &conversion_data, Vector &result,
                                   const SelectionVector &sel, idx_t offset, idx_t count, optional_idx row) {
	auto &target_type = result.GetType();
	auto &allocator = Allocator::DefaultAllocator();

	AllocatedData owned_child_data;
	VariantNestedData *child_data = nullptr;
	if (count) {
		owned_child_data = allocator.Allocate(sizeof(VariantNestedData) * count);
		child_data = reinterpret_cast<VariantNestedData *>(owned_child_data.get());
	}

	//! First get all the Object data from the VARIANT
	if (!VariantUtils::CollectNestedData(conversion_data.unified_format, VariantLogicalType::OBJECT, sel, count, row,
	                                     offset, child_data, FlatVector::Validity(result), conversion_data.error)) {
		return false;
	}

	for (idx_t i = 0; i < count; i++) {
		if (child_data[i].is_null) {
			FlatVector::SetNull(result, offset + i, true);
		}
	}

	auto &children = StructVector::GetEntries(result);
	auto &child_types = StructType::GetChildTypes(target_type);

	SelectionVector child_values_sel;
	child_values_sel.Initialize(count);

	for (idx_t child_idx = 0; child_idx < child_types.size(); child_idx++) {
		auto &child_name = child_types[child_idx].first;

		//! Then find the relevant child of the OBJECTs we're converting
		//! FIXME: there is nothing preventing an OBJECT from containing the same key twice I believe ?
		VariantPathComponent component;
		component.key = child_name;
		component.lookup_mode = VariantChildLookupMode::BY_KEY;
		if (!VariantUtils::FindChildValues(conversion_data.unified_format, component, row, child_values_sel, child_data,
		                                   count)) {
			conversion_data.error = StringUtil::Format("VARIANT(OBJECT) is missing key '%s'");
			return false;
		}
		//! Now cast all the values we found to the target type
		auto &child = *children[child_idx];
		if (!CastVariant(conversion_data, child, child_values_sel, offset, count, row)) {
			return false;
		}
	}
	return true;
}

//! * @param conversion_data The constant data relevant at all rows of the conversion
//! * @param result The typed Vector to populate in this call
//! * @param sel The selection of value indices to cast
//! * @param offset The offset into the result where to write the converted values
//! * @param count The amount of values we're converting
//! * @param row The row of the Variant to pull data from, if 'IsValid()' is true
static bool CastVariant(FromVariantConversionData &conversion_data, Vector &result, const SelectionVector &sel,
                        idx_t offset, idx_t count, optional_idx row) {
	auto &target_type = result.GetType();
	auto &error = conversion_data.error;

	if (target_type.IsNested()) {
		switch (target_type.id()) {
		case LogicalTypeId::STRUCT: {
			if (ConvertVariantToStruct(conversion_data, result, sel, offset, count, row)) {
				return true;
			}

			for (idx_t i = 0; i < count; i++) {
				auto row_index = row.IsValid() ? row.GetIndex() : i;

				//! Get the index into 'values'
				uint32_t value_index = sel[i];
				auto value =
				    VariantUtils::ConvertVariantToValue(conversion_data.unified_format, row_index, value_index);
				result.SetValue(i + offset, value.DefaultCastAs(target_type, true));
			}
			return true;
		}
		case LogicalTypeId::ARRAY:
			if (ConvertVariantToArray(conversion_data, result, sel, offset, count, row)) {
				return true;
			}
			for (idx_t i = 0; i < count; i++) {
				auto row_index = row.IsValid() ? row.GetIndex() : i;

				//! Get the index into 'values'
				uint32_t value_index = sel[i];
				auto value =
				    VariantUtils::ConvertVariantToValue(conversion_data.unified_format, row_index, value_index);
				result.SetValue(i + offset, value.DefaultCastAs(target_type, true));
			}
			return true;
		case LogicalTypeId::LIST:
		case LogicalTypeId::MAP: {
			if (ConvertVariantToList(conversion_data, result, sel, offset, count, row)) {
				return true;
			}
			for (idx_t i = 0; i < count; i++) {
				auto row_index = row.IsValid() ? row.GetIndex() : i;

				//! Get the index into 'values'
				uint32_t value_index = sel[i];
				auto value =
				    VariantUtils::ConvertVariantToValue(conversion_data.unified_format, row_index, value_index);
				result.SetValue(i + offset, value.DefaultCastAs(target_type, true));
			}
			return true;
		}
		case LogicalTypeId::UNION: {
			error = "Can't convert VARIANT";
			return false;
		}
		default: {
			error = StringUtil::Format("Nested type: '%s' not handled, can't convert VARIANT", target_type.ToString());
			return false;
		}
		};
	} else {
		EmptyConversionPayloadFromVariant empty_payload;
		switch (target_type.id()) {
		case LogicalTypeId::BOOLEAN:
			return CastVariantToPrimitive<VariantBooleanConversion>(conversion_data, result, sel, offset, count, row,
			                                                        empty_payload);
		case LogicalTypeId::TINYINT:
			return CastVariantToPrimitive<VariantDirectConversion<int8_t, VariantLogicalType::INT8>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::SMALLINT:
			return CastVariantToPrimitive<VariantDirectConversion<int16_t, VariantLogicalType::INT16>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::INTEGER:
			return CastVariantToPrimitive<VariantDirectConversion<int32_t, VariantLogicalType::INT32>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::BIGINT:
			return CastVariantToPrimitive<VariantDirectConversion<int64_t, VariantLogicalType::INT64>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::HUGEINT:
			return CastVariantToPrimitive<VariantDirectConversion<hugeint_t, VariantLogicalType::INT128>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::UTINYINT:
			return CastVariantToPrimitive<VariantDirectConversion<uint8_t, VariantLogicalType::UINT8>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::USMALLINT:
			return CastVariantToPrimitive<VariantDirectConversion<uint16_t, VariantLogicalType::UINT16>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::UINTEGER:
			return CastVariantToPrimitive<VariantDirectConversion<uint32_t, VariantLogicalType::UINT32>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::UBIGINT:
			return CastVariantToPrimitive<VariantDirectConversion<uint64_t, VariantLogicalType::UINT64>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::UHUGEINT:
			return CastVariantToPrimitive<VariantDirectConversion<uhugeint_t, VariantLogicalType::UINT128>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::FLOAT:
			return CastVariantToPrimitive<VariantDirectConversion<float, VariantLogicalType::FLOAT>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::DOUBLE:
			return CastVariantToPrimitive<VariantDirectConversion<double, VariantLogicalType::DOUBLE>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::DATE:
			return CastVariantToPrimitive<VariantDirectConversion<date_t, VariantLogicalType::DATE>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::TIMESTAMP:
			return CastVariantToPrimitive<VariantDirectConversion<timestamp_t, VariantLogicalType::TIMESTAMP_MICROS>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::TIMESTAMP_MS:
			return CastVariantToPrimitive<VariantDirectConversion<timestamp_ms_t, VariantLogicalType::TIMESTAMP_MILIS>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::TIMESTAMP_SEC:
			return CastVariantToPrimitive<VariantDirectConversion<timestamp_sec_t, VariantLogicalType::TIMESTAMP_SEC>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::TIMESTAMP_NS:
			return CastVariantToPrimitive<VariantDirectConversion<timestamp_ns_t, VariantLogicalType::TIMESTAMP_NANOS>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::BLOB: {
			StringConversionPayload string_payload(result);
			return CastVariantToPrimitive<VariantDirectConversion<string_t, VariantLogicalType::BLOB>>(
			    conversion_data, result, sel, offset, count, row, string_payload);
		}
		case LogicalTypeId::VARCHAR: {
			StringConversionPayload string_payload(result);
			return CastVariantToPrimitive<VariantDirectConversion<string_t, VariantLogicalType::VARCHAR>>(
			    conversion_data, result, sel, offset, count, row, string_payload);
		}
		case LogicalTypeId::INTERVAL:
			return CastVariantToPrimitive<VariantDirectConversion<interval_t, VariantLogicalType::INTERVAL>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::DECIMAL: {
			auto physical_type = target_type.InternalType();
			uint8_t width;
			uint8_t scale;
			target_type.GetDecimalProperties(width, scale);
			DecimalConversionPayloadFromVariant decimal_payload(width, scale);

			switch (physical_type) {
			case PhysicalType::INT16:
				return CastVariantToPrimitive<VariantDecimalConversion<int16_t>>(conversion_data, result, sel, offset,
				                                                                 count, row, decimal_payload);
			case PhysicalType::INT32:
				return CastVariantToPrimitive<VariantDecimalConversion<int32_t>>(conversion_data, result, sel, offset,
				                                                                 count, row, decimal_payload);
			case PhysicalType::INT64:
				return CastVariantToPrimitive<VariantDecimalConversion<int64_t>>(conversion_data, result, sel, offset,
				                                                                 count, row, decimal_payload);
			case PhysicalType::INT128:
				return CastVariantToPrimitive<VariantDecimalConversion<hugeint_t>>(conversion_data, result, sel, offset,
				                                                                   count, row, decimal_payload);
			default:
				throw NotImplementedException("Can't convert VARIANT to DECIMAL value of physical type: %s",
				                              EnumUtil::ToString(physical_type));
			};
		}
		case LogicalTypeId::TIME:
			return CastVariantToPrimitive<VariantDirectConversion<dtime_t, VariantLogicalType::TIME_MICROS>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::TIME_NS:
			return CastVariantToPrimitive<VariantDirectConversion<dtime_ns_t, VariantLogicalType::TIME_NANOS>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::TIME_TZ:
			return CastVariantToPrimitive<VariantDirectConversion<dtime_tz_t, VariantLogicalType::TIME_MICROS_TZ>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::TIMESTAMP_TZ:
			return CastVariantToPrimitive<
			    VariantDirectConversion<timestamp_tz_t, VariantLogicalType::TIMESTAMP_MICROS_TZ>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::UUID:
			return CastVariantToPrimitive<VariantDirectConversion<hugeint_t, VariantLogicalType::UUID>>(
			    conversion_data, result, sel, offset, count, row, empty_payload);
		case LogicalTypeId::BIT: {
			StringConversionPayload string_payload(result);
			return CastVariantToPrimitive<VariantDirectConversion<string_t, VariantLogicalType::BITSTRING>>(
			    conversion_data, result, sel, offset, count, row, string_payload);
		}
		case LogicalTypeId::BIGNUM: {
			StringConversionPayload string_payload(result);
			return CastVariantToPrimitive<VariantDirectConversion<string_t, VariantLogicalType::BIGNUM>>(
			    conversion_data, result, sel, offset, count, row, string_payload);
		}
		default:
			error = "Can't convert VARIANT";
			return false;
		};
	}
	return true;
}

static bool CastFromVARIANT(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(source.GetType().id() == LogicalTypeId::VARIANT);
	FromVariantConversionData conversion_data;
	Vector::RecursiveToUnifiedFormat(source, count, conversion_data.unified_format);

	auto success =
	    CastVariant(conversion_data, result, *ConstantVector::ZeroSelectionVector(), 0, count, optional_idx());
	if (source.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	if (!success) {
		return FinalizeErrorMessage(conversion_data, result, parameters);
	}
	return true;
}

BoundCastInfo DefaultCasts::VariantCastSwitch(BindCastInput &input, const LogicalType &source,
                                              const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::VARIANT);
	switch (target.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
	case LogicalTypeId::BIGNUM:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::LIST:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::MAP:
	case LogicalTypeId::UNION:
	case LogicalTypeId::UUID:
	case LogicalTypeId::ARRAY:
		return BoundCastInfo(CastFromVARIANT);
	case LogicalTypeId::VARCHAR: {
		if (target.IsJSONType()) {
			return BoundCastInfo(VariantCasts::CastVARIANTToJSON);
		}
		return BoundCastInfo(CastFromVARIANT);
	}
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
