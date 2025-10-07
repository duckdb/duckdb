#include "yyjson_utils.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "yyjson.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

namespace {

struct FromVariantConversionData {
public:
	explicit FromVariantConversionData(RecursiveUnifiedVectorFormat &variant_format) : variant(variant_format) {
	}

public:
	//! The input Variant column
	UnifiedVariantVectorData variant;
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
	auto &variant = conversion_data.variant;

	auto &target_type = result.GetType();
	auto result_data = FlatVector::GetData<T>(result);
	auto &result_validity = FlatVector::Validity(result);

	bool all_valid = true;
	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;

		if (!variant.RowIsValid(row_index)) {
			result_validity.SetInvalid(offset + i);
			continue;
		}
		if (!result_validity.RowIsValid(offset + i)) {
			continue;
		}

		auto type_id = variant.GetTypeId(row_index, sel[i]);
		auto byte_offset = variant.GetByteOffset(row_index, sel[i]);
		auto value_blob_data = const_data_ptr_cast(variant.GetData(row_index).GetData());
		bool converted = false;
		if (type_id != VariantLogicalType::OBJECT && type_id != VariantLogicalType::ARRAY) {
			if (OP::Convert(type_id, byte_offset, value_blob_data, result_data[i + offset], payload,
			                conversion_data.error)) {
				converted = true;
			}
		}
		if (!converted) {
			auto value = VariantUtils::ConvertVariantToValue(conversion_data.variant, row_index, sel[i]);
			if (!value.DefaultTryCastAs(target_type, true)) {
				conversion_data.error = StringUtil::Format("Can't convert VARIANT(%s) value '%s'",
				                                           EnumUtil::ToString(type_id), value.ToString());
				value = Value(target_type);
				all_valid = false;
			}
			result.SetValue(i + offset, value);
			converted = true;
		}
	}
	return all_valid;
}

static bool FindValues(UnifiedVariantVectorData &variant, idx_t row_index, SelectionVector &sel,
                       VariantNestedData &nested_data_entry) {
	for (idx_t child_idx = 0; child_idx < nested_data_entry.child_count; child_idx++) {
		auto value_id = variant.GetValuesIndex(row_index, nested_data_entry.children_idx + child_idx);
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

	auto collection_result =
	    VariantUtils::CollectNestedData(conversion_data.variant, VariantLogicalType::ARRAY, sel, count, row, offset,
	                                    child_data, FlatVector::Validity(result));
	if (!collection_result.success) {
		conversion_data.error =
		    StringUtil::Format("Expected to find VARIANT(ARRAY), found VARIANT(%s) instead, can't convert",
		                       EnumUtil::ToString(collection_result.wrong_type));
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

		FindValues(conversion_data.variant, row_index, new_sel, child_data_entry);
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

	auto collection_result =
	    VariantUtils::CollectNestedData(conversion_data.variant, VariantLogicalType::ARRAY, sel, count, row, offset,
	                                    child_data, FlatVector::Validity(result));
	if (!collection_result.success) {
		conversion_data.error =
		    StringUtil::Format("Expected to find VARIANT(ARRAY), found VARIANT(%s) instead, can't convert",
		                       EnumUtil::ToString(collection_result.wrong_type));
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

		FindValues(conversion_data.variant, row_index, new_sel, child_data_entry);
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

	auto collection_result =
	    VariantUtils::CollectNestedData(conversion_data.variant, VariantLogicalType::OBJECT, sel, count, row, offset,
	                                    child_data, FlatVector::Validity(result));
	if (!collection_result.success) {
		conversion_data.error =
		    StringUtil::Format("Expected to find VARIANT(OBJECT), found VARIANT(%s) instead, can't convert",
		                       EnumUtil::ToString(collection_result.wrong_type));
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

	SelectionVector row_sel(0, count);
	if (row.IsValid()) {
		auto row_index = row.GetIndex();
		for (idx_t i = 0; i < count; i++) {
			row_sel[i] = static_cast<uint32_t>(row_index);
		}
	}

	for (idx_t child_idx = 0; child_idx < child_types.size(); child_idx++) {
		auto &child_name = child_types[child_idx].first;

		//! Then find the relevant child of the OBJECTs we're converting
		//! FIXME: there is nothing preventing an OBJECT from containing the same key twice I believe ?
		VariantPathComponent component;
		component.key = child_name;
		component.lookup_mode = VariantChildLookupMode::BY_KEY;
		ValidityMask lookup_validity(count);
		VariantUtils::FindChildValues(conversion_data.variant, component, row_sel, child_values_sel, lookup_validity,
		                              child_data, count);
		if (!lookup_validity.AllValid()) {
			optional_idx nested_index;
			for (idx_t i = 0; i < count; i++) {
				if (!lookup_validity.RowIsValid(i)) {
					nested_index = i;
					break;
				}
			}
			D_ASSERT(nested_index.IsValid());
			auto row_index = row.IsValid() ? row.GetIndex() : nested_index.GetIndex();
			auto object_keys =
			    VariantUtils::GetObjectKeys(conversion_data.variant, row_index, child_data[nested_index.GetIndex()]);
			conversion_data.error = StringUtil::Format("VARIANT(OBJECT(%s)) is missing key '%s'",
			                                           StringUtil::Join(object_keys, ","), component.key);
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

static bool CastVariantToJSON(FromVariantConversionData &conversion_data, Vector &result, const SelectionVector &sel,
                              idx_t offset, idx_t count, optional_idx row) {
	auto &error = conversion_data.error;

	ConvertedJSONHolder json_holder;

	auto result_data = FlatVector::GetData<string_t>(result);
	json_holder.doc = yyjson_mut_doc_new(nullptr);
	for (idx_t i = 0; i < count; i++) {
		auto row_index = row.IsValid() ? row.GetIndex() : i;

		auto json_val =
		    VariantCasts::ConvertVariantToJSON(json_holder.doc, conversion_data.variant.variant, row_index, sel[i]);
		if (!json_val) {
			error = StringUtil::Format("Failed to convert to JSON object");
			return false;
		}

		size_t len;
		json_holder.stringified_json =
		    yyjson_mut_val_write_opts(json_val, YYJSON_WRITE_ALLOW_INF_AND_NAN, nullptr, &len, nullptr);
		if (!json_holder.stringified_json) {
			error = "Could not serialize the JSON to string, yyjson failed";
			return false;
		}
		string_t res(json_holder.stringified_json, NumericCast<uint32_t>(len));
		result_data[offset + i] = StringVector::AddString(result, res);
		free(json_holder.stringified_json);
		json_holder.stringified_json = nullptr;
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
			break;
		}
		case LogicalTypeId::ARRAY:
			if (ConvertVariantToArray(conversion_data, result, sel, offset, count, row)) {
				return true;
			}
			break;
		case LogicalTypeId::LIST:
		case LogicalTypeId::MAP: {
			if (ConvertVariantToList(conversion_data, result, sel, offset, count, row)) {
				return true;
			}
			break;
		}
		case LogicalTypeId::UNION: {
			error = "Can't convert VARIANT";
			for (idx_t i = 0; i < count; i++) {
				FlatVector::SetNull(result, offset + i, true);
			}
			return false;
		}
		default: {
			error = StringUtil::Format("Nested type: '%s' not handled, can't convert VARIANT", target_type.ToString());
			for (idx_t i = 0; i < count; i++) {
				FlatVector::SetNull(result, offset + i, true);
			}
			return false;
		}
		};

		bool all_valid = true;
		for (idx_t i = 0; i < count; i++) {
			auto row_index = row.IsValid() ? row.GetIndex() : i;

			//! Get the index into 'values'
			uint32_t value_index = sel[i];
			auto value = VariantUtils::ConvertVariantToValue(conversion_data.variant, row_index, value_index);
			if (!value.DefaultTryCastAs(target_type, true)) {
				value = Value(target_type);
				all_valid = false;
			}
			result.SetValue(i + offset, value);
		}
		return all_valid;
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
		case LogicalTypeId::GEOMETRY: {
			StringConversionPayload string_payload(result);
			return CastVariantToPrimitive<VariantDirectConversion<string_t, VariantLogicalType::GEOMETRY>>(
			    conversion_data, result, sel, offset, count, row, string_payload);
		}
		case LogicalTypeId::VARCHAR: {
			if (target_type.IsJSONType()) {
				return CastVariantToJSON(conversion_data, result, sel, offset, count, row);
			}
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
			for (idx_t i = 0; i < count; i++) {
				FlatVector::SetNull(result, offset + i, true);
			}
			return false;
		};
	}
}

static bool CastFromVARIANT(Vector &variant_vec, Vector &result, idx_t count, CastParameters &parameters) {
	D_ASSERT(variant_vec.GetType().id() == LogicalTypeId::VARIANT);
	RecursiveUnifiedVectorFormat variant_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, count, variant_format);
	FromVariantConversionData conversion_data(variant_format);

	reference<const SelectionVector> sel(*ConstantVector::ZeroSelectionVector());
	SelectionVector zero_sel;
	if (count >= STANDARD_VECTOR_SIZE) {
		zero_sel.Initialize(count);
		for (idx_t i = 0; i < count; i++) {
			zero_sel[i] = 0;
		}
		sel = zero_sel;
	}

	auto success = CastVariant(conversion_data, result, sel, 0, count, optional_idx());
	if (variant_vec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
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
	case LogicalTypeId::GEOMETRY:
		return BoundCastInfo(CastFromVARIANT);
	case LogicalTypeId::VARCHAR: {
		return BoundCastInfo(CastFromVARIANT);
	}
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
