#include "reader/variant/variant_shredded_conversion.hpp"
#include "column_reader.hpp"
#include "utf8proc_wrapper.hpp"

#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/blob.hpp"

namespace duckdb {

template <class T>
struct ConvertShreddedValue {
	static VariantValue Convert(T val);
	static VariantValue ConvertDecimal(T val, uint8_t width, uint8_t scale) {
		throw InternalException("ConvertShreddedValue::ConvertDecimal not implemented for type");
	}
	static VariantValue ConvertBlob(T val) {
		throw InternalException("ConvertShreddedValue::ConvertBlob not implemented for type");
	}
};

//! boolean
template <>
VariantValue ConvertShreddedValue<bool>::Convert(bool val) {
	return VariantValue(Value::BOOLEAN(val));
}
//! int8
template <>
VariantValue ConvertShreddedValue<int8_t>::Convert(int8_t val) {
	return VariantValue(Value::TINYINT(val));
}
//! int16
template <>
VariantValue ConvertShreddedValue<int16_t>::Convert(int16_t val) {
	return VariantValue(Value::SMALLINT(val));
}
//! int32
template <>
VariantValue ConvertShreddedValue<int32_t>::Convert(int32_t val) {
	return VariantValue(Value::INTEGER(val));
}
//! int64
template <>
VariantValue ConvertShreddedValue<int64_t>::Convert(int64_t val) {
	return VariantValue(Value::BIGINT(val));
}
//! float
template <>
VariantValue ConvertShreddedValue<float>::Convert(float val) {
	return VariantValue(Value::FLOAT(val));
}
//! double
template <>
VariantValue ConvertShreddedValue<double>::Convert(double val) {
	return VariantValue(Value::DOUBLE(val));
}
//! decimal4/decimal8/decimal16
template <>
VariantValue ConvertShreddedValue<int32_t>::ConvertDecimal(int32_t val, uint8_t width, uint8_t scale) {
	auto value_str = Decimal::ToString(val, width, scale);
	return VariantValue(Value(value_str));
}
template <>
VariantValue ConvertShreddedValue<int64_t>::ConvertDecimal(int64_t val, uint8_t width, uint8_t scale) {
	auto value_str = Decimal::ToString(val, width, scale);
	return VariantValue(Value(value_str));
}
template <>
VariantValue ConvertShreddedValue<hugeint_t>::ConvertDecimal(hugeint_t val, uint8_t width, uint8_t scale) {
	auto value_str = Decimal::ToString(val, width, scale);
	return VariantValue(Value(value_str));
}
//! date
template <>
VariantValue ConvertShreddedValue<date_t>::Convert(date_t val) {
	return VariantValue(Value::DATE(val));
}
//! time
template <>
VariantValue ConvertShreddedValue<dtime_t>::Convert(dtime_t val) {
	return VariantValue(Value::TIME(val));
}
//! timestamptz(6)
template <>
VariantValue ConvertShreddedValue<timestamp_tz_t>::Convert(timestamp_tz_t val) {
	return VariantValue(Value::TIMESTAMPTZ(val));
}
////! timestamptz(9)
// template <>
// VariantValue ConvertShreddedValue<timestamp_ns_tz_t>::Convert(timestamp_ns_tz_t val) {
//	return VariantValue(Value::TIMESTAMPNS_TZ(val));
//}
//! timestampntz(6)
template <>
VariantValue ConvertShreddedValue<timestamp_t>::Convert(timestamp_t val) {
	return VariantValue(Value::TIMESTAMP(val));
}
//! timestampntz(9)
template <>
VariantValue ConvertShreddedValue<timestamp_ns_t>::Convert(timestamp_ns_t val) {
	return VariantValue(Value::TIMESTAMPNS(val));
}
//! binary
template <>
VariantValue ConvertShreddedValue<string_t>::ConvertBlob(string_t val) {
	return VariantValue(Value(Blob::ToBase64(val)));
}
//! string
template <>
VariantValue ConvertShreddedValue<string_t>::Convert(string_t val) {
	if (!Utf8Proc::IsValid(val.GetData(), val.GetSize())) {
		throw InternalException("Can't decode Variant string, it isn't valid UTF8");
	}
	return VariantValue(Value(val.GetString()));
}
//! uuid
template <>
VariantValue ConvertShreddedValue<hugeint_t>::Convert(hugeint_t val) {
	return VariantValue(Value(UUID::ToString(val)));
}

template <class T, class OP, LogicalTypeId TYPE_ID>
vector<VariantValue> ConvertTypedValues(Vector &vec, Vector &metadata, Vector &blob, idx_t offset, idx_t length,
                                        idx_t total_size, const bool is_field) {
	UnifiedVectorFormat metadata_format;
	metadata.ToUnifiedFormat(length, metadata_format);
	auto metadata_data = metadata_format.GetData<string_t>(metadata_format);

	UnifiedVectorFormat typed_format;
	vec.ToUnifiedFormat(total_size, typed_format);
	auto data = typed_format.GetData<T>(typed_format);

	UnifiedVectorFormat value_format;
	blob.ToUnifiedFormat(total_size, value_format);
	auto value_data = value_format.GetData<string_t>(value_format);

	auto &validity = typed_format.validity;
	auto &value_validity = value_format.validity;
	auto &type = vec.GetType();

	//! Values only used for Decimal conversion
	uint8_t width;
	uint8_t scale;
	if (TYPE_ID == LogicalTypeId::DECIMAL) {
		type.GetDecimalProperties(width, scale);
	}

	vector<VariantValue> ret(length);
	if (validity.AllValid()) {
		for (idx_t i = 0; i < length; i++) {
			auto index = typed_format.sel->get_index(i + offset);
			if (TYPE_ID == LogicalTypeId::DECIMAL) {
				ret[i] = OP::ConvertDecimal(data[index], width, scale);
			} else if (TYPE_ID == LogicalTypeId::BLOB) {
				ret[i] = OP::ConvertBlob(data[index]);
			} else {
				ret[i] = OP::Convert(data[index]);
			}
		}
	} else {
		for (idx_t i = 0; i < length; i++) {
			auto typed_index = typed_format.sel->get_index(i + offset);
			auto value_index = value_format.sel->get_index(i + offset);
			if (validity.RowIsValid(typed_index)) {
				//! This is a leaf, partially shredded values aren't possible here
				D_ASSERT(!value_validity.RowIsValid(value_index));
				if (TYPE_ID == LogicalTypeId::DECIMAL) {
					ret[i] = OP::ConvertDecimal(data[typed_index], width, scale);
				} else if (TYPE_ID == LogicalTypeId::BLOB) {
					ret[i] = OP::ConvertBlob(data[typed_index]);
				} else {
					ret[i] = OP::Convert(data[typed_index]);
				}
			} else {
				if (is_field && !value_validity.RowIsValid(value_index)) {
					//! Value is missing for this field
					continue;
				}
				D_ASSERT(value_validity.RowIsValid(value_index));
				auto metadata_value = metadata_data[metadata_format.sel->get_index(i)];
				VariantMetadata variant_metadata(metadata_value);
				ret[i] = VariantBinaryDecoder::Decode(variant_metadata,
				                                      const_data_ptr_cast(value_data[value_index].GetData()));
			}
		}
	}
	return ret;
}

vector<VariantValue> VariantShreddedConversion::ConvertShreddedLeaf(Vector &metadata, Vector &value,
                                                                    Vector &typed_value, idx_t offset, idx_t length,
                                                                    idx_t total_size, const bool is_field) {
	D_ASSERT(!typed_value.GetType().IsNested());
	vector<VariantValue> result;

	auto &type = typed_value.GetType();
	switch (type.id()) {
	//! boolean
	case LogicalTypeId::BOOLEAN: {
		return ConvertTypedValues<bool, ConvertShreddedValue<bool>, LogicalTypeId::BOOLEAN>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! int8
	case LogicalTypeId::TINYINT: {
		return ConvertTypedValues<int8_t, ConvertShreddedValue<int8_t>, LogicalTypeId::TINYINT>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! int16
	case LogicalTypeId::SMALLINT: {
		return ConvertTypedValues<int16_t, ConvertShreddedValue<int16_t>, LogicalTypeId::SMALLINT>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! int32
	case LogicalTypeId::INTEGER: {
		return ConvertTypedValues<int32_t, ConvertShreddedValue<int32_t>, LogicalTypeId::INTEGER>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! int64
	case LogicalTypeId::BIGINT: {
		return ConvertTypedValues<int64_t, ConvertShreddedValue<int64_t>, LogicalTypeId::BIGINT>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! float
	case LogicalTypeId::FLOAT: {
		return ConvertTypedValues<float, ConvertShreddedValue<float>, LogicalTypeId::FLOAT>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! double
	case LogicalTypeId::DOUBLE: {
		return ConvertTypedValues<double, ConvertShreddedValue<double>, LogicalTypeId::DOUBLE>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! decimal4/decimal8/decimal16
	case LogicalTypeId::DECIMAL: {
		auto physical_type = type.InternalType();
		switch (physical_type) {
		case PhysicalType::INT32: {
			return ConvertTypedValues<int32_t, ConvertShreddedValue<int32_t>, LogicalTypeId::DECIMAL>(
			    typed_value, metadata, value, offset, length, total_size, is_field);
		}
		case PhysicalType::INT64: {
			return ConvertTypedValues<int64_t, ConvertShreddedValue<int64_t>, LogicalTypeId::DECIMAL>(
			    typed_value, metadata, value, offset, length, total_size, is_field);
		}
		case PhysicalType::INT128: {
			return ConvertTypedValues<hugeint_t, ConvertShreddedValue<hugeint_t>, LogicalTypeId::DECIMAL>(
			    typed_value, metadata, value, offset, length, total_size, is_field);
		}
		default:
			throw NotImplementedException("Decimal with PhysicalType (%s) not implemented for shredded Variant",
			                              EnumUtil::ToString(physical_type));
		}
	}
	//! date
	case LogicalTypeId::DATE: {
		return ConvertTypedValues<date_t, ConvertShreddedValue<date_t>, LogicalTypeId::DATE>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! time
	case LogicalTypeId::TIME: {
		return ConvertTypedValues<dtime_t, ConvertShreddedValue<dtime_t>, LogicalTypeId::TIME>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! timestamptz(6) (timestamptz(9) not implemented in DuckDB)
	case LogicalTypeId::TIMESTAMP_TZ: {
		return ConvertTypedValues<timestamp_tz_t, ConvertShreddedValue<timestamp_tz_t>, LogicalTypeId::TIMESTAMP_TZ>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! timestampntz(6)
	case LogicalTypeId::TIMESTAMP: {
		return ConvertTypedValues<timestamp_t, ConvertShreddedValue<timestamp_t>, LogicalTypeId::TIMESTAMP>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! timestampntz(9)
	case LogicalTypeId::TIMESTAMP_NS: {
		return ConvertTypedValues<timestamp_ns_t, ConvertShreddedValue<timestamp_ns_t>, LogicalTypeId::TIMESTAMP_NS>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! binary
	case LogicalTypeId::BLOB: {
		return ConvertTypedValues<string_t, ConvertShreddedValue<string_t>, LogicalTypeId::BLOB>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! string
	case LogicalTypeId::VARCHAR: {
		return ConvertTypedValues<string_t, ConvertShreddedValue<string_t>, LogicalTypeId::VARCHAR>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	//! uuid
	case LogicalTypeId::UUID: {
		return ConvertTypedValues<hugeint_t, ConvertShreddedValue<hugeint_t>, LogicalTypeId::UUID>(
		    typed_value, metadata, value, offset, length, total_size, is_field);
	}
	default:
		throw NotImplementedException("Variant shredding on type: '%s' is not implemented", type.ToString());
	}
}

namespace {

struct ShreddedVariantField {
public:
	explicit ShreddedVariantField(const string &field_name) : field_name(field_name) {
	}

public:
	string field_name;
	//! Values for the field, for all rows
	vector<VariantValue> values;
};

} // namespace

template <bool IS_REQUIRED>
static vector<VariantValue> ConvertBinaryEncoding(Vector &metadata, Vector &value, idx_t offset, idx_t length,
                                                  idx_t total_size) {
	UnifiedVectorFormat value_format;
	value.ToUnifiedFormat(total_size, value_format);
	auto value_data = value_format.GetData<string_t>(value_format);
	auto &validity = value_format.validity;

	UnifiedVectorFormat metadata_format;
	metadata.ToUnifiedFormat(length, metadata_format);
	auto metadata_data = metadata_format.GetData<string_t>(metadata_format);
	auto metadata_validity = metadata_format.validity;

	vector<VariantValue> ret(length);
	if (IS_REQUIRED) {
		for (idx_t i = 0; i < length; i++) {
			auto index = value_format.sel->get_index(i + offset);

			// Variant itself is NULL
			if (!validity.RowIsValid(index) && !metadata_validity.RowIsValid(metadata_format.sel->get_index(i))) {
				ret[i] = VariantValue(Value());
				continue;
			}

			D_ASSERT(validity.RowIsValid(index));
			auto &metadata_value = metadata_data[metadata_format.sel->get_index(i)];
			VariantMetadata variant_metadata(metadata_value);
			auto binary_value = value_data[index].GetData();
			ret[i] = VariantBinaryDecoder::Decode(variant_metadata, const_data_ptr_cast(binary_value));
		}
	} else {
		//! Even though 'typed_value' is not present, 'value' is allowed to contain NULLs because we're scanning an
		//! Object's shredded field.
		//! When 'value' is null for a row, that means the Object does not contain this field
		//! for that row.
		for (idx_t i = 0; i < length; i++) {
			auto index = value_format.sel->get_index(i + offset);
			if (validity.RowIsValid(index)) {
				auto &metadata_value = metadata_data[metadata_format.sel->get_index(i)];
				VariantMetadata variant_metadata(metadata_value);
				auto binary_value = value_data[index].GetData();
				ret[i] = VariantBinaryDecoder::Decode(variant_metadata, const_data_ptr_cast(binary_value));
			}
		}
	}
	return ret;
}

static VariantValue ConvertPartiallyShreddedObject(vector<ShreddedVariantField> &shredded_fields,
                                                   const UnifiedVectorFormat &metadata_format,
                                                   const UnifiedVectorFormat &value_format, idx_t i, idx_t offset) {
	auto ret = VariantValue(VariantValueType::OBJECT);
	auto index = value_format.sel->get_index(i + offset);
	auto value_data = value_format.GetData<string_t>(value_format);
	auto metadata_data = metadata_format.GetData<string_t>(metadata_format);
	auto &value_validity = value_format.validity;

	for (idx_t field_index = 0; field_index < shredded_fields.size(); field_index++) {
		auto &shredded_field = shredded_fields[field_index];
		auto &field_value = shredded_field.values[i];

		if (field_value.IsMissing()) {
			//! This field is missing from the value, skip it
			continue;
		}
		ret.AddChild(shredded_field.field_name, std::move(field_value));
	}

	if (value_validity.RowIsValid(index)) {
		//! Object is partially shredded, decode the object and merge the values
		auto &metadata_value = metadata_data[metadata_format.sel->get_index(i)];
		VariantMetadata variant_metadata(metadata_value);
		auto binary_value = value_data[index].GetData();
		auto unshredded = VariantBinaryDecoder::Decode(variant_metadata, const_data_ptr_cast(binary_value));
		if (unshredded.value_type != VariantValueType::OBJECT) {
			throw InvalidInputException("Partially shredded objects have to encode Object Variants in the 'value'");
		}
		for (auto &item : unshredded.object_children) {
			ret.AddChild(item.first, std::move(item.second));
		}
	}
	return ret;
}

vector<VariantValue> VariantShreddedConversion::ConvertShreddedObject(Vector &metadata, Vector &value,
                                                                      Vector &typed_value, idx_t offset, idx_t length,
                                                                      idx_t total_size, const bool is_field) {
	auto &type = typed_value.GetType();
	D_ASSERT(type.id() == LogicalTypeId::STRUCT);
	auto &fields = StructType::GetChildTypes(type);
	auto &entries = StructVector::GetEntries(typed_value);
	D_ASSERT(entries.size() == fields.size());

	//! 'value'
	UnifiedVectorFormat value_format;
	value.ToUnifiedFormat(total_size, value_format);
	auto value_data = value_format.GetData<string_t>(value_format);
	auto &validity = value_format.validity;
	(void)validity;

	//! 'metadata'
	UnifiedVectorFormat metadata_format;
	metadata.ToUnifiedFormat(length, metadata_format);
	auto metadata_data = metadata_format.GetData<string_t>(metadata_format);

	//! 'typed_value'
	UnifiedVectorFormat typed_format;
	typed_value.ToUnifiedFormat(total_size, typed_format);
	auto &typed_validity = typed_format.validity;

	//! Process all fields to get the shredded field values
	vector<ShreddedVariantField> shredded_fields;
	shredded_fields.reserve(fields.size());
	for (idx_t i = 0; i < fields.size(); i++) {
		auto &field = fields[i];
		auto &field_name = field.first;
		auto &field_vec = *entries[i];

		shredded_fields.emplace_back(field_name);
		auto &shredded_field = shredded_fields.back();
		shredded_field.values = Convert(metadata, field_vec, offset, length, total_size, true);
	}

	vector<VariantValue> ret(length);
	if (typed_validity.AllValid()) {
		for (idx_t i = 0; i < length; i++) {
			ret[i] = ConvertPartiallyShreddedObject(shredded_fields, metadata_format, value_format, i, offset);
		}
	} else {
		//! For some of the rows, the value is not an object
		for (idx_t i = 0; i < length; i++) {
			auto typed_index = typed_format.sel->get_index(i + offset);
			auto value_index = value_format.sel->get_index(i + offset);
			if (typed_validity.RowIsValid(typed_index)) {
				ret[i] = ConvertPartiallyShreddedObject(shredded_fields, metadata_format, value_format, i, offset);
			} else {
				if (is_field && !validity.RowIsValid(value_index)) {
					//! This object is a field in the parent object, the value is missing, skip it
					continue;
				}
				D_ASSERT(validity.RowIsValid(value_index));
				auto &metadata_value = metadata_data[metadata_format.sel->get_index(i)];
				VariantMetadata variant_metadata(metadata_value);
				auto binary_value = value_data[value_index].GetData();
				ret[i] = VariantBinaryDecoder::Decode(variant_metadata, const_data_ptr_cast(binary_value));
				if (ret[i].value_type == VariantValueType::OBJECT) {
					throw InvalidInputException(
					    "When 'typed_value' for a shredded Object is NULL, 'value' can not contain an Object value");
				}
			}
		}
	}
	return ret;
}

vector<VariantValue> VariantShreddedConversion::ConvertShreddedArray(Vector &metadata, Vector &value,
                                                                     Vector &typed_value, idx_t offset, idx_t length,
                                                                     idx_t total_size, const bool is_field) {
	auto &child = ListVector::GetEntry(typed_value);
	auto list_size = ListVector::GetListSize(typed_value);

	//! 'value'
	UnifiedVectorFormat value_format;
	value.ToUnifiedFormat(total_size, value_format);
	auto value_data = value_format.GetData<string_t>(value_format);

	//! 'metadata'
	UnifiedVectorFormat metadata_format;
	metadata.ToUnifiedFormat(length, metadata_format);
	auto metadata_data = metadata_format.GetData<string_t>(metadata_format);

	//! 'typed_value'
	UnifiedVectorFormat list_format;
	typed_value.ToUnifiedFormat(total_size, list_format);
	auto list_data = list_format.GetData<list_entry_t>(list_format);
	auto &validity = list_format.validity;
	auto &value_validity = value_format.validity;

	vector<VariantValue> ret(length);
	if (validity.AllValid()) {
		//! We can be sure that none of the values are binary encoded
		for (idx_t i = 0; i < length; i++) {
			auto typed_index = list_format.sel->get_index(i + offset);
			auto entry = list_data[typed_index];
			Vector child_metadata(metadata.GetValue(i));
			ret[i] = VariantValue(VariantValueType::ARRAY);
			ret[i].array_items = Convert(child_metadata, child, entry.offset, entry.length, list_size, false);
		}
	} else {
		for (idx_t i = 0; i < length; i++) {
			auto typed_index = list_format.sel->get_index(i + offset);
			auto value_index = value_format.sel->get_index(i + offset);
			if (validity.RowIsValid(typed_index)) {
				auto entry = list_data[typed_index];
				Vector child_metadata(metadata.GetValue(i));
				ret[i] = VariantValue(VariantValueType::ARRAY);
				ret[i].array_items = Convert(child_metadata, child, entry.offset, entry.length, list_size, false);
			} else {
				if (is_field && !value_validity.RowIsValid(value_index)) {
					//! Value is missing for this field
					continue;
				}
				D_ASSERT(value_validity.RowIsValid(value_index));
				auto metadata_value = metadata_data[metadata_format.sel->get_index(i)];
				VariantMetadata variant_metadata(metadata_value);
				ret[i] = VariantBinaryDecoder::Decode(variant_metadata,
				                                      const_data_ptr_cast(value_data[value_index].GetData()));
			}
		}
	}
	return ret;
}

vector<VariantValue> VariantShreddedConversion::Convert(Vector &metadata, Vector &group, idx_t offset, idx_t length,
                                                        idx_t total_size, bool is_field) {
	D_ASSERT(group.GetType().id() == LogicalTypeId::STRUCT);

	auto &group_entries = StructVector::GetEntries(group);
	auto &group_type_children = StructType::GetChildTypes(group.GetType());
	D_ASSERT(group_type_children.size() == group_entries.size());

	//! From the spec:
	//! The Parquet columns used to store variant metadata and values must be accessed by name, not by position.
	optional_ptr<Vector> value;
	optional_ptr<Vector> typed_value;
	for (idx_t i = 0; i < group_entries.size(); i++) {
		auto &name = group_type_children[i].first;
		auto &vec = group_entries[i];
		if (name == "value") {
			value = vec.get();
		} else if (name == "typed_value") {
			typed_value = vec.get();
		} else {
			throw InvalidInputException("Variant group can only contain 'value'/'typed_value', not: %s", name);
		}
	}
	if (!value) {
		throw InvalidInputException("Required column 'value' not found in Variant group");
	}

	if (typed_value) {
		auto &type = typed_value->GetType();
		vector<VariantValue> ret;
		if (type.id() == LogicalTypeId::STRUCT) {
			return ConvertShreddedObject(metadata, *value, *typed_value, offset, length, total_size, is_field);
		} else if (type.id() == LogicalTypeId::LIST) {
			return ConvertShreddedArray(metadata, *value, *typed_value, offset, length, total_size, is_field);
		} else {
			return ConvertShreddedLeaf(metadata, *value, *typed_value, offset, length, total_size, is_field);
		}
	} else {
		if (is_field) {
			return ConvertBinaryEncoding<false>(metadata, *value, offset, length, total_size);
		} else {
			//! Only 'value' is present, we can assume this to be 'required', so it can't contain NULLs
			return ConvertBinaryEncoding<true>(metadata, *value, offset, length, total_size);
		}
	}
}

} // namespace duckdb
