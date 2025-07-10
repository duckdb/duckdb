#include "reader/variant/variant_shredded_conversion.hpp"
#include "column_reader.hpp"

namespace duckdb {

static void VerifyVariantGroup(const LogicalType &group) {
#ifdef DEBUG
	D_ASSERT(group.id() == LogicalTypeId::STRUCT);
	auto &child_types = StructType::GetChildTypes(group);
	D_ASSERT(child_types.size() == 2);
	auto &value = child_types[0];
	auto &typed_value = child_types[1];
	D_ASSERT(value.first == "value");
	D_ASSERT(typed_value.first == "typed_value");
	D_ASSERT(value.second.id() == LogicalTypeId::BLOB);
#endif
}

template <class T>
struct ConvertShreddedValue {
	static VariantValue Convert(T val);
	static VariantValue ConvertDecimal(T val, uint8_t width, uint8_t scale) {
		throw InternalException("ConvertShreddedValue::ConvertDecimal not implemented for type");
	}
	static VariantValue ConvertTimezone(T val) {
		throw InternalException("ConvertShreddedValue::ConvertTimezone not implemented for type");
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
VariantValue ConvertShreddedValue<timestamp_t>::ConvertTimezone(timestamp_t val) {
	return VariantValue(Value::TIMESTAMP(val), LogicalTypeId::TIMESTAMP_TZ);
}
//! timestamptz(9)
template <>
VariantValue ConvertShreddedValue<timestamp_ns_t>::ConvertTimezone(timestamp_ns_t val) {
	return VariantValue(Value::TIMESTAMPNS(val), LogicalTypeId::TIMESTAMP_TZ);
}
//! timestamptz(6)
template <>
VariantValue ConvertShreddedValue<timestamp_t>::Convert(timestamp_t val) {
	return VariantValue(Value::TIMESTAMP(val));
}
//! timestamptz(9)
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
                                        idx_t total_size) {
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
			} else if (TYPE_ID == LogicalTypeId::TIMESTAMP_TZ) {
				ret[i] = OP::ConvertTimezone(data[index]);
			} else if (TYPE_ID == LogicalTypeId::BLOB) {
				ret[i] = OP::ConvertBlob(data[index]);
			} else {
				ret[i] = OP::Convert(data[index]);
			}
		}
	} else {
		for (idx_t i = 0; i < length; i++) {
			if (validity.RowIsValid(i)) {
				auto index = typed_format.sel->get_index(i + offset);
				//! This is a leaf, partially shredded values aren't possible here
				D_ASSERT(!value_validity.RowIsValid(i));
				if (TYPE_ID == LogicalTypeId::DECIMAL) {
					ret[i] = OP::ConvertDecimal(data[index], width, scale);
				} else if (TYPE_ID == LogicalTypeId::TIMESTAMP_TZ) {
					ret[i] = OP::ConvertTimezone(data[index]);
				} else if (TYPE_ID == LogicalTypeId::BLOB) {
					ret[i] = OP::ConvertBlob(data[index]);
				} else {
					ret[i] = OP::Convert(data[index]);
				}
			} else if (value_validity.RowIsValid(i)) {
				auto index = value_format.sel->get_index(i + offset);
				auto metadata_value = metadata_data[metadata_format.sel->get_index(i)];
				VariantMetadata variant_metadata(metadata_value);
				ret[i] =
				    VariantBinaryDecoder::Decode(variant_metadata, const_data_ptr_cast(value_data[index].GetData()));
			}
		}
	}
	return ret;
}

vector<VariantValue> VariantShreddedConversion::ConvertShreddedLeaf(Vector &metadata, Vector &value,
                                                                    Vector &typed_value, idx_t offset, idx_t length,
                                                                    idx_t total_size) {
	D_ASSERT(!typed_value.GetType().IsNested());
	vector<VariantValue> result;

	auto &type = typed_value.GetType();
	switch (type.id()) {
	//! boolean
	case LogicalTypeId::BOOLEAN: {
		return ConvertTypedValues<bool, ConvertShreddedValue<bool>, LogicalTypeId::BOOLEAN>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! int8
	case LogicalTypeId::TINYINT: {
		return ConvertTypedValues<int8_t, ConvertShreddedValue<int8_t>, LogicalTypeId::TINYINT>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! int16
	case LogicalTypeId::SMALLINT: {
		return ConvertTypedValues<int16_t, ConvertShreddedValue<int16_t>, LogicalTypeId::SMALLINT>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! int32
	case LogicalTypeId::INTEGER: {
		return ConvertTypedValues<int32_t, ConvertShreddedValue<int32_t>, LogicalTypeId::INTEGER>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! int64
	case LogicalTypeId::BIGINT: {
		return ConvertTypedValues<int64_t, ConvertShreddedValue<int64_t>, LogicalTypeId::BIGINT>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! float
	case LogicalTypeId::FLOAT: {
		return ConvertTypedValues<float, ConvertShreddedValue<float>, LogicalTypeId::FLOAT>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! double
	case LogicalTypeId::DOUBLE: {
		return ConvertTypedValues<double, ConvertShreddedValue<double>, LogicalTypeId::DOUBLE>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! decimal4/decimal8/decimal16
	case LogicalTypeId::DECIMAL: {
		auto physical_type = type.InternalType();
		switch (physical_type) {
		case PhysicalType::INT32: {
			return ConvertTypedValues<int32_t, ConvertShreddedValue<int32_t>, LogicalTypeId::DECIMAL>(
			    typed_value, metadata, value, offset, length, total_size);
		}
		case PhysicalType::INT64: {
			return ConvertTypedValues<int64_t, ConvertShreddedValue<int64_t>, LogicalTypeId::DECIMAL>(
			    typed_value, metadata, value, offset, length, total_size);
		}
		case PhysicalType::INT128: {
			return ConvertTypedValues<hugeint_t, ConvertShreddedValue<hugeint_t>, LogicalTypeId::DECIMAL>(
			    typed_value, metadata, value, offset, length, total_size);
		}
		default:
			throw NotImplementedException("Decimal with PhysicalType (%s) not implemented for shredded Variant",
			                              EnumUtil::ToString(physical_type));
		}
	}
	//! date
	case LogicalTypeId::DATE: {
		return ConvertTypedValues<date_t, ConvertShreddedValue<date_t>, LogicalTypeId::DATE>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! time
	case LogicalTypeId::TIME: {
		return ConvertTypedValues<dtime_t, ConvertShreddedValue<dtime_t>, LogicalTypeId::TIME>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! timestamptz(6) (timestamptz(9) not implemented in DuckDB)
	case LogicalTypeId::TIMESTAMP_TZ: {
		return ConvertTypedValues<timestamp_t, ConvertShreddedValue<timestamp_t>, LogicalTypeId::TIMESTAMP_TZ>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! timestampntz(6)
	case LogicalTypeId::TIMESTAMP: {
		return ConvertTypedValues<timestamp_t, ConvertShreddedValue<timestamp_t>, LogicalTypeId::TIMESTAMP>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! timestampntz(9)
	case LogicalTypeId::TIMESTAMP_NS: {
		return ConvertTypedValues<timestamp_ns_t, ConvertShreddedValue<timestamp_ns_t>, LogicalTypeId::TIMESTAMP_NS>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! binary
	case LogicalTypeId::BLOB: {
		return ConvertTypedValues<string_t, ConvertShreddedValue<string_t>, LogicalTypeId::BLOB>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! string
	case LogicalTypeId::VARCHAR: {
		return ConvertTypedValues<string_t, ConvertShreddedValue<string_t>, LogicalTypeId::VARCHAR>(
		    typed_value, metadata, value, offset, length, total_size);
	}
	//! uuid
	case LogicalTypeId::UUID: {
		return ConvertTypedValues<hugeint_t, ConvertShreddedValue<hugeint_t>, LogicalTypeId::UUID>(
		    typed_value, metadata, value, offset, length, total_size);
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

vector<VariantValue> VariantShreddedConversion::ConvertShreddedObject(Vector &metadata, Vector &value,
                                                                      Vector &typed_value, idx_t offset, idx_t length,
                                                                      idx_t total_size) {
	auto &type = typed_value.GetType();
	D_ASSERT(type.id() == LogicalTypeId::STRUCT);
	auto &fields = StructType::GetChildTypes(type);
	auto &entries = StructVector::GetEntries(typed_value);
	D_ASSERT(entries.size() == fields.size());

	//! Process all fields to get the shredded field values
	vector<ShreddedVariantField> shredded_fields;
	shredded_fields.reserve(fields.size());
	for (idx_t i = 0; i < fields.size(); i++) {
		auto &field = fields[i];
		auto &field_name = field.first;
		auto &field_type = field.second;
		auto &field_vec = *entries[i];
		VerifyVariantGroup(field_type);

		auto &variant_group_children = StructVector::GetEntries(field_vec);
		auto &field_value = *variant_group_children[0];
		auto &field_typed_value = *variant_group_children[1];

		shredded_fields.emplace_back(field_name);
		auto &shredded_field = shredded_fields.back();
		shredded_field.values = Convert(metadata, field_value, field_typed_value, offset, length, total_size);
	}

	UnifiedVectorFormat value_format;
	value.ToUnifiedFormat(total_size, value_format);
	auto value_data = value_format.GetData<string_t>(value_format);
	auto &validity = value_format.validity;

	UnifiedVectorFormat metadata_format;
	metadata.ToUnifiedFormat(length, metadata_format);
	auto metadata_data = metadata_format.GetData<string_t>(metadata_format);

	vector<VariantValue> ret(length);
	for (idx_t i = 0; i < length; i++) {
		ret[i] = VariantValue(VariantValueType::OBJECT);
		auto index = value_format.sel->get_index(i + offset);

		for (idx_t field_index = 0; field_index < shredded_fields.size(); field_index++) {
			auto &shredded_field = shredded_fields[field_index];
			auto &field_value = shredded_field.values[i];

			ret[i].AddChild(shredded_field.field_name, std::move(field_value));
		}

		if (validity.RowIsValid(index)) {
			//! Object is partially shredded, decode the object and merge the values
			auto &metadata_value = metadata_data[metadata_format.sel->get_index(i)];
			VariantMetadata variant_metadata(metadata_value);
			auto binary_value = value_data[index].GetData();
			auto unshredded = VariantBinaryDecoder::Decode(variant_metadata, const_data_ptr_cast(binary_value));
			if (unshredded.value_type != VariantValueType::OBJECT) {
				throw InvalidInputException("Partially shredded objects have to encode Object Variants in the 'value'");
			}
			for (auto &item : unshredded.object_children) {
				ret[i].AddChild(item.first, std::move(item.second));
			}
		}
	}
	return ret;
}

vector<VariantValue> VariantShreddedConversion::ConvertShreddedArray(Vector &metadata, Vector &value,
                                                                     Vector &typed_value, idx_t offset, idx_t length,
                                                                     idx_t total_size) {
	auto &child = ListVector::GetEntry(typed_value);
	VerifyVariantGroup(child.GetType());
	D_ASSERT(child.GetType().id() == LogicalTypeId::STRUCT);
	auto &child_entries = StructVector::GetEntries(child);
	D_ASSERT(child_entries.size() == 2);
	auto &child_value = *child_entries[0];
	auto &child_typed_value = *child_entries[1];
	auto list_size = ListVector::GetListSize(typed_value);

	UnifiedVectorFormat list_format;
	typed_value.ToUnifiedFormat(total_size, list_format);
	auto list_data = list_format.GetData<list_entry_t>(list_format);
	vector<VariantValue> ret(length);
	for (idx_t i = 0; i < length; i++) {
		auto index = list_format.sel->get_index(i + offset);
		auto entry = list_data[index];
		Vector child_metadata(metadata.GetValue(i));
		ret[i] = VariantValue(VariantValueType::ARRAY);
		ret[i].array_items =
		    Convert(child_metadata, child_value, child_typed_value, entry.offset, entry.length, list_size);
	}
	return ret;
}

vector<VariantValue> VariantShreddedConversion::Convert(Vector &metadata, Vector &value, Vector &typed_value,
                                                        idx_t offset, idx_t length, idx_t total_size) {
	auto &type = typed_value.GetType();
	vector<VariantValue> ret;
	if (type.id() == LogicalTypeId::STRUCT) {
		return ConvertShreddedObject(metadata, value, typed_value, offset, length, total_size);
	} else if (type.id() == LogicalTypeId::LIST) {
		return ConvertShreddedArray(metadata, value, typed_value, offset, length, total_size);
	} else {
		return ConvertShreddedLeaf(metadata, value, typed_value, offset, length, total_size);
	}
}

} // namespace duckdb
