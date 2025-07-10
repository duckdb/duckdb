#include "reader/variant/variant_shredded_conversion.hpp"
#include "column_reader.hpp"

namespace duckdb {

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
vector<VariantValue> ConvertTypedValues(VariantBinaryDecoder &decoder, Vector &vec, Vector &metadata, Vector &blob,
                                        idx_t count) {
	auto data = FlatVector::GetData<T>(vec);
	auto metadata_data = FlatVector::GetData<string_t>(metadata);
	auto value_data = FlatVector::GetData<string_t>(blob);

	auto &validity = FlatVector::Validity(vec);
	auto &value_validity = FlatVector::Validity(blob);
	auto &type = vec.GetType();

	//! Values only used for Decimal conversion
	uint8_t width;
	uint8_t scale;
	if (TYPE_ID == LogicalTypeId::DECIMAL) {
		type.GetDecimalProperties(width, scale);
	}

	vector<VariantValue> ret(count);
	if (validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			if (TYPE_ID == LogicalTypeId::DECIMAL) {
				ret[i] = OP::ConvertDecimal(data[i], width, scale);
			} else if (TYPE_ID == LogicalTypeId::TIMESTAMP_TZ) {
				ret[i] = OP::ConvertTimezone(data[i]);
			} else if (TYPE_ID == LogicalTypeId::BLOB) {
				ret[i] = OP::ConvertBlob(data[i]);
			} else {
				ret[i] = OP::Convert(data[i]);
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			if (validity.RowIsValid(i)) {
				//! This is a leaf, partially shredded values aren't possible here
				D_ASSERT(!value_validity.RowIsValid(i));
				if (TYPE_ID == LogicalTypeId::DECIMAL) {
					ret[i] = OP::ConvertDecimal(data[i], width, scale);
				} else if (TYPE_ID == LogicalTypeId::TIMESTAMP_TZ) {
					ret[i] = OP::ConvertTimezone(data[i]);
				} else if (TYPE_ID == LogicalTypeId::BLOB) {
					ret[i] = OP::ConvertBlob(data[i]);
				} else {
					ret[i] = OP::Convert(data[i]);
				}
			} else if (value_validity.RowIsValid(i)) {
				auto metadata_value = metadata_data[i];
				VariantMetadata variant_metadata(metadata_value);
				ret[i] = decoder.Decode(variant_metadata, const_data_ptr_cast(value_data[i].GetData()));
			}
		}
	}
}

static vector<VariantValue> ConvertShreddedLeaf(Vector &metadata, Vector &value, Vector &typed_value, idx_t count) {
	D_ASSERT(!typed_value.GetType().IsNested());
	vector<VariantValue> result;

	auto &type = typed_value.GetType();
	switch (type.id()) {
	//! boolean
	case LogicalTypeId::BOOLEAN: {
	}
	//! int8
	case LogicalTypeId::TINYINT: {
	}
	//! int16
	case LogicalTypeId::SMALLINT: {
	}
	//! int32
	case LogicalTypeId::INTEGER: {
	}
	//! int64
	case LogicalTypeId::BIGINT: {
	}
	//! float
	case LogicalTypeId::FLOAT: {
	}
	//! double
	case LogicalTypeId::DOUBLE: {
	}
	//! decimal4/decimal8/decimal16
	case LogicalTypeId::DECIMAL: {
	}
	//! date
	case LogicalTypeId::DATE: {
	}
	//! time
	case LogicalTypeId::TIME: {
	}
	//! timestamptz(6) (timestamptz(9) not implemented in DuckDB)
	case LogicalTypeId::TIMESTAMP_TZ: {
	}
	//! timestampntz(6)
	case LogicalTypeId::TIMESTAMP: {
	}
	//! timestampntz(9)
	case LogicalTypeId::TIMESTAMP_NS: {
	}
	//! binary
	case LogicalTypeId::BLOB: {
	}
	//! string
	case LogicalTypeId::VARCHAR: {
	}
	//! uuid
	case LogicalTypeId::UUID: {
	}
	default:
		throw NotImplementedException("Variant shredding on type: '%s' is not implemented", type.ToString());
	}
}

vector<VariantValue> VariantShreddedConversion::Convert(Vector &metadata, Vector &value, Vector &typed_value,
                                                        idx_t count) {
	auto &metadata_validity = FlatVector::Validity(metadata);
	auto &value_validity = FlatVector::Validity(value);
	auto &typed_value_validity = FlatVector::Validity(typed_value);

	auto metadata_data = FlatVector::GetData<string_t>(metadata);
	auto value_data = FlatVector::GetData<string_t>(value);

	auto &type = typed_value.GetType();
	vector<VariantValue> ret;
	if (type.id() == LogicalTypeId::STRUCT) {

	} else if (type.id() == LogicalTypeId::LIST) {

	} else {
		ret = ConvertShreddedLeaf(metadata, value, typed_value, count);
	}
	return ret;
}

} // namespace duckdb
