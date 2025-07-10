#include "reader/variant/variant_value.hpp"

namespace duckdb {

void VariantValue::AddChild(const string &key, VariantValue &&val) {
	D_ASSERT(value_type == VariantValueType::OBJECT);
	object_children.emplace(key, std::move(val));
}

void VariantValue::AddItem(VariantValue &&val) {
	D_ASSERT(value_type == VariantValueType::ARRAY);
	array_items.push_back(std::move(val));
}

yyjson_mut_val *VariantValue::ToJSON(ClientContext &context, yyjson_mut_doc *doc) const {
	switch (value_type) {
	case VariantValueType::PRIMITIVE: {
		if (primitive_value.IsNull()) {
			return yyjson_mut_null(doc);
		}
		switch (primitive_value.type().id()) {
		case LogicalTypeId::BOOLEAN: {
			if (primitive_value.GetValue<bool>()) {
				return yyjson_mut_true(doc);
			} else {
				return yyjson_mut_false(doc);
			}
		}
		case LogicalTypeId::TINYINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int8_t>());
		case LogicalTypeId::SMALLINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int16_t>());
		case LogicalTypeId::INTEGER:
			return yyjson_mut_int(doc, primitive_value.GetValue<int32_t>());
		case LogicalTypeId::BIGINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int64_t>());
		case LogicalTypeId::FLOAT:
			return yyjson_mut_real(doc, primitive_value.GetValue<float>());
		case LogicalTypeId::DOUBLE:
			return yyjson_mut_real(doc, primitive_value.GetValue<double>());
		case LogicalTypeId::VARCHAR: {
			auto value = primitive_value.GetValue<string>();
			return yyjson_mut_strncpy(doc, value.c_str(), value.size());
		}
		case LogicalTypeId::TIMESTAMP: {
			string value_str;
			if (primitive_value_type == LogicalTypeId::TIMESTAMP_TZ) {
				timestamp_tz_t micros_tz(primitive_value.GetValue<timestamp_t>());
				auto value = Value::TIMESTAMPTZ(micros_tz);
				value_str = value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
			} else {
				value_str = primitive_value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
			}
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP_NS: {
			string value_str;
			if (primitive_value_type == LogicalTypeId::TIMESTAMP_TZ) {
				auto nanos_ts = primitive_value.GetValue<timestamp_ns_t>();

				//! Convert the nanos timestamp to a micros timestamp
				date_t out_date;
				dtime_t out_time;
				int32_t out_nanos;
				Timestamp::Convert(nanos_ts, out_date, out_time, out_nanos);
				auto micros_ts = Timestamp::FromDatetime(out_date, out_time);

				//! Turn the micros timestamp into a micros_tz timestamp and serialize it
				timestamp_tz_t micros_tz_ts(micros_ts.value);
				auto value = Value::TIMESTAMPTZ(micros_tz_ts);
				value_str = value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();

				if (StringUtil::Contains(value_str, "+")) {
					//! Don't attempt this for NaN/Inf timestamps
					auto parts = StringUtil::Split(value_str, '+');
					value_str = StringUtil::Format("%s%s+%s", parts[0], to_string(out_nanos), parts[1]);
				}
			} else {
				value_str = primitive_value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
			}
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		default:
			throw InternalException("Unexpected primitive type: %s", primitive_value.type().ToString());
		}
	}
	case VariantValueType::OBJECT: {
		auto obj = yyjson_mut_obj(doc);
		for (const auto &it : object_children) {
			auto &key = it.first;
			auto value = it.second.ToJSON(context, doc);
			yyjson_mut_obj_add_val(doc, obj, key.c_str(), value);
		}
		return obj;
	}
	case VariantValueType::ARRAY: {
		auto arr = yyjson_mut_arr(doc);
		for (auto &item : array_items) {
			auto value = item.ToJSON(context, doc);
			yyjson_mut_arr_add_val(arr, value);
		}
		return arr;
	}
	default:
		throw InternalException("Can't serialize this VariantValue type to JSON");
	}
}

} // namespace duckdb
