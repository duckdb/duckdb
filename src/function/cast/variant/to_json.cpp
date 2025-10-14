#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "yyjson.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/variant_visitor.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

//! ------------ Variant -> JSON ------------

namespace {

struct JSONConverter {
	using result_type = yyjson_mut_val *;

	static yyjson_mut_val *VisitNull(yyjson_mut_doc *doc) {
		return yyjson_mut_null(doc);
	}

	static yyjson_mut_val *VisitBoolean(bool val, yyjson_mut_doc *doc) {
		return val ? yyjson_mut_true(doc) : yyjson_mut_false(doc);
	}

	template <typename T>
	static yyjson_mut_val *VisitInteger(T val, yyjson_mut_doc *doc) {
		throw InternalException("JSONConverter::VisitInteger not implemented!");
	}

	static yyjson_mut_val *VisitTime(dtime_t val, yyjson_mut_doc *doc) {
		auto val_str = Time::ToString(val);
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitTimeNanos(dtime_ns_t val, yyjson_mut_doc *doc) {
		auto val_str = Value::TIME_NS(val).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitTimeTZ(dtime_tz_t val, yyjson_mut_doc *doc) {
		auto val_str = Value::TIMETZ(val).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitTimestampSec(timestamp_sec_t val, yyjson_mut_doc *doc) {
		auto val_str = Value::TIMESTAMPSEC(val).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitTimestampMs(timestamp_ms_t val, yyjson_mut_doc *doc) {
		auto val_str = Value::TIMESTAMPMS(val).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitTimestamp(timestamp_t val, yyjson_mut_doc *doc) {
		auto val_str = Timestamp::ToString(val);
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitTimestampNanos(timestamp_ns_t val, yyjson_mut_doc *doc) {
		auto val_str = Value::TIMESTAMPNS(val).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitTimestampTZ(timestamp_tz_t val, yyjson_mut_doc *doc) {
		auto val_str = Value::TIMESTAMPTZ(val).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitFloat(float val, yyjson_mut_doc *doc) {
		return yyjson_mut_real(doc, val);
	}

	static yyjson_mut_val *VisitDouble(double val, yyjson_mut_doc *doc) {
		return yyjson_mut_real(doc, val);
	}

	static yyjson_mut_val *VisitUUID(hugeint_t val, yyjson_mut_doc *doc) {
		auto val_str = Value::UUID(val).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitDate(date_t val, yyjson_mut_doc *doc) {
		auto val_str = Date::ToString(val);
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitInterval(interval_t val, yyjson_mut_doc *doc) {
		auto val_str = Value::INTERVAL(val).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitString(const string_t &str, yyjson_mut_doc *doc) {
		return yyjson_mut_strncpy(doc, str.GetData(), str.GetSize());
	}

	static yyjson_mut_val *VisitBlob(const string_t &str, yyjson_mut_doc *doc) {
		auto val_str = Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitBignum(const string_t &str, yyjson_mut_doc *doc) {
		auto val_str = Value::BIGNUM(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString();
		return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitGeometry(const string_t &str, yyjson_mut_doc *doc) {
		auto val_str = Value::GEOMETRY(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitBitstring(const string_t &str, yyjson_mut_doc *doc) {
		auto val_str = Value::BIT(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString();
		return yyjson_mut_strncpy(doc, val_str.c_str(), val_str.size());
	}

	template <typename T>
	static yyjson_mut_val *VisitDecimal(T val, uint32_t width, uint32_t scale, yyjson_mut_doc *doc) {
		string val_str;
		if (std::is_same<T, int16_t>::value) {
			val_str = Decimal::ToString(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, int32_t>::value) {
			val_str = Decimal::ToString(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, int64_t>::value) {
			val_str = Decimal::ToString(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else if (std::is_same<T, hugeint_t>::value) {
			val_str = Decimal::ToString(val, static_cast<uint8_t>(width), static_cast<uint8_t>(scale));
		} else {
			throw InternalException("Unhandled decimal type");
		}
		return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
	}

	static yyjson_mut_val *VisitArray(const UnifiedVariantVectorData &variant, idx_t row,
	                                  const VariantNestedData &nested_data, yyjson_mut_doc *doc) {
		auto arr = yyjson_mut_arr(doc);
		auto array_items = VariantVisitor<JSONConverter>::VisitArrayItems(variant, row, nested_data, doc);
		for (auto &entry : array_items) {
			yyjson_mut_arr_add_val(arr, entry);
		}
		return arr;
	}

	static yyjson_mut_val *VisitObject(const UnifiedVariantVectorData &variant, idx_t row,
	                                   const VariantNestedData &nested_data, yyjson_mut_doc *doc) {
		auto obj = yyjson_mut_obj(doc);
		auto object_items = VariantVisitor<JSONConverter>::VisitObjectItems(variant, row, nested_data, doc);
		for (auto &entry : object_items) {
			yyjson_mut_obj_put(obj, yyjson_mut_strncpy(doc, entry.first.c_str(), entry.first.size()), entry.second);
		}
		return obj;
	}

	static yyjson_mut_val *VisitDefault(VariantLogicalType type_id, const_data_ptr_t, yyjson_mut_doc *) {
		throw InternalException("VariantLogicalType(%s) not handled", EnumUtil::ToString(type_id));
	}
};

template <>
yyjson_mut_val *JSONConverter::VisitInteger<int8_t>(int8_t val, yyjson_mut_doc *doc) {
	return yyjson_mut_sint(doc, val);
}

template <>
yyjson_mut_val *JSONConverter::VisitInteger<int16_t>(int16_t val, yyjson_mut_doc *doc) {
	return yyjson_mut_sint(doc, val);
}

template <>
yyjson_mut_val *JSONConverter::VisitInteger<int32_t>(int32_t val, yyjson_mut_doc *doc) {
	return yyjson_mut_sint(doc, val);
}

template <>
yyjson_mut_val *JSONConverter::VisitInteger<int64_t>(int64_t val, yyjson_mut_doc *doc) {
	return yyjson_mut_sint(doc, val);
}

template <>
yyjson_mut_val *JSONConverter::VisitInteger<hugeint_t>(hugeint_t val, yyjson_mut_doc *doc) {
	auto val_str = val.ToString();
	return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
}

template <>
yyjson_mut_val *JSONConverter::VisitInteger<uint8_t>(uint8_t val, yyjson_mut_doc *doc) {
	return yyjson_mut_sint(doc, val);
}

template <>
yyjson_mut_val *JSONConverter::VisitInteger<uint16_t>(uint16_t val, yyjson_mut_doc *doc) {
	return yyjson_mut_sint(doc, val);
}

template <>
yyjson_mut_val *JSONConverter::VisitInteger<uint32_t>(uint32_t val, yyjson_mut_doc *doc) {
	return yyjson_mut_sint(doc, val);
}

template <>
yyjson_mut_val *JSONConverter::VisitInteger<uint64_t>(uint64_t val, yyjson_mut_doc *doc) {
	return yyjson_mut_uint(doc, val);
}

template <>
yyjson_mut_val *JSONConverter::VisitInteger<uhugeint_t>(uhugeint_t val, yyjson_mut_doc *doc) {
	auto val_str = val.ToString();
	return yyjson_mut_rawncpy(doc, val_str.c_str(), val_str.size());
}

} // namespace

yyjson_mut_val *VariantCasts::ConvertVariantToJSON(yyjson_mut_doc *doc, const RecursiveUnifiedVectorFormat &source,
                                                   idx_t row, uint32_t values_idx) {
	UnifiedVariantVectorData variant(source);
	return VariantVisitor<JSONConverter>::Visit(variant, row, values_idx, doc);
}

} // namespace duckdb
