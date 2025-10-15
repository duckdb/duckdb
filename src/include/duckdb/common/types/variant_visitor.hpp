#pragma once

#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

template <typename Visitor, typename ReturnType = typename Visitor::result_type>
class VariantVisitor {
public:
	template <typename... Args>
	static ReturnType Visit(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_idx, Args &&...args) {
		if (!variant.RowIsValid(row)) {
			return Visitor::VisitNull(std::forward<Args>(args)...);
		}

		auto type_id = variant.GetTypeId(row, values_idx);
		auto byte_offset = variant.GetByteOffset(row, values_idx);
		auto blob_data = const_data_ptr_cast(variant.GetData(row).GetData());
		auto ptr = const_data_ptr_cast(blob_data + byte_offset);

		switch (type_id) {
		case VariantLogicalType::VARIANT_NULL:
			return Visitor::VisitNull(std::forward<Args>(args)...);
		case VariantLogicalType::BOOL_TRUE:
			return Visitor::VisitBoolean(true, std::forward<Args>(args)...);
		case VariantLogicalType::BOOL_FALSE:
			return Visitor::VisitBoolean(false, std::forward<Args>(args)...);
		case VariantLogicalType::INT8:
			return Visitor::template VisitInteger<int8_t>(Load<int8_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::INT16:
			return Visitor::template VisitInteger<int16_t>(Load<int16_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::INT32:
			return Visitor::template VisitInteger<int32_t>(Load<int32_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::INT64:
			return Visitor::template VisitInteger<int64_t>(Load<int64_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::INT128:
			return Visitor::template VisitInteger<hugeint_t>(Load<hugeint_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::UINT8:
			return Visitor::template VisitInteger<uint8_t>(Load<uint8_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::UINT16:
			return Visitor::template VisitInteger<uint16_t>(Load<uint16_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::UINT32:
			return Visitor::template VisitInteger<uint32_t>(Load<uint32_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::UINT64:
			return Visitor::template VisitInteger<uint64_t>(Load<uint64_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::UINT128:
			return Visitor::template VisitInteger<uhugeint_t>(Load<uhugeint_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::FLOAT:
			return Visitor::VisitFloat(Load<float>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::DOUBLE:
			return Visitor::VisitDouble(Load<double>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::UUID:
			return Visitor::VisitUUID(Load<hugeint_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::DATE:
			return Visitor::VisitDate(date_t(Load<int32_t>(ptr)), std::forward<Args>(args)...);
		case VariantLogicalType::INTERVAL:
			return Visitor::VisitInterval(Load<interval_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::VARCHAR:
		case VariantLogicalType::BLOB:
		case VariantLogicalType::BITSTRING:
		case VariantLogicalType::BIGNUM:
		case VariantLogicalType::GEOMETRY:
			return VisitString(type_id, variant, row, values_idx, std::forward<Args>(args)...);
		case VariantLogicalType::DECIMAL:
			return VisitDecimal(variant, row, values_idx, std::forward<Args>(args)...);
		case VariantLogicalType::ARRAY:
			return VisitArray(variant, row, values_idx, std::forward<Args>(args)...);
		case VariantLogicalType::OBJECT:
			return VisitObject(variant, row, values_idx, std::forward<Args>(args)...);
		case VariantLogicalType::TIME_MICROS:
			return Visitor::VisitTime(Load<dtime_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::TIME_NANOS:
			return Visitor::VisitTimeNanos(Load<dtime_ns_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::TIME_MICROS_TZ:
			return Visitor::VisitTimeTZ(Load<dtime_tz_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::TIMESTAMP_SEC:
			return Visitor::VisitTimestampSec(Load<timestamp_sec_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::TIMESTAMP_MILIS:
			return Visitor::VisitTimestampMs(Load<timestamp_ms_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::TIMESTAMP_MICROS:
			return Visitor::VisitTimestamp(Load<timestamp_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::TIMESTAMP_NANOS:
			return Visitor::VisitTimestampNanos(Load<timestamp_ns_t>(ptr), std::forward<Args>(args)...);
		case VariantLogicalType::TIMESTAMP_MICROS_TZ:
			return Visitor::VisitTimestampTZ(Load<timestamp_tz_t>(ptr), std::forward<Args>(args)...);
		default:
			return Visitor::VisitDefault(type_id, ptr, std::forward<Args>(args)...);
		}
	}

	template <typename... Args>
	static vector<ReturnType> VisitArrayItems(const UnifiedVariantVectorData &variant, idx_t row,
	                                          const VariantNestedData &array_data, Args &&...args) {
		vector<ReturnType> array_items;
		for (idx_t i = 0; i < array_data.child_count; i++) {
			auto values_index = variant.GetValuesIndex(row, array_data.children_idx + i);
			array_items.emplace_back(Visit(variant, row, values_index, std::forward<Args>(args)...));
		}
		return array_items;
	}

	template <typename... Args>
	static child_list_t<ReturnType> VisitObjectItems(const UnifiedVariantVectorData &variant, idx_t row,
	                                                 const VariantNestedData &object_data, Args &&...args) {
		child_list_t<ReturnType> object_items;
		for (idx_t i = 0; i < object_data.child_count; i++) {
			auto values_index = variant.GetValuesIndex(row, object_data.children_idx + i);
			auto val = Visit(variant, row, values_index, std::forward<Args>(args)...);

			auto keys_index = variant.GetKeysIndex(row, object_data.children_idx + i);
			auto &key = variant.GetKey(row, keys_index);

			object_items.emplace_back(key.GetString(), std::move(val));
		}
		return object_items;
	}

private:
	template <typename... Args>
	static ReturnType VisitArray(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_idx,
	                             Args &&...args) {
		auto decoded_nested_data = VariantUtils::DecodeNestedData(variant, row, values_idx);
		return Visitor::VisitArray(variant, row, decoded_nested_data, std::forward<Args>(args)...);
	}

	template <typename... Args>
	static ReturnType VisitObject(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_idx,
	                              Args &&...args) {
		auto decoded_nested_data = VariantUtils::DecodeNestedData(variant, row, values_idx);
		return Visitor::VisitObject(variant, row, decoded_nested_data, std::forward<Args>(args)...);
	}

	template <typename... Args>
	static ReturnType VisitString(VariantLogicalType type_id, const UnifiedVariantVectorData &variant, idx_t row,
	                              uint32_t values_idx, Args &&...args) {
		auto decoded_string = VariantUtils::DecodeStringData(variant, row, values_idx);
		if (type_id == VariantLogicalType::VARCHAR) {
			return Visitor::VisitString(decoded_string, std::forward<Args>(args)...);
		}
		if (type_id == VariantLogicalType::BLOB) {
			return Visitor::VisitBlob(decoded_string, std::forward<Args>(args)...);
		}
		if (type_id == VariantLogicalType::BIGNUM) {
			return Visitor::VisitBignum(decoded_string, std::forward<Args>(args)...);
		}
		if (type_id == VariantLogicalType::GEOMETRY) {
			return Visitor::VisitGeometry(decoded_string, std::forward<Args>(args)...);
		}
		if (type_id == VariantLogicalType::BITSTRING) {
			return Visitor::VisitBitstring(decoded_string, std::forward<Args>(args)...);
		}
		throw InternalException("String-backed variant type (%s) not handled", EnumUtil::ToString(type_id));
	}

	template <typename... Args>
	static ReturnType VisitDecimal(const UnifiedVariantVectorData &variant, idx_t row, uint32_t values_idx,
	                               Args &&...args) {
		auto decoded_decimal = VariantUtils::DecodeDecimalData(variant, row, values_idx);
		auto &width = decoded_decimal.width;
		auto &scale = decoded_decimal.scale;
		auto &ptr = decoded_decimal.value_ptr;
		if (width > DecimalWidth<hugeint_t>::max) {
			throw InternalException("Can't handle decimal of width: %d", width);
		} else if (width > DecimalWidth<int64_t>::max) {
			return Visitor::template VisitDecimal<hugeint_t>(Load<hugeint_t>(ptr), width, scale,
			                                                 std::forward<Args>(args)...);
		} else if (width > DecimalWidth<int32_t>::max) {
			return Visitor::template VisitDecimal<int64_t>(Load<int64_t>(ptr), width, scale,
			                                               std::forward<Args>(args)...);
		} else if (width > DecimalWidth<int16_t>::max) {
			return Visitor::template VisitDecimal<int32_t>(Load<int32_t>(ptr), width, scale,
			                                               std::forward<Args>(args)...);
		} else {
			return Visitor::template VisitDecimal<int16_t>(Load<int16_t>(ptr), width, scale,
			                                               std::forward<Args>(args)...);
		}
	}
};

} // namespace duckdb
