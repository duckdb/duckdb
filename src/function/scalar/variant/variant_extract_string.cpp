#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/operator/convert_to_string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "fmt/printf.h"

namespace duckdb {

static string SerializePrimitive(const VariantNode &node) {
	switch (node.GetTypeId()) {
	case VariantLogicalType::VARIANT_NULL:
		return "null";
	case VariantLogicalType::BOOL_TRUE:
		return "true";
	case VariantLogicalType::BOOL_FALSE:
		return "false";
	case VariantLogicalType::INT8:
		return ConvertToString::Operation(node.GetData<int8_t>());
	case VariantLogicalType::INT16:
		return ConvertToString::Operation(node.GetData<int16_t>());
	case VariantLogicalType::INT32:
		return ConvertToString::Operation(node.GetData<int32_t>());
	case VariantLogicalType::INT64:
		return ConvertToString::Operation(node.GetData<int64_t>());
	case VariantLogicalType::INT128:
		return ConvertToString::Operation(node.GetData<hugeint_t>());
	case VariantLogicalType::UINT8:
		return ConvertToString::Operation(node.GetData<uint8_t>());
	case VariantLogicalType::UINT16:
		return ConvertToString::Operation(node.GetData<uint16_t>());
	case VariantLogicalType::UINT32:
		return ConvertToString::Operation(node.GetData<uint32_t>());
	case VariantLogicalType::UINT64:
		return ConvertToString::Operation(node.GetData<uint64_t>());
	case VariantLogicalType::UINT128:
		return ConvertToString::Operation(node.GetData<uhugeint_t>());
	case VariantLogicalType::FLOAT:
		return ConvertToString::Operation(node.GetData<float>());
	case VariantLogicalType::DOUBLE:
		return ConvertToString::Operation(node.GetData<double>());
	case VariantLogicalType::DECIMAL: {
		const auto decimal = node.GetDecimal();
		switch (decimal.GetPhysicalType()) {
		case PhysicalType::INT16:
			return Decimal::ToString(node.GetData<int16_t>(), decimal.width, decimal.scale);
		case PhysicalType::INT32:
			return Decimal::ToString(node.GetData<int32_t>(), decimal.width, decimal.scale);
		case PhysicalType::INT64:
			return Decimal::ToString(node.GetData<int64_t>(), decimal.width, decimal.scale);
		case PhysicalType::INT128:
			return Decimal::ToString(node.GetData<hugeint_t>(), decimal.width, decimal.scale);
		default:
			throw InternalException("Unsupported VARIANT decimal physical type");
		}
	}
	case VariantLogicalType::VARCHAR: {
		const auto str = node.GetString();
		return {str.GetData(), str.GetSize()};
	}
	case VariantLogicalType::BLOB: {
		const auto str = node.GetString();
		return Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString();
	}
	case VariantLogicalType::UUID:
		return Value::UUID(node.GetData<hugeint_t>()).ToString();
	case VariantLogicalType::DATE:
		return ConvertToString::Operation(node.GetData<date_t>());
	case VariantLogicalType::TIME_MICROS:
		return ConvertToString::Operation(node.GetData<dtime_t>());
	case VariantLogicalType::TIME_NANOS:
		return Value::TIME_NS(node.GetData<dtime_ns_t>()).ToString();
	case VariantLogicalType::TIMESTAMP_SEC:
		return Value::TIMESTAMPSEC(node.GetData<timestamp_sec_t>()).ToString();
	case VariantLogicalType::TIMESTAMP_MILIS:
		return Value::TIMESTAMPMS(node.GetData<timestamp_ms_t>()).ToString();
	case VariantLogicalType::TIMESTAMP_MICROS:
		return ConvertToString::Operation(node.GetData<timestamp_t>());
	case VariantLogicalType::TIMESTAMP_NANOS:
		return Value::TIMESTAMPNS(node.GetData<timestamp_ns_t>()).ToString();
	case VariantLogicalType::TIME_MICROS_TZ:
		return Value::TIMETZ(node.GetData<dtime_tz_t>()).ToString();
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return Value::TIMESTAMPTZ(node.GetData<timestamp_tz_t>()).ToString();
	case VariantLogicalType::INTERVAL:
		return ConvertToString::Operation(node.GetData<interval_t>());
	case VariantLogicalType::BIGNUM: {
		const auto str = node.GetString();
		return Value::BIGNUM(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString();
	}
	case VariantLogicalType::BITSTRING: {
		const auto str = node.GetString();
		return Value::BIT(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString();
	}
	case VariantLogicalType::GEOMETRY: {
		const auto str = node.GetString();
		return Value::GEOMETRY(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString();
	}
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		return Value::TIMESTAMPTZNS(node.GetData<timestamp_tz_ns_t>()).ToString();
	default:
		throw NotImplementedException("Cannot stringify VARIANT type %s", EnumUtil::ToString(node.GetTypeId()));
	}
}

static bool PrimitiveNeedsQuotes(const VariantLogicalType type) {
	switch (type) {
	case VariantLogicalType::VARCHAR:
	case VariantLogicalType::BLOB:
	case VariantLogicalType::UUID:
	case VariantLogicalType::DATE:
	case VariantLogicalType::TIME_MICROS:
	case VariantLogicalType::TIME_NANOS:
	case VariantLogicalType::TIME_MICROS_TZ:
	case VariantLogicalType::TIMESTAMP_SEC:
	case VariantLogicalType::TIMESTAMP_MILIS:
	case VariantLogicalType::TIMESTAMP_MICROS:
	case VariantLogicalType::TIMESTAMP_NANOS:
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
	case VariantLogicalType::INTERVAL:
	case VariantLogicalType::BITSTRING:
	case VariantLogicalType::GEOMETRY:
		return true;
	default:
		return false;
	}
}

static void AppendJSONString(string &result, const char *data, const idx_t size) {
	result += '"';
	for (idx_t i = 0; i < size; i++) {
		const auto byte = static_cast<uint8_t>(data[i]);
		switch (byte) {
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		default:
			if (byte < 0x20) {
				result += duckdb_fmt::sprintf("\\u%04x", static_cast<unsigned int>(byte));
			} else {
				result += static_cast<char>(byte);
			}
		}
	}
	result += '"';
}

static void AppendJSONString(string &result, const string &value) {
	AppendJSONString(result, value.data(), value.size());
}

static void AppendJSONString(string &result, const string_t &value) {
	AppendJSONString(result, value.GetData(), value.GetSize());
}

static void SerializeVariant(string &tmp, const VariantNode &node) {
	switch (node.GetTypeId()) {
	case VariantLogicalType::ARRAY: {
		tmp += '[';
		bool first = true;
		for (auto child : node.GetArrayChildren()) {
			if (!first) {
				tmp += ',';
			}
			first = false;
			SerializeVariant(tmp, child);
		}
		tmp += ']';
		break;
	}
	case VariantLogicalType::OBJECT: {
		tmp += '{';
		bool first = true;
		for (const auto &[key, value] : node.GetObjectChildren()) {
			if (!first) {
				tmp += ',';
			}
			first = false;
			AppendJSONString(tmp, key);
			tmp += ':';
			SerializeVariant(tmp, value);
		}
		tmp += '}';
		break;
	}
	default: {
		const auto primitive = SerializePrimitive(node);
		if (PrimitiveNeedsQuotes(node.GetTypeId())) {
			AppendJSONString(tmp, primitive);
		} else {
			tmp += primitive;
		}
		break;
	}
	}
}

static void WriteStringResult(const optional<VariantNode> &node, VectorWriter<string_t> &string_writer) {
	if (!node || node->GetTypeId() == VariantLogicalType::VARIANT_NULL) {
		string_writer.WriteNull();
		return;
	}

	string tmp;
	switch (node->GetTypeId()) {
	case VariantLogicalType::ARRAY:
	case VariantLogicalType::OBJECT:
		SerializeVariant(tmp, *node);
		break;
	default:
		tmp = SerializePrimitive(*node);
		break;
	}
	string_writer.WriteValue(tmp);
}

static void VariantExtractStringFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantPathFunction::Execute<string_t>(input, state, result, WriteStringResult);
}

ScalarFunctionSet VariantExtractStringFun::GetFunctions() {
	return VariantPathFunction::CreateFunctionSet("variant_extract_string", VariantExtractStringFunction,
	                                              LogicalType::VARCHAR, false);
}

} // namespace duckdb
