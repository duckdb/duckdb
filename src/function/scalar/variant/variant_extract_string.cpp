#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "fmt/printf.h"

namespace duckdb {

static void AppendJSONString(string &result, const string &value);
static void AppendJSONString(string &result, const string_t &value);

template <class T>
static void AppendStringCast(string &result, const T value, StringHeap &heap, const bool add_quotes) {
	const auto val = StringCast::Operation(value, heap);
	if (add_quotes) {
		AppendJSONString(result, val);
	} else {
		result.append(val.GetData(), val.GetSize());
	}
}

static void AppendPrimitiveString(string &result, const string &value, const bool add_quotes) {
	if (add_quotes) {
		AppendJSONString(result, value);
	} else {
		result.append(value);
	}
}

static void AppendPrimitiveString(string &result, const string_t &value, const bool add_quotes) {
	if (add_quotes) {
		AppendJSONString(result, value);
	} else {
		result.append(value.GetData(), value.GetSize());
	}
}

static void SerializePrimitive(string &result, const VariantNode &node, const VariantLogicalType type, StringHeap &heap,
                               const bool add_quotes) {
	switch (type) {
	case VariantLogicalType::VARIANT_NULL:
		result.append("null");
		break;
	case VariantLogicalType::BOOL_TRUE:
		result.append("true");
		break;
	case VariantLogicalType::BOOL_FALSE:
		result.append("false");
		break;
	case VariantLogicalType::INT8:
		AppendStringCast(result, node.GetData<int8_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::INT16:
		AppendStringCast(result, node.GetData<int16_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::INT32:
		AppendStringCast(result, node.GetData<int32_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::INT64:
		AppendStringCast(result, node.GetData<int64_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::INT128:
		AppendStringCast(result, node.GetData<hugeint_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::UINT8:
		AppendStringCast(result, node.GetData<uint8_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::UINT16:
		AppendStringCast(result, node.GetData<uint16_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::UINT32:
		AppendStringCast(result, node.GetData<uint32_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::UINT64:
		AppendStringCast(result, node.GetData<uint64_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::UINT128:
		AppendStringCast(result, node.GetData<uhugeint_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::FLOAT:
		AppendStringCast(result, node.GetData<float>(), heap, add_quotes);
		break;
	case VariantLogicalType::DOUBLE:
		AppendStringCast(result, node.GetData<double>(), heap, add_quotes);
		break;
	case VariantLogicalType::DECIMAL: {
		const auto decimal = node.GetDecimal();
		switch (decimal.GetPhysicalType()) {
		case PhysicalType::INT16:
			AppendPrimitiveString(result, Decimal::ToString(node.GetData<int16_t>(), decimal.width, decimal.scale),
			                      add_quotes);
			break;
		case PhysicalType::INT32:
			AppendPrimitiveString(result, Decimal::ToString(node.GetData<int32_t>(), decimal.width, decimal.scale),
			                      add_quotes);
			break;
		case PhysicalType::INT64:
			AppendPrimitiveString(result, Decimal::ToString(node.GetData<int64_t>(), decimal.width, decimal.scale),
			                      add_quotes);
			break;
		case PhysicalType::INT128:
			AppendPrimitiveString(result, Decimal::ToString(node.GetData<hugeint_t>(), decimal.width, decimal.scale),
			                      add_quotes);
			break;
		default:
			throw InternalException("Unsupported VARIANT decimal physical type");
		}
		break;
	}
	case VariantLogicalType::VARCHAR: {
		const auto str = node.GetString();
		AppendPrimitiveString(result, str, add_quotes);
		break;
	}
	case VariantLogicalType::BLOB: {
		const auto str = node.GetString();
		AppendPrimitiveString(result, Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString(),
		                      add_quotes);
		break;
	}
	case VariantLogicalType::UUID:
		AppendPrimitiveString(result, Value::UUID(node.GetData<hugeint_t>()).ToString(), add_quotes);
		break;
	case VariantLogicalType::DATE:
		AppendStringCast(result, node.GetData<date_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::TIME_MICROS:
		AppendStringCast(result, node.GetData<dtime_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::TIME_NANOS:
		AppendPrimitiveString(result, Value::TIME_NS(node.GetData<dtime_ns_t>()).ToString(), add_quotes);
		break;
	case VariantLogicalType::TIMESTAMP_SEC:
		AppendPrimitiveString(result, Value::TIMESTAMPSEC(node.GetData<timestamp_sec_t>()).ToString(), add_quotes);
		break;
	case VariantLogicalType::TIMESTAMP_MILIS:
		AppendPrimitiveString(result, Value::TIMESTAMPMS(node.GetData<timestamp_ms_t>()).ToString(), add_quotes);
		break;
	case VariantLogicalType::TIMESTAMP_MICROS:
		AppendStringCast(result, node.GetData<timestamp_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::TIMESTAMP_NANOS:
		AppendPrimitiveString(result, Value::TIMESTAMPNS(node.GetData<timestamp_ns_t>()).ToString(), add_quotes);
		break;
	case VariantLogicalType::TIME_MICROS_TZ:
		AppendPrimitiveString(result, Value::TIMETZ(node.GetData<dtime_tz_t>()).ToString(), add_quotes);
		break;
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		AppendPrimitiveString(result, Value::TIMESTAMPTZ(node.GetData<timestamp_tz_t>()).ToString(), add_quotes);
		break;
	case VariantLogicalType::INTERVAL:
		AppendStringCast(result, node.GetData<interval_t>(), heap, add_quotes);
		break;
	case VariantLogicalType::BIGNUM: {
		const auto str = node.GetString();
		AppendPrimitiveString(result, Value::BIGNUM(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString(),
		                      add_quotes);
		break;
	}
	case VariantLogicalType::BITSTRING: {
		const auto str = node.GetString();
		AppendPrimitiveString(result, Value::BIT(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString(),
		                      add_quotes);
		break;
	}
	case VariantLogicalType::GEOMETRY: {
		const auto str = node.GetString();
		AppendPrimitiveString(result, Value::GEOMETRY(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString(),
		                      add_quotes);
		break;
	}
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		AppendPrimitiveString(result, Value::TIMESTAMPTZNS(node.GetData<timestamp_tz_ns_t>()).ToString(), add_quotes);
		break;
	default:
		throw NotImplementedException("Cannot stringify VARIANT type %s", EnumUtil::ToString(type));
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

static void SerializeVariant(string &tmp, const VariantNode &node, StringHeap &heap) {
	const auto type = node.GetTypeId();
	switch (type) {
	case VariantLogicalType::ARRAY: {
		tmp += '[';
		bool first = true;
		for (auto child : node.GetArrayChildren()) {
			if (!first) {
				tmp += ',';
			}
			first = false;
			SerializeVariant(tmp, child, heap);
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
			SerializeVariant(tmp, value, heap);
		}
		tmp += '}';
		break;
	}
	default: {
		SerializePrimitive(tmp, node, type, heap, PrimitiveNeedsQuotes(type));
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
	StringHeap heap;
	const auto type = node->GetTypeId();
	switch (type) {
	case VariantLogicalType::ARRAY:
	case VariantLogicalType::OBJECT:
		SerializeVariant(tmp, *node, heap);
		break;
	default:
		SerializePrimitive(tmp, *node, type, heap, false);
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
