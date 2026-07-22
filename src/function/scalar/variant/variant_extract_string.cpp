#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/string_cast.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/function/scalar/variant_path_function.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "fmt/format.h"

namespace duckdb {

struct VariantStringExtractLocalState : public FunctionLocalState {
	string tmp;
	StringHeap heap;
};

class VariantStringSerializer {
public:
	explicit VariantStringSerializer(VariantStringExtractLocalState &state) : tmp(state.tmp), heap(state.heap) {
	}

public:
	//! Row result writer, modeled as part of `VariantStringSerializer` so we have easy access to function scoped state.
	void operator()(const optional<VariantNode> &node, VectorWriter<string_t> &string_writer) const;

private:
	template <class T>
	void AppendStringCast(T value, bool quote) const;
	void AppendPrimitiveString(const string &value, bool quote) const;
	void AppendPrimitiveString(const string_t &value, bool quote) const;
	void SerializePrimitive(const VariantNode &node, VariantLogicalType type, bool quote) const;
	static bool PrimitiveNeedsQuotes(VariantLogicalType type);
	void AppendJSONString(const char *data, idx_t size) const;
	void SerializeVariant(const VariantNode &node) const;
	void SerializeTopLevel(const VariantNode &node) const;

private:
	string &tmp;
	StringHeap &heap;
};

void VariantStringSerializer::AppendJSONString(const char *data, const idx_t size) const {
	static constexpr char HEX_DIGITS[] = "0123456789abcdef";
	// reserve for the no-escape needed case
	tmp.reserve(tmp.size() + size + 2);
	tmp += '"';
	idx_t start = 0;
	for (idx_t i = 0; i < size; i++) {
		const auto byte = static_cast<uint8_t>(data[i]);
		char escape = '\0';
		switch (byte) {
		case '"':
			escape = '"';
			break;
		case '\\':
			escape = '\\';
			break;
		case '\b':
			escape = 'b';
			break;
		case '\f':
			escape = 'f';
			break;
		case '\n':
			escape = 'n';
			break;
		case '\r':
			escape = 'r';
			break;
		case '\t':
			escape = 't';
			break;
		default:
			if (byte >= 0x20) {
				// keep collecting bytes until we reach a byte which needs escaping
				continue;
			}
		}
		// we've either encountered a character which should be escaped, or there are no more bytes, append what
		// we have collected until now
		tmp.append(data + start, i - start);
		if (escape) {
			tmp += '\\';
			tmp += escape;
		} else {
			// escape bytes < 0x20
			const char unicode_escape[] = {'\\', 'u', '0', '0', HEX_DIGITS[byte >> 4], HEX_DIGITS[byte & 0x0F]};
			tmp.append(unicode_escape, sizeof(unicode_escape));
		}
		start = i + 1;
	}
	if (start < size) {
		tmp.append(data + start, size - start);
	}
	tmp += '"';
}

template <class T>
void VariantStringSerializer::AppendStringCast(const T value, const bool quote) const {
	const auto val = StringCast::Operation(value, heap);
	if (quote) {
		AppendJSONString(val.GetData(), val.GetSize());
	} else {
		tmp.append(val.GetData(), val.GetSize());
	}
}

void VariantStringSerializer::AppendPrimitiveString(const string &value, const bool quote) const {
	if (quote) {
		AppendJSONString(value.data(), value.size());
	} else {
		tmp.append(value);
	}
}

void VariantStringSerializer::AppendPrimitiveString(const string_t &value, const bool quote) const {
	if (quote) {
		AppendJSONString(value.GetData(), value.GetSize());
	} else {
		tmp.append(value.GetData(), value.GetSize());
	}
}

void VariantStringSerializer::SerializePrimitive(const VariantNode &node, const VariantLogicalType type,
                                                 const bool quote) const {
	switch (type) {
	case VariantLogicalType::VARIANT_NULL:
		tmp.append("null");
		break;
	case VariantLogicalType::BOOL_TRUE:
		tmp.append("true");
		break;
	case VariantLogicalType::BOOL_FALSE:
		tmp.append("false");
		break;
	case VariantLogicalType::INT8:
		AppendStringCast(node.GetData<int8_t>(), quote);
		break;
	case VariantLogicalType::INT16:
		AppendStringCast(node.GetData<int16_t>(), quote);
		break;
	case VariantLogicalType::INT32:
		AppendStringCast(node.GetData<int32_t>(), quote);
		break;
	case VariantLogicalType::INT64:
		AppendStringCast(node.GetData<int64_t>(), quote);
		break;
	case VariantLogicalType::INT128:
		AppendStringCast(node.GetData<hugeint_t>(), quote);
		break;
	case VariantLogicalType::UINT8:
		AppendStringCast(node.GetData<uint8_t>(), quote);
		break;
	case VariantLogicalType::UINT16:
		AppendStringCast(node.GetData<uint16_t>(), quote);
		break;
	case VariantLogicalType::UINT32:
		AppendStringCast(node.GetData<uint32_t>(), quote);
		break;
	case VariantLogicalType::UINT64:
		AppendStringCast(node.GetData<uint64_t>(), quote);
		break;
	case VariantLogicalType::UINT128:
		AppendStringCast(node.GetData<uhugeint_t>(), quote);
		break;
	case VariantLogicalType::FLOAT:
		AppendStringCast(node.GetData<float>(), quote);
		break;
	case VariantLogicalType::DOUBLE:
		AppendStringCast(node.GetData<double>(), quote);
		break;
	case VariantLogicalType::DECIMAL: {
		const auto decimal = node.GetDecimal();
		switch (decimal.GetPhysicalType()) {
		case PhysicalType::INT16:
			AppendPrimitiveString(Decimal::ToString(node.GetData<int16_t>(), decimal.width, decimal.scale), quote);
			break;
		case PhysicalType::INT32:
			AppendPrimitiveString(Decimal::ToString(node.GetData<int32_t>(), decimal.width, decimal.scale), quote);
			break;
		case PhysicalType::INT64:
			AppendPrimitiveString(Decimal::ToString(node.GetData<int64_t>(), decimal.width, decimal.scale), quote);
			break;
		case PhysicalType::INT128:
			AppendPrimitiveString(Decimal::ToString(node.GetData<hugeint_t>(), decimal.width, decimal.scale), quote);
			break;
		default:
			throw InternalException("Unsupported VARIANT decimal physical type");
		}
		break;
	}
	case VariantLogicalType::VARCHAR: {
		const auto str = node.GetString();
		AppendPrimitiveString(str, quote);
		break;
	}
	case VariantLogicalType::BLOB: {
		const auto str = node.GetString();
		AppendPrimitiveString(Value::BLOB(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString(), quote);
		break;
	}
	case VariantLogicalType::UUID:
		AppendPrimitiveString(Value::UUID(node.GetData<hugeint_t>()).ToString(), quote);
		break;
	case VariantLogicalType::DATE:
		AppendStringCast(node.GetData<date_t>(), quote);
		break;
	case VariantLogicalType::TIME_MICROS:
		AppendStringCast(node.GetData<dtime_t>(), quote);
		break;
	case VariantLogicalType::TIME_NANOS:
		AppendPrimitiveString(Value::TIME_NS(node.GetData<dtime_ns_t>()).ToString(), quote);
		break;
	case VariantLogicalType::TIMESTAMP_SEC:
		AppendPrimitiveString(Value::TIMESTAMPSEC(node.GetData<timestamp_sec_t>()).ToString(), quote);
		break;
	case VariantLogicalType::TIMESTAMP_MILIS:
		AppendPrimitiveString(Value::TIMESTAMPMS(node.GetData<timestamp_ms_t>()).ToString(), quote);
		break;
	case VariantLogicalType::TIMESTAMP_MICROS:
		AppendStringCast(node.GetData<timestamp_t>(), quote);
		break;
	case VariantLogicalType::TIMESTAMP_NANOS:
		AppendPrimitiveString(Value::TIMESTAMPNS(node.GetData<timestamp_ns_t>()).ToString(), quote);
		break;
	case VariantLogicalType::TIME_MICROS_TZ:
		AppendPrimitiveString(Value::TIMETZ(node.GetData<dtime_tz_t>()).ToString(), quote);
		break;
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		AppendPrimitiveString(Value::TIMESTAMPTZ(node.GetData<timestamp_tz_t>()).ToString(), quote);
		break;
	case VariantLogicalType::INTERVAL:
		AppendStringCast(node.GetData<interval_t>(), quote);
		break;
	case VariantLogicalType::BIGNUM: {
		const auto str = node.GetString();
		AppendPrimitiveString(Value::BIGNUM(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString(), quote);
		break;
	}
	case VariantLogicalType::BITSTRING: {
		const auto str = node.GetString();
		AppendPrimitiveString(Value::BIT(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString(), quote);
		break;
	}
	case VariantLogicalType::GEOMETRY: {
		const auto str = node.GetString();
		AppendPrimitiveString(Value::GEOMETRY(const_data_ptr_cast(str.GetData()), str.GetSize()).ToString(), quote);
		break;
	}
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		AppendPrimitiveString(Value::TIMESTAMPTZNS(node.GetData<timestamp_tz_ns_t>()).ToString(), quote);
		break;
	default:
		throw NotImplementedException("Cannot stringify VARIANT type %s", EnumUtil::ToString(type));
	}
}

bool VariantStringSerializer::PrimitiveNeedsQuotes(const VariantLogicalType type) {
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

void VariantStringSerializer::SerializeVariant(const VariantNode &node) const {
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
			SerializeVariant(child);
		}
		tmp += ']';
		break;
	}
	case VariantLogicalType::OBJECT: {
		tmp += '{';
		bool first = true;
		for (const auto &[key, value] : node.GetObjectChildren(VariantIterationOrder::LEXICOGRAPHIC)) {
			if (!first) {
				tmp += ',';
			}
			first = false;
			AppendJSONString(key.GetData(), key.GetSize());
			tmp += ':';
			SerializeVariant(value);
		}
		tmp += '}';
		break;
	}
	default: {
		SerializePrimitive(node, type, PrimitiveNeedsQuotes(type));
		break;
	}
	}
}

void VariantStringSerializer::SerializeTopLevel(const VariantNode &node) const {
	const auto type = node.GetTypeId();
	if (type == VariantLogicalType::ARRAY || type == VariantLogicalType::OBJECT) {
		SerializeVariant(node);
	} else {
		SerializePrimitive(node, type, false);
	}
}

void VariantStringSerializer::operator()(const optional<VariantNode> &node,
                                         VectorWriter<string_t> &string_writer) const {
	// clean up per-row state
	tmp.clear();
	heap.GetAllocator().Reset();

	if (!node || node->GetTypeId() == VariantLogicalType::VARIANT_NULL) {
		string_writer.WriteNull();
		return;
	}
	// shortcut; write value directly into result
	if (node->GetTypeId() == VariantLogicalType::VARCHAR) {
		string_writer.WriteValue(node->GetString());
		return;
	}

	SerializeTopLevel(*node);
	string_writer.WriteValue(tmp);
}

static unique_ptr<FunctionLocalState> VariantExtractStringInit(ExpressionState &, const BoundFunctionExpression &,
                                                               FunctionData *) {
	return make_uniq<VariantStringExtractLocalState>();
}

static void VariantExtractStringFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<VariantStringExtractLocalState>();

	const VariantStringSerializer serializer(local_state);
	VariantPathFunction::Execute<string_t>(input, state, result, serializer);
}

ScalarFunctionSet VariantExtractStringFun::GetFunctions() {
	return VariantPathFunction::CreateFunctionSet("variant_extract_string", VariantExtractStringFunction,
	                                              LogicalType::VARCHAR, false, VariantExtractStringInit);
}

} // namespace duckdb
