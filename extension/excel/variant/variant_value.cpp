#include "duckdb.hpp"

#include "variant_value.hpp"

namespace duckdb {

constexpr idx_t list_index_size = sizeof(variant_index_type);

static void store_index(variant_index_type data, char *buffer, idx_t idx = 0) {
	memcpy(buffer + idx * list_index_size, &data, sizeof(data));
}

static Value BufferToBlob(LogicalTypeId type_id, const void *data, idx_t size) {
	Value result = Value::BLOB(nullptr, 0);
	string &str_value = const_cast<string &>(StringValue::Get(result));
	str_value.reserve(size + 1);
	str_value.push_back(static_cast<uint8_t>(type_id));
	str_value.append((const char *)data, size);
	return result;
}

#define FIXED_VARIANT(TYPE, TYPE_ID)                                                                                   \
	template <>                                                                                                        \
	Value DUCKDB_API Variant(TYPE value) {                                                                             \
		return BufferToBlob(TYPE_ID, &value, sizeof(value));                                                           \
	}

FIXED_VARIANT(bool, LogicalTypeId::BOOLEAN)
FIXED_VARIANT(int8_t, LogicalTypeId::TINYINT)
FIXED_VARIANT(uint8_t, LogicalTypeId::UTINYINT)
FIXED_VARIANT(int16_t, LogicalTypeId::SMALLINT)
FIXED_VARIANT(uint16_t, LogicalTypeId::USMALLINT)
FIXED_VARIANT(int32_t, LogicalTypeId::INTEGER)
FIXED_VARIANT(uint32_t, LogicalTypeId::UINTEGER)
FIXED_VARIANT(int64_t, LogicalTypeId::BIGINT)
FIXED_VARIANT(uint64_t, LogicalTypeId::UBIGINT)
FIXED_VARIANT(hugeint_t, LogicalTypeId::HUGEINT)
FIXED_VARIANT(float, LogicalTypeId::FLOAT)
FIXED_VARIANT(double, LogicalTypeId::DOUBLE)
FIXED_VARIANT(date_t, LogicalTypeId::DATE)
FIXED_VARIANT(dtime_t, LogicalTypeId::TIME)
FIXED_VARIANT(timestamp_t, LogicalTypeId::TIMESTAMP)
FIXED_VARIANT(interval_t, LogicalTypeId::INTERVAL)

Value DUCKDB_API Variant(const char *value) {
	return BufferToBlob(LogicalTypeId::VARCHAR, value, strlen(value));
}

Value DUCKDB_API Variant(const string &value) {
	return BufferToBlob(LogicalTypeId::VARCHAR, value.data(), value.size());
}

static void TypeToBlob(const LogicalType &type, string &result) {
	LogicalTypeId type_id = type.id();
	result.push_back(static_cast<uint8_t>(type_id == LogicalTypeId::ENUM ? LogicalTypeId::VARCHAR : type_id));
	switch (type_id) {
	case LogicalTypeId::DECIMAL:
		result.push_back(DecimalType::GetWidth(type));
		result.push_back(DecimalType::GetScale(type));
		break;
	case LogicalTypeId::LIST:
		TypeToBlob(ListType::GetChildType(type), result);
		break;
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP: {
		auto &list = StructType::GetChildTypes(type);
		idx_t offsets = result.size();
		result.resize(offsets + MaxValue<idx_t>(list.size(), 1) * list_index_size);
		for (idx_t i = 0; i < list.size(); ++i) {
			store_index(result.size() - offsets, &result[offsets], i);
			auto &v = list[i];
			variant_index_type size = (variant_index_type)v.first.size();
			result.append((const char *)&size, list_index_size);
			result += v.first;
			TypeToBlob(v.second, result);
		}
		break;
	}
	}
}

static const char *GetBufferDumbWay(const Value &value) {
	Value &v = const_cast<Value &>(value);
	switch (value.type().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return (const char *)&v.GetReferenceUnsafe<int8_t>();
	case PhysicalType::UINT8:
		return (const char *)&v.GetReferenceUnsafe<uint8_t>();
	case PhysicalType::INT16:
		return (const char *)&v.GetReferenceUnsafe<int16_t>();
	case PhysicalType::UINT16:
		return (const char *)&v.GetReferenceUnsafe<uint16_t>();
	case PhysicalType::INT32:
		return (const char *)&v.GetReferenceUnsafe<int32_t>();
	case PhysicalType::UINT32:
		return (const char *)&v.GetReferenceUnsafe<uint32_t>();
	case PhysicalType::INT64:
		return (const char *)&v.GetReferenceUnsafe<int64_t>();
	case PhysicalType::UINT64:
		return (const char *)&v.GetReferenceUnsafe<uint64_t>();
	case PhysicalType::INT128:
		return (const char *)&v.GetReferenceUnsafe<hugeint_t>();
	case PhysicalType::FLOAT:
		return (const char *)&v.GetReferenceUnsafe<float>();
	case PhysicalType::DOUBLE:
		return (const char *)&v.GetReferenceUnsafe<double>();
	case PhysicalType::INTERVAL:
		return (const char *)&v.GetReferenceUnsafe<interval_t>();
	default:
		throw InvalidTypeException(value.type(), "Invalid physical type to encode as variant");
	}
}

static idx_t TypeSize(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return sizeof(bool);
	case PhysicalType::INT8:
	case PhysicalType::UINT8:
		return 1;
	case PhysicalType::INT16:
	case PhysicalType::UINT16:
		return 2;
	case PhysicalType::INT32:
	case PhysicalType::UINT32:
		return 4;
	case PhysicalType::INT64:
	case PhysicalType::UINT64:
		return 8;
	case PhysicalType::INT128:
		return sizeof(hugeint_t);
	case PhysicalType::FLOAT:
		return sizeof(float);
	case PhysicalType::DOUBLE:
		return sizeof(double);
	case PhysicalType::INTERVAL:
		return sizeof(interval_t);
	}
	return 0;
}

static void ValueToBlob(const Value &value, string &result) {
	auto &type = value.type();
	switch (type.InternalType()) {

	case PhysicalType::VARCHAR:
		result += StringValue::Get(value);
		return;

	case PhysicalType::LIST: {
		auto &list = ListValue::GetChildren(value);
		variant_index_type list_size = (variant_index_type)list.size();
		if (list_size == 0) {
			return;
		}
		result.append((const char *)&list_size, list_index_size);
		idx_t bitmap = result.size();
		result.resize(bitmap + (list.size() + 7) / 8);
		auto &child_type = ListType::GetChildType(type);
		idx_t child_size = TypeSize(child_type);
		if (child_size != 0 && child_type.id() != LogicalTypeId::ENUM) {
			result.reserve(result.size() + list.size() * child_size);
			for (idx_t i = 0; i < list.size(); ++i) {
				auto &v = list[i];
				if (v.IsNull()) {
					result[bitmap + i / 8] |= 1 << (i % 8);
					result.append(child_size, '\0');
				} else {
					D_ASSERT(v.type() == child_type);
					result.append(GetBufferDumbWay(v), child_size);
				}
			}
			return;
		}
		idx_t offsets = result.size();
		idx_t offsets_size = (list.size() + 1) * list_index_size;
		result.resize(offsets + offsets_size);
		store_index(offsets_size, &result[offsets]);
		bool is_any = child_type.id() == LogicalTypeId::ANY;
		for (idx_t i = 0; i < list.size(); ++i) {
			auto &v = list[i];
			if (v.IsNull()) {
				result[bitmap + i / 8] |= 1 << i % 8;
			} else {
				if (is_any) {
					TypeToBlob(v.type(), result);
				} else {
					D_ASSERT(v.type() == child_type);
				}
				ValueToBlob(v, result);
			}
			store_index(result.size() - offsets, &result[offsets], i + 1);
		}
		return;
	}

	case PhysicalType::STRUCT: {
		auto &list = StructValue::GetChildren(value);
		if (list.empty()) {
			return;
		}
		idx_t bitmap = result.size();
		result.resize(bitmap + (list.size() + 7) / 8);
		idx_t offsets = result.size();
		idx_t offsets_size = (list.size() + 1) * list_index_size;
		result.resize(offsets + offsets_size);
		store_index(offsets_size, &result[offsets]);
		auto &child_types = StructType::GetChildTypes(type);
		for (idx_t i = 0; i < list.size(); ++i) {
			auto &v = list[i];
			if (v.IsNull()) {
				result[bitmap + i / 8] |= 1 << i % 8;
			} else {
				if (child_types[i].second.id() == LogicalTypeId::ANY) {
					TypeToBlob(v.type(), result);
				} else {
					D_ASSERT(v.type() == child_types[i].second);
				}
				ValueToBlob(v, result);
			}
			store_index(result.size() - offsets, &result[offsets], i + 1);
		}
		return;
	}
	}

	if (type.id() == LogicalTypeId::ENUM) {
		result += value.ToString();
		return;
	}

	idx_t size = TypeSize(type);
	if (size == 0) {
		throw InvalidTypeException(type, "Cannot encode type as variant");
	}
	result.append(GetBufferDumbWay(value), size);
}

Value DUCKDB_API Variant(const Value &value) {
	if (value.IsNull()) {
		return Value(LogicalType::BLOB);
	}
	Value result = Value::BLOB(nullptr, 0);
	string &str_value = const_cast<string &>(StringValue::Get(result));
	TypeToBlob(value.type(), str_value);
	ValueToBlob(value, str_value);
	return result;
}

[[noreturn]] static void BadVariant() {
	throw InvalidInputException("Invalid Variant value");
}

static variant_index_type load_index(const char *buffer, idx_t idx = 0) {
	variant_index_type data;
	memcpy(&data, buffer + idx * list_index_size, sizeof(data));
	return data;
}

static LogicalType BlobToType(const char *&begin, const char *end) {
	if (begin >= end) {
		BadVariant();
	}
	LogicalTypeId type_id = static_cast<LogicalTypeId>(*begin++);
	switch (type_id) {
	case LogicalTypeId::DECIMAL: {
		if (begin + 2 > end) {
			BadVariant();
		}
		uint8_t width = begin[0];
		uint8_t scale = begin[1];
		begin += 2;
		return LogicalType::DECIMAL(width, scale);
	}
	case LogicalTypeId::LIST:
		return LogicalType::LIST(BlobToType(begin, end));
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP: {
		if (begin + list_index_size > end) {
			BadVariant();
		}
		const char *start = begin;
		idx_t list_size = load_index(start) / list_index_size;
		child_list_t<LogicalType> child_types;
		if (list_size == 0) {
			begin += 4;
		} else {
			child_types.reserve(list_size);
			for (idx_t i = 0; i < list_size; ++i) {
				begin = start + load_index(start, i);
				if (begin + list_index_size > end) {
					BadVariant();
				}
				variant_index_type key_size = load_index(begin);
				begin += list_index_size;
				if (begin + key_size > end) {
					BadVariant();
				}
				string key = string(begin, key_size);
				begin += key_size;
				child_types.push_back({move(key), BlobToType(begin, end)});
			}
		}
		return type_id == LogicalTypeId::STRUCT ? LogicalType::STRUCT(move(child_types))
		                                        : LogicalType::MAP(move(child_types));
	}
	}
	return type_id;
}

static Value BlobToFixed(const LogicalType &type, const char *begin) {
	Value v = Value::INTEGER(0);
	const_cast<LogicalType &>(v.type()) = type;
	switch (type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		memcpy(&v.GetReferenceUnsafe<int8_t>(), begin, sizeof(int8_t));
		break;
	case PhysicalType::UINT8:
		memcpy(&v.GetReferenceUnsafe<uint8_t>(), begin, sizeof(uint8_t));
		break;
	case PhysicalType::INT16:
		memcpy(&v.GetReferenceUnsafe<int16_t>(), begin, sizeof(int16_t));
		break;
	case PhysicalType::UINT16:
		memcpy(&v.GetReferenceUnsafe<uint16_t>(), begin, sizeof(uint16_t));
		break;
	case PhysicalType::INT32:
		memcpy(&v.GetReferenceUnsafe<int32_t>(), begin, sizeof(int32_t));
		break;
	case PhysicalType::UINT32:
		memcpy(&v.GetReferenceUnsafe<uint32_t>(), begin, sizeof(uint32_t));
		break;
	case PhysicalType::INT64:
		memcpy(&v.GetReferenceUnsafe<int64_t>(), begin, sizeof(int64_t));
		break;
	case PhysicalType::UINT64:
		memcpy(&v.GetReferenceUnsafe<uint64_t>(), begin, sizeof(uint64_t));
		break;
	case PhysicalType::INT128:
		memcpy(&v.GetReferenceUnsafe<hugeint_t>(), begin, sizeof(hugeint_t));
		break;
	case PhysicalType::FLOAT:
		memcpy(&v.GetReferenceUnsafe<float>(), begin, sizeof(float));
		break;
	case PhysicalType::DOUBLE:
		memcpy(&v.GetReferenceUnsafe<double>(), begin, sizeof(double));
		break;
	case PhysicalType::INTERVAL:
		memcpy(&v.GetReferenceUnsafe<interval_t>(), begin, sizeof(interval_t));
		break;
	default:
		BadVariant();
	}
	return v;
}

static Value BlobToValue(const char *begin, const char *end, const LogicalType &type) {
	D_ASSERT(begin <= end);
	idx_t blob_size = end - begin;

	switch (type.id()) {

	case LogicalTypeId::VARCHAR:
		return Value(string(begin, blob_size));
	case LogicalTypeId::JSON:
		return Value::JSON(string(begin, blob_size));
	case LogicalTypeId::BLOB:
		return Value::BLOB((const uint8_t *)begin, blob_size);

	case LogicalTypeId::LIST: {
		auto &child_type = ListType::GetChildType(type);
		Value result = Value::EMPTYLIST(child_type);
		if (blob_size == 0) {
			return result;
		}
		if (blob_size < list_index_size) {
			BadVariant();
		}
		idx_t list_size = load_index(begin);
		auto &list = const_cast<vector<Value> &>(ListValue::GetChildren(result));
		list.reserve(list_size);
		idx_t child_size = TypeSize(child_type);
		const char *bitmap = begin += list_index_size;
		begin += (list_size + 7) / 8;
		if (child_size != 0) {
			if (begin + list_size * child_size != end) {
				BadVariant();
			}
			for (idx_t i = 0; i < list_size; ++i) {
				if (!(bitmap[i / 8] & (1 << i % 8))) {
					list.push_back(BlobToFixed(child_type, begin));
				} else {
					list.emplace_back(child_type);
				}
				begin += child_size;
			}
			return result;
		}
		const char *start = begin;
		if (start + (list_size + 1) * list_index_size > end) {
			BadVariant();
		}
		bool is_any = child_type.id() == LogicalTypeId::ANY;
		for (idx_t i = 0; i < list_size; ++i) {
			if (bitmap[i / 8] & (1 << i % 8)) {
				list.push_back(is_any ? Value() : Value(child_type));
			} else {
				begin = start + load_index(start, i);
				const char *v_end = start + load_index(start, i + 1);
				if (v_end > end || begin > end) {
					BadVariant();
				}
				if (is_any) {
					LogicalType elem_type = BlobToType(begin, v_end);
					list.push_back(BlobToValue(begin, v_end, elem_type));
				} else {
					list.push_back(BlobToValue(begin, v_end, child_type));
				}
			}
		}
		return result;
	}

	case LogicalTypeId::MAP:
	case LogicalTypeId::STRUCT: {
		Value result = Value::INTEGER(0);
		const_cast<LogicalType &>(result.type()) = type;
		if (blob_size == 0) {
			return result;
		}
		auto &child_types = StructType::GetChildTypes(type);
		idx_t list_size = child_types.size();
		auto &list = const_cast<vector<Value> &>(StructValue::GetChildren(result));
		list.reserve(list_size);
		const char *bitmap = begin;
		begin += (list_size + 7) / 8;
		const char *start = begin;
		if (start + (list_size + 1) * list_index_size > end) {
			BadVariant();
		}
		for (idx_t i = 0; i < list_size; ++i) {
			auto &child_type = child_types[i].second;
			if (bitmap[i / 8] & (1 << i % 8)) {
				list.push_back(child_type.id() == LogicalTypeId::ANY ? Value() : Value(child_type));
			} else {
				begin = start + load_index(start, i);
				const char *v_end = start + load_index(start, i + 1);
				if (v_end > end || begin > end) {
					BadVariant();
				}
				if (child_type.id() == LogicalTypeId::ANY) {
					LogicalType elem_type = BlobToType(begin, v_end);
					list.push_back(BlobToValue(begin, v_end, elem_type));
				} else {
					list.push_back(BlobToValue(begin, v_end, child_type));
				}
			}
		}
		return result;
	}

	default:
		if (blob_size != TypeSize(type) || blob_size == 0) {
			BadVariant();
		}
		return BlobToFixed(type, begin);
	}
}

Value DUCKDB_API FromVariant(const Value &value) {
	if (value.IsNull()) {
		return Value();
	}
	if (value.type().id() != LogicalTypeId::BLOB) {
		throw InvalidTypeException(value.type(), "Variant requires BLOB type");
	}
	const string &str_value = StringValue::Get(value);
	const char *begin = str_value.data();
	const char *end = begin + str_value.size();
	LogicalType type = BlobToType(begin, end);
	return BlobToValue(begin, end, type);
}

} // namespace duckdb
