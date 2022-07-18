#include "duckdb/common/types.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Forward declaration for recursion
static LogicalType StructureToType(yyjson_val *val);

static LogicalType StructureToTypeArray(yyjson_val *arr) {
	if (yyjson_arr_size(arr) != 1) {
		throw InvalidInputException("Too many values in array of JSON structure");
	}
	return LogicalType::LIST(StructureToType(yyjson_arr_get_first(arr)));
}

static LogicalType StructureToTypeObject(yyjson_val *obj) {
	unordered_set<string> names;
	child_list_t<LogicalType> child_types;
	size_t idx, max;
	yyjson_val *key, *val;
	yyjson_obj_foreach(obj, idx, max, key, val) {
		val = yyjson_obj_iter_get_val(key);
		auto key_str = yyjson_get_str(key);
		if (names.find(key_str) != names.end()) {
			JSONCommon::ThrowValFormatError("Duplicate keys in object in JSON structure: %s", val);
		}
		names.insert(key_str);
		child_types.emplace_back(key_str, StructureToType(val));
	}
	D_ASSERT(yyjson_obj_size(obj) == names.size());
	return LogicalType::STRUCT(child_types);
}

static LogicalType StructureToType(yyjson_val *val) {
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
		return StructureToTypeArray(val);
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		return StructureToTypeObject(val);
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		return TransformStringToLogicalType(yyjson_get_str(val));
	default:
		throw InvalidInputException("invalid JSON structure");
	}
}

static unique_ptr<FunctionData> JSONTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (arguments[1]->return_type == LogicalTypeId::SQLNULL) {
		bound_function.return_type = LogicalTypeId::SQLNULL;
	} else if (!arguments[1]->IsFoldable()) {
		throw InvalidInputException("JSON structure must be a constant!");
	} else {
		auto structure_val = ExpressionExecutor::EvaluateScalar(*arguments[1]);
		if (!structure_val.TryCastAs(LogicalType::JSON)) {
			throw InvalidInputException("cannot cast JSON structure to string");
		}
		auto structure_string = structure_val.GetValueUnsafe<string_t>();
		auto doc = JSONCommon::ReadDocumentUnsafe(structure_string);
		if (doc.IsNull()) {
			throw InvalidInputException("malformed JSON structure");
		}
		bound_function.return_type = StructureToType(doc->root);
	}
	return make_unique<VariableReturnBindData>(bound_function.return_type);
}

static inline string_t GetString(yyjson_val *val) {
	return string_t(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
}

template <class T, class OP = TryCast>
static inline bool GetValueNumerical(yyjson_val *val, T &result, bool strict) {
	bool success;
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		return false;
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		success = OP::template Operation<string_t, T>(GetString(val), result, strict);
		break;
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		success = false;
		break;
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
		success = OP::template Operation<bool, T>(unsafe_yyjson_get_bool(val), result, strict);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
		success = OP::template Operation<uint64_t, T>(unsafe_yyjson_get_uint(val), result, strict);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
		success = OP::template Operation<int64_t, T>(unsafe_yyjson_get_sint(val), result, strict);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
		success = OP::template Operation<double, T>(unsafe_yyjson_get_real(val), result, strict);
		break;
	default:
		throw InternalException("Unknown yyjson tag in GetValueNumerical");
	}
	if (!success && strict) {
		JSONCommon::ThrowValFormatError("Failed to cast value to numerical: %s", val);
	}
	return success;
}

template <class T, class OP = TryCastToDecimal>
static inline bool GetValueDecimal(yyjson_val *val, T &result, uint8_t w, uint8_t s, bool strict) {
	bool success;
	string error_message;
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		return false;
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		success = OP::template Operation<string_t, T>(GetString(val), result, &error_message, w, s);
		break;
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		success = false;
		break;
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
		success = OP::template Operation<bool, T>(unsafe_yyjson_get_bool(val), result, &error_message, w, s);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
		success = OP::template Operation<uint64_t, T>(unsafe_yyjson_get_uint(val), result, &error_message, w, s);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
		success = OP::template Operation<int64_t, T>(unsafe_yyjson_get_sint(val), result, &error_message, w, s);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
		success = OP::template Operation<double, T>(unsafe_yyjson_get_real(val), result, &error_message, w, s);
		break;
	default:
		throw InternalException("Unknown yyjson tag in GetValueString");
	}
	if (!success && strict) {
		JSONCommon::ThrowValFormatError("Failed to cast value to numerical: %s", val);
	}
	return success;
}

static inline bool GetValueString(yyjson_val *val, string_t &result, Vector &vector) {
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		return false;
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		result = StringVector::AddString(vector, unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
		return true;
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		result = JSONCommon::WriteVal(val, vector);
		return true;
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
		result = StringCast::Operation<bool>(unsafe_yyjson_get_bool(val), vector);
		return true;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
		result = StringCast::Operation<uint64_t>(unsafe_yyjson_get_uint(val), vector);
		return true;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
		result = StringCast::Operation<int64_t>(unsafe_yyjson_get_sint(val), vector);
		return true;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
		result = StringCast::Operation<double>(unsafe_yyjson_get_real(val), vector);
		return true;
	default:
		throw InternalException("Unknown yyjson tag in GetValueString");
	}
}

//! Forward declaration for recursion
static void Transform(yyjson_val *vals[], Vector &result, const idx_t count, bool strict);

template <class T>
static void TransformNumerical(yyjson_val *vals[], Vector &result, const idx_t count, const bool strict) {
	auto data = (T *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || !GetValueNumerical<T>(val, data[i], strict)) {
			validity.SetInvalid(i);
		}
	}
}

template <class T>
static void TransformDecimal(yyjson_val *vals[], Vector &result, const idx_t count, uint8_t width, uint8_t scale,
                             const bool strict) {
	auto data = (T *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || !GetValueDecimal<T>(val, data[i], width, scale, strict)) {
			validity.SetInvalid(i);
		}
	}
}

static void TransformFromString(yyjson_val *vals[], Vector &result, const idx_t count, const LogicalType &target,
                                const bool strict) {
	Vector string_vector(LogicalTypeId::VARCHAR, count);
	auto data = (string_t *)FlatVector::GetData(string_vector);
	auto &validity = FlatVector::Validity(string_vector);

	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || yyjson_is_null(val)) {
			validity.SetInvalid(i);
		} else if (strict && !yyjson_is_str(val)) {
			JSONCommon::ThrowValFormatError("Unable to cast '%s' to " + LogicalTypeIdToString(target.id()), val);
		} else {
			data[i] = StringVector::AddString(string_vector, yyjson_get_str(val), yyjson_get_len(val));
		}
	}

	string error_message;
	if (!VectorOperations::TryCast(string_vector, result, count, &error_message, strict) && strict) {
		throw InvalidInputException(error_message);
	}
}

static void TransformToString(yyjson_val *vals[], Vector &result, const idx_t count) {
	auto data = (string_t *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || !GetValueString(val, data[i], result)) {
			validity.SetInvalid(i);
		}
	}
}

static void TransformObject(yyjson_val *vals[], Vector &result, const idx_t count, const LogicalType &type,
                            bool strict) {
	// Initialize array for the nested values
	auto nested_vals_ptr = unique_ptr<yyjson_val *[]>(new yyjson_val *[count]);
	auto nested_vals = nested_vals_ptr.get();
	// Loop through child types
	auto &child_vs = StructVector::GetEntries(result);
	for (idx_t child_i = 0; child_i < child_vs.size(); child_i++) {
		const auto &name = StructType::GetChildName(type, child_i);
		auto name_ptr = name.c_str();
		auto name_len = name.size();
		for (idx_t i = 0; i < count; i++) {
			nested_vals[i] = yyjson_obj_getn(vals[i], name_ptr, name_len);
		}
		// Transform child values
		Transform(nested_vals, *child_vs[child_i], count, strict);
	}
}

static void TransformArray(yyjson_val *vals[], Vector &result, const idx_t count, bool strict) {
	// Initialize list vector
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &list_validity = FlatVector::Validity(result);
	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!vals[i] || yyjson_is_null(vals[i])) {
			list_validity.SetInvalid(i);
		}
		auto &entry = list_entries[i];
		entry.offset = offset;
		entry.length = yyjson_arr_size(vals[i]);
		offset += entry.length;
	}
	ListVector::SetListSize(result, offset);
	ListVector::Reserve(result, offset);
	// Initialize array for the nested values
	auto nested_vals_ptr = unique_ptr<yyjson_val *[]>(new yyjson_val *[offset]);
	auto nested_vals = nested_vals_ptr.get();
	// Get array values
	size_t idx, max;
	yyjson_val *val;
	idx_t list_i = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!list_validity.RowIsValid(i)) {
			// We already marked this as invalid
			continue;
		}
		yyjson_arr_foreach(vals[i], idx, max, val) {
			nested_vals[list_i] = val;
			list_i++;
		}
	}
	D_ASSERT(list_i == offset);
	// Transform array values
	Transform(nested_vals, ListVector::GetEntry(result), offset, strict);
}

static void Transform(yyjson_val *vals[], Vector &result, const idx_t count, bool strict) {
	auto result_type = result.GetType();
	switch (result_type.id()) {
	case LogicalTypeId::SQLNULL:
		return;
	case LogicalTypeId::BOOLEAN:
		return TransformNumerical<bool>(vals, result, count, strict);
	case LogicalTypeId::TINYINT:
		return TransformNumerical<int8_t>(vals, result, count, strict);
	case LogicalTypeId::SMALLINT:
		return TransformNumerical<int16_t>(vals, result, count, strict);
	case LogicalTypeId::INTEGER:
		return TransformNumerical<int32_t>(vals, result, count, strict);
	case LogicalTypeId::BIGINT:
		return TransformNumerical<int64_t>(vals, result, count, strict);
	case LogicalTypeId::UTINYINT:
		return TransformNumerical<uint8_t>(vals, result, count, strict);
	case LogicalTypeId::USMALLINT:
		return TransformNumerical<uint16_t>(vals, result, count, strict);
	case LogicalTypeId::UINTEGER:
		return TransformNumerical<uint32_t>(vals, result, count, strict);
	case LogicalTypeId::UBIGINT:
		return TransformNumerical<uint64_t>(vals, result, count, strict);
	case LogicalTypeId::HUGEINT:
		return TransformNumerical<hugeint_t>(vals, result, count, strict);
	case LogicalTypeId::FLOAT:
		return TransformNumerical<float>(vals, result, count, strict);
	case LogicalTypeId::DOUBLE:
		return TransformNumerical<double>(vals, result, count, strict);
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(result_type);
		auto scale = DecimalType::GetScale(result_type);
		switch (result_type.InternalType()) {
		case PhysicalType::INT16:
			return TransformDecimal<int16_t>(vals, result, count, width, scale, strict);
		case PhysicalType::INT32:
			return TransformDecimal<int32_t>(vals, result, count, width, scale, strict);
		case PhysicalType::INT64:
			return TransformDecimal<int64_t>(vals, result, count, width, scale, strict);
		case PhysicalType::INT128:
			return TransformDecimal<hugeint_t>(vals, result, count, width, scale, strict);
		default:
			throw InternalException("Unimplemented physical type for decimal");
		}
	}
	case LogicalTypeId::AGGREGATE_STATE:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::DATE:
	case LogicalTypeId::INTERVAL:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::UUID:
		return TransformFromString(vals, result, count, result_type, strict);
	case LogicalTypeId::JSON:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return TransformToString(vals, result, count);
	case LogicalTypeId::STRUCT:
		return TransformObject(vals, result, count, result_type, strict);
	case LogicalTypeId::LIST:
		return TransformArray(vals, result, count, strict);
	default:
		throw InternalException("Unexpected type at JSON Transform %s", LogicalTypeIdToString(result_type.id()));
	}
}

template <bool strict>
static void TransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto &input = args.data[0];
	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);
	auto inputs = (string_t *)input_data.data;
	// Read documents
	vector<DocPointer<yyjson_doc>> docs;
	docs.reserve(count);
	yyjson_val *vals[STANDARD_VECTOR_SIZE];
	auto &result_validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			docs.emplace_back(nullptr);
			vals[i] = nullptr;
			result_validity.SetInvalid(i);
		} else {
			docs.emplace_back(JSONCommon::ReadDocument(inputs[idx]));
			vals[i] = docs.back()->root;
		}
	}
	// Transform
	Transform(vals, result, count, strict);
}

CreateScalarFunctionInfo JSONFunctions::GetTransformFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("json_transform", {LogicalType::JSON, LogicalType::JSON},
	                                               LogicalType::ANY, TransformFunction<false>, JSONTransformBind));
}

CreateScalarFunctionInfo JSONFunctions::GetTransformStrictFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("json_transform_strict", {LogicalType::JSON, LogicalType::JSON},
	                                               LogicalType::ANY, TransformFunction<true>, JSONTransformBind));
}

} // namespace duckdb
