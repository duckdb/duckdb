#include "json_transform.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"

namespace duckdb {

void JSONTransformOptions::Serialize(FieldWriter &writer) {
	writer.WriteField(strict_cast);
	writer.WriteField(error_duplicate_key);
	writer.WriteField(error_missing_key);
	writer.WriteField(error_unknown_key);
}

void JSONTransformOptions::Deserialize(FieldReader &reader) {
	strict_cast = reader.ReadRequired<bool>();
	error_duplicate_key = reader.ReadRequired<bool>();
	error_missing_key = reader.ReadRequired<bool>();
	error_unknown_key = reader.ReadRequired<bool>();
}

//! Forward declaration for recursion
static LogicalType StructureStringToType(yyjson_val *val, ClientContext &context);

static LogicalType StructureStringToTypeArray(yyjson_val *arr, ClientContext &context) {
	if (yyjson_arr_size(arr) != 1) {
		throw InvalidInputException("Too many values in array of JSON structure");
	}
	return LogicalType::LIST(StructureStringToType(yyjson_arr_get_first(arr), context));
}

static LogicalType StructureToTypeObject(yyjson_val *obj, ClientContext &context) {
	unordered_set<string> names;
	child_list_t<LogicalType> child_types;
	size_t idx, max;
	yyjson_val *key, *val;
	yyjson_obj_foreach(obj, idx, max, key, val) {
		val = yyjson_obj_iter_get_val(key);
		auto key_str = unsafe_yyjson_get_str(key);
		if (names.find(key_str) != names.end()) {
			JSONCommon::ThrowValFormatError("Duplicate keys in object in JSON structure: %s", val);
		}
		names.insert(key_str);
		child_types.emplace_back(key_str, StructureStringToType(val, context));
	}
	D_ASSERT(yyjson_obj_size(obj) == names.size());
	return LogicalType::STRUCT(child_types);
}

static LogicalType StructureStringToType(yyjson_val *val, ClientContext &context) {
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
		return StructureStringToTypeArray(val, context);
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		return StructureToTypeObject(val, context);
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		return TransformStringToLogicalType(unsafe_yyjson_get_str(val), context);
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
		auto structure_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
		if (!structure_val.DefaultTryCastAs(JSONCommon::JSONType())) {
			throw InvalidInputException("cannot cast JSON structure to string");
		}
		auto structure_string = structure_val.GetValueUnsafe<string_t>();
		JSONAllocator json_allocator(Allocator::DefaultAllocator());
		yyjson_read_err err;
		auto doc = JSONCommon::ReadDocumentUnsafe(structure_string, JSONCommon::READ_FLAG,
		                                          json_allocator.GetYYJSONAllocator(), &err);
		if (err.code != YYJSON_READ_SUCCESS) {
			JSONCommon::ThrowParseError(structure_string.GetDataUnsafe(), structure_string.GetSize(), err);
		}
		bound_function.return_type = StructureStringToType(doc->root, context);
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

static inline bool GetValueString(yyjson_val *val, yyjson_alc *alc, string_t &result, Vector &vector) {
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		return false;
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		result = string_t(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
		return true;
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		result = JSONCommon::WriteVal<yyjson_val>(val, alc);
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

void JSONTransform::GetStringVector(yyjson_val *vals[], const idx_t count, const LogicalType &target,
                                    Vector &string_vector, const bool strict) {
	auto data = (string_t *)FlatVector::GetData(string_vector);
	auto &validity = FlatVector::Validity(string_vector);

	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || yyjson_is_null(val)) {
			validity.SetInvalid(i);
		} else if (strict && !yyjson_is_str(val)) {
			JSONCommon::ThrowValFormatError("Unable to cast '%s' to " + LogicalTypeIdToString(target.id()), val);
		} else {
			data[i] = GetString(val);
		}
	}
}

static void TransformFromString(yyjson_val *vals[], Vector &result, const idx_t count, const bool strict) {
	Vector string_vector(LogicalTypeId::VARCHAR, count);
	JSONTransform::GetStringVector(vals, count, result.GetType(), string_vector, strict);

	string error_message;
	if (!VectorOperations::DefaultTryCast(string_vector, result, count, &error_message, strict) && strict) {
		throw InvalidInputException(error_message);
	}
}

template <class OP, class T>
static bool TransformStringWithFormat(Vector &string_vector, StrpTimeFormat &format, const idx_t count, Vector &result,
                                      string &error_message) {
	const auto source_strings = FlatVector::GetData<string_t>(string_vector);
	const auto &source_validity = FlatVector::Validity(string_vector);

	auto target_vals = FlatVector::GetData<T>(result);
	auto &target_validity = FlatVector::Validity(result);

	bool success = true;
	if (source_validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			if (!OP::template Operation<T>(format, source_strings[i], target_vals[i], error_message)) {
				target_validity.SetInvalid(i);
				success = false;
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			if (!source_validity.RowIsValid(i)) {
				target_validity.SetInvalid(i);
			} else if (!OP::template Operation<T>(format, source_strings[i], target_vals[i], error_message)) {
				target_validity.SetInvalid(i);
				success = false;
			}
		}
	}
	return success;
}

static void TransformFromStringWithFormat(yyjson_val *vals[], Vector &result, const idx_t count,
                                          const JSONTransformOptions &options) {
	Vector string_vector(LogicalTypeId::VARCHAR, count);
	JSONTransform::GetStringVector(vals, count, result.GetType(), string_vector, options.strict_cast);

	const auto &result_type = result.GetType().id();
	auto &format = options.date_format_map->GetFormat(result_type);

	bool success;
	string error_message;
	switch (result_type) {
	case LogicalTypeId::DATE:
		success = TransformStringWithFormat<TryParseDate, date_t>(string_vector, format, count, result, error_message);
		break;
	case LogicalTypeId::TIMESTAMP:
		success = TransformStringWithFormat<TryParseTimeStamp, timestamp_t>(string_vector, format, count, result,
		                                                                    error_message);
		break;
	default:
		throw InternalException("No date/timestamp formats for %s", LogicalTypeIdToString(result.GetType().id()));
	}

	if (options.strict_cast && !success) {
		throw CastException(error_message);
	}
}

static void TransformToString(yyjson_val *vals[], yyjson_alc *alc, Vector &result, const idx_t count) {
	auto data = (string_t *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || !GetValueString(val, alc, data[i], result)) {
			validity.SetInvalid(i);
		}
	}
}

static void Transform(yyjson_val *vals[], yyjson_alc *alc, Vector &result, const idx_t count,
                      const JSONTransformOptions &options);

void JSONTransform::TransformObject(yyjson_val *objects[], yyjson_alc *alc, const idx_t count,
                                    const vector<string> &names, const vector<Vector *> &result_vectors,
                                    const JSONTransformOptions &options) {
	D_ASSERT(alc);
	D_ASSERT(names.size() == result_vectors.size());
	const idx_t column_count = names.size();

	// Build hash map from key to column index so we don't have to linearly search using the key
	json_key_map_t<idx_t> key_map;
	vector<yyjson_val **> nested_vals;
	nested_vals.reserve(column_count);
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		key_map.insert({{names[col_idx].c_str(), names[col_idx].length()}, col_idx});
		nested_vals.push_back((yyjson_val **)alc->malloc(alc->ctx, sizeof(yyjson_val *) * count));
	}

	idx_t found_key_count;
	auto found_keys = (bool *)alc->malloc(alc->ctx, sizeof(bool) * column_count);

	size_t idx, max;
	yyjson_val *key, *val;
	for (idx_t i = 0; i < count; i++) {
		if (objects[i]) {
			found_key_count = 0;
			memset(found_keys, false, column_count);
			yyjson_obj_foreach(objects[i], idx, max, key, val) {
				auto key_ptr = yyjson_get_str(key);
				auto key_len = yyjson_get_len(key);
				auto it = key_map.find({key_ptr, key_len});
				if (it != key_map.end()) {
					const auto &col_idx = it->second;
					if (options.error_duplicate_key && found_keys[col_idx]) {
						JSONCommon::ThrowValFormatError(
						    "Duplicate key \"" + string(key_ptr, key_len) + "\" in object %s", objects[i]);
					}
					nested_vals[col_idx][i] = val;
					found_keys[col_idx] = true;
					if (++found_key_count == column_count) {
						break;
					}
				} else if (options.error_unknown_key) {
					JSONCommon::ThrowValFormatError("Object %s has unknown key \"" + string(key_ptr, key_len) + "\"",
					                                objects[i]);
				}
			}
			if (found_key_count != column_count) {
				// If 'error_missing_key, we throw an error if one of the keys was not found.
				// If not, we set the nested val to null so the recursion doesn't break
				for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
					if (!found_keys[col_idx]) {
						if (options.error_missing_key) {
							JSONCommon::ThrowValFormatError("Object %s does not have key \"" + names[col_idx] + "\"",
							                                objects[i]);
						} else {
							nested_vals[col_idx][i] = nullptr;
						}
					}
				}
			}
		} else {
			// Set nested val to null so the recursion doesn't break
			for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
				nested_vals[col_idx][i] = nullptr;
			}
		}
	}

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		Transform(nested_vals[col_idx], alc, *result_vectors[col_idx], count, options);
	}
}

static void TransformObject(yyjson_val *objects[], yyjson_alc *alc, Vector &result, const idx_t count,
                            const LogicalType &type, const JSONTransformOptions &options) {
	// Get child vectors and names
	auto &child_vs = StructVector::GetEntries(result);
	vector<string> child_names;
	vector<Vector *> child_vectors;
	child_names.reserve(child_vs.size());
	child_vectors.reserve(child_vs.size());
	for (idx_t child_i = 0; child_i < child_vs.size(); child_i++) {
		child_names.push_back(StructType::GetChildName(type, child_i));
		child_vectors.push_back(child_vs[child_i].get());
	}

	JSONTransform::TransformObject(objects, alc, count, child_names, child_vectors, options);
}

static void TransformArray(yyjson_val *arrays[], yyjson_alc *alc, Vector &result, const idx_t count,
                           const JSONTransformOptions &options) {
	// Initialize list vector
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &list_validity = FlatVector::Validity(result);
	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!arrays[i] || yyjson_is_null(arrays[i])) {
			list_validity.SetInvalid(i);
		}
		auto &entry = list_entries[i];
		entry.offset = offset;
		entry.length = yyjson_arr_size(arrays[i]);
		offset += entry.length;
	}
	ListVector::SetListSize(result, offset);
	ListVector::Reserve(result, offset);

	// Initialize array for the nested values
	auto nested_vals = (yyjson_val **)alc->malloc(alc->ctx, sizeof(yyjson_val *) * offset);

	// Get array values
	size_t idx, max;
	yyjson_val *val;
	idx_t list_i = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!list_validity.RowIsValid(i)) {
			// We already marked this as invalid
			continue;
		}
		yyjson_arr_foreach(arrays[i], idx, max, val) {
			nested_vals[list_i] = val;
			list_i++;
		}
	}
	D_ASSERT(list_i == offset);
	// Transform array values
	Transform(nested_vals, alc, ListVector::GetEntry(result), offset, options);
}

static void Transform(yyjson_val *vals[], yyjson_alc *alc, Vector &result, const idx_t count,
                      const JSONTransformOptions &options) {
	auto result_type = result.GetType();
	if (options.date_format_map && (result_type == LogicalTypeId::TIMESTAMP || result_type == LogicalTypeId::DATE)) {
		TransformFromStringWithFormat(vals, result, count, options);
		return;
	}

	switch (result_type.id()) {
	case LogicalTypeId::SQLNULL:
		return;
	case LogicalTypeId::BOOLEAN:
		return TransformNumerical<bool>(vals, result, count, options.strict_cast);
	case LogicalTypeId::TINYINT:
		return TransformNumerical<int8_t>(vals, result, count, options.strict_cast);
	case LogicalTypeId::SMALLINT:
		return TransformNumerical<int16_t>(vals, result, count, options.strict_cast);
	case LogicalTypeId::INTEGER:
		return TransformNumerical<int32_t>(vals, result, count, options.strict_cast);
	case LogicalTypeId::BIGINT:
		return TransformNumerical<int64_t>(vals, result, count, options.strict_cast);
	case LogicalTypeId::UTINYINT:
		return TransformNumerical<uint8_t>(vals, result, count, options.strict_cast);
	case LogicalTypeId::USMALLINT:
		return TransformNumerical<uint16_t>(vals, result, count, options.strict_cast);
	case LogicalTypeId::UINTEGER:
		return TransformNumerical<uint32_t>(vals, result, count, options.strict_cast);
	case LogicalTypeId::UBIGINT:
		return TransformNumerical<uint64_t>(vals, result, count, options.strict_cast);
	case LogicalTypeId::HUGEINT:
		return TransformNumerical<hugeint_t>(vals, result, count, options.strict_cast);
	case LogicalTypeId::FLOAT:
		return TransformNumerical<float>(vals, result, count, options.strict_cast);
	case LogicalTypeId::DOUBLE:
		return TransformNumerical<double>(vals, result, count, options.strict_cast);
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(result_type);
		auto scale = DecimalType::GetScale(result_type);
		switch (result_type.InternalType()) {
		case PhysicalType::INT16:
			return TransformDecimal<int16_t>(vals, result, count, width, scale, options.strict_cast);
		case PhysicalType::INT32:
			return TransformDecimal<int32_t>(vals, result, count, width, scale, options.strict_cast);
		case PhysicalType::INT64:
			return TransformDecimal<int64_t>(vals, result, count, width, scale, options.strict_cast);
		case PhysicalType::INT128:
			return TransformDecimal<hugeint_t>(vals, result, count, width, scale, options.strict_cast);
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
		return TransformFromString(vals, result, count, options.strict_cast);
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return TransformToString(vals, alc, result, count);
	case LogicalTypeId::STRUCT:
		return TransformObject(vals, alc, result, count, result_type, options);
	case LogicalTypeId::LIST:
		return TransformArray(vals, alc, result, count, options);
	default:
		throw InternalException("Unexpected type at JSON Transform %s", result_type.ToString());
	}
}

template <bool strict>
static void TransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator.GetYYJSONAllocator();

	const auto count = args.size();
	auto &input = args.data[0];
	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);
	auto inputs = (string_t *)input_data.data;
	// Read documents
	yyjson_doc *docs[STANDARD_VECTOR_SIZE];
	yyjson_val *vals[STANDARD_VECTOR_SIZE];
	auto &result_validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			docs[i] = nullptr;
			vals[i] = nullptr;
			result_validity.SetInvalid(i);
		} else {
			docs[i] = JSONCommon::ReadDocument(inputs[idx], JSONCommon::READ_FLAG, alc);
			vals[i] = docs[i]->root;
		}
	}

	const JSONTransformOptions options {strict, strict, strict, false, nullptr};

	Transform(vals, alc, result, count, options);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void GetTransformFunctionInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::ANY, TransformFunction<false>,
	                               JSONTransformBind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

CreateScalarFunctionInfo JSONFunctions::GetTransformFunction() {
	ScalarFunctionSet set("json_transform");
	GetTransformFunctionInternal(set, LogicalType::VARCHAR);
	GetTransformFunctionInternal(set, JSONCommon::JSONType());
	return CreateScalarFunctionInfo(set);
}

static void GetTransformStrictFunctionInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::ANY, TransformFunction<true>,
	                               JSONTransformBind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

CreateScalarFunctionInfo JSONFunctions::GetTransformStrictFunction() {
	ScalarFunctionSet set("json_transform_strict");
	GetTransformStrictFunctionInternal(set, LogicalType::VARCHAR);
	GetTransformStrictFunctionInternal(set, JSONCommon::JSONType());
	return CreateScalarFunctionInfo(set);
}

} // namespace duckdb
