#include "json_transform.hpp"

#include "duckdb/common/types.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"

namespace duckdb {

JSONTransformOptions::JSONTransformOptions() {
}

JSONTransformOptions::JSONTransformOptions(bool strict_cast_p, bool error_duplicate_key_p, bool error_missing_key_p,
                                           bool error_unkown_key_p)
    : strict_cast(strict_cast_p), error_duplicate_key(error_duplicate_key_p), error_missing_key(error_missing_key_p),
      error_unknown_key(error_unkown_key_p) {
}

void JSONTransformOptions::Serialize(FieldWriter &writer) {
	writer.WriteField(strict_cast);
	writer.WriteField(error_duplicate_key);
	writer.WriteField(error_missing_key);
	writer.WriteField(error_unknown_key);
	writer.WriteField(from_file);
}

void JSONTransformOptions::Deserialize(FieldReader &reader) {
	strict_cast = reader.ReadRequired<bool>();
	error_duplicate_key = reader.ReadRequired<bool>();
	error_missing_key = reader.ReadRequired<bool>();
	error_unknown_key = reader.ReadRequired<bool>();
	from_file = reader.ReadRequired<bool>();
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
	if (child_types.empty()) {
		throw InvalidInputException("Empty object in JSON structure");
	}
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

static duckdb::unique_ptr<FunctionData> JSONTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                          vector<duckdb::unique_ptr<Expression>> &arguments) {
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
			throw InvalidInputException("Cannot cast JSON structure to string");
		}
		auto structure_string = structure_val.GetValueUnsafe<string_t>();
		JSONAllocator json_allocator(Allocator::DefaultAllocator());
		yyjson_read_err err;
		auto doc = JSONCommon::ReadDocumentUnsafe(structure_string, JSONCommon::READ_FLAG,
		                                          json_allocator.GetYYJSONAllocator(), &err);
		if (err.code != YYJSON_READ_SUCCESS) {
			JSONCommon::ThrowParseError(structure_string.GetData(), structure_string.GetSize(), err);
		}
		bound_function.return_type = StructureStringToType(doc->root, context);
	}
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static inline string_t GetString(yyjson_val *val) {
	return string_t(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
}

template <class T, class OP = TryCast>
static inline bool GetValueNumerical(yyjson_val *val, T &result, JSONTransformOptions &options) {
	bool success;
	switch (unsafe_yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		return false;
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		success = OP::template Operation<string_t, T>(GetString(val), result, options.strict_cast);
		break;
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		success = false;
		break;
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
		success = OP::template Operation<bool, T>(unsafe_yyjson_get_bool(val), result, options.strict_cast);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
		success = OP::template Operation<uint64_t, T>(unsafe_yyjson_get_uint(val), result, options.strict_cast);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
		success = OP::template Operation<int64_t, T>(unsafe_yyjson_get_sint(val), result, options.strict_cast);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
		success = OP::template Operation<double, T>(unsafe_yyjson_get_real(val), result, options.strict_cast);
		break;
	default:
		throw InternalException("Unknown yyjson tag in GetValueNumerical");
	}
	if (!success && options.strict_cast) {
		options.error_message =
		    StringUtil::Format("Failed to cast value to numerical: %s", JSONCommon::ValToString(val, 50));
	}
	return success;
}

template <class T, class OP = TryCastToDecimal>
static inline bool GetValueDecimal(yyjson_val *val, T &result, uint8_t w, uint8_t s, JSONTransformOptions &options) {
	bool success;
	switch (unsafe_yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		return false;
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		success = OP::template Operation<string_t, T>(GetString(val), result, &options.error_message, w, s);
		break;
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		success = false;
		break;
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
		success = OP::template Operation<bool, T>(unsafe_yyjson_get_bool(val), result, &options.error_message, w, s);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
		success =
		    OP::template Operation<uint64_t, T>(unsafe_yyjson_get_uint(val), result, &options.error_message, w, s);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
		success = OP::template Operation<int64_t, T>(unsafe_yyjson_get_sint(val), result, &options.error_message, w, s);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
		success = OP::template Operation<double, T>(unsafe_yyjson_get_real(val), result, &options.error_message, w, s);
		break;
	default:
		throw InternalException("Unknown yyjson tag in GetValueString");
	}
	if (!success && options.strict_cast) {
		options.error_message =
		    StringUtil::Format("Failed to cast value to decimal: %s", JSONCommon::ValToString(val, 50));
	}
	return success;
}

static inline bool GetValueString(yyjson_val *val, yyjson_alc *alc, string_t &result, Vector &vector) {
	switch (unsafe_yyjson_get_tag(val)) {
	case YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE:
		return true;
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
static bool TransformNumerical(yyjson_val *vals[], Vector &result, const idx_t count, JSONTransformOptions &options) {
	auto data = (T *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || unsafe_yyjson_is_null(val)) {
			validity.SetInvalid(i);
		} else if (!GetValueNumerical<T>(val, data[i], options)) {
			validity.SetInvalid(i);
			if (options.strict_cast) {
				options.object_index = i;
				return false;
			}
		}
	}
	return true;
}

template <class T>
static bool TransformDecimal(yyjson_val *vals[], Vector &result, const idx_t count, uint8_t width, uint8_t scale,
                             JSONTransformOptions &options) {
	auto data = (T *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || unsafe_yyjson_is_null(val)) {
			validity.SetInvalid(i);
		} else if (!GetValueDecimal<T>(val, data[i], width, scale, options)) {
			validity.SetInvalid(i);
			if (options.strict_cast) {
				options.object_index = i;
				return false;
			}
		}
	}
	return true;
}

bool JSONTransform::GetStringVector(yyjson_val *vals[], const idx_t count, const LogicalType &target,
                                    Vector &string_vector, JSONTransformOptions &options) {
	if (count > STANDARD_VECTOR_SIZE) {
		string_vector.Initialize(false, count);
	}
	auto data = FlatVector::GetData<string_t>(string_vector);
	auto &validity = FlatVector::Validity(string_vector);
	validity.SetAllValid(count);

	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || unsafe_yyjson_is_null(val)) {
			validity.SetInvalid(i);
		} else if (options.strict_cast && !unsafe_yyjson_is_str(val)) {
			options.error_message = StringUtil::Format("Unable to cast '%s' to " + LogicalTypeIdToString(target.id()),
			                                           JSONCommon::ValToString(val, 50));
			options.object_index = i;
			return false;
		} else {
			data[i] = GetString(val);
		}
	}
	return true;
}

static bool TransformFromString(yyjson_val *vals[], Vector &result, const idx_t count, JSONTransformOptions &options) {
	Vector string_vector(LogicalTypeId::VARCHAR, count);
	if (!JSONTransform::GetStringVector(vals, count, result.GetType(), string_vector, options)) {
		return false;
	}

	if (!VectorOperations::DefaultTryCast(string_vector, result, count, &options.error_message) &&
	    options.strict_cast) {
		options.object_index = 0; // Can't get line number information here
		options.error_message += " (line/object number information is approximate)";
		return false;
	}
	return true;
}

template <class OP, class T>
static bool TransformStringWithFormat(Vector &string_vector, StrpTimeFormat &format, const idx_t count, Vector &result,
                                      JSONTransformOptions &options) {
	const auto source_strings = FlatVector::GetData<string_t>(string_vector);
	const auto &source_validity = FlatVector::Validity(string_vector);

	auto target_vals = FlatVector::GetData<T>(result);
	auto &target_validity = FlatVector::Validity(result);

	if (source_validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			if (!OP::template Operation<T>(format, source_strings[i], target_vals[i], options.error_message)) {
				target_validity.SetInvalid(i);
				if (options.strict_cast) {
					options.object_index = i;
					return false;
				}
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			if (!source_validity.RowIsValid(i)) {
				target_validity.SetInvalid(i);
			} else if (!OP::template Operation<T>(format, source_strings[i], target_vals[i], options.error_message)) {
				target_validity.SetInvalid(i);
				if (options.strict_cast) {
					options.object_index = i;
					return false;
				}
			}
		}
	}
	return true;
}

static bool TransformFromStringWithFormat(yyjson_val *vals[], Vector &result, const idx_t count,
                                          JSONTransformOptions &options) {
	Vector string_vector(LogicalTypeId::VARCHAR, count);
	if (!JSONTransform::GetStringVector(vals, count, result.GetType(), string_vector, options)) {
		return false;
	}

	const auto &result_type = result.GetType().id();
	auto &format = options.date_format_map->GetFormat(result_type);

	switch (result_type) {
	case LogicalTypeId::DATE:
		return TransformStringWithFormat<TryParseDate, date_t>(string_vector, format, count, result, options);
	case LogicalTypeId::TIMESTAMP:
		return TransformStringWithFormat<TryParseTimeStamp, timestamp_t>(string_vector, format, count, result, options);
	default:
		throw InternalException("No date/timestamp formats for %s", LogicalTypeIdToString(result.GetType().id()));
	}
}

static bool TransformToString(yyjson_val *vals[], yyjson_alc *alc, Vector &result, const idx_t count) {
	auto data = (string_t *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || unsafe_yyjson_is_null(vals[i])) {
			validity.SetInvalid(i);
		} else if (!GetValueString(val, alc, data[i], result)) {
			validity.SetInvalid(i);
		}
	}
	// Can always transform to string
	return true;
}

bool JSONTransform::TransformObject(yyjson_val *objects[], yyjson_alc *alc, const idx_t count,
                                    const vector<string> &names, const vector<Vector *> &result_vectors,
                                    JSONTransformOptions &options) {
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

	bool success = true;

	size_t idx, max;
	yyjson_val *key, *val;
	for (idx_t i = 0; i < count; i++) {
		if (objects[i] && !unsafe_yyjson_is_null(objects[i])) {
			if (!unsafe_yyjson_is_obj(objects[i]) && options.strict_cast) {
				options.error_message =
				    StringUtil::Format("Expected OBJECT, but got %s: %s", JSONCommon::ValTypeToString(objects[i]),
				                       JSONCommon::ValToString(objects[i], 50));
				options.object_index = i;
				success = false;
				break;
			}
			found_key_count = 0;
			memset(found_keys, false, column_count);
			yyjson_obj_foreach(objects[i], idx, max, key, val) {
				auto key_ptr = unsafe_yyjson_get_str(key);
				auto key_len = unsafe_yyjson_get_len(key);
				auto it = key_map.find({key_ptr, key_len});
				if (it != key_map.end()) {
					const auto &col_idx = it->second;
					if (options.error_duplicate_key && found_keys[col_idx]) {
						options.error_message =
						    StringUtil::Format("Duplicate key \"" + string(key_ptr, key_len) + "\" in object %s",
						                       JSONCommon::ValToString(objects[i], 50));
						options.object_index = i;
						success = false;
						break;
					}
					nested_vals[col_idx][i] = val;
					found_keys[col_idx] = true;
					found_key_count++;
				} else if (options.error_unknown_key) {
					options.error_message =
					    StringUtil::Format("Object %s has unknown key \"" + string(key_ptr, key_len) + "\"",
					                       JSONCommon::ValToString(objects[i], 50));
					options.object_index = i;
					success = false;
				}
			}
			if (found_key_count != column_count) {
				// If 'error_missing_key, we throw an error if one of the keys was not found.
				// If not, we set the nested val to null so the recursion doesn't break
				for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
					if (!found_keys[col_idx]) {
						if (options.error_missing_key) {
							options.error_message =
							    StringUtil::Format("Object %s does not have key \"" + names[col_idx] + "\"",
							                       JSONCommon::ValToString(objects[i], 50));
							options.object_index = i;
							success = false;
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

	if (!success) {
		if (!options.from_file) {
			throw InvalidInputException(options.error_message);
		}
		return false;
	}

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		if (JSONTransform::Transform(nested_vals[col_idx], alc, *result_vectors[col_idx], count, options)) {
			continue;
		}
		if (!options.from_file) {
			throw InvalidInputException(options.error_message);
		}
		return false;
	}

	return success;
}

static bool TransformObjectInternal(yyjson_val *objects[], yyjson_alc *alc, Vector &result, const idx_t count,
                                    const LogicalType &type, JSONTransformOptions &options) {
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

	return JSONTransform::TransformObject(objects, alc, count, child_names, child_vectors, options);
}

static bool TransformArray(yyjson_val *arrays[], yyjson_alc *alc, Vector &result, const idx_t count,
                           JSONTransformOptions &options) {
	// Initialize list vector
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &list_validity = FlatVector::Validity(result);
	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!arrays[i] || unsafe_yyjson_is_null(arrays[i])) {
			list_validity.SetInvalid(i);
		} else if (!unsafe_yyjson_is_arr(arrays[i])) {
			if (options.strict_cast) {
				options.error_message =
				    StringUtil::Format("Expected ARRAY, but got %s: %s", JSONCommon::ValTypeToString(arrays[i]),
				                       JSONCommon::ValToString(arrays[i], 50));
				options.object_index = i;
				return false;
			} else {
				list_validity.SetInvalid(i);
			}
		} else {
			auto &entry = list_entries[i];
			entry.offset = offset;
			entry.length = unsafe_yyjson_get_len(arrays[i]);
			offset += entry.length;
		}
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
	auto success = JSONTransform::Transform(nested_vals, alc, ListVector::GetEntry(result), offset, options);
	if (!success && options.from_file) {
		// Set object index in case of error in nested list so we can get accurate line number information
		for (idx_t i = 0; i < count; i++) {
			if (!list_validity.RowIsValid(i)) {
				continue;
			}
			auto &entry = list_entries[i];
			if (options.object_index >= entry.offset && options.object_index < entry.offset + entry.length) {
				options.object_index = i;
			}
		}
	}
	return success;
}

bool TransformToJSON(yyjson_val *vals[], yyjson_alc *alc, Vector &result, const idx_t count) {
	auto data = (string_t *)FlatVector::GetData(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val) {
			validity.SetInvalid(i);
		} else {
			data[i] = JSONCommon::WriteVal(val, alc);
		}
	}
	// Can always transform to JSON
	return true;
}

bool JSONTransform::Transform(yyjson_val *vals[], yyjson_alc *alc, Vector &result, const idx_t count,
                              JSONTransformOptions &options) {
	auto result_type = result.GetType();
	if ((result_type == LogicalTypeId::TIMESTAMP || result_type == LogicalTypeId::DATE) && options.date_format_map) {
		// Auto-detected date/timestamp format during sampling
		return TransformFromStringWithFormat(vals, result, count, options);
	}

	if (JSONCommon::LogicalTypeIsJSON(result_type)) {
		return TransformToJSON(vals, alc, result, count);
	}

	switch (result_type.id()) {
	case LogicalTypeId::SQLNULL:
		return true;
	case LogicalTypeId::BOOLEAN:
		return TransformNumerical<bool>(vals, result, count, options);
	case LogicalTypeId::TINYINT:
		return TransformNumerical<int8_t>(vals, result, count, options);
	case LogicalTypeId::SMALLINT:
		return TransformNumerical<int16_t>(vals, result, count, options);
	case LogicalTypeId::INTEGER:
		return TransformNumerical<int32_t>(vals, result, count, options);
	case LogicalTypeId::BIGINT:
		return TransformNumerical<int64_t>(vals, result, count, options);
	case LogicalTypeId::UTINYINT:
		return TransformNumerical<uint8_t>(vals, result, count, options);
	case LogicalTypeId::USMALLINT:
		return TransformNumerical<uint16_t>(vals, result, count, options);
	case LogicalTypeId::UINTEGER:
		return TransformNumerical<uint32_t>(vals, result, count, options);
	case LogicalTypeId::UBIGINT:
		return TransformNumerical<uint64_t>(vals, result, count, options);
	case LogicalTypeId::HUGEINT:
		return TransformNumerical<hugeint_t>(vals, result, count, options);
	case LogicalTypeId::FLOAT:
		return TransformNumerical<float>(vals, result, count, options);
	case LogicalTypeId::DOUBLE:
		return TransformNumerical<double>(vals, result, count, options);
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(result_type);
		auto scale = DecimalType::GetScale(result_type);
		switch (result_type.InternalType()) {
		case PhysicalType::INT16:
			return TransformDecimal<int16_t>(vals, result, count, width, scale, options);
		case PhysicalType::INT32:
			return TransformDecimal<int32_t>(vals, result, count, width, scale, options);
		case PhysicalType::INT64:
			return TransformDecimal<int64_t>(vals, result, count, width, scale, options);
		case PhysicalType::INT128:
			return TransformDecimal<hugeint_t>(vals, result, count, width, scale, options);
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
		return TransformFromString(vals, result, count, options);
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return TransformToString(vals, alc, result, count);
	case LogicalTypeId::STRUCT:
		return TransformObjectInternal(vals, alc, result, count, result_type, options);
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

	JSONTransformOptions options(strict, strict, strict, false);

	if (!JSONTransform::Transform(vals, alc, result, count, options)) {
		throw InvalidInputException(options.error_message);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void GetTransformFunctionInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::ANY, TransformFunction<false>,
	                               JSONTransformBind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetTransformFunction() {
	ScalarFunctionSet set("json_transform");
	GetTransformFunctionInternal(set, LogicalType::VARCHAR);
	GetTransformFunctionInternal(set, JSONCommon::JSONType());
	return set;
}

static void GetTransformStrictFunctionInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::ANY, TransformFunction<true>,
	                               JSONTransformBind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetTransformStrictFunction() {
	ScalarFunctionSet set("json_transform_strict");
	GetTransformStrictFunctionInternal(set, LogicalType::VARCHAR);
	GetTransformStrictFunctionInternal(set, JSONCommon::JSONType());
	return set;
}

} // namespace duckdb
