#include "json_transform.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "json_functions.hpp"
#include "json_scan.hpp"

namespace duckdb {

JSONTransformOptions::JSONTransformOptions() : parameters(false, &error_message) {
}

JSONTransformOptions::JSONTransformOptions(bool strict_cast_p, bool error_duplicate_key_p, bool error_missing_key_p,
                                           bool error_unkown_key_p)
    : strict_cast(strict_cast_p), error_duplicate_key(error_duplicate_key_p), error_missing_key(error_missing_key_p),
      error_unknown_key(error_unkown_key_p), parameters(false, &error_message) {
}

//! Forward declaration for recursion
static LogicalType StructureStringToType(yyjson_val *val, ClientContext &context);

static LogicalType StructureStringToTypeArray(yyjson_val *arr, ClientContext &context) {
	if (yyjson_arr_size(arr) != 1) {
		throw BinderException("Too many values in array of JSON structure");
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
		throw BinderException("Empty object in JSON structure");
	}
	return LogicalType::STRUCT(child_types);
}

static LogicalType StructureStringToType(yyjson_val *val, ClientContext &context) {
	switch (yyjson_get_tag(val)) {
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
		return StructureStringToTypeArray(val, context);
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		return StructureToTypeObject(val, context);
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
		return TransformStringToLogicalType(unsafe_yyjson_get_str(val), context);
	default:
		throw BinderException("invalid JSON structure");
	}
}

static unique_ptr<FunctionData> JSONTransformBind(ClientContext &context, ScalarFunction &bound_function,
                                                  vector<unique_ptr<Expression>> &arguments) {
	D_ASSERT(bound_function.arguments.size() == 2);
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("JSON structure must be a constant!");
	}
	auto structure_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	if (structure_val.IsNull() || arguments[1]->return_type == LogicalTypeId::SQLNULL) {
		bound_function.return_type = LogicalTypeId::SQLNULL;
	} else {
		if (!structure_val.DefaultTryCastAs(LogicalType::JSON())) {
			throw BinderException("Cannot cast JSON structure to string");
		}
		auto structure_string = structure_val.GetValueUnsafe<string_t>();
		JSONAllocator json_allocator(Allocator::DefaultAllocator());
		auto doc = JSONCommon::ReadDocument(structure_string, JSONCommon::READ_FLAG, json_allocator.GetYYAlc());
		bound_function.return_type = StructureStringToType(doc->root, context);
	}
	return make_uniq<VariableReturnBindData>(bound_function.return_type);
}

static inline string_t GetString(yyjson_val *val) {
	return string_t(unsafe_yyjson_get_str(val), unsafe_yyjson_get_len(val));
}

template <class T, class OP = TryCast>
static inline bool GetValueNumerical(yyjson_val *val, T &result, JSONTransformOptions &options) {
	D_ASSERT(unsafe_yyjson_get_tag(val) != (YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE));
	bool success;
	switch (unsafe_yyjson_get_tag(val)) {
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_RAW | YYJSON_SUBTYPE_NONE:
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
	D_ASSERT(unsafe_yyjson_get_tag(val) != (YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE));
	bool success;
	switch (unsafe_yyjson_get_tag(val)) {
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_RAW | YYJSON_SUBTYPE_NONE:
		success = OP::template Operation<string_t, T>(GetString(val), result, options.parameters, w, s);
		break;
	case YYJSON_TYPE_ARR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE:
		success = false;
		break;
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_TRUE:
	case YYJSON_TYPE_BOOL | YYJSON_SUBTYPE_FALSE:
		success = OP::template Operation<bool, T>(unsafe_yyjson_get_bool(val), result, options.parameters, w, s);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_UINT:
		success = OP::template Operation<uint64_t, T>(unsafe_yyjson_get_uint(val), result, options.parameters, w, s);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_SINT:
		success = OP::template Operation<int64_t, T>(unsafe_yyjson_get_sint(val), result, options.parameters, w, s);
		break;
	case YYJSON_TYPE_NUM | YYJSON_SUBTYPE_REAL:
		success = OP::template Operation<double, T>(unsafe_yyjson_get_real(val), result, options.parameters, w, s);
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
	D_ASSERT(unsafe_yyjson_get_tag(val) != (YYJSON_TYPE_NULL | YYJSON_SUBTYPE_NONE));
	switch (unsafe_yyjson_get_tag(val)) {
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NOESC:
	case YYJSON_TYPE_STR | YYJSON_SUBTYPE_NONE:
	case YYJSON_TYPE_RAW | YYJSON_SUBTYPE_NONE:
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
	auto data = FlatVector::GetData<T>(result);
	auto &validity = FlatVector::Validity(result);

	bool success = true;
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || unsafe_yyjson_is_null(val)) {
			validity.SetInvalid(i);
		} else if (!GetValueNumerical<T>(val, data[i], options)) {
			validity.SetInvalid(i);
			if (success && options.strict_cast) {
				options.object_index = i;
				success = false;
			}
		}
	}
	return success;
}

template <class T>
static bool TransformDecimal(yyjson_val *vals[], Vector &result, const idx_t count, uint8_t width, uint8_t scale,
                             JSONTransformOptions &options) {
	auto data = FlatVector::GetData<T>(result);
	auto &validity = FlatVector::Validity(result);

	bool success = true;
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || unsafe_yyjson_is_null(val)) {
			validity.SetInvalid(i);
		} else if (!GetValueDecimal<T>(val, data[i], width, scale, options)) {
			validity.SetInvalid(i);
			if (success && options.strict_cast) {
				options.object_index = i;
				success = false;
			}
		}
	}
	return success;
}

bool JSONTransform::GetStringVector(yyjson_val *vals[], const idx_t count, const LogicalType &target,
                                    Vector &string_vector, JSONTransformOptions &options) {
	if (count > STANDARD_VECTOR_SIZE) {
		string_vector.Initialize(false, count);
	}
	auto data = FlatVector::GetData<string_t>(string_vector);
	auto &validity = FlatVector::Validity(string_vector);
	validity.SetAllValid(count);

	bool success = true;
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || unsafe_yyjson_is_null(val)) {
			validity.SetInvalid(i);
			continue;
		}

		if (!unsafe_yyjson_is_str(val)) {
			validity.SetInvalid(i);
			if (success && options.strict_cast && !unsafe_yyjson_is_str(val)) {
				options.error_message = StringUtil::Format("Unable to cast '%s' to " + EnumUtil::ToString(target.id()),
				                                           JSONCommon::ValToString(val, 50));
				options.object_index = i;
				success = false;
			}
			continue;
		}

		data[i] = GetString(val);
	}
	return success;
}

static bool TransformFromString(yyjson_val *vals[], Vector &result, const idx_t count, JSONTransformOptions &options) {
	Vector string_vector(LogicalTypeId::VARCHAR, count);

	bool success = true;
	if (!JSONTransform::GetStringVector(vals, count, result.GetType(), string_vector, options)) {
		success = false;
	}

	if (!VectorOperations::DefaultTryCast(string_vector, result, count, &options.error_message) &&
	    options.strict_cast) {
		options.object_index = 0; // Can't get line number information here
		options.error_message +=
		    "\n If this error occurred during read_json, line/object number information is approximate";
		success = false;
	}
	return success;
}

template <class OP, class T>
static bool TransformStringWithFormat(Vector &string_vector, StrpTimeFormat &format, const idx_t count, Vector &result,
                                      JSONTransformOptions &options) {
	const auto source_strings = FlatVector::GetData<string_t>(string_vector);
	const auto &source_validity = FlatVector::Validity(string_vector);

	auto target_vals = FlatVector::GetData<T>(result);
	auto &target_validity = FlatVector::Validity(result);

	bool success = true;
	for (idx_t i = 0; i < count; i++) {
		if (!source_validity.RowIsValid(i)) {
			target_validity.SetInvalid(i);
		} else if (!OP::template Operation<T>(format, source_strings[i], target_vals[i], options.error_message)) {
			target_validity.SetInvalid(i);
			if (success && options.strict_cast) {
				options.object_index = i;
				success = false;
			}
		}
	}
	return success;
}

static bool TransformFromStringWithFormat(yyjson_val *vals[], Vector &result, const idx_t count,
                                          JSONTransformOptions &options) {
	Vector string_vector(LogicalTypeId::VARCHAR, count);
	bool success = true;
	if (!JSONTransform::GetStringVector(vals, count, result.GetType(), string_vector, options)) {
		success = false;
	}

	const auto &result_type = result.GetType().id();
	auto &format = options.date_format_map->GetFormat(result_type);

	switch (result_type) {
	case LogicalTypeId::DATE:
		if (!TransformStringWithFormat<TryParseDate, date_t>(string_vector, format, count, result, options)) {
			success = false;
		}
		break;
	case LogicalTypeId::TIMESTAMP:
		if (!TransformStringWithFormat<TryParseTimeStamp, timestamp_t>(string_vector, format, count, result, options)) {
			success = false;
		}
		break;
	default:
		throw InternalException("No date/timestamp formats for %s", EnumUtil::ToString(result.GetType().id()));
	}
	return success;
}

static bool TransformToString(yyjson_val *vals[], yyjson_alc *alc, Vector &result, const idx_t count) {
	auto data = FlatVector::GetData<string_t>(result);
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
                                    JSONTransformOptions &options,
                                    optional_ptr<const vector<ColumnIndex>> column_indices, bool error_unknown_key) {
	if (column_indices && column_indices->empty()) {
		column_indices = nullptr;
	}
	D_ASSERT(alc);
	D_ASSERT(names.size() == result_vectors.size());
	D_ASSERT(!column_indices || column_indices->size() == names.size());
	const idx_t column_count = names.size();

	// Build hash map from key to column index so we don't have to linearly search using the key
	json_key_map_t<idx_t> key_map;
	vector<yyjson_val **> nested_vals;
	nested_vals.reserve(column_count);
	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		key_map.insert({{names[col_idx].c_str(), names[col_idx].length()}, col_idx});
		nested_vals.push_back(JSONCommon::AllocateArray<yyjson_val *>(alc, count));
	}

	idx_t found_key_count;
	auto found_keys = JSONCommon::AllocateArray<bool>(alc, column_count);

	bool success = true;

	size_t idx, max;
	yyjson_val *key, *val;
	for (idx_t i = 0; i < count; i++) {
		const auto &obj = objects[i];
		if (!obj || unsafe_yyjson_is_null(obj)) {
			// Set nested val to null so the recursion doesn't break
			for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
				nested_vals[col_idx][i] = nullptr;
			}
			continue;
		}

		if (!unsafe_yyjson_is_obj(obj)) {
			// Set nested val to null so the recursion doesn't break
			for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
				nested_vals[col_idx][i] = nullptr;
			}
			if (success && options.strict_cast && obj) {
				options.error_message =
				    StringUtil::Format("Expected OBJECT, but got %s: %s", JSONCommon::ValTypeToString(obj),
				                       JSONCommon::ValToString(obj, 50));
				options.object_index = i;
				success = false;
			}
			continue;
		}

		found_key_count = 0;
		memset(found_keys, false, column_count);
		yyjson_obj_foreach(objects[i], idx, max, key, val) {
			auto key_ptr = unsafe_yyjson_get_str(key);
			auto key_len = unsafe_yyjson_get_len(key);
			auto it = key_map.find({key_ptr, key_len});
			if (it != key_map.end()) {
				const auto &col_idx = it->second;
				if (found_keys[col_idx]) {
					if (success && options.error_duplicate_key) {
						options.error_message =
						    StringUtil::Format("Duplicate key \"" + string(key_ptr, key_len) + "\" in object %s",
						                       JSONCommon::ValToString(objects[i], 50));
						options.object_index = i;
						success = false;
					}
				} else {
					nested_vals[col_idx][i] = val;
					found_keys[col_idx] = true;
					found_key_count++;
				}
			} else if (success && error_unknown_key && options.error_unknown_key) {
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
				if (found_keys[col_idx]) {
					continue;
				}
				nested_vals[col_idx][i] = nullptr;

				if (success && options.error_missing_key) {
					options.error_message = StringUtil::Format("Object %s does not have key \"" + names[col_idx] + "\"",
					                                           JSONCommon::ValToString(objects[i], 50));
					options.object_index = i;
					success = false;
				}
			}
		}
	}

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		auto child_column_index = column_indices ? &(*column_indices)[col_idx] : nullptr;
		if (!JSONTransform::Transform(nested_vals[col_idx], alc, *result_vectors[col_idx], count, options,
		                              child_column_index)) {
			success = false;
		}
	}

	if (!options.delay_error && !success) {
		throw InvalidInputException(options.error_message);
	}

	return success;
}

static bool TransformObjectInternal(yyjson_val *objects[], yyjson_alc *alc, Vector &result, const idx_t count,
                                    JSONTransformOptions &options, optional_ptr<const ColumnIndex> column_index) {
	if (column_index && column_index->ChildIndexCount() == 0) {
		column_index = nullptr;
	}

	// Set validity first
	auto &result_validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &obj = objects[i];
		if (!obj || unsafe_yyjson_is_null(obj)) {
			result_validity.SetInvalid(i);
		}
	}

	// Get child vectors and names
	auto &child_vs = StructVector::GetEntries(result);
	vector<string> child_names;
	vector<Vector *> child_vectors;

	const auto child_count = column_index ? column_index->ChildIndexCount() : child_vs.size();
	child_names.reserve(child_count);
	child_vectors.reserve(child_count);

	unordered_set<idx_t> projected_indices;
	for (idx_t child_i = 0; child_i < child_count; child_i++) {
		const auto actual_i = column_index ? column_index->GetChildIndex(child_i).GetPrimaryIndex() : child_i;
		projected_indices.insert(actual_i);

		child_names.push_back(StructType::GetChildName(result.GetType(), actual_i));
		child_vectors.push_back(child_vs[actual_i].get());
	}

	for (idx_t child_i = 0; child_i < child_vs.size(); child_i++) {
		if (projected_indices.find(child_i) == projected_indices.end()) {
			child_vs[child_i]->SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(*child_vs[child_i], true);
		}
	}

	auto child_indices = column_index ? &column_index->GetChildIndexes() : nullptr;
	const auto error_unknown_key = child_count == child_vs.size(); // Nothing projected out, error if unknown
	return JSONTransform::TransformObject(objects, alc, count, child_names, child_vectors, options, child_indices,
	                                      error_unknown_key);
}

static bool TransformArrayToList(yyjson_val *arrays[], yyjson_alc *alc, Vector &result, const idx_t count,
                                 JSONTransformOptions &options) {
	bool success = true;

	// Initialize list vector
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &list_validity = FlatVector::Validity(result);
	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto &arr = arrays[i];
		if (!arr || unsafe_yyjson_is_null(arr)) {
			list_validity.SetInvalid(i);
			continue;
		}

		if (!unsafe_yyjson_is_arr(arr)) {
			list_validity.SetInvalid(i);
			if (success && options.strict_cast) {
				options.error_message =
				    StringUtil::Format("Expected ARRAY, but got %s: %s", JSONCommon::ValTypeToString(arrays[i]),
				                       JSONCommon::ValToString(arrays[i], 50));
				options.object_index = i;
				success = false;
			}
			continue;
		}

		auto &entry = list_entries[i];
		entry.offset = offset;
		entry.length = unsafe_yyjson_get_len(arr);
		offset += entry.length;
	}
	ListVector::SetListSize(result, offset);
	ListVector::Reserve(result, offset);

	// Initialize array for the nested values
	auto nested_vals = JSONCommon::AllocateArray<yyjson_val *>(alc, offset);

	// Get array values
	size_t idx, max;
	yyjson_val *val;
	idx_t list_i = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!list_validity.RowIsValid(i)) {
			continue; // We already marked this as invalid
		}
		yyjson_arr_foreach(arrays[i], idx, max, val) {
			nested_vals[list_i] = val;
			list_i++;
		}
	}
	D_ASSERT(list_i == offset);

	if (!success) {
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

	// Transform array values
	if (!JSONTransform::Transform(nested_vals, alc, ListVector::GetEntry(result), offset, options, nullptr)) {
		success = false;
	}

	if (!options.delay_error && !success) {
		throw InvalidInputException(options.error_message);
	}

	return success;
}

static bool TransformArrayToArray(yyjson_val *arrays[], yyjson_alc *alc, Vector &result, const idx_t count,
                                  JSONTransformOptions &options) {
	bool success = true;

	// Initialize array vector
	auto &result_validity = FlatVector::Validity(result);
	auto array_size = ArrayType::GetSize(result.GetType());
	auto child_count = count * array_size;

	for (idx_t i = 0; i < count; i++) {
		const auto &arr = arrays[i];
		if (!arr || unsafe_yyjson_is_null(arr)) {
			result_validity.SetInvalid(i);
			continue;
		}

		if (!unsafe_yyjson_is_arr(arr)) {
			result_validity.SetInvalid(i);
			if (success && options.strict_cast) {
				options.error_message =
				    StringUtil::Format("Expected ARRAY, but got %s: %s", JSONCommon::ValTypeToString(arrays[i]),
				                       JSONCommon::ValToString(arrays[i], 50));
				options.object_index = i;
				success = false;
			}
			continue;
		}

		auto json_arr_size = unsafe_yyjson_get_len(arr);
		if (json_arr_size != array_size) {
			result_validity.SetInvalid(i);
			if (success && options.strict_cast) {
				options.error_message =
				    StringUtil::Format("Expected array of size %u, but got '%s' with size %u", array_size,
				                       JSONCommon::ValToString(arrays[i], 50), json_arr_size);
				options.object_index = i;
				success = false;
			}
			continue;
		}
	}

	// Initialize array for the nested values
	auto nested_vals = JSONCommon::AllocateArray<yyjson_val *>(alc, child_count);

	// Get array values
	size_t idx, max;
	yyjson_val *val;
	idx_t nested_elem_idx = 0;
	for (idx_t i = 0; i < count; i++) {
		if (!result_validity.RowIsValid(i)) {
			// We already marked this as invalid, but we still need to increment nested_elem_idx
			// and set the nullptrs (otherwise indexing will break after compaction)
			for (idx_t j = 0; j < array_size; j++) {
				nested_vals[nested_elem_idx] = nullptr;
				nested_elem_idx++;
			};
		} else {
			yyjson_arr_foreach(arrays[i], idx, max, val) {
				nested_vals[nested_elem_idx] = val;
				nested_elem_idx++;
			}
		}
	}

	if (!success) {
		// Set object index in case of error in nested array so we can get accurate line number information
		for (idx_t i = 0; i < count; i++) {
			if (!result_validity.RowIsValid(i)) {
				continue;
			}
			auto offset = i * array_size;
			if (options.object_index >= offset && options.object_index < offset + array_size) {
				options.object_index = i;
			}
		}
	}

	// Transform array values
	if (!JSONTransform::Transform(nested_vals, alc, ArrayVector::GetEntry(result), child_count, options, nullptr)) {
		success = false;
	}

	if (!options.delay_error && !success) {
		throw InvalidInputException(options.error_message);
	}

	return success;
}

static bool TransformObjectToMap(yyjson_val *objects[], yyjson_alc *alc, Vector &result, const idx_t count,
                                 JSONTransformOptions &options) {
	// Pre-allocate list vector
	idx_t list_size = 0;
	for (idx_t i = 0; i < count; i++) {
		const auto &obj = objects[i];
		if (!obj || !unsafe_yyjson_is_obj(obj)) {
			continue;
		}
		list_size += unsafe_yyjson_get_len(obj);
	}
	ListVector::Reserve(result, list_size);
	ListVector::SetListSize(result, list_size);

	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &list_validity = FlatVector::Validity(result);

	auto keys = JSONCommon::AllocateArray<yyjson_val *>(alc, list_size);
	auto vals = JSONCommon::AllocateArray<yyjson_val *>(alc, list_size);

	bool success = true;
	idx_t list_offset = 0;

	size_t idx, max;
	yyjson_val *key, *val;
	for (idx_t i = 0; i < count; i++) {
		const auto &obj = objects[i];
		if (!obj || unsafe_yyjson_is_null(obj)) {
			list_validity.SetInvalid(i);
			continue;
		}

		if (!unsafe_yyjson_is_obj(obj)) {
			list_validity.SetInvalid(i);
			if (success && options.strict_cast && !unsafe_yyjson_is_obj(obj)) {
				options.error_message =
				    StringUtil::Format("Expected OBJECT, but got %s: %s", JSONCommon::ValTypeToString(obj),
				                       JSONCommon::ValToString(obj, 50));
				options.object_index = i;
				success = false;
			}
			continue;
		}

		auto &list_entry = list_entries[i];
		list_entry.offset = list_offset;
		list_entry.length = unsafe_yyjson_get_len(obj);

		yyjson_obj_foreach(obj, idx, max, key, val) {
			keys[list_offset] = key;
			vals[list_offset] = val;
			list_offset++;
		}
	}
	D_ASSERT(list_offset == list_size);

	// Transform keys
	if (!JSONTransform::Transform(keys, alc, MapVector::GetKeys(result), list_size, options, nullptr)) {
		throw ConversionException(
		    StringUtil::Format(options.error_message + ". Cannot default to NULL, because map keys cannot be NULL"));
	}

	// Transform values
	if (!JSONTransform::Transform(vals, alc, MapVector::GetValues(result), list_size, options, nullptr)) {
		success = false;
	}

	if (!options.delay_error && !success) {
		throw InvalidInputException(options.error_message);
	}

	return success;
}

bool TransformToJSON(yyjson_val *vals[], yyjson_alc *alc, Vector &result, const idx_t count) {
	auto data = FlatVector::GetData<string_t>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		const auto &val = vals[i];
		if (!val || unsafe_yyjson_is_null(val)) {
			validity.SetInvalid(i);
		} else {
			data[i] = JSONCommon::WriteVal(val, alc);
		}
	}
	// Can always transform to JSON
	return true;
}

bool TransformValueIntoUnion(yyjson_val **vals, yyjson_alc *alc, Vector &result, const idx_t count,
                             JSONTransformOptions &options) {
	auto type = result.GetType();

	auto fields = UnionType::CopyMemberTypes(type);
	vector<string> names;
	for (const auto &field : fields) {
		names.push_back(field.first);
	}

	bool success = true;

	auto &validity = FlatVector::Validity(result);

	auto set_error = [&](idx_t i, const string &message) {
		validity.SetInvalid(i);
		result.SetValue(i, Value(nullptr));
		if (success && options.strict_cast) {
			options.error_message = message;
			options.object_index = i;
			success = false;
		}
	};

	for (idx_t i = 0; i < count; i++) {
		const auto &obj = vals[i];

		if (!obj || unsafe_yyjson_is_null(vals[i])) {
			validity.SetInvalid(i);
			result.SetValue(i, Value(nullptr));
			continue;
		}

		if (!unsafe_yyjson_is_obj(obj)) {
			set_error(i,
			          StringUtil::Format("Expected an object representing a union, got %s", yyjson_get_type_desc(obj)));
			continue;
		}

		auto len = unsafe_yyjson_get_len(obj);
		if (len > 1) {
			set_error(i, "Found object containing more than one key, instead of union");
			continue;
		} else if (len == 0) {
			set_error(i, "Found empty object, instead of union");
			continue;
		}

		auto key = unsafe_yyjson_get_first(obj);
		auto val = yyjson_obj_iter_get_val(key);

		auto tag = std::find(names.begin(), names.end(), unsafe_yyjson_get_str(key));
		if (tag == names.end()) {
			set_error(i, StringUtil::Format("Found object containing unknown key, instead of union: %s",
			                                unsafe_yyjson_get_str(key)));
			continue;
		}

		idx_t actual_tag = tag - names.begin();

		Vector single(UnionType::GetMemberType(type, actual_tag), 1);
		if (!JSONTransform::Transform(&val, alc, single, 1, options, nullptr)) {
			success = false;
		}

		result.SetValue(i, Value::UNION(fields, actual_tag, single.GetValue(0)));
	}

	return success;
}

bool JSONTransform::Transform(yyjson_val *vals[], yyjson_alc *alc, Vector &result, const idx_t count,
                              JSONTransformOptions &options, optional_ptr<const ColumnIndex> column_index) {
	auto result_type = result.GetType();
	if ((result_type == LogicalTypeId::TIMESTAMP || result_type == LogicalTypeId::DATE) && options.date_format_map &&
	    options.date_format_map->HasFormats(result_type.id())) {
		// Auto-detected date/timestamp format during sampling
		return TransformFromStringWithFormat(vals, result, count, options);
	}

	if (result_type.IsJSONType()) {
		return TransformToJSON(vals, alc, result, count);
	}

	switch (result_type.id()) {
	case LogicalTypeId::SQLNULL:
		FlatVector::Validity(result).SetAllInvalid(count);
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
	case LogicalTypeId::UHUGEINT:
		return TransformNumerical<uhugeint_t>(vals, result, count, options);
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
		return TransformObjectInternal(vals, alc, result, count, options, column_index);
	case LogicalTypeId::LIST:
		return TransformArrayToList(vals, alc, result, count, options);
	case LogicalTypeId::MAP:
		return TransformObjectToMap(vals, alc, result, count, options);
	case LogicalTypeId::UNION:
		return TransformValueIntoUnion(vals, alc, result, count, options);
	case LogicalTypeId::ARRAY:
		return TransformArrayToArray(vals, alc, result, count, options);
	default:
		throw NotImplementedException("Cannot read a value of type %s from a json file", result_type.ToString());
	}
}

static bool TransformFunctionInternal(Vector &input, const idx_t count, Vector &result, yyjson_alc *alc,
                                      JSONTransformOptions &options) {
	UnifiedVectorFormat input_data;
	input.ToUnifiedFormat(count, input_data);
	auto inputs = UnifiedVectorFormat::GetData<string_t>(input_data);

	// Read documents
	auto docs = JSONCommon::AllocateArray<yyjson_doc *>(alc, count);
	auto vals = JSONCommon::AllocateArray<yyjson_val *>(alc, count);
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

	auto success = JSONTransform::Transform(vals, alc, result, count, options, nullptr);
	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	return success;
}

template <bool strict>
static void TransformFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator.GetYYAlc();

	JSONTransformOptions options(strict, strict, strict, false);
	if (!TransformFunctionInternal(args.data[0], args.size(), result, alc, options)) {
		throw InvalidInputException(options.error_message);
	}
}

static void GetTransformFunctionInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::ANY, TransformFunction<false>,
	                               JSONTransformBind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetTransformFunction() {
	ScalarFunctionSet set("json_transform");
	GetTransformFunctionInternal(set, LogicalType::VARCHAR);
	GetTransformFunctionInternal(set, LogicalType::JSON());
	return set;
}

static void GetTransformStrictFunctionInternal(ScalarFunctionSet &set, const LogicalType &input_type) {
	set.AddFunction(ScalarFunction({input_type, LogicalType::VARCHAR}, LogicalType::ANY, TransformFunction<true>,
	                               JSONTransformBind, nullptr, nullptr, JSONFunctionLocalState::Init));
}

ScalarFunctionSet JSONFunctions::GetTransformStrictFunction() {
	ScalarFunctionSet set("json_transform_strict");
	GetTransformStrictFunctionInternal(set, LogicalType::VARCHAR);
	GetTransformStrictFunctionInternal(set, LogicalType::JSON());
	return set;
}

static bool JSONToAnyCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &lstate = parameters.local_state->Cast<JSONFunctionLocalState>();
	lstate.json_allocator.Reset();
	auto alc = lstate.json_allocator.GetYYAlc();

	JSONTransformOptions options(true, true, true, true);
	options.delay_error = true;

	auto success = TransformFunctionInternal(source, count, result, alc, options);
	if (!success) {
		HandleCastError::AssignError(options.error_message, parameters);
	}
	return success;
}

BoundCastInfo JSONToAnyCastBind(BindCastInput &input, const LogicalType &source, const LogicalType &target) {
	return BoundCastInfo(JSONToAnyCast, nullptr, JSONFunctionLocalState::InitCastLocalState);
}

void JSONFunctions::RegisterJSONTransformCastFunctions(CastFunctionSet &casts) {
	// JSON can be cast to anything
	for (const auto &type : LogicalType::AllTypes()) {
		LogicalType target_type;
		switch (type.id()) {
		case LogicalTypeId::STRUCT:
			target_type = LogicalType::STRUCT({{"any", LogicalType::ANY}});
			break;
		case LogicalTypeId::LIST:
			target_type = LogicalType::LIST(LogicalType::ANY);
			break;
		case LogicalTypeId::MAP:
			target_type = LogicalType::MAP(LogicalType::ANY, LogicalType::ANY);
			break;
		case LogicalTypeId::UNION:
			target_type = LogicalType::UNION({{"any", LogicalType::ANY}});
			break;
		case LogicalTypeId::ARRAY:
			target_type = LogicalType::ARRAY(LogicalType::ANY, optional_idx());
			break;
		case LogicalTypeId::VARCHAR:
			// We skip this one here as it's handled in json_functions.cpp
			continue;
		default:
			target_type = type;
		}
		// Going from JSON to another type has the same cost as going from VARCHAR to that type
		const auto json_to_target_cost = casts.ImplicitCastCost(LogicalType::VARCHAR, target_type);
		casts.RegisterCastFunction(LogicalType::JSON(), target_type, JSONToAnyCastBind, json_to_target_cost);
	}
}

} // namespace duckdb
