#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! Fast equality check for non-object JSON values.
//! Avoids serialization for null, bool, and string (the common cases).
//! Falls back to serialization only for numbers and arrays.
static bool LeafValuesEqual(yyjson_mut_val *old_val, yyjson_mut_val *new_val, yyjson_alc *alc) {
	// Different type or subtype: always different
	if (yyjson_mut_get_tag(old_val) != yyjson_mut_get_tag(new_val)) {
		return false;
	}
	// Null or bool: tag match is sufficient (subtype encodes true/false)
	if (unsafe_yyjson_is_null(old_val) || yyjson_mut_is_bool(old_val)) {
		return true;
	}
	// String: compare length then content directly (no serialization)
	if (yyjson_mut_is_str(old_val)) {
		auto old_len = unsafe_yyjson_get_len(old_val);
		if (old_len != unsafe_yyjson_get_len(new_val)) {
			return false;
		}
		return memcmp(unsafe_yyjson_get_str(old_val), unsafe_yyjson_get_str(new_val), old_len) == 0;
	}
	// Numbers, arrays: serialize and compare
	auto old_str = JSONCommon::WriteVal<yyjson_mut_val>(old_val, alc);
	auto new_str = JSONCommon::WriteVal<yyjson_mut_val>(new_val, alc);
	return old_str == new_str;
}

//! Compute the minimal RFC 7396 merge patch that transforms old_val into new_val.
//! Returns nullptr when old_val and new_val are equal (no diff).
static yyjson_mut_val *MergeDiff(yyjson_mut_doc *doc, yyjson_mut_val *old_val, yyjson_mut_val *new_val,
                                 yyjson_alc *alc) {
	// Both objects: compute recursive structural diff
	if (yyjson_mut_is_obj(old_val) && yyjson_mut_is_obj(new_val)) {
		auto builder = yyjson_mut_obj(doc);
		bool has_diff = false;

		// Keys in old but not in new: removed (emit null)
		{
			idx_t idx, max;
			yyjson_mut_val *key, *old_child;
			yyjson_mut_obj_foreach(old_val, idx, max, key, old_child) {
				if (!yyjson_mut_obj_getn(new_val, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key))) {
					yyjson_mut_obj_add(builder, yyjson_mut_val_mut_copy(doc, key), yyjson_mut_null(doc));
					has_diff = true;
				}
			}
		}

		// Keys in new: added or changed
		{
			idx_t idx, max;
			yyjson_mut_val *key, *new_child;
			yyjson_mut_obj_foreach(new_val, idx, max, key, new_child) {
				auto old_child = yyjson_mut_obj_getn(old_val, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
				if (!old_child) {
					// Key added
					yyjson_mut_obj_add(builder, yyjson_mut_val_mut_copy(doc, key),
					                   yyjson_mut_val_mut_copy(doc, new_child));
					has_diff = true;
				} else {
					// Key exists in both: recurse
					auto sub_diff = MergeDiff(doc, old_child, new_child, alc);
					if (sub_diff) {
						yyjson_mut_obj_add(builder, yyjson_mut_val_mut_copy(doc, key), sub_diff);
						has_diff = true;
					}
				}
			}
		}

		return has_diff ? builder : nullptr;
	}

	// Not both objects: use fast leaf comparison
	if (LeafValuesEqual(old_val, new_val, alc)) {
		return nullptr;
	}
	return yyjson_mut_val_mut_copy(doc, new_val);
}

static inline void MergeDiffReadObjects(yyjson_mut_doc *doc, Vector &input, yyjson_mut_val *objs[], const idx_t count) {
	UnifiedVectorFormat input_data;
	auto &input_vector = input;
	input_vector.ToUnifiedFormat(count, input_data);
	auto inputs = UnifiedVectorFormat::GetData<string_t>(input_data);

	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			objs[i] = nullptr;
		} else {
			objs[i] =
			    yyjson_val_mut_copy(doc, JSONCommon::ReadDocument(inputs[idx], JSONCommon::READ_FLAG, &doc->alc)->root);
		}
	}
}

//! Compute the RFC 7396 merge patch that transforms old into new
static void MergeDiffFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	auto doc = JSONCommon::CreateDocument(alc);
	const auto count = args.size();

	auto old_vals = JSONCommon::AllocateArray<yyjson_mut_val *>(alc, count);
	MergeDiffReadObjects(doc, args.data[0], old_vals, count);

	auto new_vals = JSONCommon::AllocateArray<yyjson_mut_val *>(alc, count);
	MergeDiffReadObjects(doc, args.data[1], new_vals, count);

	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		if (!new_vals[i]) {
			// new is SQL NULL: result is NULL
			result_validity.SetInvalid(i);
		} else if (!old_vals[i]) {
			// old is SQL NULL: result is the new value
			result_data[i] = JSONCommon::WriteVal<yyjson_mut_val>(new_vals[i], alc);
		} else if (yyjson_mut_is_obj(old_vals[i]) && yyjson_mut_is_obj(new_vals[i])) {
			// Both objects: compute diff
			auto diff = MergeDiff(doc, old_vals[i], new_vals[i], alc);
			if (!diff) {
				// No changes: return empty object
				diff = yyjson_mut_obj(doc);
			}
			result_data[i] = JSONCommon::WriteVal<yyjson_mut_val>(diff, alc);
		} else {
			// Not both objects: return new value
			result_data[i] = JSONCommon::WriteVal<yyjson_mut_val>(new_vals[i], alc);
		}
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

	JSONAllocator::AddBuffer(result, alc);
}

ScalarFunctionSet JSONFunctions::GetMergeDiffFunction() {
	ScalarFunction fun("json_merge_diff", {LogicalType::JSON(), LogicalType::JSON()}, LogicalType::JSON(),
	                   MergeDiffFunction, nullptr, nullptr, nullptr, JSONFunctionLocalState::Init);
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	return ScalarFunctionSet(fun);
}

} // namespace duckdb
