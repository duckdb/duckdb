#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

//! RFC 7396 merge patch; a missing or non-object orig is treated as an empty object
static yyjson_mut_val *MergePatchRecursive(yyjson_mut_doc *doc, yyjson_mut_val *orig, yyjson_mut_val *patch) {
	if (!yyjson_mut_is_obj(patch)) {
		return yyjson_mut_val_mut_copy(doc, patch);
	}

	auto builder = yyjson_mut_obj(doc);
	if (!yyjson_mut_is_obj(orig)) {
		// yyjson_mut_obj_getn on a non-object returns nullptr, so lookups below need no special case
		orig = nullptr;
	}

	// Copy orig keys that the patch does not touch
	if (orig) {
		idx_t idx, max;
		yyjson_mut_val *key, *orig_val;
		yyjson_mut_obj_foreach(orig, idx, max, key, orig_val) {
			auto patch_val = yyjson_mut_obj_getn(patch, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
			if (!patch_val) {
				yyjson_mut_obj_add(builder, yyjson_mut_val_mut_copy(doc, key), yyjson_mut_val_mut_copy(doc, orig_val));
			}
		}
	}

	// Merge patch keys; null removes the key
	idx_t idx, max;
	yyjson_mut_val *key, *patch_val;
	yyjson_mut_obj_foreach(patch, idx, max, key, patch_val) {
		if (unsafe_yyjson_is_null(patch_val)) {
			continue;
		}
		auto orig_val = yyjson_mut_obj_getn(orig, unsafe_yyjson_get_str(key), unsafe_yyjson_get_len(key));
		auto merged_val = MergePatchRecursive(doc, orig_val, patch_val);
		yyjson_mut_obj_add(builder, yyjson_mut_val_mut_copy(doc, key), merged_val);
	}

	return builder;
}

static inline yyjson_mut_val *MergePatch(yyjson_mut_doc *doc, yyjson_mut_val *orig, yyjson_mut_val *patch) {
	if ((yyjson_mut_get_tag(orig) != (YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE)) ||
	    (yyjson_mut_get_tag(patch) != (YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE))) {
		// If either is not an object, we just return the second argument
		return patch;
	}

	// Both are object, do the merge
	return MergePatchRecursive(doc, orig, patch);
}

static inline void ReadObjects(yyjson_mut_doc *doc, const Vector &input, yyjson_mut_val *objs[]) {
	auto entries = input.Values<string_t>();

	// Read the documents
	for (idx_t i = 0; i < input.size(); i++) {
		auto entry = entries[i];
		if (!entry.IsValid()) {
			objs[i] = nullptr;
		} else {
			objs[i] = yyjson_val_mut_copy(
			    doc, JSONCommon::ReadDocument(entry.GetValue(), JSONCommon::READ_FLAG, &doc->alc)->root);
		}
	}
}

//! Follows MySQL behaviour
static void MergePatchFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = JSONFunctionLocalState::ResetAndGet(state);
	auto alc = lstate.json_allocator->GetYYAlc();

	auto doc = JSONCommon::CreateDocument(alc);
	const auto count = args.size();

	// Read the first json arg
	auto origs = JSONCommon::AllocateArray<yyjson_mut_val *>(alc, count);
	ReadObjects(doc, args.data[0], origs);

	// Read the next json args one by one and merge them into the first json arg
	auto patches = JSONCommon::AllocateArray<yyjson_mut_val *>(alc, count);
	for (idx_t arg_idx = 1; arg_idx < args.data.size(); arg_idx++) {
		ReadObjects(doc, args.data[arg_idx], patches);
		for (idx_t i = 0; i < count; i++) {
			if (patches[i] == nullptr) {
				// Next json arg is NULL, obj becomes NULL
				origs[i] = nullptr;
			} else if (origs[i] == nullptr) {
				// Current obj is NULL, obj becomes next json arg
				origs[i] = patches[i];
			} else {
				// Neither is NULL, merge them
				origs[i] = MergePatch(doc, origs[i], patches[i]);
			}
		}
	}

	// Write to result vector
	auto result_data = FlatVector::Writer<string_t>(result, count);
	for (idx_t i = 0; i < count; i++) {
		if (origs[i] == nullptr) {
			result_data.WriteNull();
		} else {
			result_data.WriteStringRef(JSONCommon::WriteVal<yyjson_mut_val>(origs[i], alc));
		}
	}
	JSONAllocator::AddBuffer(result, alc);
}

ScalarFunctionSet JSONFunctions::GetMergePatchFunction() {
	ScalarFunction fun("json_merge_patch", {}, LogicalType::JSON(), MergePatchFunction, nullptr, nullptr,
	                   JSONFunctionLocalState::Init);
	fun.GetSignature().AddParameter("json1", LogicalType::JSON()).AddParameter("json2", LogicalType::JSON());
	fun.GetSignature().AddArgsParameter("args", LogicalType::JSON());
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	return ScalarFunctionSet(fun);
}

} // namespace duckdb
