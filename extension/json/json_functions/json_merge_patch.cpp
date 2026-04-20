#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static inline yyjson_mut_val *MergePatch(yyjson_mut_doc *doc, yyjson_mut_val *orig, yyjson_mut_val *patch) {
	if ((yyjson_mut_get_tag(orig) != (YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE)) ||
	    (yyjson_mut_get_tag(patch) != (YYJSON_TYPE_OBJ | YYJSON_SUBTYPE_NONE))) {
		// If either is not an object, we just return the second argument
		return patch;
	}

	// Both are object, do the merge
	return yyjson_mut_merge_patch(doc, orig, patch);
}

static inline void ReadObjects(yyjson_mut_doc *doc, Vector &input, yyjson_mut_val *objs[], const idx_t count) {
	auto entries = input.Values<string_t>(count);

	// Read the documents
	for (idx_t i = 0; i < count; i++) {
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
	ReadObjects(doc, args.data[0], origs, count);

	// Read the next json args one by one and merge them into the first json arg
	auto patches = JSONCommon::AllocateArray<yyjson_mut_val *>(alc, count);
	for (idx_t arg_idx = 1; arg_idx < args.data.size(); arg_idx++) {
		ReadObjects(doc, args.data[arg_idx], patches, count);
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
	auto result_data = FlatVector::ScatterWriter<string_t>(result);
	for (idx_t i = 0; i < count; i++) {
		if (origs[i] == nullptr) {
			result_data.SetInvalid(i);
		} else {
			result_data[i].AssignWithoutCopying(JSONCommon::WriteVal<yyjson_mut_val>(origs[i], alc));
		}
	}
	JSONAllocator::AddBuffer(result, alc);
}

ScalarFunctionSet JSONFunctions::GetMergePatchFunction() {
	ScalarFunction fun("json_merge_patch", {LogicalType::JSON(), LogicalType::JSON()}, LogicalType::JSON(),
	                   MergePatchFunction, nullptr, nullptr, JSONFunctionLocalState::Init);
	fun.varargs = LogicalType::JSON();
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	return ScalarFunctionSet(fun);
}

} // namespace duckdb
