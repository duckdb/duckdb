//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_function_helpers
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/filter/table_filter_functions.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {

struct SelectivityTrackingLocalState : public FunctionLocalState {
	SelectivityTrackingLocalState(idx_t n_vectors_to_check_p, float selectivity_threshold_p) : stats() {
		stats.Enable(selectivity_threshold_p, n_vectors_to_check_p);
	}

	void Update(idx_t accepted, idx_t processed) {
		stats.Update(accepted, processed);
	}

	bool IsActive() const {
		return stats.IsActive();
	}

	SelectivityTrackingState stats;
};

inline unique_ptr<FunctionLocalState> InitSelectivityTrackingLocalState(idx_t n_vectors_to_check,
                                                                        float selectivity_threshold) {
	if (n_vectors_to_check == 0) {
		return nullptr;
	}
	return make_uniq<SelectivityTrackingLocalState>(n_vectors_to_check, selectivity_threshold);
}

inline void SetConstantBooleanResult(Vector &result, bool value) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<bool>(result)[0] = value;
}

inline void SelectionToBooleanResult(idx_t count, const SelectionVector &sel, idx_t sel_count, Vector &result) {
	if (count == 0 || sel_count == 0) {
		SetConstantBooleanResult(result, false);
		return;
	}
	if (sel_count == count) {
		SetConstantBooleanResult(result, true);
		return;
	}
	result.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::Validity(result).SetAllValid(count);
	auto result_data = FlatVector::GetData<bool>(result);
	for (idx_t i = 0; i < count; i++) {
		result_data[i] = false;
	}
	for (idx_t i = 0; i < sel_count; i++) {
		result_data[sel.get_index(i)] = true;
	}
}

inline void SetAllTrue(DataChunk &args, Vector &result) {
	SetConstantBooleanResult(result, true);
}

template <class TRACKING_STATE, class EXECUTOR>
inline void ExecuteWithSelectivityTracking(DataChunk &args, Vector &result, TRACKING_STATE *tracking_state,
                                           EXECUTOR &&execute) {
	if (tracking_state && !tracking_state->IsActive()) {
		SetAllTrue(args, result);
		tracking_state->Update(0, 0);
		return;
	}
	auto approved_count = execute();
	if (tracking_state) {
		tracking_state->Update(approved_count, args.size());
	}
}

void TableFilterFunctionSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                  const ScalarFunction &function);
unique_ptr<FunctionData> TableFilterFunctionDeserialize(Deserializer &deserializer, ScalarFunction &function);

inline string FormatOptionalFilterString(const string &child_filter_string) {
	if (child_filter_string.empty()) {
		return "optional";
	}
	return "optional: " + child_filter_string;
}

} // namespace duckdb
