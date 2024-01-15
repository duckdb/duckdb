#include "duckdb/execution/operator/csv_scanner/state_machine/csv_state_machine.hpp"
#include "duckdb/execution/operator/csv_scanner/sniffer/csv_sniffer.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/execution/operator/csv_scanner/state_machine/csv_state_machine_cache.hpp"

namespace duckdb {

CSVStateMachine::CSVStateMachine(CSVReaderOptions &options_p, const CSVStateMachineOptions &state_machine_options_p,
                                 CSVStateMachineCache &csv_state_machine_cache)
    : transition_array(csv_state_machine_cache.Get(state_machine_options_p)),
      state_machine_options(state_machine_options_p), options(options_p) {
	dialect_options.state_machine_options = state_machine_options;
}

CSVStateMachine::CSVStateMachine(const StateMachine &transition_array_p, const CSVReaderOptions &options_p)
    : transition_array(transition_array_p), state_machine_options(options_p.dialect_options.state_machine_options),
      options(options_p), dialect_options(options.dialect_options) {
	dialect_options.state_machine_options = state_machine_options;
}

void CSVStateMachine::InitializeSelectionVector(vector<SelectionVector> &selection_vector, idx_t num_cols) {
	if (selection_vector.empty()) {
		selection_vector.resize(num_cols);
		// precompute these selection vectors
		for (idx_t i = 0; i < selection_vector.size(); i++) {
			selection_vector[i].Initialize();
			for (idx_t j = 0; j < STANDARD_VECTOR_SIZE; j++) {
				selection_vector[i][j] = i + (num_cols * j);
			}
		}
	}
}

const vector<SelectionVector> &CSVStateMachine::GetSelectionVector() {
	std::call_once(call_once_flag, this->InitializeSelectionVector, selection_vector, dialect_options.num_cols);
	return selection_vector;
}

} // namespace duckdb
