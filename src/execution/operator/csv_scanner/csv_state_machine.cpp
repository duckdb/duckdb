#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine_cache.hpp"

namespace duckdb {

CSVStateMachine::CSVStateMachine(const CSVStateMachineOptions &state_machine_options_p,
	                         CSVStateMachineCache &csv_state_machine_cache)
    : state_machine_options(state_machine_options_p),
      transition_array(csv_state_machine_cache.Get(state_machine_options)) {
}

CSVStateMachineSniffing::CSVStateMachineSniffing(CSVReaderOptions &options_p, const CSVStateMachineOptions &state_machine_options,
                                                 CSVStateMachineCache &csv_state_machine_cache_p)
    : CSVStateMachine(state_machine_options,csv_state_machine_cache_p), options(options_p){
	dialect_options.state_machine_options = state_machine_options;
	dialect_options.has_format = options.dialect_options.has_format;
	dialect_options.date_format = options.dialect_options.date_format;
	dialect_options.skip_rows = options.dialect_options.skip_rows;
}


} // namespace duckdb
