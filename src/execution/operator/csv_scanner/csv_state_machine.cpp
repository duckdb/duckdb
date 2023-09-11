#include "duckdb/execution/operator/scan/csv/csv_state_machine.hpp"
#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/execution/operator/scan/csv/csv_state_machine_cache.hpp"

namespace duckdb {

CSVStateMachine::CSVStateMachine(CSVReaderOptions &options_p, const CSVStateMachineOptions &state_machine_options,
                                 shared_ptr<CSVBufferManager> buffer_manager_p,
                                 CSVStateMachineCache &csv_state_machine_cache_p)
    : csv_state_machine_cache(csv_state_machine_cache_p), options(options_p),
      csv_buffer_iterator(std::move(buffer_manager_p)),
      transition_array(csv_state_machine_cache.Get(state_machine_options)) {
	dialect_options.state_machine_options = state_machine_options;
	dialect_options.has_format = options.dialect_options.has_format;
	dialect_options.date_format = options.dialect_options.date_format;
	dialect_options.skip_rows = options.dialect_options.skip_rows;
}

void CSVStateMachine::Reset() {
	csv_buffer_iterator.Reset();
}

void CSVStateMachine::VerifyUTF8() {
	auto utf_type = Utf8Proc::Analyze(value.c_str(), value.size());
	if (utf_type == UnicodeType::INVALID) {
		int64_t error_line = cur_rows;
		throw InvalidInputException("Error in file \"%s\" at line %llu: "
		                            "%s. Parser options:\n%s",
		                            options.file_path, error_line, ErrorManager::InvalidUnicodeError(value, "CSV file"),
		                            options.ToString());
	}
}
} // namespace duckdb
