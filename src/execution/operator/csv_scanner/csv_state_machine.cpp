#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/csv_sniffer.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/main/error_manager.hpp"
#include "duckdb/execution/operator/persistent/csv_scanner/csv_state_machine_cache.hpp"

#include <iomanip>

namespace duckdb {

CSVStateMachine::CSVStateMachine(CSVReaderOptions &options_p, char quote_p, char escape_p, char delim_p,
                                 shared_ptr<CSVBufferManager> buffer_manager_p,
                                 CSVStateMachineCache &csv_state_machine_cache_p)
    : csv_state_machine_cache(csv_state_machine_cache_p), options(options_p),
      csv_buffer_iterator(std::move(buffer_manager_p)),
      transition_array(csv_state_machine_cache.Get(delim_p, quote_p, escape_p)) {
	dialect_options.quote = quote_p;
	dialect_options.escape = escape_p;
	dialect_options.delimiter = delim_p;
	dialect_options.has_format = options.dialect_options.has_format;
	dialect_options.date_format = options.dialect_options.date_format;
	dialect_options.skip_rows = options.dialect_options.skip_rows;
}

void CSVStateMachine::Reset() {
	csv_buffer_iterator.Reset();
}

void CSVStateMachine::Print() {
	std::cout << "{" << std::endl;
	for (uint32_t i = 0; i < NUM_STATES; ++i) {
		std::cout << "    {";
		for (uint32_t j = 0; j < NUM_TRANSITIONS; ++j) {
			std::cout << std::setw(3) << static_cast<int>(transition_array[i][j]);
			if (j != NUM_TRANSITIONS - 1) {
				std::cout << ",";
			}
			if ((j + 1) % 16 == 0) {
				std::cout << std::endl << "     ";
			}
		}
		std::cout << "}";
		if (i != NUM_STATES - 1) {
			std::cout << ",";
		}
		std::cout << std::endl;
	}
	std::cout << "};" << std::endl;
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
