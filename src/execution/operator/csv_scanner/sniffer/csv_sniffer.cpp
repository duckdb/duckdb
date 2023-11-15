#include <utility>

#include "duckdb/execution/operator/scan/csv/csv_sniffer.hpp"

namespace duckdb {

CSVSniffer::CSVSniffer(CSVReaderOptions &options_p, shared_ptr<CSVBufferManager> buffer_manager_p,
                       CSVStateMachineCache &state_machine_cache_p, SetColumns set_columns_p)
    : state_machine_cache(state_machine_cache_p), options(options_p), buffer_manager(std::move(buffer_manager_p)),
      set_columns(set_columns_p) {

	// Check if any type is BLOB
	for (auto &type : options.sql_type_list) {
		if (type.id() == LogicalTypeId::BLOB) {
			throw InvalidInputException(
			    "CSV auto-detect for blobs not supported: there may be invalid UTF-8 in the file");
		}
	}

	// Initialize Format Candidates
	for (const auto &format_template : format_template_candidates) {
		auto &logical_type = format_template.first;
		best_format_candidates[logical_type].clear();
	}
	// Initialize max columns found to either 0 or however many were set
	max_columns_found = set_columns.Size();
}

bool SetColumns::IsSet() {
	if (!types) {
		return false;
	}
	return !types->empty();
}

idx_t SetColumns::Size() {
	if (!types) {
		return 0;
	}
	return types->size();
}

template <class T>
void MatchAndReplace(CSVOption<T> &original, CSVOption<T> &sniffed, string name) {
	if (original.IsSetByUser()) {
		// We verify that the user input matches the sniffed value
		if (original != sniffed) {
			throw InvalidInputException(
			    "CSV Sniffer: Sniffer detected value different than the user input for the %s options",
			    std::move(name));
		}
	} else {
		// We replace the value of original with the sniffed value
		original.Set(sniffed.GetValue(), false);
	}
}
void MatchAndRepaceUserSetVariables(DialectOptions &original, DialectOptions &sniffed) {
	MatchAndReplace(original.header, sniffed.header, "Header");
	MatchAndReplace(original.new_line, sniffed.new_line, "New Line");
	MatchAndReplace(original.skip_rows, sniffed.skip_rows, "Skip Rows");
	MatchAndReplace(original.state_machine_options.delimiter, sniffed.state_machine_options.delimiter, "Delimiter");
	MatchAndReplace(original.state_machine_options.quote, sniffed.state_machine_options.quote, "Quote");
	MatchAndReplace(original.state_machine_options.escape, sniffed.state_machine_options.escape, "Escape");
	MatchAndReplace(original.date_format[LogicalTypeId::DATE], sniffed.date_format[LogicalTypeId::DATE], "Date Format");
	MatchAndReplace(original.date_format[LogicalTypeId::TIMESTAMP], sniffed.date_format[LogicalTypeId::TIMESTAMP],
	                "Timestamp Format");
}
// Set the CSV Options in the reference
void CSVSniffer::SetResultOptions() {
	MatchAndRepaceUserSetVariables(options.dialect_options, best_candidate->dialect_options);
	if (options.dialect_options.header.GetValue()) {
		options.dialect_options.true_start = best_start_with_header;
	} else {
		options.dialect_options.true_start = best_start_without_header;
	}
}

SnifferResult CSVSniffer::SniffCSV() {
	// 1. Dialect Detection
	DetectDialect();
	// 2. Type Detection
	DetectTypes();
	// 3. Type Refinement
	RefineTypes();
	// 4. Header Detection
	DetectHeader();
	if (set_columns.IsSet()) {
		SetResultOptions();
		// We do not need to run type refinement, since the types have been given by the user
		return SnifferResult({}, {});
	}
	// 5. Type Replacement
	ReplaceTypes();
	D_ASSERT(best_sql_types_candidates_per_column_idx.size() == names.size());
	// We are done, Set the CSV Options in the reference. Construct and return the result.
	SetResultOptions();
	return SnifferResult(detected_types, names);
}

} // namespace duckdb
