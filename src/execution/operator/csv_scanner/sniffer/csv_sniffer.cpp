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

// Set the CSV Options in the reference
void CSVSniffer::SetResultOptions() {
	bool og_header = options.dialect_options.header;
	options.dialect_options = best_candidate->dialect_options;
	options.dialect_options.new_line = best_candidate->dialect_options.new_line;
	options.skip_rows_set = options.dialect_options.skip_rows > 0;
	if (options.has_header) {
		// If header was manually set, we ignore the sniffer findings
		options.dialect_options.header = og_header;
	}
	options.has_header = true;
	if (options.dialect_options.header) {
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
