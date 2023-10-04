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
}

bool SetColumns::IsSet(){
	return !types;
}

idx_t SetColumns::Size(){
	if (!types){
		return 0;
	}
	return types->size();
}

SnifferResult CSVSniffer::SniffCSV() {
	if (set_columns.IsSet()){
		// We already know how many columns the snifefr must return
		max_columns_found = set_columns.Size();
	}
	// 1. Dialect Detection
	DetectDialect();
//

	// 2. Type Detection
	DetectTypes();
	// 3. Header Detection
	DetectHeader();
	D_ASSERT(best_sql_types_candidates_per_column_idx.size() == names.size());
	// 4. Type Replacement
	ReplaceTypes();
	// 5. Type Refinement
	RefineTypes();
	// We are done, construct and return the result.

	// Set the CSV Options in the reference
	options.dialect_options = best_candidate->dialect_options;
	options.has_header = best_candidate->dialect_options.header;
	options.skip_rows_set = options.dialect_options.skip_rows > 0;
	if (options.has_header) {
		options.dialect_options.true_start = best_start_with_header;
	} else {
		options.dialect_options.true_start = best_start_without_header;
	}

	// Return the types and names
	return SnifferResult(detected_types, names);
}

} // namespace duckdb
